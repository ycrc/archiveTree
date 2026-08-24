#!/usr/bin/env python3
"""
Archive a directory tree into multiple S3 objects and remove the tree.

Behavior:

- Files with size > SIZE_CUTOFF are uploaded as individual S3 objects.
- Smaller files are packed into tar archives whose total size is approximately
  SIZE_GROUPING bytes; each tar is uploaded as an S3 object.
- All objects for a single archive share the same archive_id (UUID) and
  S3 key prefix: {object_path}/{root_name}/{archive_id}/...
- A single inventory JSON is written alongside the removed tree, listing
  for each file:
    * which S3 object it resides in (object_id, object_key, object_type)
    * checksum and other metadata.

Deep Glacier–aware:

- You can set --storage-class to GLACIER / DEEP_ARCHIVE / GLACIER_IR.
- The storage class for each object is recorded in the inventory.
- Verification uses head_object only (safe for Glacier classes).

Checksum verification:

- By default, each uploaded object is verified with a whole-object S3
  checksum (ChecksumAlgorithm=CRC64NVME, ChecksumType=FULL_OBJECT), read
  back via head_object(ChecksumMode=ENABLED) and compared against a CRC64NVME
  computed locally just before upload. This works even for Glacier storage
  classes, since it never reads the object body back.
- CRC64NVME (not the SHA256 already in the inventory) is used because S3
  only supports whole-object checksums for multipart uploads with the CRC
  family of algorithms (CRC32/CRC32C/CRC64NVME); SHA256/SHA1 multipart
  checksums are always composite (a hash of per-part hashes), which can't be
  compared against a single hash of the whole local file. Since nearly
  everything this tool uploads is large enough to go through multipart
  upload, SHA256 verification would rarely apply in practice.
- Computing CRC64NVME requires the optional `awscrt` package. If it isn't
  installed, or the target endpoint doesn't support whole-object checksums
  (probed with a tiny throwaway object before doing any real work),
  archiveTree automatically falls back to size-only verification and prints
  a warning.
- Pass --no-checksum-verify to skip the probe and always use size-only
  verification.

Parallel:

- Large-file uploads and tar-group uploads are done in parallel using a
  ThreadPoolExecutor controlled by --max-workers.
"""

import argparse
import base64
import io
import os
import sys
import shutil
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed

import boto3
from boto3.s3.transfer import TransferConfig
from botocore.exceptions import ClientError

import archive_config
from archive_common import (
    vprint,
    print_config,
    build_inventory,
    compute_crc64nvme_b64,
    create_tar,
    crt_checksums,
    partition_by_size,
    group_small_files,
    write_inventory_file,
)

# Optional tqdm for progress bars
try:
    from tqdm import tqdm
except ImportError:
    tqdm = None

# Storage classes that keep objects offline until an explicit restore
# request completes. GLACIER_IR (Glacier Instant Retrieval) is deliberately
# NOT here: despite the name, its objects are readable immediately with a
# plain GET and RestoreObject against them is rejected outright, so treating
# it as cold would make anything archived that way permanently unreadable.
RESTORE_REQUIRED_CLASSES = {"GLACIER", "DEEP_ARCHIVE"}


# ---------- S3 + Progress ----------

class ProgressCallback:
    """S3 upload progress callback."""

    def __init__(self, filename, verbose=False, label="Upload"):
        self.verbose = verbose
        self._size = os.path.getsize(filename)
        self._seen = 0
        if verbose and tqdm:
            self._pbar = tqdm(
                total=self._size,
                unit="B",
                unit_scale=True,
                desc=label
            )
        else:
            self._pbar = None

    def __call__(self, bytes_amount):
        self._seen += bytes_amount
        if self._pbar:
            self._pbar.update(bytes_amount)

    def close(self):
        if self._pbar:
            self._pbar.close()


def get_s3_client(profile=None, endpoint_url=None):
    if profile:
        session = boto3.session.Session(profile_name=profile)
    else:
        session = boto3.session.Session()
    if endpoint_url:
        return session.client("s3", endpoint_url=endpoint_url)
    return session.client("s3")


def probe_write_access(s3_client, bucket, object_path, verbose=False):
    """
    Verify PutObject permission on the target prefix by uploading a tiny
    throwaway object, then try to delete it.

    Unlike probe_checksum_support() (a feature probe that treats any
    failure as "unsupported" and falls back gracefully), a failure to
    upload here always means "can't write" -- it must propagate, not be
    swallowed, so callers can fail fast before building any tars. Deleting
    the probe object afterward is best-effort only: a delete failure
    doesn't mean the archive itself can't write, just that this one
    leftover object needs manual cleanup.
    """
    opath = object_path.strip("/")
    key = f"{opath}/.archiveTree-probe-{uuid.uuid4().hex}" if opath else f".archiveTree-probe-{uuid.uuid4().hex}"

    vprint(verbose, f"Probing s3://{bucket}/{key} for write access...")
    s3_client.put_object(Bucket=bucket, Key=key, Body=b"archiveTree write probe")

    try:
        s3_client.delete_object(Bucket=bucket, Key=key)
    except Exception as e:
        vprint(verbose, f"WARNING: could not delete write-probe object {key}: {e}")


def probe_checksum_support(s3_client, bucket, object_path, verbose=False):
    """
    Determine whether the target S3 endpoint supports whole-object CRC64NVME
    checksums (ChecksumAlgorithm=CRC64NVME, ChecksumType=FULL_OBJECT) by
    uploading and verifying a tiny throwaway object, then deleting it.

    The probe forces the upload through the *multipart* code path (via a
    near-zero multipart_threshold), even though the payload is tiny. This
    matters: a plain single-part PutObject is always a genuine whole-object
    checksum with no ambiguity (and doesn't even accept ChecksumType), but
    real archive objects from this tool are almost always large enough to
    go through multipart upload, where ChecksumType=FULL_OBJECT is what
    determines whether S3 returns a true whole-object checksum instead of a
    composite hash-of-part-hashes that won't match the locally computed
    value. Testing the single-part path would give a false positive.

    This is a capability probe, not a data-integrity check: any failure
    (unsupported params, unexpected response shape, network hiccup) is
    treated as "not supported" so the archive run can fall back to
    size-only verification instead of aborting. Real per-object checksum
    failures during the actual archive are handled separately in
    upload_file_to_s3(), which raises instead of swallowing errors.

    Returns True if whole-object CRC64NVME checksums are supported.
    """
    opath = object_path.strip("/")
    prefix = f"{opath}/.archiveTree-probe" if opath else ".archiveTree-probe"
    probe_key = f"{prefix}-{uuid.uuid4().hex}"
    probe_body = b"archiveTree checksum capability probe"
    expected_crc = crt_checksums.crc64nvme(probe_body, 0) & 0xFFFFFFFFFFFFFFFF
    expected_b64 = base64.b64encode(expected_crc.to_bytes(8, byteorder="big")).decode("ascii")

    vprint(verbose, "Probing S3 endpoint for whole-object CRC64NVME checksum support...")

    # multipart_chunksize must still meet S3's 5MB minimum part size even
    # though our one and only part is much smaller than that (a single part
    # is always allowed to be the "last part", which has no minimum size).
    force_multipart = TransferConfig(
        multipart_threshold=1, multipart_chunksize=5 * 1024 * 1024
    )

    supported = False
    try:
        s3_client.upload_fileobj(
            io.BytesIO(probe_body),
            bucket,
            probe_key,
            ExtraArgs={"ChecksumAlgorithm": "CRC64NVME", "ChecksumType": "FULL_OBJECT"},
            Config=force_multipart,
        )
        resp = s3_client.head_object(Bucket=bucket, Key=probe_key, ChecksumMode="ENABLED")
        remote_b64 = resp.get("ChecksumCRC64NVME")
        if remote_b64 == expected_b64:
            supported = True
        else:
            vprint(
                verbose,
                "Checksum probe: endpoint did not return a matching whole-object "
                "CRC64NVME checksum.",
            )
    except Exception as e:
        vprint(verbose, f"Checksum probe failed, treating as unsupported: {e}")
    finally:
        try:
            s3_client.delete_object(Bucket=bucket, Key=probe_key)
        except Exception:
            pass

    return supported


def upload_file_to_s3(path, bucket, key, storage_class, s3_client,
                      verbose=False, label="Upload",
                      expected_checksum=None, strict_checksum=False):
    """
    Upload a local file to S3 with progress, then verify it.

    If strict_checksum is True, the object is uploaded with a whole-object
    CRC64NVME checksum (ChecksumAlgorithm=CRC64NVME, ChecksumType=FULL_OBJECT)
    and verified against expected_checksum (base64, matching S3's
    ChecksumCRC64NVME format) via head_object(ChecksumMode=ENABLED).
    Otherwise, only the remote size is verified via head_object (as before).

    Both verification modes use head_object only, so they're safe for
    Glacier storage classes (never reads the object body back).

    Returns:
        storage_class_actual (str): StorageClass from HEAD (for inventory).
        size_remote (int): ContentLength from HEAD.
    """
    if strict_checksum and expected_checksum is None:
        raise ValueError("strict_checksum=True requires expected_checksum")

    extra_args = {"StorageClass": storage_class}
    if strict_checksum:
        extra_args["ChecksumAlgorithm"] = "CRC64NVME"
        extra_args["ChecksumType"] = "FULL_OBJECT"

    vprint(verbose, f"Uploading {path} to s3://{bucket}/{key} "
                    f"(StorageClass={storage_class}) ...")

    progress = ProgressCallback(path, verbose=verbose, label=label)

    with open(path, "rb") as f:
        s3_client.upload_fileobj(
            f,
            bucket,
            key,
            ExtraArgs=extra_args,
            Callback=progress
        )

    progress.close()

    # Verify via HEAD (safe even for Glacier classes)
    head_kwargs = {"Bucket": bucket, "Key": key}
    if strict_checksum:
        head_kwargs["ChecksumMode"] = "ENABLED"
    try:
        resp = s3_client.head_object(**head_kwargs)
    except ClientError as e:
        raise RuntimeError(f"Verification head_object failed for s3://{bucket}/{key}: {e}") from e

    local_size = os.path.getsize(path)
    remote_size = resp.get("ContentLength")
    remote_storage_class = resp.get("StorageClass", storage_class)

    if local_size != remote_size:
        raise RuntimeError(
            f"Verification failed for s3://{bucket}/{key}: "
            f"local {local_size} != remote {remote_size}"
        )

    if strict_checksum:
        remote_checksum = resp.get("ChecksumCRC64NVME")
        if not remote_checksum:
            raise RuntimeError(
                f"Checksum verification failed for s3://{bucket}/{key}: "
                "no ChecksumCRC64NVME returned despite ChecksumMode=ENABLED "
                "(unexpected after a successful capability probe)."
            )
        if remote_checksum != expected_checksum:
            raise RuntimeError(
                f"Checksum verification FAILED for s3://{bucket}/{key}: "
                f"expected crc64nvme={expected_checksum}, got {remote_checksum}."
            )
        vprint(verbose, f"Verified CRC64NVME checksum for s3://{bucket}/{key}.")

    if remote_storage_class in RESTORE_REQUIRED_CLASSES:
        vprint(
            verbose,
            f"NOTE: s3://{bucket}/{key} is stored in {remote_storage_class}. "
            "You will need to run a restore request before downloading it later."
        )

    vprint(verbose, f"Verified upload of {local_size} bytes to "
                    f"s3://{bucket}/{key} (StorageClass={remote_storage_class}).")

    return remote_storage_class, remote_size


# ---------- Main ----------

def build_arg_parser():
    parser = argparse.ArgumentParser(
        description="Archive a directory tree to multiple S3 objects."
    )
    parser.add_argument("directory")
    parser.add_argument(
        "bucket", nargs="?", default=None,
        help="S3 bucket name. Optional if 'bucket' is set in the [s3] "
             "section of the config file.",
    )
    parser.add_argument(
        "object_path", nargs="?", default=None,
        help="Base S3 prefix under which archive objects will be stored. "
             "Optional if 'object_path' is set in the [s3] section of the "
             "config file.",
    )
    parser.add_argument(
        "--storage-class",
        default=None,
        help=("""S3 storage class for newly created objects.
            Examples: STANDARD, STANDARD_IA, ONEZONE_IA, INTELLIGENT_TIERING,
            GLACIER, GLACIER_IR, DEEP_ARCHIVE. Falls back to 'storage_class'
            in the [s3] section of the config file, then STANDARD. """
         ),
    )
    parser.add_argument("--scratch-dir", default=None)
    parser.add_argument("--compression", choices=["none", "gz"], default="none")
    parser.add_argument(
        "--delete", action="store_true",
        help="Delete the source directory tree after a successful archive. Default: keep it.",
    )
    parser.add_argument("--profile", default=None)
    parser.add_argument("--endpoint-url", default=None)
    parser.add_argument(
        "--config-file", default=None,
        help=(
            "Path to config file with an [s3] section supplying defaults for "
            "bucket/object_path/profile/endpoint_url/storage_class "
            f"(default: {archive_config.DEFAULT_CONFIG_FILE})."
        ),
    )
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument(
        "--no-checksum-verify", action="store_true",
        help=(
            "Skip the whole-object CRC64NVME checksum verification (and its "
            "startup capability probe) and use size-only verification "
            "instead, like archiveTree did before this feature existed. "
            "archiveTree normally detects lack of support (missing awscrt, "
            "or an endpoint that doesn't support it) automatically and "
            "falls back on its own, so you should only need this if the "
            "probe itself is problematic for your endpoint."
        ),
    )
    parser.add_argument(
        "--size-cutoff",
        type=int,
        default=1_000_000_000,
        help=(
            "Files larger than this in bytes are stored as individual S3 objects. "
            "Default: 1e9."
        ),
    )
    parser.add_argument(
        "--size-grouping",
        type=int,
        default=10_000_000_000,
        help=(
            "Target total size (in bytes) for tar groups of small files. "
            "Default: 1e10."
        ),
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=4,
        help="Maximum number of parallel upload workers (default: 4).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be uploaded, but do not create tars, upload, or delete anything.",
    )
    parser.add_argument(
        "--inventory-dir",
        default=None,
        help=(
            "Directory in which to write the local inventory JSON file. "
            "Defaults to the parent of the archived directory."
        ),
    )

    parser.add_argument(
        "--no-summary",
        action="store_false",
        dest="summary",
        help="Print a summary of files, bytes, and objects created.",
    )

    return parser


def run(args):
    verbose = args.verbose

    config_path = os.path.expanduser(args.config_file or archive_config.DEFAULT_CONFIG_FILE)
    s3_config = archive_config.load_config_file(config_path, section="s3")

    bucket = archive_config.require(
        archive_config.resolve(args.bucket, None, s3_config, "bucket"),
        "S3 bucket", "bucket (positional)", None, "bucket",
    )
    object_path = archive_config.require(
        archive_config.resolve(args.object_path, None, s3_config, "object_path"),
        "S3 object path", "object_path (positional)", None, "object_path",
    )
    profile = args.profile or s3_config.get("profile")
    endpoint_url = args.endpoint_url or s3_config.get("endpoint_url")
    # --storage-class defaults to None rather than "STANDARD" so an explicit
    # "--storage-class STANDARD" is distinguishable from the flag being
    # absent. Comparing against the literal default instead made the config
    # file silently outrank the command line for that one value -- asking
    # for STANDARD while the config said DEEP_ARCHIVE got you DEEP_ARCHIVE,
    # with its 180-day minimum billing and hours-long retrieval.
    storage_class = archive_config.resolve(
        args.storage_class, None, s3_config, "storage_class"
    ) or "STANDARD"

    root_dir = os.path.abspath(args.directory)

    if not os.path.isdir(root_dir):
        print(f"ERROR: {root_dir} is not a directory", file=sys.stderr)
        sys.exit(1)

    # Resolve and verify the inventory output directory early, before doing
    # any real work, so we don't fail after a long upload.
    if args.inventory_dir is not None:
        inv_dir = os.path.abspath(args.inventory_dir)
        if not os.path.isdir(inv_dir):
            print(f"ERROR: --inventory-dir {inv_dir} is not a directory",
                  file=sys.stderr)
            sys.exit(1)
    else:
        inv_dir = os.path.dirname(root_dir)

    if not os.access(inv_dir, os.W_OK):
        print(
            f"ERROR: inventory directory {inv_dir!r} is not writable. "
            "Use --inventory-dir to specify a writable location.",
            file=sys.stderr,
        )
        sys.exit(1)

    s3_client = get_s3_client(profile, endpoint_url)

    valid_storage_classes = s3_client.meta.service_model.shape_for("StorageClass").enum
    if storage_class not in valid_storage_classes:
        print(
            f"ERROR: --storage-class {storage_class!r} is not valid. "
            f"Choose from: {', '.join(sorted(valid_storage_classes))}",
            file=sys.stderr,
        )
        sys.exit(1)

    print_config(verbose, "Configuration", {
        "directory": root_dir,
        "backend": "s3",
        "bucket": bucket,
        "object_path": object_path,
        "profile": profile,
        "endpoint_url": endpoint_url,
        "storage_class": storage_class,
        "config_file": config_path,
        "no_checksum_verify": args.no_checksum_verify,
        "scratch_dir": args.scratch_dir,
        "compression": args.compression,
        "delete": args.delete,
        "size_cutoff": args.size_cutoff,
        "size_grouping": args.size_grouping,
        "max_workers": args.max_workers,
        "dry_run": args.dry_run,
        "inventory_dir": inv_dir,
        "summary": args.summary,
    })

    # Verify credentials and bucket access now, before building the inventory
    # or any tars, so a bad --profile/--endpoint-url/bucket fails fast instead
    # of after a long run. Skipped on --dry-run, which never touches S3.
    strict_checksum = False
    if not args.dry_run:
        try:
            s3_client.head_bucket(Bucket=bucket)
        except Exception as e:
            print(
                f"ERROR: cannot access bucket {bucket!r} (check --profile, "
                f"--endpoint-url, AWS credentials, and bucket name/permissions): {e}",
                file=sys.stderr,
            )
            sys.exit(1)

        try:
            probe_write_access(s3_client, bucket, object_path, verbose=verbose)
        except Exception as e:
            print(
                f"ERROR: cannot write to s3://{bucket}/{object_path} (check "
                "--profile, --endpoint-url, AWS credentials, and write "
                f"permissions on that bucket/prefix): {e}",
                file=sys.stderr,
            )
            sys.exit(1)

        if args.no_checksum_verify:
            vprint(verbose, "Checksum verification disabled via --no-checksum-verify; "
                            "using size-only upload verification.")
        elif crt_checksums is None:
            print(
                "WARNING: the 'awscrt' package is not installed, so whole-object "
                "CRC64NVME checksums can't be computed locally. Falling back to "
                "size-only upload verification. Install awscrt (or pass "
                "--no-checksum-verify to silence this warning) to enable strict "
                "checksum verification before delete.",
                file=sys.stderr,
            )
        else:
            strict_checksum = probe_checksum_support(
                s3_client, bucket, object_path, verbose=verbose
            )
            if strict_checksum:
                vprint(verbose, "Endpoint supports whole-object CRC64NVME checksums; "
                                "will verify each uploaded object against it.")
            else:
                print(
                    "WARNING: target S3 endpoint does not appear to support "
                    "whole-object CRC64NVME checksums (ChecksumAlgorithm=CRC64NVME, "
                    "ChecksumType=FULL_OBJECT). Falling back to size-only "
                    "upload verification. Pass --no-checksum-verify to skip "
                    "this probe on future runs against this endpoint.",
                    file=sys.stderr,
                )

    vprint(verbose, "Building inventory...")
    inventory, _ = build_inventory(root_dir, verbose=verbose,
                                   max_workers=args.max_workers)

    # Partition files into large and small sets
    size_cutoff = args.size_cutoff
    size_grouping = args.size_grouping
    files = inventory["files"]

    large_files, small_files = partition_by_size(files, size_cutoff)

    vprint(
        verbose,
        f"Total files: {len(files)}; "
        f"large: {len(large_files)}; small: {len(small_files)}"
    )

    # Base S3 prefix: {object_path}/{root_name}/{archive_id}
    base_name = os.path.basename(root_dir.rstrip(os.sep))
    archive_id = inventory["inventory_id"]
    opath = object_path.strip("/")

    if opath:
        base_prefix = f"{opath}/{base_name}/{archive_id}"
    else:
        base_prefix = f"{base_name}/{archive_id}"

    # Group small files into tars of approximately size_grouping bytes
    groups = group_small_files(small_files, size_grouping)

    vprint(verbose, f"Created {len(groups)} small-file groups.")

    compression = None if args.compression == "none" else "gz"
    tar_suffix = ".tar.gz" if compression == "gz" else ".tar"

    # Build job list for parallel processing
    jobs = []
    object_counter = 0

    # Large-file jobs
    for rec in large_files:
        obj_id = f"file-{object_counter:06d}"
        object_counter += 1
        jobs.append({
            "kind": "file",
            "obj_id": obj_id,
            "rec": rec,
        })

    # Tar-group jobs
    for g_idx, group in enumerate(groups):
        obj_id = f"tar-{object_counter:06d}"
        object_counter += 1
        jobs.append({
            "kind": "tar",
            "obj_id": obj_id,
            "group": group,
            "group_index": g_idx,
        })

    objects = []

    def process_job(job):
        kind = job["kind"]
        obj_id = job["obj_id"]

        if kind == "file":
            rec = job["rec"]
            rel = rec["relative_path"]
            abs_path = rec["absolute_path"]
            key = f"{base_prefix}/files/{rel}"
            key = key.replace("//", "/")

            file_crc64nvme = None
            if strict_checksum:
                vprint(verbose, f"[worker] Hashing {rel} (CRC64NVME)...")
                file_crc64nvme = compute_crc64nvme_b64(abs_path, verbose=verbose, use_tqdm=False)

            storage_class_actual, size_remote = upload_file_to_s3(
                abs_path,
                bucket,
                key,
                storage_class,
                s3_client,
                verbose=verbose,
                label=f"Upload {rel}",
                expected_checksum=file_crc64nvme,
                strict_checksum=strict_checksum,
            )

            obj_meta = {
                "id": obj_id,
                "type": "file",
                "s3_key": key,
                "size_bytes": size_remote,
                "storage_class": storage_class_actual,
                "relative_path": rel,
            }
            if file_crc64nvme is not None:
                obj_meta["crc64nvme"] = file_crc64nvme

            rec["object_id"] = obj_id
            rec["object_type"] = "file"
            rec["object_key"] = key

            return obj_meta

        elif kind == "tar":
            group = job["group"]
            g_idx = job["group_index"]
            abs_paths = [r["absolute_path"] for r in group]

            vprint(verbose, f"[worker] Creating tar for group {g_idx} with {len(group)} files...")
            tar_path = create_tar(
                root_dir,
                abs_paths,
                scratch_dir=args.scratch_dir,
                tar_compression=compression,
                verbose=verbose,
                group_index=g_idx,
                archive_id=archive_id,
            )

            tar_name = f"group_{g_idx:06d}{tar_suffix}"
            key = f"{base_prefix}/groups/{tar_name}"
            key = key.replace("//", "/")

            tar_crc64nvme = None
            if strict_checksum:
                vprint(verbose, f"[worker] Hashing tar for group {g_idx} (CRC64NVME)...")
                tar_crc64nvme = compute_crc64nvme_b64(tar_path, verbose=verbose, use_tqdm=False)

            storage_class_actual, size_remote = upload_file_to_s3(
                tar_path,
                bucket,
                key,
                storage_class,
                s3_client,
                verbose=verbose,
                label=f"Upload group {g_idx}",
                expected_checksum=tar_crc64nvme,
                strict_checksum=strict_checksum,
            )

            obj_meta = {
                "id": obj_id,
                "type": "tar",
                "s3_key": key,
                "size_bytes": size_remote,
                "storage_class": storage_class_actual,
                "file_count": len(group),
                "group_index": g_idx,
            }
            if tar_crc64nvme is not None:
                obj_meta["crc64nvme"] = tar_crc64nvme

            for rec in group:
                rec["object_id"] = obj_id
                rec["object_type"] = "tar"
                rec["object_key"] = key

            vprint(verbose, f"[worker] Removing temporary tar {tar_path}")
            try:
                os.remove(tar_path)
            except OSError:
                pass

            return obj_meta

        else:
            raise RuntimeError(f"Unknown job kind: {kind}")

    # --- DRY RUN MODE ----------------------------------------------------
    if args.dry_run:
        total_files = len(files)
        total_bytes = sum(rec["size_bytes"] for rec in files)
        num_objects = len(jobs)

        print("\nDRY RUN SUMMARY:")
        print(f"  Total files:      {total_files}")
        print(f"  Total bytes:      {total_bytes}")
        print(f"  Objects to create:{num_objects}")
        print(f"  Large files:      {len(large_files)}")
        print(f"  Small-file groups:{len(groups)}")
        print("\nNo uploads performed.")
        return

    # Run jobs in parallel
    max_workers = max(1, args.max_workers)
    vprint(verbose, f"Starting upload with up to {max_workers} workers...")
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(process_job, job) for job in jobs]
        for fut in as_completed(futures):
            obj_meta = fut.result()
            objects.append(obj_meta)

    # Inventory file path – use --inventory-dir if given, else parent of root_dir
    invname = f"{base_name}.inventory.{inventory['inventory_id']}.json.gz"
    invpath = os.path.join(inv_dir, invname)

    # Always write the inventory file locally, once uploads have succeeded
    write_inventory_file(
        inventory, {"backend": "s3", "s3_bucket": bucket}, objects, invpath, verbose=verbose
    )

    # Also upload the inventory file to S3 using STANDARD storage class,
    # under the same archive prefix, in an "inventory" subdir:
    #   s3://bucket/{base_prefix}/inventory/{invname}
    inv_s3_key = f"{base_prefix}/inventory/{invname}"
    inv_s3_key = inv_s3_key.replace("//", "/")

    vprint(verbose, f"Uploading inventory to s3://{bucket}/{inv_s3_key} "
                    f"(StorageClass=STANDARD) ...")
    inv_crc64nvme = compute_crc64nvme_b64(invpath, verbose=verbose, use_tqdm=False) if strict_checksum else None
    _inv_storage_class, _inv_size = upload_file_to_s3(
        invpath,
        bucket,
        inv_s3_key,
        storage_class="STANDARD",           # always STANDARD for inventory
        s3_client=s3_client,
        verbose=verbose,
        label="Upload inventory",
        expected_checksum=inv_crc64nvme,
        strict_checksum=strict_checksum,
    )

    # --- SUMMARY ---------------------------------------------------------
    # Printed before the optional delete, so it appears for every successful
    # archive rather than only for --delete runs (the default is to keep the
    # source, which used to return early and skip this entirely).
    if args.summary:
        print("\nArchive summary:")
        print(f"  Total files:      {len(files)}")
        print(f"  Total bytes:      {sum(rec['size_bytes'] for rec in files)}")
        print(f"  Objects created:  {len(objects)}")

    # Now (optionally) delete the original directory tree
    if not args.delete:
        print("Source directory left intact (pass --delete to remove it). "
              "Inventory is available locally and in S3.")
        print(f"Inventory file: {invpath}")
        return

    vprint(verbose, f"Removing directory tree {root_dir}")
    shutil.rmtree(root_dir)

    vprint(verbose, "Done.")
    print(f"Inventory file: {invpath}")


def main():
    run(build_arg_parser().parse_args())


if __name__ == "__main__":
    main()
