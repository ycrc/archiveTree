#!/usr/bin/env python3
"""
Restore a directory tree from an inventory file and its archived objects in S3.

Supports inventories produced by the multi-object archive script, where:

- Some files may be stored as individual S3 objects ("type": "file").
- Other files are stored inside tar archives ("type": "tar") containing groups
  of files.

Deep Glacier–aware:

- Detects GLACIER / DEEP_ARCHIVE / GLACIER_IR storage classes.
- Before downloading, checks all required objects via head_object:
    * If all are "ready", proceeds with parallel download.
    * If any are not ready:
        - Prints status for each required object.
        - If --auto-request-restore is set, submits restore requests for
          cold Glacier/Deep Archive objects.
        - Then exits without downloading.

Parallel:

- Downloads and extraction are done in parallel per-object using a
  ThreadPoolExecutor controlled by --max-workers.
- Optional checksum verification can also use multiple workers.
"""

import argparse
import os
import sys
import uuid
import tempfile
import shutil
from concurrent.futures import ThreadPoolExecutor, as_completed

import boto3
from botocore.exceptions import ClientError

import archive_config
from archive_common import (
    vprint,
    print_config,
    select_relpaths,
    extract_tar,
    verify_object_checksum,
    verify_restored_files,
    restore_permissions_and_ownership,
    write_summary_csv,
    check_inventory_version,
    load_inventory_file,
    detect_backend,
    restore_script_for_backend,
)

# Optional tqdm for progress bars
try:
    from tqdm import tqdm
except ImportError:
    tqdm = None


# Storage classes whose objects are offline until an explicit restore
# request completes. GLACIER_IR (Glacier Instant Retrieval) is deliberately
# NOT here: its objects are readable immediately with a plain GET, report no
# Restore header, and RestoreObject against them is rejected outright.
# Treating it as cold classified every such object as "cold" forever, so the
# preflight refused to download and --auto-request-restore couldn't help --
# making anything archived with --storage-class GLACIER_IR unrestorable.
RESTORE_REQUIRED_CLASSES = {"GLACIER", "DEEP_ARCHIVE"}


# ---------- Utility ----------

def get_s3_client(profile=None, endpoint_url=None):
    """Create a boto3 S3 client with optional profile and endpoint."""
    if profile:
        session = boto3.session.Session(profile_name=profile)
    else:
        session = boto3.session.Session()

    if endpoint_url:
        return session.client("s3", endpoint_url=endpoint_url)
    return session.client("s3")


class DownloadProgressCallback:
    """Progress callback for S3 downloads."""

    def __init__(self, total_bytes, verbose=False, label="Download"):
        self.verbose = verbose
        self._total = total_bytes
        self._seen = 0
        if verbose and tqdm and total_bytes:
            self._pbar = tqdm(
                total=total_bytes,
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


# ---------- Glacier / Deep Archive Helpers ----------

def decode_head_403(bucket, key, s3_client, verbose=False):
    """
    Recover the real error code behind a bare 403 from head_object.

    HeadObject is an HTTP HEAD request, so S3 returns no response body and
    botocore has nothing to parse a code out of -- AccessDenied, NoSuchKey and
    a KMS decrypt denial all surface identically as "(403) Forbidden". A
    one-byte ranged GET hits the same object but does return an XML error
    body, so the underlying code and message become visible.

    Returns (code, message), either of which may be None if the probe could
    not name the cause.
    """
    try:
        s3_client.get_object(Bucket=bucket, Key=key, Range="bytes=0-0")
    except ClientError as e:
        err = e.response.get("Error", {})
        code = err.get("Code") or None
        vprint(verbose, f"s3://{bucket}/{key}: GetObject probe reports {code}")
        return code, err.get("Message") or None
    except Exception as e:  # noqa: BLE001 - probe must never mask the original
        vprint(verbose, f"s3://{bucket}/{key}: GetObject probe failed: {e}")
        return None, None
    # GET succeeded where HEAD failed; nothing further to report.
    return None, None


def classify_object_status(bucket, key, s3_client, verbose=False):
    """
    Inspect a single S3 object and classify its availability.

    Returns a dict with:
      {
        "status": one of {"ready", "cold", "restoring", "error"},
        "storage_class": str or None,
        "restore_header": str or None,
        "error": str or None,
        "error_code": str or None,   # S3 code (or HTTP status) when status is "error"
      }
    """
    try:
        resp = s3_client.head_object(Bucket=bucket, Key=key)
    except ClientError as e:
        meta = e.response.get("ResponseMetadata", {})
        http_status = meta.get("HTTPStatusCode")
        code = e.response.get("Error", {}).get("Code") or None
        msg = f"head_object failed: {e}"

        # A 403 from HEAD carries no body, so re-probe with GET to find out
        # whether this is really AccessDenied, a missing key, or a KMS denial.
        if http_status == 403:
            real_code, real_msg = decode_head_403(
                bucket, key, s3_client, verbose=verbose
            )
            if real_code:
                code = real_code
                msg = f"{msg} [GetObject reports {real_code}: {real_msg}]"

        # Fall back to the bare HTTP status when no code could be named.
        if not code or code == str(http_status):
            code = str(http_status) if http_status else None

        vprint(verbose, f"s3://{bucket}/{key}: ERROR {msg}")
        return {
            "status": "error",
            "storage_class": None,
            "restore_header": None,
            "error": msg,
            "error_code": code,
        }

    storage_class = resp.get("StorageClass", "STANDARD")
    restore_hdr = resp.get("Restore")

    if storage_class not in RESTORE_REQUIRED_CLASSES:
        # Immediately retrievable (STANDARD, IA, GLACIER_IR, ...)
        vprint(verbose, f"s3://{bucket}/{key} is in {storage_class} and ready.")
        return {
            "status": "ready",
            "storage_class": storage_class,
            "restore_header": restore_hdr,
            "error": None,
            "error_code": None,
        }

    # In Glacier / Deep Archive class
    vprint(verbose, f"s3://{bucket}/{key} is in storage class {storage_class}")

    if restore_hdr and 'ongoing-request="false"' in restore_hdr:
        # Already temporarily restored
        vprint(verbose, f"Object s3://{bucket}/{key} is already restored "
                        f"(Restore header: {restore_hdr})")
        return {
            "status": "ready",
            "storage_class": storage_class,
            "restore_header": restore_hdr,
            "error": None,
            "error_code": None,
        }

    if restore_hdr and 'ongoing-request="true"' in restore_hdr:
        # Restore in progress
        vprint(verbose, f"Object s3://{bucket}/{key} restore in progress "
                        f"(Restore header: {restore_hdr})")
        return {
            "status": "restoring",
            "storage_class": storage_class,
            "restore_header": restore_hdr,
            "error": None,
            "error_code": None,
        }

    # Cold, no restore requested yet
    vprint(verbose, f"Object s3://{bucket}/{key} is cold (no restore in progress).")
    return {
        "status": "cold",
        "storage_class": storage_class,
        "restore_header": restore_hdr,
        "error": None,
        "error_code": None,
    }


def request_restore(bucket, key, s3_client, restore_days, restore_tier, verbose=False):
    """
    Submit a restore request for a cold Glacier/Deep Archive object.
    """
    vprint(verbose, f"Requesting restore for s3://{bucket}/{key} "
                    f"(tier={restore_tier}, days={restore_days})")
    restore_request = {
        "Days": restore_days,
        "GlacierJobParameters": {
            "Tier": restore_tier
        },
    }
    try:
        s3_client.restore_object(Bucket=bucket, Key=key, RestoreRequest=restore_request)
    except ClientError as e:
        print(f"ERROR: Failed to submit restore request for s3://{bucket}/{key}: {e}", file=sys.stderr)
        return False

    print(
        f"Submitted restore request for s3://{bucket}/{key} "
        f"(tier={restore_tier}, days={restore_days})."
    )
    return True


def preflight_check_objects(
    bucket,
    jobs,
    s3_client,
    auto_request=False,
    restore_days=7,
    restore_tier="Standard",
    verbose=False,
):
    """
    Preflight all required objects before any download.

    - Checks each object's availability via head_object.
    - Prints status for each required object.
    - If any are not "ready":
        * If auto_request=True: submits restore requests for "cold" objects.
        * Exits the program without starting download.

    If all required objects are ready, returns normally.
    """
    if not jobs:
        vprint(verbose, "No S3 objects required for this restore (nothing to do).")
        return

    print("Checking availability of required S3 objects...\n")

    statuses = {}  # object_id -> status dict
    any_not_ready = False

    for oid, _rels, obj in jobs:
        key = obj.get("s3_key")
        if not key:
            statuses[oid] = {
                "status": "error",
                "storage_class": None,
                "restore_header": None,
                "error": "missing s3_key in inventory",
            }
            any_not_ready = True
            continue

        st = classify_object_status(bucket, key, s3_client, verbose=verbose)
        statuses[oid] = st
        if st["status"] != "ready":
            any_not_ready = True

    # Print status for each required object
    print("Object availability status:")
    for oid, _rels, obj in jobs:
        key = obj.get("s3_key", "?")
        st = statuses.get(oid, {"status": "error", "storage_class": None, "restore_header": None, "error": "unknown"})
        status = st["status"]
        sc = st["storage_class"]
        hdr = st["restore_header"]
        err = st["error"]

        if status == "ready":
            msg = "ready"
        elif status == "cold":
            msg = "cold (in Glacier/Deep Archive, no restore in progress)"
        elif status == "restoring":
            msg = "restoring (restore in progress)"
        else:
            msg = f"error ({err})"

        print(f"  object_id={oid}")
        print(f"    key:           {key}")
        print(f"    storage_class: {sc}")
        print(f"    status:        {msg}")
        if hdr:
            print(f"    Restore hdr:   {hdr}")
        print()

    if not any_not_ready:
        print("All required objects are ready for download.\n")
        return

    # At least one object is not ready
    print("One or more required objects are not currently available for download.\n")

    if auto_request:
        print("Submitting restore requests for cold Glacier/Deep Archive objects...\n")
        for oid, _rels, obj in jobs:
            key = obj.get("s3_key")
            st = statuses.get(oid)
            if not st:
                continue
            if st["status"] == "cold":
                request_restore(
                    bucket,
                    key,
                    s3_client,
                    restore_days=restore_days,
                    restore_tier=restore_tier,
                    verbose=verbose,
                )

        print(
            "\nRestore requests (if any) have been submitted. "
            "Wait for AWS to complete the restore, then rerun this script."
        )
    else:
        print(
            "Use --auto-request-restore to automatically submit restore requests "
            "for cold Glacier/Deep Archive objects, or restore them manually, then "
            "rerun this script once they are ready."
        )

    # Exit without doing any downloads
    sys.exit(1)


# ---------- Core Operations ----------

def download_tar_from_s3(
    bucket,
    key,
    s3_client,
    scratch_dir=None,
    expected_size=None,
    verbose=False,
):
    """Download tar from S3 to a temporary file in scratch_dir.

    Assumes preflight_check_objects has already verified that the object is ready.
    """
    vprint(verbose, f"Downloading tar s3://{bucket}/{key}")

    # Determine tar suffix by key extension (used only for nicer filename)
    if key.endswith(".tar.gz"):
        suffix = ".tar.gz"
    elif key.endswith(".tar"):
        suffix = ".tar"
    else:
        suffix = ".tar"

    if scratch_dir is None:
        scratch_dir = tempfile.gettempdir()
    os.makedirs(scratch_dir, exist_ok=True)

    local_tar = os.path.join(
        scratch_dir,
        f"restore_{uuid.uuid4().hex}{suffix}"
    )

    total_bytes = expected_size

    progress = DownloadProgressCallback(
        total_bytes or 0,
        verbose=verbose,
        label=f"Download tar ({os.path.basename(key)})"
    ) if total_bytes else None

    vprint(verbose, f"Downloading tar to {local_tar} ...")
    with open(local_tar, "wb") as f:
        if progress:
            s3_client.download_fileobj(bucket, key, f, Callback=progress)
            progress.close()
        else:
            s3_client.download_fileobj(bucket, key, f)

    if expected_size is not None:
        actual_size = os.path.getsize(local_tar)
        if actual_size != expected_size:
            raise RuntimeError(
                f"Downloaded tar size mismatch for s3://{bucket}/{key}: "
                f"expected {expected_size}, got {actual_size}"
            )

    vprint(verbose, "Tar download complete.")
    return local_tar


def download_file_from_s3(
    bucket,
    key,
    dest_path,
    s3_client,
    expected_size=None,
    verbose=False,
    mode=None,
):
    """Download a single file object from S3 to dest_path.

    Assumes preflight_check_objects has already verified that the object is ready.

    If `mode` is given (the original file's POSIX permission bits, from the
    inventory record's "mode" field), it's applied via chmod after download.
    """
    vprint(verbose, f"Downloading file s3://{bucket}/{key} -> {dest_path}")

    if dest_path:
        os.makedirs(os.path.dirname(dest_path), exist_ok=True)

    total_bytes = expected_size

    progress = DownloadProgressCallback(
        total_bytes or 0,
        verbose=verbose,
        label=f"Download file ({os.path.basename(key)})"
    ) if total_bytes else None

    with open(dest_path, "wb") as f:
        if progress:
            s3_client.download_fileobj(bucket, key, f, Callback=progress)
            progress.close()
        else:
            s3_client.download_fileobj(bucket, key, f)

    if expected_size is not None:
        actual_size = os.path.getsize(dest_path)
        if actual_size != expected_size:
            raise RuntimeError(
                f"Downloaded file size mismatch for s3://{bucket}/{key}: "
                f"expected {expected_size}, got {actual_size}"
            )

    if mode is not None:
        try:
            os.chmod(dest_path, mode)
        except OSError as e:
            print(f"WARNING: failed to restore permissions on {dest_path}: {e}", file=sys.stderr)

    vprint(verbose, f"File download complete: {dest_path}")


# ---------- Main ----------

def build_arg_parser():
    parser = argparse.ArgumentParser(
        description="Restore a directory tree from an inventory file and S3 archive."
    )
    parser.add_argument(
        "inventory_file",
        help="Path to the inventory JSON file produced by the archive script."
    )
    parser.add_argument(
        "--profile",
        default=None,
        help="AWS profile name from ~/.aws/credentials or ~/.aws/config."
    )
    parser.add_argument(
        "--endpoint-url",
        default=None,
        help="Custom S3 endpoint URL (for S3-compatible storage)."
    )
    parser.add_argument(
        "--config-file", default=None,
        help=(
            "Path to config file with an [s3] section supplying defaults "
            f"for profile/endpoint_url (default: {archive_config.DEFAULT_CONFIG_FILE})."
        ),
    )
    parser.add_argument(
        "--scratch-dir",
        default=None,
        help="Directory for temporary downloaded tars (default: system temp)."
    )
    parser.add_argument(
        "--restore-dir",
        default=None,
        help=(
            "Override the original root_dir from the inventory. "
            "If not set, the original path in inventory['root_dir'] is used."
        ),
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Allow restoring into an existing, non-empty directory."
    )
    parser.add_argument(
        "--only-path",
        action="append",
        default=None,
        help="Restore only file(s) whose relative path equals this value (may repeat)."
    )
    parser.add_argument(
        "--only-prefix",
        action="append",
        default=None,
        help="Restore only files whose relative path starts with this prefix (may repeat)."
    )
    parser.add_argument(
        "--verify-checksums",
        action="store_true",
        help="After extraction, recompute SHA256 checksums and compare to inventory."
    )
    parser.add_argument(
        "--no-restore-ownership",
        action="store_false",
        dest="restore_ownership",
        help=(
            "Do not restore each file's original uid/gid, even when running "
            "as root. Ownership is otherwise restored automatically whenever "
            "the restore runs as root and the inventory recorded it (archives "
            "written before uid/gid were recorded are unaffected either way). "
            "Use this to restore an administrator-made archive into a scratch "
            "area owned by the invoking user."
        ),
    )
    parser.add_argument(
        "--summary-csv",
        default=None,
        help="Write a CSV summary of restored files to this path."
    )
    parser.add_argument(
        "--keep-tar",
        action="store_true",
        help="Do not delete the downloaded tar(s) after restore."
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be done, but do not download/extract/verify or write files."
    )
    parser.add_argument(
        "--auto-request-restore",
        action="store_true",
        help=(
            "If an object is in GLACIER / DEEP_ARCHIVE and not yet restored, "
            "submit a restore request up front and exit. Only when all required "
            "objects are ready will downloads proceed."
        ),
    )
    parser.add_argument(
        "--restore-days",
        type=int,
        default=7,
        help="Number of days to keep restored copies when auto-requesting restores (default: 7).",
    )
    parser.add_argument(
        "--restore-tier",
        choices=["Bulk", "Standard", "Expedited"],
        default="Standard",
        help=(
            "Glacier restore tier to use when auto-requesting restores. "
            "Note: Some classes/endpoints may not support 'Expedited'."
        ),
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=4,
        help="Maximum number of parallel download/verify workers (default: 4).",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Print progress messages and show progress bars (if tqdm is installed)."
    )

    return parser


def run(args):
    verbose = args.verbose

    config_path = os.path.expanduser(args.config_file or archive_config.DEFAULT_CONFIG_FILE)
    s3_config = archive_config.load_config_file(config_path, section="s3")
    profile = args.profile or s3_config.get("profile")
    endpoint_url = args.endpoint_url or s3_config.get("endpoint_url")

    # Load inventory
    inv_path = os.path.abspath(args.inventory_file)
    if not os.path.isfile(inv_path):
        print(f"ERROR: Inventory file not found: {inv_path}", file=sys.stderr)
        sys.exit(1)

    vprint(verbose, f"Loading inventory from {inv_path}")
    inventory = load_inventory_file(inv_path)

    version_error = check_inventory_version(inventory)
    if version_error:
        print(f"ERROR: {version_error}", file=sys.stderr)
        sys.exit(1)

    archive = inventory.get("archive")
    if not archive:
        print("ERROR: Inventory file does not contain 'archive' section.", file=sys.stderr)
        sys.exit(1)

    if archive.get("backend") not in (None, "s3"):
        actual_backend = detect_backend(inventory)
        print(
            f"ERROR: This inventory was not produced by archive_to_s3.py "
            f"(archive.backend={archive.get('backend')!r}). Use "
            f"{restore_script_for_backend(actual_backend)} instead.",
            file=sys.stderr,
        )
        sys.exit(1)

    bucket = archive.get("s3_bucket")
    objects = archive.get("objects")

    if not bucket or objects is None:
        print("ERROR: 'archive' section is missing 's3_bucket' or 'objects'.", file=sys.stderr)
        sys.exit(1)

    # Map object_id -> metadata
    objects_by_id = {obj["id"]: obj for obj in objects}

    # Determine restore root
    if args.restore_dir:
        restore_root = os.path.abspath(args.restore_dir)
    else:
        original_root = inventory.get("root_dir")
        if not original_root:
            print("ERROR: Inventory missing 'root_dir' and --restore-dir not provided.", file=sys.stderr)
            sys.exit(1)
        restore_root = os.path.abspath(original_root)

    print_config(verbose, "Configuration", {
        "inventory_file": inv_path,
        "backend": "s3",
        "bucket": bucket,
        "profile": profile,
        "endpoint_url": endpoint_url,
        "config_file": config_path,
        "restore_dir": restore_root,
        "scratch_dir": args.scratch_dir,
        "overwrite": args.overwrite,
        "only_path": args.only_path,
        "only_prefix": args.only_prefix,
        "verify_checksums": args.verify_checksums,
        "summary_csv": args.summary_csv,
        "keep_tar": args.keep_tar,
        "dry_run": args.dry_run,
        "auto_request_restore": args.auto_request_restore,
        "restore_days": args.restore_days,
        "restore_tier": args.restore_tier,
        "max_workers": args.max_workers,
    })

    # Determine subset of relpaths to restore
    selected_relpaths = select_relpaths(
        inventory,
        only_paths=args.only_path,
        only_prefixes=args.only_prefix,
        verbose=verbose,
    )

    # Build mapping: relpath -> record, and object_id -> relpaths to restore
    records_by_rel = {rec["relative_path"]: rec for rec in inventory.get("files", [])}
    object_to_relpaths = {}
    total_bytes = 0

    for rel in selected_relpaths:
        rec = records_by_rel.get(rel)
        if rec is None:
            continue
        oid = rec.get("object_id")
        if oid is None:
            vprint(verbose, f"WARNING: file {rel} has no object_id in inventory; skipping.")
            continue
        object_to_relpaths.setdefault(oid, set()).add(rel)
        total_bytes += rec.get("size_bytes", 0)

    # Dry-run: just report what would be done
    if args.dry_run:
        print("DRY RUN: no data will be downloaded or written.")
        print(f"  Inventory file: {inv_path}")
        print(f"  Restore root:   {restore_root}")
        print(f"  Files to restore: {len(selected_relpaths)}")
        print(f"  Total bytes (from inventory): {total_bytes}")
        if args.only_path:
            print(f"  Filters --only-path:   {args.only_path}")
        if args.only_prefix:
            print(f"  Filters --only-prefix: {args.only_prefix}")

        print("  Objects involved:")
        for oid, rels in object_to_relpaths.items():
            obj = objects_by_id.get(oid, {})
            key = obj.get("s3_key", "?")
            otype = obj.get("type", "?")
            obj_size = obj.get("size_bytes", "?")
            subset_bytes = sum(
                records_by_rel[r]["size_bytes"] for r in rels if r in records_by_rel
            )
            print(f"    object_id={oid} type={otype} key={key}")
            print(f"      object_size={obj_size} subset_files={len(rels)} subset_bytes={subset_bytes}")

        if args.summary_csv:
            print(f"  (Summary CSV would be written to: {args.summary_csv})")
        if args.verify_checksums:
            print("  (Checksums would be verified after extraction.)")
        if args.auto_request_restore:
            print("  (If run without --dry-run, restore requests would be submitted "
                  "for any cold Glacier/Deep Archive objects.)")

        sys.exit(0)

    # Check restore target
    if os.path.exists(restore_root):
        if not os.path.isdir(restore_root):
            print(f"ERROR: Restore target exists and is not a directory: {restore_root}", file=sys.stderr)
            sys.exit(1)
        if os.listdir(restore_root) and not args.overwrite:
            print(
                f"ERROR: Restore target directory {restore_root} is not empty. "
                "Use --overwrite to allow restoring here.",
                file=sys.stderr,
            )
            sys.exit(1)
    else:
        os.makedirs(restore_root, exist_ok=True)

    vprint(verbose, f"Restoring into directory: {restore_root}")

    # S3 client
    s3_client = get_s3_client(profile=profile, endpoint_url=endpoint_url)

    # Build jobs for parallel restore
    jobs = []
    for oid, rels in object_to_relpaths.items():
        obj = objects_by_id.get(oid)
        if obj is None:
            vprint(verbose, f"WARNING: object_id={oid} not found in archive.objects; skipping.")
            continue
        jobs.append((oid, rels, obj))

    # Preflight check all required objects (Glacier-friendly)
    preflight_check_objects(
        bucket=bucket,
        jobs=jobs,
        s3_client=s3_client,
        auto_request=args.auto_request_restore,
        restore_days=args.restore_days,
        restore_tier=args.restore_tier,
        verbose=verbose,
    )

    temp_tars = []

    def restore_object_job(oid, rels, obj):
        """Worker: restore one object (file or tar)."""
        otype = obj.get("type")
        key = obj.get("s3_key")
        obj_size = obj.get("size_bytes")

        if not key or not otype:
            vprint(verbose, f"WARNING: object_id={oid} missing type or key; skipping.")
            return None

        if otype == "file":
            for rel in rels:
                dest = os.path.join(restore_root, rel)
                vprint(verbose, f"[worker] Restoring single-file object_id={oid} to {dest}")
                rec = records_by_rel.get(rel)
                mode = rec.get("mode") if rec else None
                download_file_from_s3(
                    bucket,
                    key,
                    dest,
                    s3_client,
                    expected_size=obj_size,
                    verbose=verbose,
                    mode=mode,
                )
                verify_object_checksum(dest, obj, verbose=verbose)
            return None

        elif otype == "tar":
            vprint(verbose, f"[worker] Restoring tar object_id={oid} ({key})")
            tar_path = download_tar_from_s3(
                bucket,
                key,
                s3_client,
                scratch_dir=args.scratch_dir,
                expected_size=obj_size,
                verbose=verbose,
            )

            # Check the downloaded object against the whole-object checksum
            # recorded at archive time (when there is one) before trusting
            # its contents -- a corrupt tar is worth reporting as such,
            # rather than as a confusing extraction failure.
            verify_object_checksum(tar_path, obj, verbose=verbose)

            extract_tar(tar_path, restore_root, selected_relpaths=rels, verbose=verbose)

            if args.keep_tar:
                return tar_path
            else:
                vprint(verbose, f"[worker] Removing temporary tar {tar_path}")
                try:
                    os.remove(tar_path)
                except OSError as e:
                    print(f"WARNING: Failed to remove temporary tar {tar_path}: {e}", file=sys.stderr)
                return None
        else:
            vprint(verbose, f"WARNING: Unknown object type '{otype}' for object_id={oid}; skipping.")
            return None

    # Parallel restore
    max_workers = max(1, args.max_workers)
    vprint(verbose, f"Starting restore with up to {max_workers} workers...")
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [
            executor.submit(restore_object_job, oid, rels, obj)
            for (oid, rels, obj) in jobs
        ]
        for fut in as_completed(futures):
            tpath = fut.result()
            if tpath:
                temp_tars.append(tpath)

    # Restore permission bits (and, as root, original uid/gid) now that all
    # file content has been written: files first, then directories
    # deepest-first, so a restrictive directory mode never blocks a chmod
    # still to come inside it.
    restore_permissions_and_ownership(
        inventory, restore_root, subset_relpaths=selected_relpaths,
        only_prefixes=args.only_prefix, only_paths=args.only_path,
        restore_ownership=args.restore_ownership, verbose=verbose,
    )

    # Optional checksum verification
    verify_status = {}
    if args.verify_checksums:
        vprint(verbose, "Verifying restored files against inventory checksums...")
        verify_status = verify_restored_files(
            inventory,
            restore_root,
            subset_relpaths=selected_relpaths,
            verbose=verbose,
            max_workers=max_workers,
        )
    else:
        verify_status = {rel: "not_checked" for rel in selected_relpaths}

    # Optional summary CSV
    if args.summary_csv:
        write_summary_csv(
            inventory,
            restore_root,
            subset_relpaths=selected_relpaths,
            verify_status=verify_status,
            csv_path=args.summary_csv,
            verbose=verbose
        )

    # If keep-tar was set and some tars were kept, just list them
    if args.keep_tar and temp_tars:
        vprint(verbose, "Temporary tars kept:")
        for t in temp_tars:
            vprint(verbose, f"  {t}")

    print("Restore completed successfully.")


def main():
    run(build_arg_parser().parse_args())


if __name__ == "__main__":
    main()
