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

Parallel:

- Large-file uploads and tar-group uploads are done in parallel using a
  ThreadPoolExecutor controlled by --max-workers.
"""

import argparse
import os
import sys
import tarfile
import tempfile
import shutil
import hashlib
import json
import uuid
import pwd
import stat
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

import boto3
from botocore.exceptions import ClientError

# Optional tqdm for progress bars
try:
    from tqdm import tqdm
except ImportError:
    tqdm = None

GLACIER_CLASSES = {"GLACIER", "DEEP_ARCHIVE", "GLACIER_IR"}


# ---------- Utility Functions ----------

def vprint(verbose, *args, **kwargs):
    """Print only if verbose."""
    if verbose:
        print(*args, **kwargs)


def compute_sha256(path, verbose=False, use_tqdm=True):
    """Compute SHA256 checksum of a file with optional progress bar."""
    filesize = os.path.getsize(path)
    h = hashlib.sha256()

    show_bar = verbose and tqdm and use_tqdm
    pbar = tqdm(total=filesize, unit="B", unit_scale=True,
                desc=f"hash {os.path.basename(path)}") if show_bar else None

    chunk_size = 8 * 1024 * 1024
    with open(path, "rb") as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            h.update(chunk)
            if pbar:
                pbar.update(len(chunk))

    if pbar:
        pbar.close()

    return h.hexdigest()


def get_owner(stat_result):
    """Get username from stat, fall back to uid."""
    try:
        return pwd.getpwuid(stat_result.st_uid).pw_name
    except Exception:
        return str(stat_result.st_uid)

def build_inventory(root_dir, verbose=False):
    """Walk directory and compute inventory."""
    file_records = []
    file_paths = []

    file_list = []
    for dirpath, _, filenames in os.walk(root_dir):
        for name in filenames:
            file_list.append(os.path.join(dirpath, name))

    iterator = file_list
    if verbose and tqdm:
        iterator = tqdm(file_list, desc="Inventory", unit="files")

    root_dir = os.path.abspath(root_dir)

    for path in iterator:
        relpath = os.path.relpath(path, root_dir)

        # Use lstat so we see the symlink itself, not the target
        try:
            st = os.lstat(path)
        except FileNotFoundError:
            # File disappeared between walk and lstat; skip with a warning
            vprint(verbose, f"WARNING: {path} vanished during inventory; skipping.")
            continue

        if stat.S_ISLNK(st.st_mode):
            # Symlink (possibly broken). Hash the link target string.
            try:
                link_target = os.readlink(path)
            except OSError as e:
                vprint(verbose, f"WARNING: failed to readlink({path}): {e}")
                link_target = ""

            sha = hashlib.sha256(link_target.encode("utf-8")).hexdigest()
            record = {
                "relative_path": relpath,
                "absolute_path": path,
                "size_bytes": st.st_size,      # length of link target string
                "ctime": datetime.fromtimestamp(st.st_ctime).isoformat(),
                "owner": get_owner(st),
                "sha256": sha,
                "is_symlink": True,
                "symlink_target": link_target,
            }
        else:
            # Regular file (or other non-symlink); hash contents
            sha = compute_sha256(path, verbose=verbose)
            record = {
                "relative_path": relpath,
                "absolute_path": path,
                "size_bytes": st.st_size,
                "ctime": datetime.fromtimestamp(st.st_ctime).isoformat(),
                "owner": get_owner(st),
                "sha256": sha,
                "is_symlink": False,
            }

        file_records.append(record)
        file_paths.append(path)

    inventory = {
        "inventory_id": str(uuid.uuid4()),
        "root_dir": root_dir,
        "created_at": datetime.utcnow().isoformat() + "Z",
        "total_files": len(file_records),
        "total_bytes": sum(r["size_bytes"] for r in file_records),
        "files": file_records,
    }

    return inventory, file_paths


def create_tar(root_dir, abs_paths, scratch_dir=None, tar_compression=None,
               verbose=False, group_index=None):
    """Tar up a subset of files with optional progress bar."""
    root_dir = os.path.abspath(root_dir)
    base_name = os.path.basename(root_dir.rstrip(os.sep))
    suffix = ".tar.gz" if tar_compression == "gz" else ".tar"
    if scratch_dir is None:
        scratch_dir = tempfile.gettempdir()

    os.makedirs(scratch_dir, exist_ok=True)
    if group_index is None:
        name_part = uuid.uuid4().hex
    else:
        name_part = f"group_{group_index:06d}"
    tar_path = os.path.join(scratch_dir, f"{base_name}_{name_part}{suffix}")

    mode = "w:gz" if tar_compression == "gz" else "w"

    iterator = abs_paths
    if verbose and tqdm:
        iterator = tqdm(abs_paths, desc=f"Tar {name_part}", unit="files")

    with tarfile.open(tar_path, mode, dereference=False) as tar:
        for path in iterator:
            relpath = os.path.relpath(path, root_dir)
            tar.add(path, arcname=relpath)

    return tar_path


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


def upload_file_to_s3(path, bucket, key, storage_class, s3_client,
                      verbose=False, label="Upload"):
    """
    Upload a local file to S3 with progress and verify size.

    Returns:
        storage_class_actual (str): StorageClass from HEAD (for inventory).
        size_remote (int): ContentLength from HEAD.
    """
    extra_args = {"StorageClass": storage_class}

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
    try:
        resp = s3_client.head_object(Bucket=bucket, Key=key)
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

    if remote_storage_class in GLACIER_CLASSES:
        vprint(
            verbose,
            f"NOTE: s3://{bucket}/{key} is stored in {remote_storage_class}. "
            "You will need to run a restore request before downloading it later."
        )

    vprint(verbose, f"Verified upload of {local_size} bytes to "
                    f"s3://{bucket}/{key} (StorageClass={remote_storage_class}).")

    return remote_storage_class, remote_size


# ---------- Inventory Writing ----------

def write_inventory_file(inventory, bucket, objects, inventory_path, verbose=False):
    """
    Write inventory JSON file to inventory_path.

    `objects` is a list of dicts describing each S3 object, e.g.:
      {
        "id": "tar-000001",
        "type": "tar",
        "s3_key": "...",
        "size_bytes": 123,
        "storage_class": "DEEP_ARCHIVE",
        "file_count": 42
      }
    """
    inv = dict(inventory)
    inv["archive"] = {
        "s3_bucket": bucket,
        "archive_id": inventory["inventory_id"],
        "objects": objects,
    }

    os.makedirs(os.path.dirname(inventory_path), exist_ok=True)

    with open(inventory_path, "w") as f:
        json.dump(inv, f, indent=2, sort_keys=True)

    vprint(verbose, f"Wrote inventory file {inventory_path}")


# ---------- Main ----------

def main():
    parser = argparse.ArgumentParser(
        description="Archive a directory tree to multiple S3 objects."
    )
    parser.add_argument("directory")
    parser.add_argument("bucket")
    parser.add_argument(
        "object_path",
        help="Base S3 prefix under which archive objects will be stored."
    )
    parser.add_argument(
        "--storage-class",
        default="STANDARD",
        help=("""S3 storage class for newly created objects.
            Examples: STANDARD, STANDARD_IA, ONEZONE_IA, INTELLIGENT_TIERING,
            GLACIER, GLACIER_IR, DEEP_ARCHIVE. Default: STANDARD. """
         ),
    )
    parser.add_argument("--scratch-dir", default=None)
    parser.add_argument("--compression", choices=["none", "gz"], default="none")
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--profile", default=None)
    parser.add_argument("--endpoint-url", default=None)
    parser.add_argument("--verbose", action="store_true")
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
        "--no-summary",
        action="store_false",
        dest="summary",
        help="Print a summary of files, bytes, and objects created.",
    )

    args = parser.parse_args()
    verbose = args.verbose

    root_dir = os.path.abspath(args.directory)

    if not os.path.isdir(root_dir):
        print(f"ERROR: {root_dir} is not a directory", file=sys.stderr)
        sys.exit(1)

    vprint(verbose, "Building inventory...")
    inventory, _ = build_inventory(root_dir, verbose=verbose)

    # Partition files into large and small sets
    size_cutoff = args.size_cutoff
    size_grouping = args.size_grouping
    files = inventory["files"]

    large_files = [rec for rec in files if rec["size_bytes"] > size_cutoff]
    small_files = [rec for rec in files if rec["size_bytes"] <= size_cutoff]

    vprint(
        verbose,
        f"Total files: {len(files)}; "
        f"large: {len(large_files)}; small: {len(small_files)}"
    )

    # Base S3 prefix: {object_path}/{root_name}/{archive_id}
    base_name = os.path.basename(root_dir.rstrip(os.sep))
    archive_id = inventory["inventory_id"]
    opath = args.object_path.strip("/")

    if opath:
        base_prefix = f"{opath}/{base_name}/{archive_id}"
    else:
        base_prefix = f"{base_name}/{archive_id}"

    s3_client = get_s3_client(args.profile, args.endpoint_url)

    # Group small files into tars of approximately size_grouping bytes
    groups = []
    current = []
    current_size = 0

    for rec in small_files:
        current.append(rec)
        current_size += rec["size_bytes"]
        if current_size >= size_grouping:
            groups.append(current)
            current = []
            current_size = 0

    if current:
        groups.append(current)

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

            storage_class_actual, size_remote = upload_file_to_s3(
                abs_path,
                args.bucket,
                key,
                args.storage_class,
                s3_client,
                verbose=verbose,
                label=f"Upload {rel}",
            )

            obj_meta = {
                "id": obj_id,
                "type": "file",
                "s3_key": key,
                "size_bytes": size_remote,
                "storage_class": storage_class_actual,
                "relative_path": rel,
            }

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
            )

            tar_name = f"group_{g_idx:06d}{tar_suffix}"
            key = f"{base_prefix}/groups/{tar_name}"
            key = key.replace("//", "/")

            storage_class_actual, size_remote = upload_file_to_s3(
                tar_path,
                args.bucket,
                key,
                args.storage_class,
                s3_client,
                verbose=verbose,
                label=f"Upload group {g_idx}",
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

    # Inventory file path in parent directory
    parent = os.path.dirname(root_dir)
    invname = f"{base_name}.inventory.{inventory['inventory_id']}.json"
    invpath = os.path.join(parent, invname)

    # Always write the inventory file locally, once uploads have succeeded
    write_inventory_file(inventory, args.bucket, objects, invpath, verbose=verbose)

    # Also upload the inventory file to S3 using STANDARD storage class,
    # under the same archive prefix, in an "inventory" subdir:
    #   s3://bucket/{base_prefix}/inventory/{invname}
    inv_s3_key = f"{base_prefix}/inventory/{invname}"
    inv_s3_key = inv_s3_key.replace("//", "/")

    vprint(verbose, f"Uploading inventory to s3://{args.bucket}/{inv_s3_key} "
                    f"(StorageClass=STANDARD) ...")
    _inv_storage_class, _inv_size = upload_file_to_s3(
        invpath,
        args.bucket,
        inv_s3_key,
        storage_class="STANDARD",           # always STANDARD for inventory
        s3_client=s3_client,
        verbose=verbose,
        label="Upload inventory",
    )

    # Now (optionally) delete the original directory tree
    if not args.force:
        resp = input(
            f"\nAll uploads and inventory writes completed. "
            f"About to DELETE directory tree:\n  {root_dir}\n"
            f"Type 'yes' to proceed, anything else to keep it: "
        ).strip()
        if resp.lower() != "yes":
            print("User declined delete. Original directory left intact. "
                  "Inventory is available locally and in S3.")
            return

    vprint(verbose, f"Removing directory tree {root_dir}")
    shutil.rmtree(root_dir)

    # --- SUMMARY ---------------------------------------------------------
    if args.summary:
        total_files = len(files)
        total_bytes = sum(rec["size_bytes"] for rec in files)
        num_objects = len(objects)

        print("\nArchive summary:")
        print(f"  Total files:      {total_files}")
        print(f"  Total bytes:      {total_bytes}")
        print(f"  Objects created:  {num_objects}")

    vprint(verbose, "Done.")
    
if __name__ == "__main__":
    main()
