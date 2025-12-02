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
import json
import uuid
import hashlib
import tarfile
import tempfile
import shutil
import csv
from concurrent.futures import ThreadPoolExecutor, as_completed

import boto3
from botocore.exceptions import ClientError

# Optional tqdm for progress bars
try:
    from tqdm import tqdm
except ImportError:
    tqdm = None


GLACIER_CLASSES = {"GLACIER", "DEEP_ARCHIVE", "GLACIER_IR"}


# ---------- Utility ----------

def vprint(verbose, *args, **kwargs):
    """Print only if verbose is True."""
    if verbose:
        print(*args, **kwargs)


def compute_sha256(path, verbose=False, use_tqdm=True):
    """Compute SHA256 checksum of a file with optional progress bar."""
    filesize = os.path.getsize(path)
    h = hashlib.sha256()
    chunk_size = 8 * 1024 * 1024

    show_bar = verbose and tqdm and use_tqdm
    pbar = tqdm(
        total=filesize,
        unit="B",
        unit_scale=True,
        desc=f"hash {os.path.basename(path)}"
    ) if show_bar else None

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


def select_relpaths(inventory, only_paths=None, only_prefixes=None, verbose=False):
    """
    Determine which relative paths from the inventory to restore,
    based on --only-path and --only-prefix options.
    """
    files = inventory.get("files", [])
    all_relpaths = [rec["relative_path"] for rec in files]
    all_set = set(all_relpaths)

    if not only_paths and not only_prefixes:
        selected = all_set
    else:
        selected = set()
        if only_paths:
            for p in only_paths:
                if p in all_set:
                    selected.add(p)
                else:
                    vprint(verbose, f"WARNING: --only-path '{p}' not found in inventory.")
        if only_prefixes:
            for pref in only_prefixes:
                matched = [rp for rp in all_relpaths if rp.startswith(pref)]
                if not matched:
                    vprint(verbose, f"WARNING: --only-prefix '{pref}' matched no files.")
                selected.update(matched)

    vprint(verbose, f"Selected {len(selected)} of {len(all_relpaths)} files from inventory.")
    return selected


# ---------- Glacier / Deep Archive Helpers ----------

def classify_object_status(bucket, key, s3_client, verbose=False):
    """
    Inspect a single S3 object and classify its availability.

    Returns a dict with:
      {
        "status": one of {"ready", "cold", "restoring", "error"},
        "storage_class": str or None,
        "restore_header": str or None,
        "error": str or None,
      }
    """
    try:
        resp = s3_client.head_object(Bucket=bucket, Key=key)
    except ClientError as e:
        msg = f"head_object failed: {e}"
        vprint(verbose, f"s3://{bucket}/{key}: ERROR {msg}")
        return {
            "status": "error",
            "storage_class": None,
            "restore_header": None,
            "error": msg,
        }

    storage_class = resp.get("StorageClass", "STANDARD")
    restore_hdr = resp.get("Restore")

    if storage_class not in GLACIER_CLASSES:
        # Not in a Glacier class; ready to download
        vprint(verbose, f"s3://{bucket}/{key} is in {storage_class} and ready.")
        return {
            "status": "ready",
            "storage_class": storage_class,
            "restore_header": restore_hdr,
            "error": None,
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
        }

    # Cold, no restore requested yet
    vprint(verbose, f"Object s3://{bucket}/{key} is cold (no restore in progress).")
    return {
        "status": "cold",
        "storage_class": storage_class,
        "restore_header": restore_hdr,
        "error": None,
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
):
    """Download a single file object from S3 to dest_path.

    Assumes preflight_check_objects has already verified that the object is ready.
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

    vprint(verbose, f"File download complete: {dest_path}")


def extract_tar(tar_path, restore_root, selected_relpaths=None, verbose=False):
    """
    Extract tar into restore_root.

    If selected_relpaths is not None (set of relative file paths),
    only those files (and needed directories) are extracted.
    """
    if tar_path.endswith(".tar.gz"):
        mode = "r:gz"
    else:
        mode = "r"

    vprint(verbose, f"Extracting {tar_path} into {restore_root} ...")

    if selected_relpaths is not None:
        selected_relpaths = set(selected_relpaths)

    with tarfile.open(tar_path, mode) as tar:
        members = tar.getmembers()
        iterator = members
        if verbose and tqdm:
            iterator = tqdm(members, desc="Extract", unit="files")

        for member in iterator:
            name = member.name

            if selected_relpaths is not None:
                if member.isdir():
                    # Extract directory only if any selected file is under it
                    dir_name = name.rstrip("/")
                    needed = any(
                        (rp == dir_name) or rp.startswith(dir_name + "/")
                        for rp in selected_relpaths
                    )
                    if not needed:
                        continue
                else:
                    if name not in selected_relpaths:
                        continue

            tar.extract(member, path=restore_root)

    vprint(verbose, "Extraction complete.")


def verify_restored_files(inventory, restore_root,
                          subset_relpaths=None, verbose=False,
                          max_workers=1):
    """
    Verify that each file listed in inventory (optionally subset) has the expected
    SHA256 after extraction into restore_root.

    For regular files: hash file contents.
    For symlinks (is_symlink=True): hash the link target string.

    Returns:
        status_map: dict[relative_path] -> status string
                    ("ok", "missing", "checksum_mismatch")

    Raises:
        RuntimeError if any file is missing or mismatched.
    """
    files = inventory.get("files", [])
    records_by_rel = {rec["relative_path"]: rec for rec in files}

    if subset_relpaths is not None:
        target_relpaths = [rp for rp in subset_relpaths if rp in records_by_rel]
    else:
        target_relpaths = list(records_by_rel.keys())

    if not target_relpaths:
        vprint(verbose, "No files to verify.")
        return {}

    status_map = {}
    mismatches = []

    # ---------- single-threaded path (keep tqdm behavior) ----------
    if max_workers <= 1:
        iterator = target_relpaths
        if verbose and tqdm:
            iterator = tqdm(target_relpaths, desc="Verify", unit="files")

        for relpath in iterator:
            rec = records_by_rel[relpath]
            expected_sha = rec.get("sha256")
            is_symlink = rec.get("is_symlink", False)
            full_path = os.path.join(restore_root, relpath)

            if is_symlink:
                # verify the symlink itself
                if not os.path.islink(full_path):
                    status_map[relpath] = "missing"
                    mismatches.append((full_path, "missing"))
                    continue
                try:
                    link_target = os.readlink(full_path)
                except OSError:
                    status_map[relpath] = "missing"
                    mismatches.append((full_path, "missing"))
                    continue
                actual_sha = hashlib.sha256(link_target.encode("utf-8")).hexdigest()
            else:
                # regular file
                if not os.path.isfile(full_path):
                    status_map[relpath] = "missing"
                    mismatches.append((full_path, "missing"))
                    continue
                actual_sha = compute_sha256(full_path, verbose=verbose)

            if actual_sha != expected_sha:
                status_map[relpath] = "checksum_mismatch"
                mismatches.append((full_path, "checksum_mismatch"))
            else:
                status_map[relpath] = "ok"

    # ---------- parallel path ----------
    else:
        def verify_one(relpath):
            rec = records_by_rel[relpath]
            expected_sha = rec.get("sha256")
            is_symlink = rec.get("is_symlink", False)
            full_path = os.path.join(restore_root, relpath)

            if is_symlink:
                if not os.path.islink(full_path):
                    return relpath, "missing"
                try:
                    link_target = os.readlink(full_path)
                except OSError:
                    return relpath, "missing"
                actual_sha = hashlib.sha256(link_target.encode("utf-8")).hexdigest()
            else:
                if not os.path.isfile(full_path):
                    return relpath, "missing"
                # no per-file tqdm in parallel mode
                actual_sha = compute_sha256(full_path, verbose=False, use_tqdm=False)

            if actual_sha != expected_sha:
                return relpath, "checksum_mismatch"
            else:
                return relpath, "ok"

        vprint(verbose, f"Verifying {len(target_relpaths)} files with {max_workers} workers...")
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(verify_one, rel): rel for rel in target_relpaths}
            for fut in as_completed(futures):
                relpath, status = fut.result()
                status_map[relpath] = status
                if status != "ok":
                    full_path = os.path.join(restore_root, relpath)
                    mismatches.append((full_path, status))

    if mismatches:
        msg_lines = ["Verification FAILED for the following files:"]
        for path, reason in mismatches:
            msg_lines.append(f"  {reason}: {path}")
        raise RuntimeError("\n".join(msg_lines))

    vprint(verbose, "All verified files match expected checksums.")
    return status_map


def write_summary_csv(inventory, restore_root, subset_relpaths, verify_status,
                      csv_path, verbose=False):
    """
    Write a CSV summary of restored files.

    Columns:
      relative_path, full_path, size_bytes, verify_status
    """
    files = inventory.get("files", [])
    records_by_rel = {rec["relative_path"]: rec for rec in files}

    if subset_relpaths is None:
        subset_relpaths = set(records_by_rel.keys())

    os.makedirs(os.path.dirname(os.path.abspath(csv_path)), exist_ok=True)

    vprint(verbose, f"Writing summary CSV to {csv_path}")
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["relative_path", "full_path", "size_bytes", "verify_status"])

        for rel in sorted(subset_relpaths):
            rec = records_by_rel.get(rel)
            if rec is None:
                continue
            full_path = os.path.join(restore_root, rel)
            size_bytes = rec.get("size_bytes", "")
            status = verify_status.get(rel, "not_checked") if verify_status else "not_checked"
            writer.writerow([rel, full_path, size_bytes, status])


# ---------- Main ----------

def main():
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

    args = parser.parse_args()
    verbose = args.verbose

    # Load inventory
    inv_path = os.path.abspath(args.inventory_file)
    if not os.path.isfile(inv_path):
        print(f"ERROR: Inventory file not found: {inv_path}", file=sys.stderr)
        sys.exit(1)

    vprint(verbose, f"Loading inventory from {inv_path}")
    with open(inv_path, "r", encoding="utf-8") as f:
        inventory = json.load(f)

    archive = inventory.get("archive")
    if not archive:
        print("ERROR: Inventory file does not contain 'archive' section.", file=sys.stderr)
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
    s3_client = get_s3_client(profile=args.profile, endpoint_url=args.endpoint_url)

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
                download_file_from_s3(
                    bucket,
                    key,
                    dest,
                    s3_client,
                    expected_size=obj_size,
                    verbose=verbose,
                )
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


if __name__ == "__main__":
    main()
