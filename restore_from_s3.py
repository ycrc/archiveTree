#!/usr/bin/env python3
"""
Restore a directory tree from an inventory file and its archived tar in S3.

Given:
  * an inventory JSON produced by the archive_to_s3 script

This script:
  * reads S3 bucket/key and metadata from the inventory
  * optionally filters to a subset of files (by relative path or prefix)
  * (unless --dry-run) downloads the tar archive to scratch space
  * (unless --dry-run) unpacks the archive into the restore path
  * (optional) verifies checksums of restored files against the inventory
  * (optional) writes a CSV summary of restored files
  * cleans up the temporary tar file (unless --keep-tar is used)

Options:
  --profile         : AWS credentials profile
  --endpoint-url    : Custom S3 endpoint (for S3-compatible storage)
  --scratch-dir     : Where to put the downloaded tar
  --restore-dir     : Override the original root_dir in the inventory
  --overwrite       : Allow restoring into an existing non-empty directory
  --only-path       : Exact relative path(s) to restore (may repeat)
  --only-prefix     : Relative path prefix(es) to restore (may repeat)
  --verify-checksums: After extraction, recompute SHA256 and compare
  --summary-csv     : Write a CSV summary of restored files
  --keep-tar        : Do not delete the downloaded tar after restore
  --dry-run         : Show what would be done, but do nothing
  --verbose         : Print steps and show progress bars (if tqdm installed)
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

import boto3
from botocore.exceptions import ClientError

# Optional tqdm for progress bars
try:
    from tqdm import tqdm
except ImportError:
    tqdm = None


# ---------- Utility ----------

def vprint(verbose, *args, **kwargs):
    """Print only if verbose is True."""
    if verbose:
        print(*args, **kwargs)


def compute_sha256(path, verbose=False):
    """Compute SHA256 checksum of a file with optional progress bar."""
    filesize = os.path.getsize(path)
    h = hashlib.sha256()
    chunk_size = 8 * 1024 * 1024

    if verbose and tqdm:
        pbar = tqdm(
            total=filesize,
            unit="B",
            unit_scale=True,
            desc=f"hash {os.path.basename(path)}"
        )
    else:
        pbar = None

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

    def __init__(self, total_bytes, verbose=False):
        self.verbose = verbose
        self._total = total_bytes
        self._seen = 0
        if verbose and tqdm and total_bytes:
            self._pbar = tqdm(
                total=total_bytes,
                unit="B",
                unit_scale=True,
                desc="Download"
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


# ---------- Core Operations ----------

def download_tar_from_s3(bucket, key, s3_client, scratch_dir=None, expected_size=None, verbose=False):
    """Download tar from S3 to a temporary file in scratch_dir."""
    vprint(verbose, f"Preparing to download s3://{bucket}/{key}")

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

    # Get size for progress bar if not provided
    total_bytes = expected_size
    if total_bytes is None:
        try:
            head = s3_client.head_object(Bucket=bucket, Key=key)
            total_bytes = head.get("ContentLength")
        except ClientError:
            total_bytes = None

    progress = DownloadProgressCallback(total_bytes or 0, verbose=verbose) if total_bytes else None

    vprint(verbose, f"Downloading to {local_tar} ...")
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
                f"Downloaded tar size mismatch: expected {expected_size}, got {actual_size}"
            )

    vprint(verbose, "Download complete.")
    return local_tar


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


def verify_restored_files(inventory, restore_root, subset_relpaths=None, verbose=False):
    """
    Verify that each file listed in inventory (optionally subset) has the expected
    SHA256 after extraction into restore_root.

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

    iterator = target_relpaths
    if verbose and tqdm:
        iterator = tqdm(target_relpaths, desc="Verify", unit="files")

    mismatches = []
    status_map = {}

    for relpath in iterator:
        rec = records_by_rel[relpath]
        expected_sha = rec.get("sha256")
        full_path = os.path.join(restore_root, relpath)

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

    if mismatches:
        msg_lines = ["Verification FAILED for the following files:"]
        for path, reason in mismatches:
            msg_lines.append(f"  {reason}: {path}")
        raise RuntimeError("\n".join(msg_lines))

    vprint(verbose, "All verified files match expected checksums.")
    return status_map


def write_summary_csv(inventory, restore_root, subset_relpaths, verify_status, csv_path, verbose=False):
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
        help="Directory for temporary downloaded tar (default: system temp)."
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
        help="After extraction, recompute SHA256 of each file and compare to inventory."
    )
    parser.add_argument(
        "--summary-csv",
        default=None,
        help="Write a CSV summary of restored files to this path."
    )
    parser.add_argument(
        "--keep-tar",
        action="store_true",
        help="Do not delete the downloaded tar after restore."
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be done, but do not download/extract/verify or write files."
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
    key = archive.get("s3_key")
    expected_tar_size = archive.get("tar_size_bytes")

    if not bucket or not key:
        print("ERROR: 'archive' section is missing s3_bucket or s3_key.", file=sys.stderr)
        sys.exit(1)

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

    # Basic size stats for subset
    records_by_rel = {rec["relative_path"]: rec for rec in inventory.get("files", [])}
    total_bytes = sum(
        records_by_rel[r]["size_bytes"] for r in selected_relpaths if r in records_by_rel
    )

    if args.dry_run:
        print("DRY RUN: no data will be downloaded or written.")
        print(f"  Inventory file: {inv_path}")
        print(f"  S3 object: s3://{bucket}/{key}")
        print(f"  Restore root: {restore_root}")
        print(f"  Files to restore: {len(selected_relpaths)}")
        print(f"  Total bytes (from inventory): {total_bytes}")
        if args.only_path:
            print(f"  Filters --only-path: {args.only_path}")
        if args.only_prefix:
            print(f"  Filters --only-prefix: {args.only_prefix}")
        if args.summary_csv:
            print(f"  (Summary CSV would be written to: {args.summary_csv})")
        if args.verify_checksums:
            print("  (Checksums would be verified after extraction.)")
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

    # Download tar
    tar_path = download_tar_from_s3(
        bucket,
        key,
        s3_client,
        scratch_dir=args.scratch_dir,
        expected_size=expected_tar_size,
        verbose=verbose
    )

    # Extract tar (subset-aware)
    extract_tar(tar_path, restore_root, selected_relpaths=selected_relpaths, verbose=verbose)

    # Optional checksum verification
    verify_status = {}
    if args.verify_checksums:
        vprint(verbose, "Verifying restored files against inventory checksums...")
        verify_status = verify_restored_files(
            inventory,
            restore_root,
            subset_relpaths=selected_relpaths,
            verbose=verbose
        )
    else:
        # mark all selected as not_checked
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

    # Cleanup tar unless --keep-tar
    if not args.keep_tar:
        vprint(verbose, f"Removing temporary tar {tar_path}")
        try:
            os.remove(tar_path)
        except OSError as e:
            print(f"WARNING: Failed to remove temporary tar: {e}", file=sys.stderr)

    vprint(verbose, "Restore completed successfully.")


if __name__ == "__main__":
    main()
