#!/usr/bin/env python3
"""
Transport-agnostic logic shared by the S3 and Globus archive/restore scripts:

- Inventory building (walking a directory, hashing files, recording symlinks).
- Tar creation/extraction for small-file groups.
- Checksum verification of restored files.
- Summary CSV writing.
- Inventory JSON writing.
- Size-based partitioning/grouping of files for archiving.

Nothing in this module imports boto3 or globus_sdk; backend-specific upload/
download logic stays in the individual archive_to_*.py / restore_from_*.py
scripts.
"""

import os
import json
import uuid
import hashlib
import tarfile
import tempfile
import stat
import pwd
import csv
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# Optional tqdm for progress bars
try:
    from tqdm import tqdm
except ImportError:
    tqdm = None


# ---------- Utility ----------

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


# ---------- Inventory Building ----------

def _checksum_one(path, root_dir, verbose=False):
    """
    Compute the inventory record for a single path.

    Returns (record, path) on success, or (None, path) if the file vanished.
    Designed to be called from a thread pool.
    """
    root_dir = os.path.abspath(root_dir)
    relpath = os.path.relpath(path, root_dir)

    # Use lstat so we see the symlink itself, not the target
    try:
        st = os.lstat(path)
    except FileNotFoundError:
        return None, path, "vanished"

    if not stat.S_ISLNK(st.st_mode) and not stat.S_ISREG(st.st_mode):
        # Sockets, FIFOs, device files, etc. can't be meaningfully archived
        # (open() fails on them, e.g. OSError: No such device or address).
        return None, path, "not a regular file or symlink"

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
        # Regular file (or other non-symlink); hash contents.
        # Suppress per-file tqdm bars when running in parallel to avoid
        # interleaved output; the outer progress bar covers overall progress.
        sha = compute_sha256(path, verbose=verbose, use_tqdm=False)
        record = {
            "relative_path": relpath,
            "absolute_path": path,
            "size_bytes": st.st_size,
            "ctime": datetime.fromtimestamp(st.st_ctime).isoformat(),
            "owner": get_owner(st),
            "sha256": sha,
            "is_symlink": False,
        }

    return record, path, None


def build_inventory(root_dir, verbose=False, max_workers=1):
    """Walk directory and compute inventory, hashing files in parallel."""
    file_records = []
    file_paths = []

    file_list = []
    for dirpath, _, filenames in os.walk(root_dir):
        for name in filenames:
            file_list.append(os.path.join(dirpath, name))

    root_dir = os.path.abspath(root_dir)

    vprint(verbose, f"Computing checksums with up to {max_workers} workers "
                    f"({len(file_list)} files)...")

    pbar = None
    if verbose and tqdm:
        pbar = tqdm(total=len(file_list), desc="Inventory", unit="files")

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(_checksum_one, path, root_dir, verbose): path
            for path in file_list
        }
        for fut in as_completed(futures):
            record, path, skip_reason = fut.result()
            if record is None:
                vprint(verbose, f"WARNING: skipping {path}: {skip_reason}.")
            else:
                file_records.append(record)
                file_paths.append(path)
            if pbar:
                pbar.update(1)

    if pbar:
        pbar.close()

    # Restore original filesystem order (os.walk order) for determinism.
    walk_order = {p: i for i, p in enumerate(file_list)}
    file_records.sort(key=lambda r: walk_order.get(r["absolute_path"], 0))
    file_paths.sort(key=lambda p: walk_order.get(p, 0))

    inventory = {
        "inventory_id": str(uuid.uuid4()),
        "root_dir": root_dir,
        "created_at": datetime.utcnow().isoformat() + "Z",
        "total_files": len(file_records),
        "total_bytes": sum(r["size_bytes"] for r in file_records),
        "files": file_records,
    }

    return inventory, file_paths


def partition_by_size(files, size_cutoff):
    """Split inventory file records into (large_files, small_files) by size_cutoff."""
    large_files = [rec for rec in files if rec["size_bytes"] > size_cutoff]
    small_files = [rec for rec in files if rec["size_bytes"] <= size_cutoff]
    return large_files, small_files


def group_small_files(small_files, size_grouping):
    """Group small file records into batches of approximately size_grouping bytes."""
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

    return groups


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


# ---------- Restore-side selection / verification ----------

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


# ---------- Inventory Writing ----------

def write_inventory_file(inventory, backend_meta, objects, inventory_path, verbose=False):
    """
    Write inventory JSON file to inventory_path.

    `backend_meta` is a dict of backend-specific fields merged into the
    inventory's "archive" section alongside "archive_id" and "objects", e.g.
    {"s3_bucket": bucket} for S3 or
    {"backend": "globus", "globus_dest_collection": ..., "globus_dest_path": ...}
    for Globus.

    `objects` is a list of dicts describing each archived object, e.g.:
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
        **backend_meta,
        "archive_id": inventory["inventory_id"],
        "objects": objects,
    }

    os.makedirs(os.path.dirname(inventory_path), exist_ok=True)

    with open(inventory_path, "w") as f:
        json.dump(inv, f, indent=2, sort_keys=True)

    vprint(verbose, f"Wrote inventory file {inventory_path}")
