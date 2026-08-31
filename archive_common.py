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

import argparse
import base64
import os
import sys
import inspect
import json
import gzip
import uuid
import hashlib
import tarfile
import tempfile
import stat
import pwd
import csv
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

# Optional tqdm for progress bars
try:
    from tqdm import tqdm
except ImportError:
    tqdm = None

# Optional awscrt, required to compute CRC64NVME checksums locally. Used by
# the S3 backend to verify uploads, and by the S3 restore path to re-check a
# downloaded object against the checksum recorded in the inventory. Neither
# boto3 nor globus_sdk is imported here (see module docstring); awscrt is a
# standalone, optional checksum library.
try:
    from awscrt import checksums as crt_checksums
except ImportError:
    crt_checksums = None

# tarfile.TarFile.extract() grew a `filter` parameter in Python 3.11.4/3.12
# (PEP 706), and some distributions (notably RHEL) backported it into 3.9.
# Where it exists, the *default* filter clears permission bits ("some mode
# bits are cleared") and rejects absolute/escaping symlink targets, which
# silently degrades restore fidelity -- see extract_tar().
TARFILE_SUPPORTS_FILTER = (
    "filter" in inspect.signature(tarfile.TarFile.extract).parameters
)


# ---------- Utility ----------

def vprint(verbose, *args, **kwargs):
    """Print only if verbose."""
    if verbose:
        print(*args, **kwargs)


def print_config(verbose, label, settings):
    """
    Print a labeled, sorted "key = value" listing of the effective
    configuration for this run, when --verbose. Meant to make it obvious
    exactly which value (from a CLI flag, an env var, or the config file)
    actually ended up in effect, since several of those can come from any
    of the three and silently diverge from what's on the command line.
    """
    if not verbose:
        return
    print(f"{label}:")
    width = max((len(k) for k in settings), default=0)
    for key in sorted(settings):
        print(f"  {key:<{width}} = {settings[key]!r}")


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


def compute_crc64nvme_b64(path, verbose=False, use_tqdm=True):
    """
    Compute the CRC64NVME checksum of a local file, base64-encoded to match
    S3's ChecksumCRC64NVME response field. Requires the optional `awscrt`
    package; callers must check crt_checksums is not None first.
    """
    filesize = os.path.getsize(path)
    crc = 0

    show_bar = verbose and tqdm and use_tqdm
    pbar = tqdm(total=filesize, unit="B", unit_scale=True,
                desc=f"crc64nvme {os.path.basename(path)}") if show_bar else None

    chunk_size = 8 * 1024 * 1024
    with open(path, "rb") as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            crc = crt_checksums.crc64nvme(chunk, crc) & 0xFFFFFFFFFFFFFFFF
            if pbar:
                pbar.update(len(chunk))

    if pbar:
        pbar.close()

    digest = crc.to_bytes(8, byteorder="big")
    return base64.b64encode(digest).decode("ascii")


def verify_object_checksum(local_path, obj, verbose=False):
    """
    Re-check a just-fetched archive object against whatever whole-object
    checksum the inventory recorded for it at archive time, before its
    contents are trusted (extracted, or accepted as a restored file).

    This is a cheaper and more precise check than --verify-checksums: it
    catches a corrupted download/copy at the object level, where the error
    message can name the object, instead of surfacing later as a pile of
    mismatched files -- or, for a damaged tar, as an extraction failure
    with no indication of why.

    Not every object carries one: S3 objects have "crc64nvme" only when the
    endpoint supported whole-object checksums, local tar objects have
    "sha256" only when --verify-checksum was used, and Globus objects have
    neither (Globus verifies checksums in transit itself). Returns the name
    of the algorithm actually checked, or None when the object carries no
    usable checksum. Raises RuntimeError on mismatch.
    """
    expected_sha = obj.get("sha256")
    if expected_sha:
        actual = compute_sha256(local_path, verbose=verbose, use_tqdm=False)
        if actual != expected_sha:
            raise RuntimeError(
                f"Checksum verification FAILED for archived object "
                f"{obj.get('id', '?')} at {local_path}: expected "
                f"sha256={expected_sha}, got {actual}."
            )
        vprint(verbose, f"Verified sha256 for object {obj.get('id', '?')}.")
        return "sha256"

    expected_crc = obj.get("crc64nvme")
    if expected_crc and crt_checksums is not None:
        actual = compute_crc64nvme_b64(local_path, verbose=verbose, use_tqdm=False)
        if actual != expected_crc:
            raise RuntimeError(
                f"Checksum verification FAILED for archived object "
                f"{obj.get('id', '?')} at {local_path}: expected "
                f"crc64nvme={expected_crc}, got {actual}."
            )
        vprint(verbose, f"Verified crc64nvme for object {obj.get('id', '?')}.")
        return "crc64nvme"

    return None


def version_string():
    """
    Human-readable identification of *which* archiveTree is running.

    Deliberately reports more than a version number, because the number alone
    does not distinguish installs. A `uv tool install` pinned to a released
    tag and an editable checkout of a working tree mid-development both report
    whatever is in pyproject.toml -- so a stale install and a current one can
    print the same version while behaving differently, which is exactly the
    confusion this is meant to resolve. The module directory is the part that
    actually disambiguates them.
    """
    try:
        from importlib.metadata import PackageNotFoundError, version
        try:
            ver = version("archiveTree")
        except PackageNotFoundError:
            ver = "unknown (not installed as a distribution)"
    except ImportError:  # pragma: no cover - importlib.metadata is stdlib >=3.8
        ver = "unknown"

    py = "{}.{}.{}".format(*sys.version_info[:3])
    return "\n".join([
        f"archiveTree {ver}",
        f"  modules: {os.path.dirname(os.path.abspath(__file__))}",
        f"  python:  {sys.executable} ({py})",
    ])


class VersionAction(argparse.Action):
    """
    An argparse --version that prints version_string() verbatim.

    argparse's built-in action="version" routes its text through
    HelpFormatter, which reflows it into a single wrapped paragraph -- which
    would collapse the module and interpreter lines that are the entire
    reason version_string() reports more than a number.
    """

    def __init__(self, option_strings, dest, help=None):
        super().__init__(option_strings=option_strings, dest=dest, nargs=0, help=help)

    def __call__(self, parser, namespace, values, option_string=None):
        print(version_string())
        parser.exit()


def get_owner(stat_result):
    """Get username from stat, fall back to uid."""
    try:
        return pwd.getpwuid(stat_result.st_uid).pw_name
    except Exception:
        return str(stat_result.st_uid)


def can_restore_ownership():
    """
    True if this process can chown a file to an arbitrary uid/gid -- i.e. is
    running as root.

    An unprivileged process can only ever chgrp to a group it already belongs
    to, and can never change a file's owner at all, so attempting it would
    just produce an EPERM per file. The restore path uses this to decide
    whether to try at all, rather than warning once per file.
    """
    return hasattr(os, "geteuid") and os.geteuid() == 0


def inventory_owner_count(inventory):
    """
    Number of distinct uids recorded across an inventory's files and
    directories. Used to warn when a multi-user tree is about to be restored
    by a non-root process, which would collapse every file onto the invoking
    user.
    """
    uids = set()
    for rec in inventory.get("files", []):
        uid = rec.get("uid")
        if uid is not None:
            uids.add(uid)
    for rec in inventory.get("directories", []):
        uid = rec.get("uid")
        if uid is not None:
            uids.add(uid)
    return len(uids)


# ---------- Inventory Building ----------

def _checksum_one(path, root_dir, verbose=False):
    """
    Compute the inventory record for a single path.

    Returns (record, path, None) on success, or (None, path, skip_reason) if
    the file vanished, isn't a regular file/symlink, or couldn't be read.
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
            "uid": st.st_uid,
            "gid": st.st_gid,
            "sha256": sha,
            "is_symlink": True,
            "symlink_target": link_target,
            "mode": stat.S_IMODE(st.st_mode),
        }
    else:
        # Regular file (or other non-symlink); hash contents.
        # Suppress per-file tqdm bars when running in parallel to avoid
        # interleaved output; the outer progress bar covers overall progress.
        try:
            sha = compute_sha256(path, verbose=verbose, use_tqdm=False)
        except OSError as e:
            return None, path, f"unreadable: {e}"
        record = {
            "relative_path": relpath,
            "absolute_path": path,
            "size_bytes": st.st_size,
            "ctime": datetime.fromtimestamp(st.st_ctime).isoformat(),
            "owner": get_owner(st),
            "uid": st.st_uid,
            "gid": st.st_gid,
            "sha256": sha,
            "is_symlink": False,
            "mode": stat.S_IMODE(st.st_mode),
        }

    return record, path, None


def build_inventory(root_dir, verbose=False, max_workers=1):
    """Walk directory and compute inventory, hashing files in parallel."""
    root_dir = os.path.abspath(root_dir)

    file_records = []
    file_paths = []
    unreadable_files = []

    file_list = []
    dir_relpaths = []
    # Per-directory metadata captured during the walk: mode plus the numeric
    # uid/gid the restore path reapplies when running as root. All three come
    # from one lstat() and are all None together if it fails, mirroring how a
    # per-file record's fields are treated as optional by every reader.
    dir_meta = {}
    scan_pbar = None
    if verbose and tqdm:
        scan_pbar = tqdm(desc="Scanning", unit="files")
    for dirpath, dirnames, filenames in os.walk(root_dir):
        relpath = os.path.relpath(dirpath, root_dir)
        if relpath == ".":
            relpath = ""
        dir_relpaths.append(relpath)
        try:
            dst = os.lstat(dirpath)
            dir_meta[relpath] = {
                "mode": stat.S_IMODE(dst.st_mode),
                "uid": dst.st_uid,
                "gid": dst.st_gid,
            }
        except OSError as e:
            vprint(verbose, f"WARNING: failed to stat directory {dirpath}: {e}")
            dir_meta[relpath] = {"mode": None, "uid": None, "gid": None}
        if scan_pbar is not None:
            scan_pbar.set_postfix_str(dirpath, refresh=False)
        for name in filenames:
            file_list.append(os.path.join(dirpath, name))
            if scan_pbar is not None:
                scan_pbar.update(1)
        # os.walk() classifies a symlink pointing at a directory as a
        # "directory" (its is_dir() follows the link) and puts it in
        # dirnames, not filenames -- even though followlinks=False (the
        # default here) means it's never descended into. Without this, such
        # a symlink is silently dropped: not walked, and never picked up as
        # a file either. Archive it the same way as a symlink-to-a-file: as
        # a leaf entry via file_list/_checksum_one(), not its target's
        # contents.
        for name in dirnames:
            full_path = os.path.join(dirpath, name)
            if os.path.islink(full_path):
                file_list.append(full_path)
                if scan_pbar is not None:
                    scan_pbar.update(1)
    if scan_pbar is not None:
        scan_pbar.close()

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
                if skip_reason and skip_reason.startswith("unreadable"):
                    relpath = os.path.relpath(path, root_dir)
                    unreadable_files.append({"relative_path": relpath, "reason": skip_reason})
                    print(
                        f"WARNING: could not read {path} ({skip_reason}); "
                        "skipped, not included in this archive.",
                        file=sys.stderr,
                    )
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

    # Roll up file_count/total_bytes for every directory (including empty
    # ones), recursively covering everything in its subtree. "" denotes the
    # root directory itself.
    def _new_dir_stats(relpath):
        # Seeded from the walk's lstat() where we have one; a directory that
        # only ever shows up as some file's ancestor (not in dir_relpaths)
        # gets all-None metadata, same as a failed stat.
        meta = dir_meta.get(relpath) or {}
        return {
            "file_count": 0,
            "total_bytes": 0,
            "mode": meta.get("mode"),
            "uid": meta.get("uid"),
            "gid": meta.get("gid"),
        }

    dir_stats = {relpath: _new_dir_stats(relpath) for relpath in dir_relpaths}
    for rec in file_records:
        parent = os.path.dirname(rec["relative_path"])
        while True:
            stats = dir_stats.setdefault(parent, _new_dir_stats(parent))
            stats["file_count"] += 1
            stats["total_bytes"] += rec["size_bytes"]
            if parent == "":
                break
            parent = os.path.dirname(parent)

    directories = [
        {
            "relative_path": relpath,
            "file_count": stats["file_count"],
            "total_bytes": stats["total_bytes"],
            "mode": stats["mode"],
            "uid": stats["uid"],
            "gid": stats["gid"],
        }
        for relpath, stats in sorted(dir_stats.items())
    ]

    inventory = {
        "inventory_id": str(uuid.uuid4()),
        "root_dir": root_dir,
        "created_at": datetime.now(timezone.utc).replace(tzinfo=None).isoformat() + "Z",
        "total_files": len(file_records),
        "total_bytes": sum(r["size_bytes"] for r in file_records),
        "files": file_records,
        "directories": directories,
        "unreadable_files": unreadable_files,
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
               verbose=False, group_index=None, archive_id=None):
    """
    Tar up a subset of files with optional progress bar.

    archive_id (the inventory's UUID) is included in the local scratch
    filename to keep concurrent archive runs from colliding: the name is
    otherwise derived only from the source directory's basename and the
    group index, so two runs over the same tree -- or over two different
    trees with the same basename -- sharing a scratch directory (the system
    temp dir, by default) would write to the same path and clobber each
    other's tars mid-upload. It affects only this local filename, never the
    object key/path recorded in the inventory, which callers build
    separately.
    """
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
    if archive_id:
        name_part = f"{archive_id}_{name_part}"
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


def _extract_member(tar, member, restore_root):
    """
    tarfile.TarFile.extract() creates missing parent directories via a
    check-then-create (os.path.exists() then os.makedirs(), with no
    exist_ok) that isn't atomic. Restore scripts call extract_tar()
    concurrently from multiple worker threads, one per tar-group object; if
    two of those tar files share an as-yet-uncreated parent directory (e.g.
    both contain files under "b/"), both threads can pass the exists check
    and race on makedirs(), and the loser raises FileExistsError even though
    nothing is actually wrong. (Directory-type members don't have this
    problem -- tarfile's makedir() already catches FileExistsError itself.)
    A single retry is always safe here: by the time the exception is
    raised, the directory has already been created by the winning thread,
    so the retried extract() sees it exists and proceeds normally.

    Extraction is pinned to the "fully_trusted" filter wherever the running
    interpreter supports one (see TARFILE_SUPPORTS_FILTER). These tars are
    produced by this tool from a tree it just walked, not fetched from an
    untrusted third party, and the default filter actively damages a
    restore: it clears permission bits (0664 comes back as 0644, setgid is
    dropped entirely) and rejects symlinks whose targets are absolute or
    point outside the tree -- both of which archiveTree deliberately records
    and is expected to reproduce. Pinning it also makes behavior
    independent of the interpreter's patch level, rather than silently
    changing when a distro backports PEP 706 or the default flips in
    Python 3.14.
    """
    kwargs = {"filter": "fully_trusted"} if TARFILE_SUPPORTS_FILTER else {}
    try:
        tar.extract(member, path=restore_root, **kwargs)
    except FileExistsError:
        tar.extract(member, path=restore_root, **kwargs)


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

            _extract_member(tar, member, restore_root)

    vprint(verbose, "Extraction complete.")


# ---------- Restore-side selection / verification ----------

def detect_backend(inventory):
    """Return "globus", "local", or "s3" (default), based on
    inventory["archive"]["backend"]."""
    archive = inventory.get("archive") or {}
    backend = archive.get("backend")
    return backend if backend in ("globus", "local") else "s3"


def restore_script_for_backend(backend):
    """Return the restore_from_*.py script name for a detect_backend() value."""
    return {"globus": "restore_from_globus.py", "local": "restore_from_local.py"}.get(
        backend, "restore_from_s3.py"
    )


def path_matches_prefix(relpath, prefix):
    """
    True if relpath is `prefix` itself or lies beneath it, matching on path
    boundaries rather than raw string prefixes.

    A plain str.startswith() would make --only-prefix 'logs' also select a
    sibling directory named 'logs2' (and 'logs.bak', 'logsomething', ...),
    silently restoring far more than asked for. Trailing slashes on the
    given prefix are ignored, so 'logs' and 'logs/' behave identically --
    browse_inventory.py emits the latter.
    """
    pref = prefix.rstrip("/")
    if not pref:
        return True
    return relpath == pref or relpath.startswith(pref + "/")


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
                matched = [rp for rp in all_relpaths if path_matches_prefix(rp, pref)]
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


def restore_permissions_and_ownership(inventory, restore_root, subset_relpaths=None,
                                      only_prefixes=None, only_paths=None,
                                      restore_ownership=True, verbose=False):
    """
    Reapply recorded metadata after all file content has been written: files
    first, then directories deepest-first, so a restrictive directory mode
    never blocks a chmod still to come inside it.

    This is the single chokepoint all three backends call. restore_ownership
    is what the user asked for (it defaults on, and --no-restore-ownership
    turns it off); whether ownership is *actually* restored additionally
    requires running as root, which is resolved here rather than in each
    backend.

    Returns the number of paths whose ownership could not be applied.
    """
    apply_ownership = restore_ownership and can_restore_ownership()

    if restore_ownership and not apply_ownership and inventory_owner_count(inventory) > 1:
        # Only worth saying when the tree actually spans several owners: a
        # single-owner tree restored by its owner is already correct, but a
        # multi-user tree silently collapsing onto the invoking user is the
        # failure mode an administrator most needs to be told about.
        print(
            "WARNING: this inventory records files owned by multiple users, but "
            "this restore is not running as root -- every restored file will be "
            "owned by the invoking user. Re-run under sudo to restore original "
            "ownership.",
            file=sys.stderr,
        )

    failures = restore_file_permissions(
        inventory, restore_root, subset_relpaths=subset_relpaths,
        restore_ownership=apply_ownership, verbose=verbose,
    )
    failures += restore_directory_permissions(
        inventory, restore_root, only_prefixes=only_prefixes,
        only_paths=only_paths, restore_ownership=apply_ownership, verbose=verbose,
    )

    if failures:
        # Unconditional, not --verbose-gated: a restore that came back
        # partially mis-owned is something an administrator has to know about,
        # and the per-path warnings above can easily scroll away.
        print(
            f"WARNING: failed to restore ownership on {failures} path(s); "
            "those paths are owned by the invoking user instead.",
            file=sys.stderr,
        )

    return failures


def restore_file_permissions(inventory, restore_root, subset_relpaths=None,
                             restore_ownership=False, verbose=False):
    """
    Apply each restored file's recorded POSIX permission bits, from its
    inventory record's "mode" field.

    Files restored as their own object are already chmod'ed by the backend
    that downloaded them, but files that travelled inside a tar group are
    not: they get whatever tarfile's extraction produced. That used to be
    the original mode by happy accident, but is no longer dependable --
    where the interpreter supports extraction filters, the default one
    clears permission bits, so 0664 comes back as 0644 and setgid is
    dropped. extract_tar() pins the trusted filter to avoid that, and this
    pass makes the outcome correct regardless of interpreter behavior, by
    applying the mode the inventory actually recorded.

    Applied before restore_directory_permissions(), so a directory whose
    recorded mode lacks write/execute doesn't block chmod'ing the files
    inside it. Symlinks are skipped for chmod: os.chmod() would follow the
    link and change the *target's* mode, and a symlink's own bits aren't
    meaningful on Linux anyway. Best-effort -- a failure warns rather than
    aborting a restore whose file contents are already correct and verified.

    With restore_ownership=True (see can_restore_ownership(); only meaningful
    as root), each record's recorded numeric uid/gid is applied too. Two
    things matter here:

    - **chown comes before chmod, always.** On Linux chown() clears the
      setuid/setgid bits, and since 2.2.13 it does so even for root. Doing it
      the other way round would silently strip the setgid bit that this
      codebase goes out of its way to preserve everywhere else (see the
      TARFILE_SUPPORTS_FILTER comment and extract_tar()).
    - **Symlinks use os.lchown(), not os.chown().** Unlike mode, a symlink's
      ownership *is* meaningful, and chown() would retarget the link and
      change the ownership of whatever it points at -- possibly something
      outside the restored tree entirely.

    A record with no uid/gid (any inventory written before ownership was
    recorded) is simply left alone, exactly like a null mode.
    """
    records_by_rel = {rec["relative_path"]: rec for rec in inventory.get("files", [])}
    if subset_relpaths is None:
        target_relpaths = records_by_rel.keys()
    else:
        target_relpaths = [rp for rp in subset_relpaths if rp in records_by_rel]

    applied = 0
    chowned = 0
    chown_failures = 0
    for relpath in target_relpaths:
        rec = records_by_rel[relpath]
        mode = rec.get("mode")
        uid = rec.get("uid")
        gid = rec.get("gid")
        is_symlink = rec.get("is_symlink", False)
        full_path = os.path.join(restore_root, relpath)

        if is_symlink:
            # Nothing to chmod, but ownership still applies -- via lchown, so
            # the link itself is retargeted rather than its destination.
            if not restore_ownership or uid is None or gid is None:
                continue
            if not os.path.islink(full_path):
                continue
            try:
                os.lchown(full_path, uid, gid)
                chowned += 1
            except OSError as e:
                chown_failures += 1
                print(f"WARNING: failed to restore ownership on {full_path}: {e}",
                      file=sys.stderr)
            continue

        if not os.path.isfile(full_path) or os.path.islink(full_path):
            continue

        # Ownership first: chown() clears setuid/setgid, so the chmod below
        # has to be what lands last.
        if restore_ownership and uid is not None and gid is not None:
            try:
                os.chown(full_path, uid, gid)
                chowned += 1
            except OSError as e:
                chown_failures += 1
                print(f"WARNING: failed to restore ownership on {full_path}: {e}",
                      file=sys.stderr)

        if mode is None:
            continue
        try:
            os.chmod(full_path, mode)
            applied += 1
        except OSError as e:
            print(f"WARNING: failed to restore permissions on {full_path}: {e}", file=sys.stderr)

    vprint(verbose, f"Restored permission bits on {applied} file(s).")
    if restore_ownership:
        vprint(verbose, f"Restored ownership on {chowned} file(s)/symlink(s).")
    return chown_failures


def restore_directory_permissions(inventory, restore_root, only_prefixes=None,
                                  only_paths=None, restore_ownership=False,
                                  verbose=False):
    """
    Create any recorded directory that's empty (tar extraction only creates
    the parent dirs its member files need, so nothing else ever creates an
    originally-empty directory), then best-effort restore each directory's
    recorded POSIX permission bits.

    only_prefixes/only_paths should be the same --only-prefix/--only-path
    values (if any) the restore itself was filtered by. With neither given
    (a full restore), every recorded directory is in scope. With
    --only-prefix, only directories at or under one of those prefixes are
    created -- an empty directory outside the subset is correctly left
    uncreated, same as any file outside it. With only --only-path (which
    names individual files, not trees), no empty directory is in scope at
    all: creating them would materialize the entire archived skeleton
    around a single restored file. Both filters must therefore be passed
    in; knowing only about --only-prefix makes a --only-path-only restore
    indistinguishable from an unfiltered one.

    Permission restoration itself still only touches directories that
    actually exist under restore_root by this point -- either just created
    here, or already populated by file extraction. Applied deepest-first,
    after all file content has been written, so setting a restrictive mode
    (e.g. missing write/execute bits) on a parent never blocks creating or
    chmod'ing something still to come inside it.

    With restore_ownership=True (only meaningful as root), each directory's
    recorded numeric uid/gid is applied immediately before its chmod, for the
    same reason as in restore_file_permissions(): chown() clears setuid/setgid
    on Linux even for root, so the chmod has to land last. Directories created
    by the empty-directory pass above are covered automatically, since that
    pass runs first within this same function.
    """
    directories = inventory.get("directories", [])
    filtered = bool(only_prefixes or only_paths)

    def in_scope(relpath):
        if not filtered:
            return True
        if not only_prefixes:
            # --only-path only: individual files, no directory trees.
            return False
        stripped = relpath.rstrip("/")
        for pref in only_prefixes:
            pref_stripped = pref.rstrip("/")
            # In scope if the directory is at/under a selected prefix, or is
            # an ancestor of one (its own recorded mode still applies to the
            # path leading down to the subset).
            if (path_matches_prefix(stripped, pref_stripped)
                    or pref_stripped.startswith(stripped + "/")):
                return True
        return False

    for rec in directories:
        relpath = rec["relative_path"]
        if not relpath or not in_scope(relpath):
            continue
        full_path = os.path.join(restore_root, relpath)
        if os.path.isdir(full_path):
            continue
        try:
            os.makedirs(full_path, exist_ok=True)
        except OSError as e:
            print(f"WARNING: failed to create directory {full_path}: {e}", file=sys.stderr)

    # Deepest first: more path separators means deeper. The empty string
    # (restore_root itself) sorts last, as depth 0.
    ordered = sorted(
        directories,
        key=lambda rec: rec["relative_path"].count(os.sep),
        reverse=True,
    )

    chowned = 0
    chown_failures = 0
    for rec in ordered:
        mode = rec.get("mode")
        uid = rec.get("uid")
        gid = rec.get("gid")
        full_path = os.path.join(restore_root, rec["relative_path"])
        if not os.path.isdir(full_path):
            continue

        # Ownership first: chown() clears setuid/setgid, so chmod lands last.
        if restore_ownership and uid is not None and gid is not None:
            try:
                os.chown(full_path, uid, gid)
                chowned += 1
            except OSError as e:
                chown_failures += 1
                print(f"WARNING: failed to restore ownership on {full_path}: {e}",
                      file=sys.stderr)

        if mode is None:
            continue
        try:
            os.chmod(full_path, mode)
        except OSError as e:
            print(f"WARNING: failed to restore permissions on {full_path}: {e}", file=sys.stderr)

    vprint(verbose, "Restored directory permissions where recorded.")
    if restore_ownership:
        vprint(verbose, f"Restored ownership on {chowned} director(ies).")
    return chown_failures


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


# ---------- Inventory Versioning ----------

# Schema version stamped into every newly written inventory file's
# top-level "format_version" key. Bump this (and add the new value to
# SUPPORTED_INVENTORY_VERSIONS) any time the top-level inventory JSON
# schema changes in a way readers need to know about.
CURRENT_INVENTORY_VERSION = 1

# Versions this codebase's readers accept.
SUPPORTED_INVENTORY_VERSIONS = (1,)


def check_inventory_version(inventory):
    """
    Return an error string if inventory["format_version"] (default 1 if
    absent) is not supported, else None.
    """
    version = inventory.get("format_version", 1)
    if version not in SUPPORTED_INVENTORY_VERSIONS:
        return (
            f"Inventory file has format_version={version!r}, which this "
            f"version of archiveTree does not support "
            f"(supported: {SUPPORTED_INVENTORY_VERSIONS}). "
            "You may need a newer version of archiveTree to read this file."
        )
    return None


# ---------- Inventory Writing ----------

def write_inventory_file(inventory, backend_meta, objects, inventory_path, verbose=False, gzip_output=True):
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

    Written gzip-compressed by default (inventory files can get large on
    trees with millions of entries); load_inventory_file() transparently
    reads either form. `inventory_path` should end in ".json.gz" when
    gzip_output is True, for discoverability, but this function doesn't
    enforce that.
    """
    inv = dict(inventory)
    inv["format_version"] = CURRENT_INVENTORY_VERSION
    inv["archive"] = {
        **backend_meta,
        "archive_id": inventory["inventory_id"],
        "objects": objects,
    }

    inventory_dir = os.path.dirname(inventory_path)
    if inventory_dir:
        os.makedirs(inventory_dir, exist_ok=True)

    opener = gzip.open if gzip_output else open
    with opener(inventory_path, "wt", encoding="utf-8") as f:
        json.dump(inv, f, indent=2, sort_keys=True)

    vprint(verbose, f"Wrote inventory file {inventory_path}")


def load_inventory_file(inventory_path):
    """
    Load an inventory JSON file, transparently handling gzip-compressed
    files (detected via magic bytes, not the ".gz" suffix, so this works
    regardless of how the file was named) alongside older plain-text
    inventories written before gzip output was added.
    """
    with open(inventory_path, "rb") as f:
        magic = f.read(2)
    opener = gzip.open if magic == b"\x1f\x8b" else open
    with opener(inventory_path, "rt", encoding="utf-8") as f:
        return json.load(f)
