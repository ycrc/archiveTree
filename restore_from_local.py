#!/usr/bin/env python3
"""
Restore a directory tree from an inventory file and its archived objects
under a locally-mounted destination directory.

Supports inventories produced by archive_to_local.py, where:

- Some files may be stored as individual objects ("type": "file").
- Other files are stored inside tar archives ("type": "tar") containing
  groups of files.

Unlike S3/Globus, objects here are ordinary files already reachable on this
machine's filesystem -- there is no "cold storage" concept, so this script
has no preflight-availability/restore-request step. Tar-type objects are
extracted directly from their location under local_dest_dir rather than
first copied into --scratch-dir: extract_tar() accepts any path, and
local_dest_dir is already local, so an intermediate copy would just be
wasted I/O -- unlike S3/Globus, where the object doesn't exist as a local
file until downloaded.

Parallel:

- Copies and extraction are done in parallel per-object using a
  ThreadPoolExecutor controlled by --max-workers.
- Optional checksum verification can also use multiple workers.
"""

import argparse
import os
import sys
import shutil
from concurrent.futures import ThreadPoolExecutor, as_completed

import archive_config
from archive_common import (
    vprint,
    select_relpaths,
    extract_tar,
    verify_restored_files,
    restore_directory_permissions,
    write_summary_csv,
    check_inventory_version,
    load_inventory_file,
)


# ---------- Core Operations ----------

def copy_file_from_local(src_path, dest_path, expected_size=None, verbose=False, mode=None):
    """Copy a single file object from local_dest_dir to dest_path (restore target)."""
    vprint(verbose, f"Copying {src_path} -> {dest_path}")

    if not os.path.isfile(src_path):
        raise RuntimeError(f"Archived object not found: {src_path}")

    if dest_path:
        os.makedirs(os.path.dirname(dest_path), exist_ok=True)

    shutil.copy2(src_path, dest_path)

    if expected_size is not None:
        actual_size = os.path.getsize(dest_path)
        if actual_size != expected_size:
            raise RuntimeError(
                f"Copied file size mismatch for {src_path}: "
                f"expected {expected_size}, got {actual_size}"
            )

    if mode is not None:
        try:
            os.chmod(dest_path, mode)
        except OSError as e:
            print(f"WARNING: failed to restore permissions on {dest_path}: {e}", file=sys.stderr)

    vprint(verbose, f"File copy complete: {dest_path}")


# ---------- Main ----------

def build_arg_parser():
    parser = argparse.ArgumentParser(
        description="Restore a directory tree from an inventory file and a local archive."
    )
    parser.add_argument(
        "inventory_file",
        help="Path to the inventory JSON file produced by the archive script."
    )
    parser.add_argument(
        "--config-file", default=None,
        help=(
            "Path to config file (currently unused by this backend, kept for "
            f"consistency with other backends; default: {archive_config.DEFAULT_CONFIG_FILE})."
        ),
    )
    parser.add_argument(
        "--scratch-dir",
        default=None,
        help=(
            "Unused by this backend: tar objects are extracted directly from "
            "local_dest_dir instead of being copied to a scratch location first. "
            "Kept for CLI consistency with the S3/Globus backends."
        ),
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
        "--dry-run",
        action="store_true",
        help="Show what would be done, but do not copy/extract/verify or write files."
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=4,
        help="Maximum number of parallel copy/verify workers (default: 4).",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Print progress messages."
    )

    return parser


def run(args):
    verbose = args.verbose

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

    if archive.get("backend") != "local":
        print(
            f"ERROR: This inventory was not produced by archive_to_local.py "
            f"(archive.backend={archive.get('backend')!r}). Use restore_from_s3.py "
            "or restore_from_globus.py for that backend's inventories.",
            file=sys.stderr,
        )
        sys.exit(1)

    local_dest_dir = archive.get("local_dest_dir")
    objects = archive.get("objects")

    if not local_dest_dir or objects is None:
        print("ERROR: 'archive' section is missing 'local_dest_dir' or 'objects'.", file=sys.stderr)
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
        print("DRY RUN: no data will be copied or written.")
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
            local_path = obj.get("local_path", "?")
            otype = obj.get("type", "?")
            obj_size = obj.get("size_bytes", "?")
            subset_bytes = sum(
                records_by_rel[r]["size_bytes"] for r in rels if r in records_by_rel
            )
            print(f"    object_id={oid} type={otype} local_path={local_path}")
            print(f"      object_size={obj_size} subset_files={len(rels)} subset_bytes={subset_bytes}")

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

    # Build jobs for parallel restore
    jobs = []
    for oid, rels in object_to_relpaths.items():
        obj = objects_by_id.get(oid)
        if obj is None:
            vprint(verbose, f"WARNING: object_id={oid} not found in archive.objects; skipping.")
            continue
        jobs.append((oid, rels, obj))

    def restore_object_job(oid, rels, obj):
        """Worker: restore one object (file or tar)."""
        otype = obj.get("type")
        local_path = obj.get("local_path")
        obj_size = obj.get("size_bytes")

        if not local_path or not otype:
            vprint(verbose, f"WARNING: object_id={oid} missing type or local_path; skipping.")
            return

        src_path = os.path.join(local_dest_dir, local_path)

        if otype == "file":
            for rel in rels:
                dest = os.path.join(restore_root, rel)
                vprint(verbose, f"[worker] Restoring single-file object_id={oid} to {dest}")
                rec = records_by_rel.get(rel)
                mode = rec.get("mode") if rec else None
                copy_file_from_local(
                    src_path,
                    dest,
                    expected_size=obj_size,
                    verbose=verbose,
                    mode=mode,
                )

        elif otype == "tar":
            vprint(verbose, f"[worker] Restoring tar object_id={oid} ({src_path})")
            if not os.path.isfile(src_path):
                raise RuntimeError(f"Archived object not found: {src_path}")
            if obj_size is not None:
                actual_size = os.path.getsize(src_path)
                if actual_size != obj_size:
                    raise RuntimeError(
                        f"Tar size mismatch for {src_path}: "
                        f"expected {obj_size}, got {actual_size}"
                    )
            extract_tar(src_path, restore_root, selected_relpaths=rels, verbose=verbose)

        else:
            vprint(verbose, f"WARNING: Unknown object type '{otype}' for object_id={oid}; skipping.")

    # Parallel restore
    max_workers = max(1, args.max_workers)
    vprint(verbose, f"Starting restore with up to {max_workers} workers...")
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [
            executor.submit(restore_object_job, oid, rels, obj)
            for (oid, rels, obj) in jobs
        ]
        for fut in as_completed(futures):
            fut.result()

    # Restore directory permissions now that all file content has been
    # written (deepest-first, so a restrictive parent mode never blocks
    # writes still to come inside it).
    restore_directory_permissions(inventory, restore_root, verbose=verbose)

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

    print("Restore completed successfully.")


def main():
    run(build_arg_parser().parse_args())


if __name__ == "__main__":
    main()
