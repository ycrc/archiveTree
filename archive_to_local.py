#!/usr/bin/env python3
"""
Archive a directory tree into multiple objects under a locally-mounted
destination directory (e.g. an NFS or Lustre mount point), and remove the
tree.

`dest_dir` must already be reachable as an ordinary directory on this
machine -- this tool does not mount anything itself. There is no network
transport involved, so there is no equivalent of S3's CRC64NVME whole-object
checksum workaround: that machinery exists purely because S3 can't return a
genuine whole-object checksum for a multipart upload except via the CRC
family. A plain filesystem copy has no such limitation, so when
--verify-checksum is passed, verification simply recomputes SHA256 (the same
hash already in the inventory) against the destination file.

Behavior:

- Files with size > SIZE_CUTOFF are copied as individual objects.
- Smaller files are packed into tar archives whose total size is approximately
  SIZE_GROUPING bytes; each tar is copied as an object.
- All objects for a single archive share the same archive_id (UUID) and
  destination prefix: {dest_dir}/{root_name}/{archive_id}/...
- A single inventory JSON is written alongside the removed tree, listing
  for each file:
    * which object it resides in (object_id, object_key, object_type)
    * checksum and other metadata.

Checksum verification:

- By default, each copied object is verified by comparing source and
  destination file sizes only.
- Pass --verify-checksum to additionally recompute SHA256 of the destination
  and compare it against the checksum already computed for the source file
  during inventory building (for tar-group objects, against a SHA256 of the
  freshly-created local tar, computed just before the copy).

Parallel:

- Large-file copies and tar-group copies are done in parallel using a
  ThreadPoolExecutor controlled by --max-workers.
"""

import argparse
import os
import sys
import shutil
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed

import archive_config
from archive_common import (
    vprint,
    print_config,
    compute_sha256,
    build_inventory,
    create_tar,
    partition_by_size,
    group_small_files,
    write_inventory_file,
)


def copy_file_to_local(src_path, dest_path, verbose=False, label="Copy",
                        expected_sha256=None, strict_checksum=False):
    """
    Copy src_path to dest_path (creating parent dirs as needed) via
    shutil.copy2, then verify.

    If strict_checksum is True, dest_path's SHA256 is recomputed and
    compared to expected_sha256; otherwise only sizes are compared.

    Returns size_bytes (int) of the copied file.
    """
    if strict_checksum and expected_sha256 is None:
        raise ValueError("strict_checksum=True requires expected_sha256")

    os.makedirs(os.path.dirname(dest_path), exist_ok=True)

    vprint(verbose, f"Copying {src_path} -> {dest_path} ({label}) ...")
    shutil.copy2(src_path, dest_path)

    local_size = os.path.getsize(src_path)
    dest_size = os.path.getsize(dest_path)

    if local_size != dest_size:
        raise RuntimeError(
            f"Verification failed for {dest_path}: "
            f"source {local_size} != destination {dest_size}"
        )

    if strict_checksum:
        actual_sha256 = compute_sha256(dest_path, verbose=verbose, use_tqdm=False)
        if actual_sha256 != expected_sha256:
            raise RuntimeError(
                f"Checksum verification FAILED for {dest_path}: "
                f"expected sha256={expected_sha256}, got {actual_sha256}."
            )
        vprint(verbose, f"Verified SHA256 checksum for {dest_path}.")

    vprint(verbose, f"Verified copy of {local_size} bytes to {dest_path}.")
    return dest_size


def probe_write_access(dest_dir, verbose=False):
    """
    Verify dest_dir is actually writable by creating and removing a tiny
    throwaway file, rather than trusting os.access()'s permission-bit
    check alone -- which can be misleading (e.g. running as root bypasses
    it entirely) or miss real-world failure modes a plain stat can't see,
    like a read-only NFS export, a full filesystem/quota, or restrictive
    ACLs. Raises OSError on failure.
    """
    probe_path = os.path.join(dest_dir, f".archiveTree-probe-{uuid.uuid4().hex}")
    vprint(verbose, f"Probing {probe_path} for write access...")
    with open(probe_path, "wb") as f:
        f.write(b"archiveTree write probe")
    os.remove(probe_path)


# ---------- Main ----------

def build_arg_parser():
    parser = argparse.ArgumentParser(
        description="Archive a directory tree to a locally-mounted destination directory."
    )
    parser.add_argument("directory")
    parser.add_argument(
        "dest_dir", nargs="?", default=None,
        help="Locally-mounted destination directory under which archive "
             "objects will be stored. Optional if 'dest_dir' is set in the "
             "[local] section of the config file.",
    )
    parser.add_argument("--scratch-dir", default=None)
    parser.add_argument("--compression", choices=["none", "gz"], default="none")
    parser.add_argument(
        "--delete", action="store_true",
        help="Delete the source directory tree after a successful archive. Default: keep it.",
    )
    parser.add_argument(
        "--config-file", default=None,
        help=(
            "Path to config file with a [local] section supplying a default "
            f"dest_dir (default: {archive_config.DEFAULT_CONFIG_FILE})."
        ),
    )
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument(
        "--verify-checksum", action="store_true",
        help=(
            "After each copy, recompute SHA256 of the destination file and "
            "compare it to the checksum computed for the source file during "
            "inventory building (for tar-group objects, compare against a "
            "SHA256 of the freshly-created local tar instead). Off by "
            "default: size-only verification, like archiveTree's S3 backend "
            "falls back to when whole-object checksums aren't available."
        ),
    )
    parser.add_argument(
        "--size-cutoff",
        type=int,
        default=1_000_000_000,
        help=(
            "Files larger than this in bytes are stored as individual objects. "
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
        help="Maximum number of parallel copy workers (default: 4).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be copied, but do not create tars, copy, or delete anything.",
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
    local_config = archive_config.load_config_file(config_path, section="local")

    dest_dir = archive_config.require(
        archive_config.resolve(args.dest_dir, None, local_config, "dest_dir"),
        "local destination directory", "dest_dir (positional)", None, "dest_dir",
    )

    root_dir = os.path.abspath(args.directory)

    if not os.path.isdir(root_dir):
        print(f"ERROR: {root_dir} is not a directory", file=sys.stderr)
        sys.exit(1)

    # Resolve and verify the inventory output directory early, before doing
    # any real work, so we don't fail after a long copy.
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

    dest_dir = os.path.abspath(dest_dir)

    print_config(verbose, "Configuration", {
        "directory": root_dir,
        "backend": "local",
        "dest_dir": dest_dir,
        "config_file": config_path,
        "scratch_dir": args.scratch_dir,
        "compression": args.compression,
        "delete": args.delete,
        "verify_checksum": args.verify_checksum,
        "size_cutoff": args.size_cutoff,
        "size_grouping": args.size_grouping,
        "max_workers": args.max_workers,
        "dry_run": args.dry_run,
        "inventory_dir": inv_dir,
        "summary": args.summary,
    })

    # Verify the destination is reachable and writable now, before building
    # the inventory or any tars, so a bad/unmounted dest_dir fails fast
    # instead of after a long run. Skipped on --dry-run, which never touches
    # dest_dir.
    if not args.dry_run:
        if not os.path.isdir(dest_dir):
            print(
                f"ERROR: destination directory {dest_dir!r} does not exist or "
                "is not a directory. Make sure it is mounted and reachable.",
                file=sys.stderr,
            )
            sys.exit(1)
        try:
            probe_write_access(dest_dir, verbose=verbose)
        except OSError as e:
            print(
                f"ERROR: destination directory {dest_dir!r} is not writable: {e}",
                file=sys.stderr,
            )
            sys.exit(1)

    strict_checksum = args.verify_checksum

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

    # Base destination prefix: {dest_dir}/{root_name}/{archive_id}
    base_name = os.path.basename(root_dir.rstrip(os.sep))
    archive_id = inventory["inventory_id"]
    base_prefix = os.path.join(dest_dir, base_name, archive_id)

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
            dest_path = os.path.join(base_prefix, "files", rel)

            size_remote = copy_file_to_local(
                abs_path,
                dest_path,
                verbose=verbose,
                label=f"Copy {rel}",
                expected_sha256=rec["sha256"] if strict_checksum else None,
                strict_checksum=strict_checksum,
            )

            obj_meta = {
                "id": obj_id,
                "type": "file",
                "local_path": os.path.relpath(dest_path, dest_dir),
                "size_bytes": size_remote,
                "relative_path": rel,
            }

            rec["object_id"] = obj_id
            rec["object_type"] = "file"
            rec["object_key"] = dest_path

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
            dest_path = os.path.join(base_prefix, "groups", tar_name)

            tar_sha256 = None
            if strict_checksum:
                vprint(verbose, f"[worker] Hashing tar for group {g_idx} (SHA256)...")
                tar_sha256 = compute_sha256(tar_path, verbose=verbose, use_tqdm=False)

            size_remote = copy_file_to_local(
                tar_path,
                dest_path,
                verbose=verbose,
                label=f"Copy group {g_idx}",
                expected_sha256=tar_sha256,
                strict_checksum=strict_checksum,
            )

            obj_meta = {
                "id": obj_id,
                "type": "tar",
                "local_path": os.path.relpath(dest_path, dest_dir),
                "size_bytes": size_remote,
                "file_count": len(group),
                "group_index": g_idx,
            }
            if tar_sha256 is not None:
                obj_meta["sha256"] = tar_sha256

            for rec in group:
                rec["object_id"] = obj_id
                rec["object_type"] = "tar"
                rec["object_key"] = dest_path

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
        print("\nNo copies performed.")
        return

    # Run jobs in parallel
    max_workers = max(1, args.max_workers)
    vprint(verbose, f"Starting copy with up to {max_workers} workers...")
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(process_job, job) for job in jobs]
        for fut in as_completed(futures):
            obj_meta = fut.result()
            objects.append(obj_meta)

    # Inventory file path -- use --inventory-dir if given, else parent of root_dir
    invname = f"{base_name}.inventory.{inventory['inventory_id']}.json.gz"
    invpath = os.path.join(inv_dir, invname)

    # Always write the inventory file locally, once copies have succeeded
    write_inventory_file(
        inventory, {"backend": "local", "local_dest_dir": dest_dir}, objects, invpath, verbose=verbose
    )

    # Also copy the inventory file into dest_dir, under the same archive
    # prefix, in an "inventory" subdir: {base_prefix}/inventory/{invname}
    inv_dest_path = os.path.join(base_prefix, "inventory", invname)

    vprint(verbose, f"Copying inventory to {inv_dest_path} ...")
    inv_sha256 = compute_sha256(invpath, verbose=verbose, use_tqdm=False) if strict_checksum else None
    copy_file_to_local(
        invpath,
        inv_dest_path,
        verbose=verbose,
        label="Copy inventory",
        expected_sha256=inv_sha256,
        strict_checksum=strict_checksum,
    )

    # Now (optionally) delete the original directory tree
    if not args.delete:
        print("Source directory left intact (pass --delete to remove it). "
              "Inventory is available locally and at the destination.")
        print(f"Inventory file: {invpath}")
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
    print(f"Inventory file: {invpath}")


def main():
    run(build_arg_parser().parse_args())


if __name__ == "__main__":
    main()
