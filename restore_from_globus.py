#!/usr/bin/env python3
"""
Restore a directory tree from an inventory file and its archived objects on
a Globus collection.

Mirrors restore_from_s3.py's behavior, minus all Glacier/cold-storage
preflight logic (not applicable: the Globus destination is a plain
collection, not tiered storage).

Supports inventories produced by archive_to_globus.py, where:

- Some files are stored as individual objects ("type": "file").
- Other files are stored inside tar archives ("type": "tar") containing
  groups of files.

Unlike the S3 script, all needed objects are pulled in one (or a few, if
the object count exceeds --max-items-per-task) Globus Transfer task(s)
rather than per-object downloads, since Globus Transfer tasks are async
and server-managed. Progress is reported at the task level.

Because Globus Transfer operates on collection-relative paths, the restore
target (--restore-dir or the inventory's original root_dir) and
--scratch-dir (where downloaded tars land before extraction) must both be
reachable via the destination collection, i.e. located under --dest-mount.
"""

import argparse
import os
import sys
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed

import globus_auth
import globus_config
import globus_transfer

from archive_common import (
    vprint,
    print_config,
    select_relpaths,
    extract_tar,
    verify_restored_files,
    restore_directory_permissions,
    write_summary_csv,
    check_inventory_version,
    load_inventory_file,
    detect_backend,
    restore_script_for_backend,
)


def build_arg_parser():
    parser = argparse.ArgumentParser(
        description="Restore a directory tree from an inventory file and a Globus archive."
    )
    parser.add_argument(
        "inventory_file", nargs="?", default=None,
        help="Path to the inventory JSON file produced by archive_to_globus.py "
             "(not required when using --globus-logout).",
    )
    parser.add_argument(
        "--archive-collection", default=None,
        help="Override for the Globus collection holding archived objects; "
             "defaults to inventory['archive']['globus_dest_collection'].",
    )
    parser.add_argument(
        "--dest-collection", default=None,
        help="Globus collection UUID mapped to this machine's filesystem (restore target).",
    )
    parser.add_argument(
        "--dest-mount", default=None,
        help="Local absolute path corresponding to --dest-collection's root.",
    )
    parser.add_argument("--client-id", default=None, help="Globus Native App client ID.")
    parser.add_argument(
        "--token-cache", default=None,
        help=f"Path to cached Globus tokens (default: {globus_auth.DEFAULT_TOKEN_CACHE}).",
    )
    parser.add_argument(
        "--login-domain", default=None,
        help="Require the interactive Globus login to use an identity from this "
             "domain (e.g. yale.edu), via session_required_single_domain. Only "
             "takes effect on a fresh login -- run --globus-logout first if a "
             "token cache already exists. Needed for collections that restrict "
             "sessions to a specific institutional domain.",
    )
    parser.add_argument(
        "--config-file", default=None,
        help=f"Path to config file (default: {globus_config.DEFAULT_CONFIG_FILE}).",
    )
    parser.add_argument(
        "--globus-logout", action="store_true",
        help="Revoke and delete cached Globus tokens, then exit.",
    )
    parser.add_argument(
        "--scratch-dir", default=None,
        help="Directory for downloaded tars; must be under --dest-mount (default: a subdir of restore-dir).",
    )
    parser.add_argument(
        "--restore-dir", default=None,
        help="Override the original root_dir from the inventory. "
             "If not set, the original path in inventory['root_dir'] is used.",
    )
    parser.add_argument(
        "--overwrite", action="store_true",
        help="Allow restoring into an existing, non-empty directory.",
    )
    parser.add_argument(
        "--only-path", action="append", default=None,
        help="Restore only file(s) whose relative path equals this value (may repeat).",
    )
    parser.add_argument(
        "--only-prefix", action="append", default=None,
        help="Restore only files whose relative path starts with this prefix (may repeat).",
    )
    parser.add_argument(
        "--verify-checksums", action="store_true",
        help="After extraction, recompute SHA256 checksums and compare to inventory.",
    )
    parser.add_argument(
        "--summary-csv", default=None,
        help="Write a CSV summary of restored files to this path.",
    )
    parser.add_argument(
        "--keep-tar", action="store_true",
        help="Do not delete the downloaded tar(s) after restore.",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Show what would be done, but do not transfer/extract/verify or write files.",
    )
    parser.add_argument(
        "--max-workers", type=int, default=4,
        help="Maximum number of parallel local extraction/verify workers (default: 4).",
    )
    parser.add_argument(
        "--poll-interval", type=int, default=15,
        help="Seconds between task-status polls when --verbose (default: 15).",
    )
    parser.add_argument(
        "--max-items-per-task", type=int, default=10000,
        help="Split into sequential batched transfer tasks if more items than this (default: 10000).",
    )
    parser.add_argument(
        "--max-batch-bytes", type=int, default=100_000_000_000,
        help="Split into sequential batched transfer tasks if the tar objects "
             "to download (individually-transferred files riding along in a "
             "batch don't count) would exceed this many bytes; bounds peak "
             "local scratch-disk usage to roughly one batch's worth of "
             "downloaded tars. Default: 1e11 (100GB).",
    )
    parser.add_argument(
        "--verbose", action="store_true",
        help="Print progress messages and show progress bars (if tqdm is installed).",
    )

    return parser


def run(args):
    verbose = args.verbose

    config_file = os.path.expanduser(
        args.config_file or os.environ.get("GLOBUS_ARCHIVE_CONFIG") or globus_config.DEFAULT_CONFIG_FILE
    )
    config = globus_config.load_config_file(config_file)

    client_id = globus_config.resolve(args.client_id, "GLOBUS_ARCHIVE_CLIENT_ID", config, "client_id")
    token_cache = globus_config.resolve(
        args.token_cache, "GLOBUS_ARCHIVE_TOKEN_CACHE", config, "token_cache"
    ) or globus_auth.DEFAULT_TOKEN_CACHE
    login_domain = globus_config.resolve(
        args.login_domain, "GLOBUS_ARCHIVE_LOGIN_DOMAIN", config, "login_domain"
    )

    if args.globus_logout:
        globus_auth.logout(client_id=client_id, cache_path=token_cache)
        print(f"Logged out; removed cached tokens at {token_cache}.")
        return

    if not args.inventory_file:
        print("ERROR: 'inventory_file' is required (unless using --globus-logout).", file=sys.stderr)
        sys.exit(1)

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

    if archive.get("backend") != "globus":
        actual_backend = detect_backend(inventory)
        print(
            "ERROR: This inventory was not produced by archive_to_globus.py "
            f"(archive.backend={archive.get('backend')!r}). Use "
            f"{restore_script_for_backend(actual_backend)} instead.",
            file=sys.stderr,
        )
        sys.exit(1)

    objects = archive.get("objects")
    if objects is None:
        print("ERROR: 'archive' section is missing 'objects'.", file=sys.stderr)
        sys.exit(1)

    archive_collection = globus_config.resolve(
        args.archive_collection, "GLOBUS_ARCHIVE_ARCHIVE_COLLECTION", config, "archive_collection"
    ) or archive.get("globus_dest_collection")
    archive_collection = globus_config.require(
        archive_collection, "archive collection", "--archive-collection",
        "GLOBUS_ARCHIVE_ARCHIVE_COLLECTION", "archive_collection",
    )

    # Resolved (but not required) unconditionally, including on --dry-run,
    # purely so --verbose's configuration listing below always reflects
    # what's actually configured; required below only once we're past the
    # dry-run early exit and actually about to transfer.
    dest_collection = globus_config.resolve(
        args.dest_collection, "GLOBUS_ARCHIVE_SOURCE_COLLECTION", config, "source_collection"
    )
    dest_mount = globus_config.resolve(
        args.dest_mount, "GLOBUS_ARCHIVE_SOURCE_MOUNT", config, "source_mount"
    )

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
        "backend": "globus",
        "client_id": client_id,
        "token_cache": token_cache,
        "login_domain": login_domain,
        "archive_collection": archive_collection,
        "dest_collection": dest_collection,
        "dest_mount": dest_mount,
        "config_file": config_file,
        "restore_dir": restore_root,
        "scratch_dir": args.scratch_dir,
        "overwrite": args.overwrite,
        "only_path": args.only_path,
        "only_prefix": args.only_prefix,
        "verify_checksums": args.verify_checksums,
        "summary_csv": args.summary_csv,
        "keep_tar": args.keep_tar,
        "dry_run": args.dry_run,
        "max_workers": args.max_workers,
        "poll_interval": args.poll_interval,
        "max_items_per_task": args.max_items_per_task,
        "max_batch_bytes": args.max_batch_bytes,
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
        print("DRY RUN: no data will be transferred or written.")
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
            gpath = obj.get("globus_path", "?")
            otype = obj.get("type", "?")
            obj_size = obj.get("size_bytes", "?")
            subset_bytes = sum(
                records_by_rel[r]["size_bytes"] for r in rels if r in records_by_rel
            )
            print(f"    object_id={oid} type={otype} path={gpath}")
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

    # Require full Globus config now that we're actually transferring
    # (dest_collection/dest_mount were already resolved above, for the
    # --verbose configuration listing).
    client_id = globus_config.require(
        client_id, "Globus client ID", "--client-id", "GLOBUS_ARCHIVE_CLIENT_ID", "client_id"
    )
    dest_collection = globus_config.require(
        dest_collection, "destination collection", "--dest-collection",
        "GLOBUS_ARCHIVE_SOURCE_COLLECTION", "source_collection",
    )
    dest_mount = globus_config.require(
        dest_mount, "destination mount", "--dest-mount",
        "GLOBUS_ARCHIVE_SOURCE_MOUNT", "source_mount",
    )

    globus_transfer.require_under_mount(restore_root, dest_mount, "the restore target", "--dest-mount")

    if args.scratch_dir:
        scratch_dir = os.path.abspath(args.scratch_dir)
        created_scratch_dir = False
    else:
        scratch_dir = os.path.join(restore_root, f".restore_globus_scratch_{uuid.uuid4().hex}")
        created_scratch_dir = True

    # Build jobs for restore: one per needed object
    jobs = []
    for oid, rels in object_to_relpaths.items():
        obj = objects_by_id.get(oid)
        if obj is None:
            vprint(verbose, f"WARNING: object_id={oid} not found in archive.objects; skipping.")
            continue
        jobs.append((oid, rels, obj))

    if not jobs:
        vprint(verbose, "No objects required for this restore (nothing to do).")
    else:
        globus_transfer.require_under_mount(scratch_dir, dest_mount, "scratch directory", "--dest-mount")
        os.makedirs(scratch_dir, exist_ok=True)

        # Assign each object a local landing path, and remember it for
        # extraction (tar) or as the final destination (file).
        local_paths = {}
        for oid, rels, obj in jobs:
            otype = obj.get("type")
            if otype == "tar":
                gpath = obj.get("globus_path", "")
                suffix = ".tar.gz" if gpath.endswith(".tar.gz") else ".tar"
                local_paths[oid] = os.path.join(scratch_dir, f"restore_{oid}{suffix}")
            elif otype == "file":
                # A "file" object may cover multiple selected relpaths only
                # if the same object_id were shared, which never happens for
                # "file"-type objects (one file per object); take the single one.
                (rel,) = tuple(rels)
                local_paths[oid] = os.path.join(restore_root, rel)
            else:
                vprint(verbose, f"WARNING: Unknown object type '{otype}' for object_id={oid}; skipping.")

        transfer_client = globus_auth.get_transfer_client(
            client_id, cache_path=token_cache, verbose=verbose, login_domain=login_domain
        )

        def extract_job(oid, rels, obj):
            if obj.get("type") != "tar":
                return None
            tar_path = local_paths.get(oid)
            if not tar_path or not os.path.isfile(tar_path):
                vprint(verbose, f"WARNING: expected tar for object_id={oid} not found at {tar_path}")
                return None
            extract_tar(tar_path, restore_root, selected_relpaths=rels, verbose=verbose)
            if args.keep_tar:
                return tar_path
            vprint(verbose, f"Removing temporary tar {tar_path}")
            try:
                os.remove(tar_path)
            except OSError as e:
                print(f"WARNING: Failed to remove temporary tar {tar_path}: {e}", file=sys.stderr)
            return None

        planned_batches = list(globus_transfer.batches_by_count_and_bytes(
            jobs, args.max_items_per_task, args.max_batch_bytes,
            item_bytes=lambda item: item[2]["size_bytes"] if item[2].get("type") == "tar" else None,
        ))
        vprint(
            verbose,
            f"Transferring {len(jobs)} object(s) in {len(planned_batches)} "
            f"batch(es) from Globus collection {archive_collection}...",
        )

        temp_tars = []
        max_workers = max(1, args.max_workers)

        for batch_num, batch in enumerate(planned_batches):
            transfer_data = globus_transfer.new_transfer(
                transfer_client, archive_collection, dest_collection,
                label=f"restore {os.path.basename(restore_root)} batch{batch_num}",
                verify_checksum=True,
            )
            batch_bytes = sum(obj["size_bytes"] for oid, rels, obj in batch)
            for oid, rels, obj in batch:
                if oid not in local_paths:
                    continue
                source_path = obj["globus_path"]
                dest_local_path = local_paths[oid]
                if obj.get("type") == "file":
                    os.makedirs(os.path.dirname(dest_local_path), exist_ok=True)
                dest_path = globus_transfer.local_path_to_collection_relative(
                    dest_local_path, dest_mount, "/"
                )
                transfer_data.add_item(source_path, dest_path)

            globus_transfer.submit_and_wait(
                transfer_client, transfer_data, client_id=client_id,
                token_cache=token_cache, verbose=verbose, poll_interval=args.poll_interval,
                total_bytes=batch_bytes, desc=f"Transfer batch{batch_num}",
                login_domain=login_domain,
            )

            # Restore original permission bits for individually-transferred
            # ("file"-type) objects in this batch. Tar members get theirs
            # from tarfile extraction below.
            for oid, rels, obj in batch:
                if obj.get("type") != "file":
                    continue
                local_path = local_paths.get(oid)
                if not local_path:
                    continue
                (rel,) = tuple(rels)
                rec = records_by_rel.get(rel)
                mode = rec.get("mode") if rec else None
                if mode is None:
                    continue
                try:
                    os.chmod(local_path, mode)
                except OSError as e:
                    print(f"WARNING: failed to restore permissions on {local_path}: {e}", file=sys.stderr)

            # Extract and remove this batch's downloaded tars immediately,
            # rather than deferring until every batch has downloaded.
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = [executor.submit(extract_job, oid, rels, obj) for (oid, rels, obj) in batch]
                for fut in as_completed(futures):
                    tpath = fut.result()
                    if tpath:
                        temp_tars.append(tpath)

        if created_scratch_dir:
            try:
                os.rmdir(scratch_dir)
            except OSError:
                pass

        if args.keep_tar and temp_tars:
            vprint(verbose, "Temporary tars kept:")
            for t in temp_tars:
                vprint(verbose, f"  {t}")

    # Restore directory permissions now that all file content has been
    # written (deepest-first, so a restrictive parent mode never blocks
    # writes still to come inside it).
    restore_directory_permissions(inventory, restore_root, only_prefixes=args.only_prefix, verbose=verbose)

    # Optional checksum verification
    max_workers = max(1, args.max_workers)
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
            verbose=verbose,
        )

    print("Restore completed successfully.")


def main():
    run(build_arg_parser().parse_args())


if __name__ == "__main__":
    main()
