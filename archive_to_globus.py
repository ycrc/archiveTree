#!/usr/bin/env python3
"""
Archive a directory tree to a Globus collection and remove the tree.

Mirrors archive_to_s3.py's behavior, targeting a Globus Transfer collection
instead of an S3 bucket:

- Files with size > SIZE_CUTOFF are transferred as individual objects.
- Smaller files are packed into tar archives whose total size is approximately
  SIZE_GROUPING bytes; each tar is transferred as one object.
- All objects for a single archive share the same archive_id (UUID) and
  destination path prefix: {dest_path}/{root_name}/{archive_id}/...
- A single inventory JSON is written alongside the removed tree, listing
  for each file which object it resides in (object_id, object_key,
  object_type) plus checksum and other metadata.

Unlike the S3 script, transfers happen as one (or a few, if the file/tar
count exceeds --max-items-per-task) Globus Transfer task(s), since Globus
Transfer tasks are async and server-managed rather than a synchronous
per-object call. Progress is reported at the task level (bytes/files
transferred, polled every --poll-interval seconds), not per-object.

Because Globus Transfer operates on collection-relative paths, --scratch-dir
(where tars are built) and the inventory output directory must both be
reachable via the source collection, i.e. located under --source-mount.
"""

import argparse
import os
import sys
import shutil
from concurrent.futures import ThreadPoolExecutor, as_completed

import globus_sdk

import globus_auth
import globus_config
import globus_transfer

from archive_common import (
    vprint,
    build_inventory,
    create_tar,
    partition_by_size,
    group_small_files,
    write_inventory_file,
)


def build_arg_parser():
    parser = argparse.ArgumentParser(
        description="Archive a directory tree to a Globus collection."
    )
    parser.add_argument(
        "directory", nargs="?", default=None,
        help="Directory to archive (not required when using --globus-logout).",
    )
    parser.add_argument("--dest-collection", default=None,
                        help="Destination (archive) Globus collection UUID.")
    parser.add_argument(
        "--dest-path", default=None,
        help="Base path within the destination collection under which archive objects are stored.",
    )
    parser.add_argument(
        "--source-collection", default=None,
        help="Globus collection UUID mapped to this machine's filesystem.",
    )
    parser.add_argument(
        "--source-mount", default=None,
        help="Local absolute path corresponding to --source-collection's root.",
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
    parser.add_argument("--scratch-dir", default=None,
                        help="Directory for locally-built tars; must be under --source-mount.")
    parser.add_argument("--compression", choices=["none", "gz"], default="none")
    parser.add_argument(
        "--delete", action="store_true",
        help="Delete the source directory tree after a successful archive. Default: keep it.",
    )
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument(
        "--size-cutoff", type=int, default=1_000_000_000,
        help="Files larger than this in bytes are transferred as individual objects. Default: 1e9.",
    )
    parser.add_argument(
        "--size-grouping", type=int, default=10_000_000_000,
        help="Target total size (in bytes) for tar groups of small files. Default: 1e10.",
    )
    parser.add_argument(
        "--max-workers", type=int, default=4,
        help="Maximum number of parallel local tar-build workers (default: 4).",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Show what would be transferred, but do not create tars, transfer, or delete anything.",
    )
    parser.add_argument(
        "--inventory-dir", default=None,
        help="Directory in which to write the local inventory JSON file. Defaults to the parent of the archived directory.",
    )
    parser.add_argument(
        "--no-summary", action="store_false", dest="summary",
        help="Print a summary of files, bytes, and objects created.",
    )
    parser.add_argument(
        "--no-verify-checksum-transfer", action="store_true",
        help="Disable Globus's built-in verify_checksum transfer option (on by default).",
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
        help="Split into sequential batched transfer tasks if the built tars "
             "(large files riding along in a batch don't count) would exceed "
             "this many bytes; bounds peak local scratch-disk usage to "
             "roughly one batch's worth of tars. Default: 1e11 (100GB).",
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

    if not args.directory:
        print("ERROR: 'directory' is required (unless using --globus-logout).", file=sys.stderr)
        sys.exit(1)

    root_dir = os.path.abspath(args.directory)
    if not os.path.isdir(root_dir):
        print(f"ERROR: {root_dir} is not a directory", file=sys.stderr)
        sys.exit(1)

    # Resolve and verify the inventory output directory early, before doing
    # any real work, so we don't fail after a long transfer.
    if args.inventory_dir is not None:
        inv_dir = os.path.abspath(args.inventory_dir)
        if not os.path.isdir(inv_dir):
            print(f"ERROR: --inventory-dir {inv_dir} is not a directory", file=sys.stderr)
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

    dest_path = globus_config.resolve(args.dest_path, "GLOBUS_ARCHIVE_DEST_PATH", config, "dest_path")
    dest_path = globus_config.require(
        dest_path, "destination path", "--dest-path", "GLOBUS_ARCHIVE_DEST_PATH", "dest_path"
    )

    # Unless this is a dry run, we'll eventually transfer, so require the
    # full Globus config now -- before building the inventory or any tars --
    # so a missing value fails fast instead of after a long run.
    if not args.dry_run:
        client_id = globus_config.require(
            client_id, "Globus client ID", "--client-id", "GLOBUS_ARCHIVE_CLIENT_ID", "client_id"
        )
        source_collection = globus_config.resolve(
            args.source_collection, "GLOBUS_ARCHIVE_SOURCE_COLLECTION", config, "source_collection"
        )
        source_collection = globus_config.require(
            source_collection, "source collection", "--source-collection",
            "GLOBUS_ARCHIVE_SOURCE_COLLECTION", "source_collection",
        )
        source_mount = globus_config.resolve(
            args.source_mount, "GLOBUS_ARCHIVE_SOURCE_MOUNT", config, "source_mount"
        )
        source_mount = globus_config.require(
            source_mount, "source mount", "--source-mount",
            "GLOBUS_ARCHIVE_SOURCE_MOUNT", "source_mount",
        )
        dest_collection = globus_config.resolve(
            args.dest_collection, "GLOBUS_ARCHIVE_DEST_COLLECTION", config, "dest_collection"
        )
        dest_collection = globus_config.require(
            dest_collection, "destination collection", "--dest-collection",
            "GLOBUS_ARCHIVE_DEST_COLLECTION", "dest_collection",
        )
        globus_transfer.require_under_mount(root_dir, source_mount, "the archived directory", "--source-mount")

        transfer_client = globus_auth.get_transfer_client(
            client_id, cache_path=token_cache, verbose=verbose, login_domain=login_domain
        )

        # Verify both collections are actually reachable now -- login works,
        # the collection permits it, and (for the source) --source-mount is
        # a real, listable path on that collection -- before building the
        # inventory or any tars, so a bad collection/mount/permission fails
        # fast instead of after a long, wasted run.
        vprint(verbose, "Checking access to source and destination collections...")
        try:
            _, transfer_client = globus_transfer.call_with_auth_retry(
                lambda tc: tc.operation_ls(source_collection, path=source_mount),
                transfer_client, client_id=client_id, token_cache=token_cache,
                login_domain=login_domain, verbose=verbose,
            )
        except globus_sdk.TransferAPIError as e:
            print(
                f"ERROR: cannot access {source_collection}:{source_mount} "
                "(check --source-collection/--source-mount, and that you're "
                f"logged in with an identity this collection allows): {e}",
                file=sys.stderr,
            )
            sys.exit(1)

        try:
            _, transfer_client = globus_transfer.call_with_auth_retry(
                lambda tc: tc.operation_ls(dest_collection, path="/"),
                transfer_client, client_id=client_id, token_cache=token_cache,
                login_domain=login_domain, verbose=verbose,
            )
        except globus_sdk.TransferAPIError as e:
            print(
                f"ERROR: cannot access destination collection {dest_collection} "
                "(check --dest-collection, and that you're logged in with an "
                f"identity this collection allows): {e}",
                file=sys.stderr,
            )
            sys.exit(1)

    vprint(verbose, "Building inventory...")
    inventory, _ = build_inventory(root_dir, verbose=verbose, max_workers=args.max_workers)

    size_cutoff = args.size_cutoff
    size_grouping = args.size_grouping
    files = inventory["files"]

    large_files, small_files = partition_by_size(files, size_cutoff)
    vprint(
        verbose,
        f"Total files: {len(files)}; large: {len(large_files)}; small: {len(small_files)}",
    )

    base_name = os.path.basename(root_dir.rstrip(os.sep))
    archive_id = inventory["inventory_id"]
    dpath = dest_path.strip("/")

    if dpath:
        base_prefix = f"{dpath}/{base_name}/{archive_id}"
    else:
        base_prefix = f"{base_name}/{archive_id}"

    groups = group_small_files(small_files, size_grouping)
    vprint(verbose, f"Created {len(groups)} small-file groups.")

    compression = None if args.compression == "none" else "gz"
    tar_suffix = ".tar.gz" if compression == "gz" else ".tar"

    # Build job list
    jobs = []
    object_counter = 0

    for rec in large_files:
        obj_id = f"file-{object_counter:06d}"
        object_counter += 1
        jobs.append({"kind": "file", "obj_id": obj_id, "rec": rec})

    for g_idx, group in enumerate(groups):
        obj_id = f"tar-{object_counter:06d}"
        object_counter += 1
        jobs.append({
            "kind": "tar", "obj_id": obj_id, "group": group, "group_index": g_idx,
            "est_bytes": sum(r["size_bytes"] for r in group),
        })

    # --- DRY RUN MODE ------------------------------------------------------
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
        print(f"  Destination path prefix: /{base_prefix}")
        print("\nNo transfers performed.")
        return

    tar_jobs = [j for j in jobs if j["kind"] == "tar"]

    if args.scratch_dir:
        scratch_dir = os.path.abspath(args.scratch_dir)
        created_scratch_dir = False
    else:
        scratch_dir = os.path.join(inv_dir, f".archive_globus_scratch_{archive_id}")
        created_scratch_dir = True

    if tar_jobs:
        globus_transfer.require_under_mount(scratch_dir, source_mount, "scratch directory", "--source-mount")
        os.makedirs(scratch_dir, exist_ok=True)

    def build_tar_job(job):
        group = job["group"]
        g_idx = job["group_index"]
        abs_paths = [r["absolute_path"] for r in group]
        vprint(verbose, f"[worker] Creating tar for group {g_idx} with {len(group)} files...")
        tar_path = create_tar(
            root_dir, abs_paths, scratch_dir=scratch_dir,
            tar_compression=compression, verbose=verbose, group_index=g_idx,
        )
        job["tar_path"] = tar_path
        job["tar_size"] = os.path.getsize(tar_path)
        return job

    def add_item_for_job(transfer_data, job):
        if job["kind"] == "file":
            rec = job["rec"]
            rel = rec["relative_path"]
            abs_path = rec["absolute_path"]
            source_path = globus_transfer.local_path_to_collection_relative(
                abs_path, source_mount, "/"
            )
            dest_full_path = "/" + f"{base_prefix}/files/{rel}".replace("//", "/")
        else:
            g_idx = job["group_index"]
            tar_name = f"group_{g_idx:06d}{tar_suffix}"
            source_path = globus_transfer.local_path_to_collection_relative(
                job["tar_path"], source_mount, "/"
            )
            dest_full_path = "/" + f"{base_prefix}/groups/{tar_name}".replace("//", "/")

        transfer_data.add_item(source_path, dest_full_path)
        job["dest_full_path"] = dest_full_path

    verify_checksum = not args.no_verify_checksum_transfer
    objects = []
    task_ids = []

    if jobs:
        # transfer_client was already obtained during the preflight
        # collection-access checks above; reused here rather than fetched
        # again.
        planned_batches = list(globus_transfer.batches_by_count_and_bytes(
            jobs, args.max_items_per_task, args.max_batch_bytes,
            item_bytes=lambda j: j.get("est_bytes"),
        ))
        vprint(
            verbose,
            f"Transferring {len(jobs)} object(s) in {len(planned_batches)} "
            f"batch(es) to Globus collection {dest_collection}...",
        )

        for batch_num, batch in enumerate(planned_batches):
            batch_tar_jobs = [j for j in batch if j["kind"] == "tar"]

            if batch_tar_jobs:
                vprint(
                    verbose,
                    f"Building {len(batch_tar_jobs)} tar(s) for batch {batch_num} "
                    f"with up to {args.max_workers} workers...",
                )
                with ThreadPoolExecutor(max_workers=max(1, args.max_workers)) as executor:
                    futures = [executor.submit(build_tar_job, j) for j in batch_tar_jobs]
                    for fut in as_completed(futures):
                        fut.result()

            transfer_data = globus_transfer.new_transfer(
                transfer_client, source_collection, dest_collection,
                label=f"archive {base_name} {archive_id} batch{batch_num}",
                verify_checksum=verify_checksum,
            )
            batch_bytes = sum(
                j["rec"]["size_bytes"] if j["kind"] == "file" else j["tar_size"]
                for j in batch
            )
            for job in batch:
                add_item_for_job(transfer_data, job)

            task = globus_transfer.submit_and_wait(
                transfer_client, transfer_data, client_id=client_id,
                token_cache=token_cache, verbose=verbose, poll_interval=args.poll_interval,
                total_bytes=batch_bytes, desc=f"Transfer batch{batch_num}",
                login_domain=login_domain,
            )
            task_ids.append(task["task_id"])

            for job in batch:
                obj_id = job["obj_id"]
                dest_full_path = job["dest_full_path"]

                if job["kind"] == "file":
                    rec = job["rec"]
                    rec["object_id"] = obj_id
                    rec["object_type"] = "file"
                    rec["object_key"] = dest_full_path
                    objects.append({
                        "id": obj_id,
                        "type": "file",
                        "globus_path": dest_full_path,
                        "size_bytes": rec["size_bytes"],
                        "relative_path": rec["relative_path"],
                    })
                else:
                    group = job["group"]
                    for rec in group:
                        rec["object_id"] = obj_id
                        rec["object_type"] = "tar"
                        rec["object_key"] = dest_full_path
                    objects.append({
                        "id": obj_id,
                        "type": "tar",
                        "globus_path": dest_full_path,
                        "size_bytes": job["tar_size"],
                        "file_count": len(group),
                        "group_index": job["group_index"],
                    })

            # Tars for this batch are removed as soon as this batch's
            # transfer succeeds, rather than deferred to the end, so a
            # mid-run failure leaves at most one batch's tars on disk.
            for job in batch_tar_jobs:
                tar_path = job.get("tar_path")
                if tar_path:
                    vprint(verbose, f"Removing temporary tar {tar_path}")
                    try:
                        os.remove(tar_path)
                    except OSError:
                        pass

        if created_scratch_dir:
            try:
                os.rmdir(scratch_dir)
            except OSError:
                pass
    else:
        vprint(verbose, "No files to transfer.")

    # Inventory file path – use --inventory-dir if given, else parent of root_dir
    invname = f"{base_name}.inventory.{inventory['inventory_id']}.json.gz"
    invpath = os.path.join(inv_dir, invname)

    write_inventory_file(
        inventory,
        {
            "backend": "globus",
            "globus_dest_collection": dest_collection,
            "globus_dest_path": dest_path,
            "transfer_task_ids": task_ids,
        },
        objects,
        invpath,
        verbose=verbose,
    )

    # Also transfer the inventory file itself to the archive collection,
    # under the same archive prefix, in an "inventory" subdir:
    #   {dest_collection}:/{base_prefix}/inventory/{invname}
    globus_transfer.require_under_mount(invpath, source_mount, "the inventory file", "--source-mount")

    inv_dest_path = "/" + f"{base_prefix}/inventory/{invname}".replace("//", "/")
    inv_source_path = globus_transfer.local_path_to_collection_relative(invpath, source_mount, "/")

    vprint(verbose, f"Transferring inventory to {dest_collection}:{inv_dest_path} ...")
    # transfer_client was already obtained during the preflight
    # collection-access checks (dry-run always returns before reaching this
    # point, so it's guaranteed to be set here).
    inv_transfer_data = globus_transfer.new_transfer(
        transfer_client, source_collection, dest_collection,
        label=f"archive {base_name} {archive_id} inventory",
        verify_checksum=verify_checksum,
    )
    inv_transfer_data.add_item(inv_source_path, inv_dest_path)
    globus_transfer.submit_and_wait(
        transfer_client, inv_transfer_data, client_id=client_id,
        token_cache=token_cache, verbose=verbose, poll_interval=args.poll_interval,
        login_domain=login_domain,
    )

    # Now (optionally) delete the original directory tree
    if not args.delete:
        print("Source directory left intact (pass --delete to remove it). "
              "Inventory is available locally and on the Globus collection.")
        print(f"Inventory file: {invpath}")
        return

    vprint(verbose, f"Removing directory tree {root_dir}")
    shutil.rmtree(root_dir)

    # --- SUMMARY -------------------------------------------------------
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
