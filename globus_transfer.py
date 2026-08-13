#!/usr/bin/env python3
"""
Globus Transfer task construction/submission/polling shared by
archive_to_globus.py and restore_from_globus.py.
"""

import os
import sys

import globus_sdk

import globus_auth

# Optional tqdm for progress bars
try:
    from tqdm import tqdm
except ImportError:
    tqdm = None


def _is_under(path, mount_prefix):
    """True if abspath `path` is mount_prefix itself or lies beneath it."""
    rel = os.path.relpath(path, mount_prefix)
    return rel == os.curdir or (not rel.startswith(os.pardir + os.sep) and rel != os.pardir)


def require_under_mount(path, mount_prefix, what, mount_flag):
    """
    Ensure `path` lies under mount_prefix, since it must be reachable via a
    Globus collection mapped to that mount; exit with a clear error
    otherwise.
    """
    path = os.path.abspath(path)
    mount_prefix = os.path.abspath(mount_prefix)
    if not _is_under(path, mount_prefix):
        print(
            f"ERROR: {what} ({path}) is not under {mount_flag} ({mount_prefix}). "
            "It must be reachable via the corresponding Globus collection.",
            file=sys.stderr,
        )
        sys.exit(1)


def batches(items, batch_size):
    """Yield successive batch_size-sized chunks of items."""
    for i in range(0, len(items), batch_size):
        yield items[i:i + batch_size]


def local_path_to_collection_relative(abs_path, local_mount_prefix, collection_base_path):
    """
    Map a local absolute path to a path relative to a Globus collection,
    given the local directory prefix that corresponds to that collection's
    root (local_mount_prefix) and a base path within the collection
    (collection_base_path).
    """
    abs_path = os.path.abspath(abs_path)
    local_mount_prefix = os.path.abspath(local_mount_prefix)

    if not _is_under(abs_path, local_mount_prefix):
        raise ValueError(
            f"Path {abs_path!r} is not under the configured local mount "
            f"prefix {local_mount_prefix!r}; check --source-mount/--dest-mount."
        )

    rel = os.path.relpath(abs_path, local_mount_prefix)
    if rel == ".":
        collection_path = collection_base_path
    else:
        collection_path = f"{collection_base_path.rstrip('/')}/{rel}"

    return "/" + collection_path.lstrip("/")


def new_transfer(transfer_client, source_collection, destination_collection, label,
                  verify_checksum=True, sync_level="checksum"):
    """Build a new (empty) TransferData task, ready for add_item() calls."""
    return globus_sdk.TransferData(
        transfer_client,
        source_endpoint=source_collection,
        destination_endpoint=destination_collection,
        label=label,
        verify_checksum=verify_checksum,
        sync_level=sync_level,
    )


def submit_and_wait(transfer_client, transfer_data, client_id=None,
                     token_cache=globus_auth.DEFAULT_TOKEN_CACHE,
                     verbose=False, poll_interval=15,
                     total_bytes=None, desc=None):
    """
    Submit a TransferData task and block until it completes.

    On a ConsentRequired error, re-runs the interactive login with the
    additionally required scopes, builds a fresh TransferClient from the
    updated token cache, and retries submission once with that client
    (which is also used for the subsequent polling below).

    If verbose, tqdm is installed, and total_bytes is given, shows a
    tqdm progress bar driven by the task's cumulative bytes_transferred
    instead of printing a status line per poll.

    Returns the final task document (dict-like GlobusHTTPResponse).
    Raises RuntimeError if the task does not succeed.
    """
    try:
        submit_result = transfer_client.submit_transfer(transfer_data)
    except globus_sdk.TransferAPIError as err:
        if not err.info.consent_required:
            raise
        if not client_id:
            raise RuntimeError(
                "Globus requires additional consent for this transfer, but no "
                "client_id was provided to retry the login."
            ) from err
        print(
            "Encountered a ConsentRequired error; you must login again to "
            "grant additional consents.\n"
        )
        globus_auth.interactive_login(
            client_id,
            scopes=err.info.consent_required.required_scopes,
            cache_path=token_cache,
        )
        transfer_client = globus_auth.get_transfer_client(
            client_id, cache_path=token_cache, verbose=verbose
        )
        submit_result = transfer_client.submit_transfer(transfer_data)

    task_id = submit_result["task_id"]
    if verbose:
        print(f"Submitted Globus transfer task_id={task_id}")

    pbar = None
    last_bytes = 0
    if verbose and tqdm and total_bytes:
        pbar = tqdm(total=total_bytes, unit="B", unit_scale=True, desc=desc or "Transfer")

    def _update_progress():
        nonlocal last_bytes
        task = transfer_client.get_task(task_id)
        if pbar:
            bytes_transferred = task.get("bytes_transferred") or 0
            pbar.update(bytes_transferred - last_bytes)
            last_bytes = bytes_transferred
            pbar.set_postfix(files=task.get("files_transferred"))
        elif verbose:
            print(
                f"  [{task_id}] status={task['status']} "
                f"bytes_transferred={task.get('bytes_transferred')} "
                f"files_transferred={task.get('files_transferred')}"
            )
        return task

    while not transfer_client.task_wait(task_id, timeout=poll_interval,
                                         polling_interval=poll_interval):
        if verbose:
            _update_progress()

    task = _update_progress() if verbose else transfer_client.get_task(task_id)

    if pbar:
        pbar.close()

    if task["status"] != "SUCCEEDED":
        raise RuntimeError(
            f"Globus transfer task {task_id} finished with status "
            f"{task['status']!r}: {task.get('fatal_error')}"
        )

    if verbose:
        print(
            f"Transfer task_id={task_id} succeeded: "
            f"{task.get('bytes_transferred')} bytes, "
            f"{task.get('files_transferred')} files."
        )

    return task
