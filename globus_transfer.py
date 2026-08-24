#!/usr/bin/env python3
"""
Globus Transfer task construction/submission/polling shared by
archive_to_globus.py and restore_from_globus.py.
"""

import inspect
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


def batches_by_count_and_bytes(items, max_count, max_bytes, item_bytes):
    """
    Yield successive batches of items, starting a new batch whenever adding
    the next item would make the current batch exceed max_count items or
    max_bytes cumulative size (as reported by item_bytes(item), which may
    return None/0 for items that should count toward max_count but not
    toward max_bytes -- e.g. large files that transfer directly and never
    touch scratch space).

    A batch is only closed once it is non-empty, so a single item that
    alone exceeds max_bytes still gets its own (oversized) batch rather
    than never being yielded.
    """
    batch = []
    batch_bytes = 0
    for item in items:
        b = item_bytes(item) or 0
        would_exceed = len(batch) >= max_count or (batch_bytes + b) > max_bytes
        if batch and would_exceed:
            yield batch
            batch = []
            batch_bytes = 0
        batch.append(item)
        batch_bytes += b
    if batch:
        yield batch


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


# globus-sdk v4 removed TransferData's long-deprecated `transfer_client`
# first parameter, so its signature became (source_endpoint,
# destination_endpoint, *, ...). Passing a client positionally alongside
# source_endpoint= therefore failed outright with "got multiple values for
# argument 'source_endpoint'". Both versions are in play for this project --
# the pixi environment resolves globus-sdk 4.x, while the EasyBuild module
# provides 3.44 via Globus-CLI -- so detect it rather than pinning either
# way. On v3 the client is still passed (it fetches a submission_id up front
# for submit-retry idempotency, exactly as before); on v4 it's omitted, and
# submit_transfer() takes care of the submission_id itself.
_TRANSFERDATA_ACCEPTS_CLIENT = (
    "transfer_client" in inspect.signature(globus_sdk.TransferData.__init__).parameters
)


def new_transfer(transfer_client, source_collection, destination_collection, label,
                  verify_checksum=True, sync_level="checksum"):
    """Build a new (empty) TransferData task, ready for add_item() calls."""
    extra = {"transfer_client": transfer_client} if _TRANSFERDATA_ACCEPTS_CLIENT else {}
    return globus_sdk.TransferData(
        source_endpoint=source_collection,
        destination_endpoint=destination_collection,
        label=label,
        verify_checksum=verify_checksum,
        sync_level=sync_level,
        **extra,
    )


def _relogin_for_transfer_error(err, client_id=None,
                                 token_cache=globus_auth.DEFAULT_TOKEN_CACHE,
                                 login_domain=None, verbose=False):
    """
    Given a TransferAPIError, either recover by re-running the interactive
    login and return a fresh TransferClient built from the updated token
    cache, or re-raise if the error isn't one of the two kinds this can
    recover from:

    - ConsentRequired: re-runs the interactive login with the additionally
      required scopes.
    - session_required_single_domain (a collection restricting access to
      identities from a specific institutional domain, e.g. a cached login
      that predates --login-domain being set): re-runs the interactive
      login requesting a session for that domain -- login_domain, if
      given, picks which required domain to request when the server lists
      more than one; otherwise the first one listed is used.
    """
    required_domains = (
        err.info.authorization_parameters.session_required_single_domain
        if err.info.authorization_parameters else None
    )
    if err.info.consent_required:
        if not client_id:
            raise RuntimeError(
                "Globus requires additional consent for this operation, but no "
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
    elif required_domains:
        if not client_id:
            raise RuntimeError(
                "This collection requires a login session from one of "
                f"{required_domains}, but the cached login doesn't have one, "
                "and no client_id was provided to retry the login."
            ) from err
        domain = login_domain or required_domains[0]
        print(
            f"Encountered a domain-restricted login error: this collection "
            f"requires a session from one of {required_domains}. Logging in "
            f"again, requesting an identity from {domain!r} (pass "
            "--login-domain to choose a different one if needed)...\n"
        )
        globus_auth.interactive_login(
            client_id, cache_path=token_cache, login_domain=domain,
        )
    else:
        raise err

    return globus_auth.get_transfer_client(
        client_id, cache_path=token_cache, verbose=verbose
    )


def call_with_auth_retry(func, transfer_client, client_id=None,
                          token_cache=globus_auth.DEFAULT_TOKEN_CACHE,
                          login_domain=None, verbose=False):
    """
    Call func(transfer_client) and return (result, transfer_client) -- the
    possibly-refreshed client, so callers can keep reusing it afterward.

    On a ConsentRequired or session_required_single_domain
    TransferAPIError, recovers the same way submit_and_wait() does (see
    _relogin_for_transfer_error()) and retries func once with a fresh
    client. Used for preflight collection-access checks (e.g.
    operation_ls), which can hit the exact same recoverable errors a
    transfer submission can, and should self-heal from them the same way.
    """
    try:
        return func(transfer_client), transfer_client
    except globus_sdk.TransferAPIError as err:
        transfer_client = _relogin_for_transfer_error(
            err, client_id=client_id, token_cache=token_cache,
            login_domain=login_domain, verbose=verbose,
        )
        return func(transfer_client), transfer_client


def submit_and_wait(transfer_client, transfer_data, client_id=None,
                     token_cache=globus_auth.DEFAULT_TOKEN_CACHE,
                     verbose=False, poll_interval=15,
                     total_bytes=None, desc=None, login_domain=None):
    """
    Submit a TransferData task and block until it completes.

    Retries submission once via call_with_auth_retry() on a recoverable
    auth error -- see that function's docstring. The (possibly refreshed)
    client it returns is also used for the polling below.

    If verbose, tqdm is installed, and total_bytes is given, shows a
    tqdm progress bar driven by the task's cumulative bytes_transferred
    instead of printing a status line per poll.

    Returns the final task document (dict-like GlobusHTTPResponse).
    Raises RuntimeError if the task does not succeed.
    """
    submit_result, transfer_client = call_with_auth_retry(
        lambda tc: tc.submit_transfer(transfer_data),
        transfer_client, client_id=client_id, token_cache=token_cache,
        login_domain=login_domain, verbose=verbose,
    )

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
