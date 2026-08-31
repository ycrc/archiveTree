#!/usr/bin/env python3
"""
Globus authentication helpers shared by archive_to_globus.py and
restore_from_globus.py.

Uses a Native App login flow (interactive, browser-based) the first time,
then caches a refresh token locally so subsequent runs are non-interactive.
Kept separate from archive_common.py so the pure-S3 scripts never need
globus_sdk importable.
"""

import json
import os
import stat
import sys

import globus_sdk
from globus_sdk.scopes import TransferScopes

DEFAULT_TOKEN_CACHE = os.path.expanduser("~/.globus_archive_tokens.json")


def load_tokens(cache_path=DEFAULT_TOKEN_CACHE):
    """Load cached tokens from cache_path, or None if absent/unreadable."""
    if not os.path.isfile(cache_path):
        return None
    try:
        with open(cache_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except (OSError, json.JSONDecodeError):
        return None


def save_tokens(tokens, cache_path=DEFAULT_TOKEN_CACHE):
    """Persist tokens to cache_path with owner-only permissions."""
    os.makedirs(os.path.dirname(cache_path) or ".", exist_ok=True)
    with open(cache_path, "w", encoding="utf-8") as f:
        json.dump(tokens, f)
    os.chmod(cache_path, stat.S_IRUSR | stat.S_IWUSR)


def interactive_login(client_id, scopes=TransferScopes.all, cache_path=DEFAULT_TOKEN_CACHE,
                       login_domain=None):
    """
    Run the Native App browser login flow, persist the resulting refresh
    token, and return the token dict for the transfer.api.globus.org
    resource server.

    login_domain, if given, is passed as session_required_single_domain so
    the resulting Globus Auth session is tied to an identity from that
    domain (e.g. "yale.edu") rather than whatever the account's primary
    identity happens to be -- picking the right identity provider on the
    login page alone does not do this; some collections enforce it via a
    session policy that only session_required_single_domain satisfies.
    """
    auth_client = globus_sdk.NativeAppAuthClient(client_id)
    auth_client.oauth2_start_flow(requested_scopes=scopes, refresh_tokens=True)
    # Only pass session_required_single_domain when there's actually a domain
    # to require. globus-sdk v3 treated None as "not provided", but v4 uses a
    # MISSING sentinel for that and serializes an explicit None into the
    # authorize URL as the literal string "None" -- which would ask Globus
    # for an identity from a domain named "None" on every ordinary login.
    domain_kwargs = (
        {"session_required_single_domain": login_domain} if login_domain else {}
    )
    authorize_url = auth_client.oauth2_get_authorize_url(**domain_kwargs)

    print(f"Please go to this URL and login:\n\n{authorize_url}\n")
    auth_code = input("Please enter the code you get after login here: ").strip()

    token_response = auth_client.oauth2_exchange_code_for_tokens(auth_code)
    transfer_data = token_response.by_resource_server["transfer.api.globus.org"]

    tokens = {
        "refresh_token": transfer_data["refresh_token"],
        "access_token": transfer_data["access_token"],
        "expires_at_seconds": transfer_data["expires_at_seconds"],
    }
    save_tokens(tokens, cache_path=cache_path)
    return tokens


def _refuse_root_login(cache_path):
    """
    Exit rather than start an interactive login when a missing token cache is
    almost certainly an artifact of sudo rather than a genuine first login.

    `sudo archive --backend globus ...` resets HOME to root's, so cache_path
    becomes /root/.globus_archive_tokens.json -- a path that does not exist
    because the *invoking* user has never logged in as root, not because they
    have never logged in. Left alone, get_transfer_client() reads that as
    "first run" and starts a browser login. Non-interactively that is an
    EOFError on the code prompt; interactively it is worse, minting a second
    long-lived refresh token for the user's Globus identity into root's home,
    where their own --globus-logout will never find it to revoke.

    Note this keys on the situation, not on how cache_path was derived: a
    config file carrying `token_cache = ~/.globus_archive_tokens.json`
    expands to root's copy under sudo exactly like the bare default does, so
    setting it explicitly is not protection.

    The "Archiving other users' files" section of README.md describes the
    intended sudo invocation.
    """
    if os.environ.get("ARCHIVETREE_ALLOW_ROOT_LOGIN"):
        return
    sudo_user = os.environ.get("SUDO_USER")
    if not sudo_user:
        return
    if not (hasattr(os, "geteuid") and os.geteuid() == 0):
        return

    user_cache = os.path.join(
        os.path.expanduser("~" + sudo_user), ".globus_archive_tokens.json"
    )
    print(
        "\n".join([
            f"ERROR: no cached Globus login at {cache_path}.",
            "",
            f"  Running as root under sudo: HOME is "
            f"{os.path.expanduser('~')!r}, so that is root's token cache,",
            f"  not {sudo_user}'s. Continuing would start a browser login and write",
            "  a second refresh token for your Globus identity into root's home,",
            "  where `--globus-logout` will not find it to revoke.",
            "",
            "  Point at the existing cache instead:",
            "",
            f"    sudo env GLOBUS_ARCHIVE_TOKEN_CACHE={user_cache} \\",
            "      <command> ...",
            "",
            f"  or pass --token-cache {user_cache}.",
            "",
            "  If a root-owned token cache is genuinely intended (a dedicated admin",
            "  identity rather than a borrowed one), set ARCHIVETREE_ALLOW_ROOT_LOGIN=1",
            "  to permit the login.",
        ]),
        file=sys.stderr,
    )
    sys.exit(1)


def get_transfer_client(client_id, cache_path=DEFAULT_TOKEN_CACHE, verbose=False, login_domain=None):
    """
    Return a globus_sdk.TransferClient authorized via a cached refresh
    token, logging in interactively if no valid cache exists.

    login_domain is forwarded to interactive_login() for the initial login
    only; it has no effect if a token cache already exists (log out first
    with --globus-logout to force a fresh login that honors it).
    """
    auth_client = globus_sdk.NativeAppAuthClient(client_id)
    tokens = load_tokens(cache_path)

    if tokens is None:
        # Refuse an accidental root login before offering one; see the helper.
        _refuse_root_login(cache_path)
        if verbose:
            print("No cached Globus login found; starting interactive login...")
        tokens = interactive_login(client_id, cache_path=cache_path, login_domain=login_domain)

    def on_refresh(token_response):
        transfer_data = token_response.by_resource_server["transfer.api.globus.org"]
        save_tokens(
            {
                "refresh_token": transfer_data["refresh_token"],
                "access_token": transfer_data["access_token"],
                "expires_at_seconds": transfer_data["expires_at_seconds"],
            },
            cache_path=cache_path,
        )

    def build_authorizer(tok):
        return globus_sdk.RefreshTokenAuthorizer(
            tok["refresh_token"],
            auth_client,
            access_token=tok["access_token"],
            expires_at=tok["expires_at_seconds"],
            on_refresh=on_refresh,
        )

    try:
        authorizer = build_authorizer(tokens)
    except globus_sdk.AuthAPIError as e:
        # The cached refresh token is no longer usable -- revoked from the
        # Globus web console, expired after a long idle period, or issued
        # for a different client_id. Recover the same way the transfer
        # scripts recover from ConsentRequired: say what happened and log
        # in again, rather than surfacing a bare AuthAPIError traceback the
        # user has to decode into "run --globus-logout".
        print(
            f"Cached Globus login at {cache_path} is no longer valid ({e.code or e}); "
            "starting a fresh login...\n"
        )
        tokens = interactive_login(client_id, cache_path=cache_path, login_domain=login_domain)
        authorizer = build_authorizer(tokens)

    return globus_sdk.TransferClient(authorizer=authorizer)


def logout(client_id=None, cache_path=DEFAULT_TOKEN_CACHE):
    """
    Revoke the cached refresh/access tokens (if client_id is given and a
    cache exists) and delete the cache file, forcing interactive login
    on the next run.
    """
    tokens = load_tokens(cache_path)
    if tokens and client_id:
        auth_client = globus_sdk.NativeAppAuthClient(client_id)
        for key in ("refresh_token", "access_token"):
            token = tokens.get(key)
            if token:
                try:
                    auth_client.oauth2_revoke_token(token)
                except globus_sdk.GlobusAPIError:
                    pass

    if os.path.isfile(cache_path):
        os.remove(cache_path)
