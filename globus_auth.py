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


def interactive_login(client_id, scopes=TransferScopes.all, cache_path=DEFAULT_TOKEN_CACHE):
    """
    Run the Native App browser login flow, persist the resulting refresh
    token, and return the token dict for the transfer.api.globus.org
    resource server.
    """
    auth_client = globus_sdk.NativeAppAuthClient(client_id)
    auth_client.oauth2_start_flow(requested_scopes=scopes, refresh_tokens=True)
    authorize_url = auth_client.oauth2_get_authorize_url()

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


def get_transfer_client(client_id, cache_path=DEFAULT_TOKEN_CACHE, verbose=False):
    """
    Return a globus_sdk.TransferClient authorized via a cached refresh
    token, logging in interactively if no valid cache exists.
    """
    auth_client = globus_sdk.NativeAppAuthClient(client_id)
    tokens = load_tokens(cache_path)

    if tokens is None:
        if verbose:
            print("No cached Globus login found; starting interactive login...")
        tokens = interactive_login(client_id, cache_path=cache_path)

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

    authorizer = globus_sdk.RefreshTokenAuthorizer(
        tokens["refresh_token"],
        auth_client,
        access_token=tokens["access_token"],
        expires_at=tokens["expires_at_seconds"],
        on_refresh=on_refresh,
    )
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
