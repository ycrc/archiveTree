#!/usr/bin/env python3
"""
Unified restore entrypoint: picks a backend (s3, globus, or local) via
--backend, by auto-detecting it from the inventory file's own
archive.backend field, or (only if neither of those resolves one) the
[archive] section of a config file, then dispatches straight into that
backend's existing restore_from_s3.py / restore_from_globus.py /
restore_from_local.py logic.

The inventory file's own recorded backend outranks the config file's
default on purpose: unlike archive.py (where there's no inventory yet and
a config default is exactly what's needed), a restore always names an
inventory that already, unambiguously, says which backend archived it --
a leftover default from some other archive run should never override that
and send the restore into the wrong backend's script.

Thin dispatcher, not a merged implementation -- see archive.py's docstring
for why. Once the backend is known, every other flag (including
--config-file) is handled exactly as if that backend's script had been
invoked directly.
"""

import os
import sys

import archive_common
import archive_config
import restore_from_globus
import restore_from_local
import restore_from_s3

from archive import extract_dispatch_flags


def _value_taking_flags():
    """
    Union, across all three restore_from_*.py backends' argument parsers,
    of every optional flag that consumes a value -- i.e. everything except
    store_true/store_false/help flags (nargs == 0). Derived from the
    parsers themselves (rather than a hand-maintained list) so it can't
    drift out of sync as backend-specific flags are added or removed.
    """
    flags = set()
    for parser in (
        restore_from_s3.build_arg_parser(),
        restore_from_globus.build_arg_parser(),
        restore_from_local.build_arg_parser(),
    ):
        for action in parser._actions:
            if action.option_strings and action.nargs != 0:
                flags.update(action.option_strings)
    return flags


def _guess_inventory_file(remaining):
    """
    Best-effort: the first token that isn't itself a flag, and isn't the
    value of a preceding flag, is almost always the inventory_file
    positional -- whether it's the very first token ("restore.py
    inventory.json.gz [flags...]") or flags precede it ("restore.py
    --restore-dir X --verbose inventory.json.gz"). Used only to
    auto-detect the backend when --backend isn't given; the backend's own
    parser does the real, authoritative argument parsing afterward. If this
    guess is wrong, auto-detection simply fails to find a readable
    inventory file and falls through to the config file's default backend
    (or a "can't determine backend" error if there isn't one either) --
    it never causes a *silent* wrong-backend dispatch, since each backend
    script's own guard still catches an inventory it didn't produce.

    Every flag known to any of the three backends that consumes a value
    (not just --config-file) is skipped along with its value -- we don't
    yet know which backend's flags are actually in play here, so all of
    them have to be treated as potentially present.
    """
    value_flags = _value_taking_flags()
    skip_next = False
    for tok in remaining:
        if skip_next:
            skip_next = False
            continue
        flag = tok.split("=", 1)[0]
        if flag in value_flags:
            if "=" not in tok:
                skip_next = True
            continue
        if not tok.startswith("-"):
            return tok
    return None


def main(argv=None):
    argv = sys.argv[1:] if argv is None else list(argv)

    cli_backend, cli_config_file, remaining = extract_dispatch_flags(argv)

    config_path = os.path.expanduser(
        cli_config_file or os.environ.get("GLOBUS_ARCHIVE_CONFIG") or archive_config.DEFAULT_CONFIG_FILE
    )
    archive_section = archive_config.load_config_file(config_path, section="archive")

    # Highest priority: an explicit --backend flag.
    backend = None
    if cli_backend:
        backend, err = archive_config.resolve_backend(cli_backend, archive_section)
        if err:
            print(err, file=sys.stderr)
            sys.exit(1)

    # Next: auto-detect from the inventory file's own archive.backend field
    # -- see the module docstring for why this outranks the config default.
    if backend is None:
        inv_candidate = _guess_inventory_file(remaining)
        if inv_candidate and os.path.isfile(inv_candidate):
            try:
                inventory = archive_common.load_inventory_file(inv_candidate)
                backend = archive_common.detect_backend(inventory)
            except Exception:
                backend = None

    # Last resort: the config file's default backend (e.g. no inventory
    # file could be found/read yet, such as with -h/--help).
    if backend is None:
        backend, err = archive_config.resolve_backend(None, archive_section)
        if err:
            print(err, file=sys.stderr)
            sys.exit(1)

    if backend is None:
        if "-h" in remaining or "--help" in remaining:
            print(
                "usage: restore.py [--backend {s3,globus,local}] [--config-file PATH] inventory_file ...\n\n"
                "No backend selected yet -- pass --backend s3|globus|local, set 'backend' "
                f"in the [archive] section of {config_path}, or point this at a "
                "readable inventory file to auto-detect it, then re-run with "
                "--help to see that backend's full flag list."
            )
            sys.exit(0)
        print(
            "ERROR: could not determine backend. Pass --backend {s3,globus,local}, set "
            f"'backend' in the [archive] section of {config_path}, or point this "
            "at a readable inventory file to auto-detect it.",
            file=sys.stderr,
        )
        sys.exit(1)

    backend_mod = {"s3": restore_from_s3, "globus": restore_from_globus, "local": restore_from_local}[backend]
    backend_parser = backend_mod.build_arg_parser()
    backend_parser.set_defaults(config_file=config_path)

    backend_mod.run(backend_parser.parse_args(remaining))


if __name__ == "__main__":
    main()
