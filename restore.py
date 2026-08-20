#!/usr/bin/env python3
"""
Unified restore entrypoint: picks a backend (s3, globus, or local) via
--backend, the [archive] section of a config file, or by auto-detecting it
from the inventory file's own archive.backend field, then dispatches
straight into that backend's existing restore_from_s3.py /
restore_from_globus.py / restore_from_local.py logic.

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


def _guess_inventory_file(remaining):
    """
    Best-effort: the first token not starting with '-' is almost always the
    inventory_file positional (the common case is just "restore.py
    inventory.json.gz [flags...]"). Used only to auto-detect the backend
    when neither --backend nor config specifies one; the backend's own
    parser does the real, authoritative argument parsing afterward. If this
    guess is wrong, auto-detection simply fails to find a readable inventory
    file and falls through to a clear "can't determine backend" error --
    it never causes a wrong or silent restore.

    --config-file's own value is explicitly skipped (extract_dispatch_flags
    leaves "--config-file VALUE" in `remaining` so it still reaches the
    backend parser), since otherwise it would always be mistaken for the
    inventory file whenever --config-file precedes it on the command line.
    """
    skip_next = False
    for tok in remaining:
        if skip_next:
            skip_next = False
            continue
        if tok == "--config-file":
            skip_next = True
            continue
        if tok.startswith("--config-file="):
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
    backend, err = archive_config.resolve_backend(cli_backend, archive_section)

    if err:
        print(err, file=sys.stderr)
        sys.exit(1)

    if backend is None:
        inv_candidate = _guess_inventory_file(remaining)
        if inv_candidate and os.path.isfile(inv_candidate):
            try:
                inventory = archive_common.load_inventory_file(inv_candidate)
                backend = archive_common.detect_backend(inventory)
            except Exception:
                backend = None

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
