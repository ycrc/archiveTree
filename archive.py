#!/usr/bin/env python3
"""
Unified archive entrypoint: picks a backend (s3, globus, or local) via
--backend or the [archive] section of a config file, then dispatches
straight into that backend's existing archive_to_s3.py / archive_to_globus.py
/ archive_to_local.py logic.

This is a thin dispatcher, not a merged implementation -- S3, Globus, and
local have fundamentally different transfer execution models (S3 uploads
per-object in parallel; Globus submits one or a few async, server-managed
transfer tasks; local copies per-object in parallel to a mounted directory),
so the actual work stays in the three backend-specific scripts. Once the
backend is known, every other flag (including --config-file, which the
backend script re-resolves against its own config section) is handled
exactly as if that backend's script had been invoked directly.
"""

import os
import sys

import archive_common
import archive_config
import archive_to_globus
import archive_to_local
import archive_to_s3


def extract_dispatch_flags(argv):
    """
    Pull --backend/--backend=X out of argv, and peek (without removing)
    --config-file/--config-file=X. Everything else -- including flags this
    module has never heard of, and their values -- is left completely
    untouched and in its original order, for the backend's own parser to
    make sense of.

    This is deliberately NOT done with argparse.parse_known_args(): that
    can't safely peek two global flags out of an argv stream that also
    contains backend-specific flags it doesn't recognize, because it has no
    way to know whether an unrecognized flag takes a value -- e.g. with
    "--storage-class GLACIER mydir", an unaware parser can misclassify
    "GLACIER" as the positional "directory" argument. Plain string matching
    on the two flags this module actually owns has no such ambiguity.

    Returns (backend_or_None, config_file_or_None, remaining_argv).
    """
    backend = None
    config_file = None
    remaining = []
    i = 0
    while i < len(argv):
        tok = argv[i]
        if tok == "--backend" and i + 1 < len(argv):
            backend = argv[i + 1]
            i += 2
            continue
        if tok.startswith("--backend="):
            backend = tok.split("=", 1)[1]
            i += 1
            continue
        if tok == "--config-file" and i + 1 < len(argv):
            config_file = argv[i + 1]
            remaining.append(tok)
            remaining.append(argv[i + 1])
            i += 2
            continue
        if tok.startswith("--config-file="):
            config_file = tok.split("=", 1)[1]
            remaining.append(tok)
            i += 1
            continue
        remaining.append(tok)
        i += 1
    return backend, config_file, remaining


def main(argv=None):
    argv = sys.argv[1:] if argv is None else list(argv)

    # Handled before anything else so it works with no backend selected, no
    # config file, and no readable inventory -- the situations where you are
    # most likely to be asking which install you just invoked.
    if "--version" in argv:
        print(archive_common.version_string())
        sys.exit(0)

    cli_backend, cli_config_file, remaining = extract_dispatch_flags(argv)

    config_path = os.path.expanduser(
        cli_config_file or os.environ.get("GLOBUS_ARCHIVE_CONFIG") or archive_config.DEFAULT_CONFIG_FILE
    )
    archive_section = archive_config.load_config_file(config_path, section="archive")
    backend, err = archive_config.resolve_backend(cli_backend, archive_section)

    if err or backend is None:
        if not err and ("-h" in remaining or "--help" in remaining):
            print(
                "usage: archive.py [--backend {s3,globus,local}] [--config-file PATH] directory ...\n\n"
                "No backend selected yet -- pass --backend s3|globus|local, or set "
                f"'backend' in the [archive] section of {config_path}, then "
                "re-run with --help to see that backend's full flag list."
            )
            sys.exit(0)
        print(
            err or (
                "ERROR: no backend specified. Pass --backend {s3,globus,local} or set "
                f"'backend' in the [archive] section of {config_path}."
            ),
            file=sys.stderr,
        )
        sys.exit(1)

    backend_mod = {"s3": archive_to_s3, "globus": archive_to_globus, "local": archive_to_local}[backend]
    backend_parser = backend_mod.build_arg_parser()
    backend_parser.set_defaults(config_file=config_path)

    backend_mod.run(backend_parser.parse_args(remaining))


if __name__ == "__main__":
    main()
