#!/usr/bin/env python3
"""
Apply recorded permissions and ownership to an already-restored tree.

The ownership pass is transport-agnostic -- it reads uid/gid/mode from the
inventory and applies them to files already on disk -- so unlike the
restore_from_*.py scripts this needs no backend, no credentials, and imports
nothing beyond archive_common (and hence nothing beyond the standard library).

The case it exists for is the Globus backend under sudo. Globus Connect
Server does its file I/O as the local account your Globus identity maps to,
*not* as the user running the client, so a `sudo restore --backend globus`
has root creating the destination directories and the data mover -- someone
else entirely -- failing to write into them. Splitting the two phases fixes
that: run the restore unprivileged, so the transfer writes as the identity
doing the transferring, then run this under sudo to put ownership back.

It is equally useful for the other two backends whenever a restore ran
unprivileged and ownership needs applying afterwards, and for re-asserting a
tree's recorded metadata after something else has disturbed it.
"""

import argparse
import os
import sys

from archive_common import (
    VersionAction,
    can_restore_ownership,
    check_inventory_version,
    inventory_owner_count,
    load_inventory_file,
    restore_permissions_and_ownership,
    select_relpaths,
    vprint,
)


def build_arg_parser():
    parser = argparse.ArgumentParser(
        description=(
            "Apply the permissions and ownership recorded in an inventory to a "
            "tree that has already been restored. Requires root."
        )
    )
    parser.add_argument(
        "--version", action=VersionAction,
        help="Show version, module location, and interpreter, then exit.",
    )
    parser.add_argument(
        "inventory_file",
        help="Path to the inventory JSON file produced by the archive script.",
    )
    parser.add_argument(
        "--restore-dir", required=True,
        help="Root of the already-restored tree to apply metadata to.",
    )
    parser.add_argument(
        "--only-path", action="append", default=None,
        help=("Limit to this relative path (repeatable). Same semantics as the "
              "restore scripts' --only-path."),
    )
    parser.add_argument(
        "--only-prefix", action="append", default=None,
        help=("Limit to relative paths under this prefix (repeatable). Same "
              "semantics as the restore scripts' --only-prefix."),
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help=("Report what would change, without changing anything. Compares "
              "each path's current uid/gid/mode against the inventory."),
    )
    parser.add_argument("--verbose", action="store_true")
    return parser


def _iter_recorded(inventory, restore_root, subset_relpaths):
    """
    Yield (full_path, record) for every file and directory the run covers.

    Files are filtered by subset_relpaths (the --only-* selection).
    Directories are not, which mirrors restore_directory_permissions(): its
    prefix filter gates only the creation of missing empty directories, while
    the metadata pass applies to every recorded directory that exists on disk.
    Paths absent under restore_root are reported as such by the caller, which
    is the same "only touches what is actually there" rule.
    """
    for rec in inventory.get("files", []):
        rel = rec.get("relative_path")
        if subset_relpaths is not None and rel not in subset_relpaths:
            continue
        yield os.path.join(restore_root, rel), rec
    for rec in inventory.get("directories", []):
        rel = rec.get("relative_path")
        full = restore_root if rel in (".", "") else os.path.join(restore_root, rel)
        yield full, rec


def _dry_run(inventory, restore_root, subset_relpaths, verbose):
    """Report pending changes without applying any. Returns an exit status."""
    import stat as stat_mod

    changes = missing = 0
    for full_path, rec in _iter_recorded(inventory, restore_root, subset_relpaths):
        try:
            st = os.lstat(full_path)
        except OSError:
            missing += 1
            vprint(verbose, f"  not present: {full_path}")
            continue

        uid, gid, mode = rec.get("uid"), rec.get("gid"), rec.get("mode")
        deltas = []
        if uid is not None and gid is not None and (st.st_uid, st.st_gid) != (uid, gid):
            deltas.append(f"owner {st.st_uid}:{st.st_gid} -> {uid}:{gid}")
        # Symlink modes are meaningless on Linux and are never applied.
        if mode is not None and not rec.get("is_symlink") \
                and stat_mod.S_IMODE(st.st_mode) != mode:
            deltas.append(f"mode {oct(stat_mod.S_IMODE(st.st_mode))} -> {oct(mode)}")
        if deltas:
            changes += 1
            print(f"  {full_path}: {'; '.join(deltas)}")

    print(f"\nDry run: {changes} path(s) would change.")
    if missing:
        print(f"{missing} path(s) in the inventory are not present under "
              f"{restore_root} and would be skipped.")
    return 0


def run(args):
    verbose = args.verbose

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

    restore_root = os.path.abspath(args.restore_dir)
    if not os.path.isdir(restore_root):
        print(f"ERROR: --restore-dir is not a directory: {restore_root}", file=sys.stderr)
        sys.exit(1)

    owners = inventory_owner_count(inventory)
    if owners == 0:
        print(
            "ERROR: this inventory records no uid/gid, so there is no ownership "
            "to apply. It was written before ownership was recorded -- re-archive "
            "with a current version if you need it.",
            file=sys.stderr,
        )
        sys.exit(1)

    # Fail before touching anything rather than silently applying modes only:
    # applying ownership is this command's entire purpose.
    if not args.dry_run and not can_restore_ownership():
        print(
            "ERROR: applying ownership requires root; re-run under sudo.\n"
            "  (--dry-run works unprivileged if you only want to see what "
            "would change.)",
            file=sys.stderr,
        )
        sys.exit(1)

    selected_relpaths = select_relpaths(
        inventory,
        only_paths=args.only_path,
        only_prefixes=args.only_prefix,
        verbose=verbose,
    )

    vprint(verbose, f"Applying metadata under {restore_root}")
    vprint(verbose, f"Inventory records {owners} distinct owner(s).")

    if args.dry_run:
        sys.exit(_dry_run(inventory, restore_root, selected_relpaths, verbose))

    failures = restore_permissions_and_ownership(
        inventory, restore_root, subset_relpaths=selected_relpaths,
        only_prefixes=args.only_prefix, only_paths=args.only_path,
        restore_ownership=True, verbose=verbose,
    )
    sys.exit(1 if failures else 0)


def main():
    run(build_arg_parser().parse_args())


if __name__ == "__main__":
    main()
