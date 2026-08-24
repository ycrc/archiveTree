#!/usr/bin/env python3
"""
list_glacier_status.py

Given an inventory JSON (produced by archive_to_s3.py), list the S3 storage
class and Glacier/Deep Archive restore status for each archived object.

For each object, prints:
  - object_id
  - type (file / tar)
  - s3_key
  - storage class
  - status:
      ready      : available for download
      cold       : in Glacier/Deep Archive, no restore in progress
      restoring  : restore in progress
      error      : head_object failed or inventory issue
  - Restore header (if present)

Usage:
  list_glacier_status.py INVENTORY.json [--profile PROFILE] [--endpoint-url URL]
"""

import argparse
import os
import sys

from archive_common import check_inventory_version, load_inventory_file, vprint

# Reuse the restore script's own classification logic rather than keeping a
# second copy in sync by hand. The duplicate that used to live here is how
# the GLACIER_IR misclassification ended up needing fixing in two places.
from restore_from_s3 import classify_object_status, get_s3_client


def main():
    parser = argparse.ArgumentParser(
        description=(
            "List S3 storage class and Glacier/Deep Archive status for objects "
            "in an archive inventory."
        )
    )
    parser.add_argument(
        "inventory_file",
        help="Path to the inventory JSON file produced by archive_to_s3.py",
    )
    parser.add_argument(
        "--profile",
        default=None,
        help="AWS profile name from ~/.aws/credentials or ~/.aws/config.",
    )
    parser.add_argument(
        "--endpoint-url",
        default=None,
        help="Custom S3 endpoint URL (for S3-compatible storage).",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Print extra diagnostic information.",
    )
    parser.add_argument(
        "--only-status",
        choices=["ready", "cold", "restoring", "error"],
        default=None,
        help="If set, only show objects with this status.",
    )

    args = parser.parse_args()
    verbose = args.verbose

    inv_path = os.path.abspath(args.inventory_file)
    if not os.path.isfile(inv_path):
        print(f"ERROR: Inventory file not found: {inv_path}", file=sys.stderr)
        sys.exit(1)

    # Load inventory
    inventory = load_inventory_file(inv_path)

    version_error = check_inventory_version(inventory)
    if version_error:
        print(f"ERROR: {version_error}", file=sys.stderr)
        sys.exit(1)

    archive = inventory.get("archive")
    if not archive:
        print("ERROR: Inventory file does not contain 'archive' section.", file=sys.stderr)
        sys.exit(1)

    bucket = archive.get("s3_bucket")
    objects = archive.get("objects")

    if not bucket or objects is None:
        print("ERROR: 'archive' section is missing 's3_bucket' or 'objects'.", file=sys.stderr)
        sys.exit(1)

    s3_client = get_s3_client(profile=args.profile, endpoint_url=args.endpoint_url)

    print(f"Inventory: {inv_path}")
    print(f"S3 bucket: {bucket}")
    print(f"Total objects in archive: {len(objects)}\n")

    # Header
    print("object_id  type   storage_class  status      key")
    print("---------  -----  -------------  ----------  ---")

    for obj in objects:
        oid = obj.get("id", "?")
        otype = obj.get("type", "?")
        key = obj.get("s3_key")

        if not key:
            status = "error"
            sc = None
            hdr = None
            err = "missing s3_key in inventory"
            st = {
                "status": status,
                "storage_class": sc,
                "restore_header": hdr,
                "error": err,
            }
        else:
            st = classify_object_status(bucket, key, s3_client, verbose=verbose)

        status = st["status"]
        sc = st["storage_class"]
        hdr = st["restore_header"]
        err = st["error"]

        if args.only_status and status != args.only_status:
            continue

        key_display = key if key is not None else "MISSING_KEY"
        sc_display = sc if sc is not None else "?"
        status_display = status

        print(f"{oid:9}  {otype:5}  {sc_display:13}  {status_display:10}  {key_display}")

        if hdr and verbose:
            print(f"    Restore header: {hdr}")
        if err and verbose and status == "error":
            print(f"    Error: {err}")

    print()  # trailing newline


if __name__ == "__main__":
    main()
