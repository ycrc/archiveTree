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
import json
import os
import sys

import boto3
from botocore.exceptions import ClientError

GLACIER_CLASSES = {"GLACIER", "DEEP_ARCHIVE", "GLACIER_IR"}


def vprint(verbose, *args, **kwargs):
    if verbose:
        print(*args, **kwargs)


def get_s3_client(profile=None, endpoint_url=None):
    """Create a boto3 S3 client with optional profile and endpoint."""
    if profile:
        session = boto3.session.Session(profile_name=profile)
    else:
        session = boto3.session.Session()

    if endpoint_url:
        return session.client("s3", endpoint_url=endpoint_url)
    return session.client("s3")


def classify_object_status(bucket, key, s3_client, verbose=False):
    """
    Inspect a single S3 object and classify its availability.

    Returns a dict with:
      {
        "status": one of {"ready", "cold", "restoring", "error"},
        "storage_class": str or None,
        "restore_header": str or None,
        "error": str or None,
      }
    """
    try:
        resp = s3_client.head_object(Bucket=bucket, Key=key)
    except ClientError as e:
        msg = f"head_object failed: {e}"
        vprint(verbose, f"s3://{bucket}/{key}: ERROR {msg}")
        return {
            "status": "error",
            "storage_class": None,
            "restore_header": None,
            "error": msg,
        }

    storage_class = resp.get("StorageClass", "STANDARD")
    restore_hdr = resp.get("Restore")

    if storage_class not in GLACIER_CLASSES:
        # Not in a Glacier class; ready to download
        return {
            "status": "ready",
            "storage_class": storage_class,
            "restore_header": restore_hdr,
            "error": None,
        }

    # In Glacier / Deep Archive
    if restore_hdr and 'ongoing-request="false"' in restore_hdr:
        # Already temporarily restored
        return {
            "status": "ready",
            "storage_class": storage_class,
            "restore_header": restore_hdr,
            "error": None,
        }

    if restore_hdr and 'ongoing-request="true"' in restore_hdr:
        # Restore in progress
        return {
            "status": "restoring",
            "storage_class": storage_class,
            "restore_header": restore_hdr,
            "error": None,
        }

    # Cold, no restore requested yet
    return {
        "status": "cold",
        "storage_class": storage_class,
        "restore_header": restore_hdr,
        "error": None,
    }


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
    with open(inv_path, "r", encoding="utf-8") as f:
        inventory = json.load(f)

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
