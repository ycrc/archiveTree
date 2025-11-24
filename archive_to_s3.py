#!/usr/bin/env python3
"""
Archive a directory tree into a tar file, upload it to S3, and remove the tree.

Adds:
  * optional AWS profile
  * optional S3 endpoint URL
  * --verbose : progress bars + detailed steps
"""

import argparse
import os
import sys
import tarfile
import tempfile
import shutil
import hashlib
import json
import uuid
import pwd
from datetime import datetime

import boto3
from botocore.exceptions import ClientError

# Optional tqdm for progress bars
try:
    from tqdm import tqdm
except ImportError:
    tqdm = None


# ---------- Utility Functions ----------

def vprint(verbose, *args, **kwargs):
    """Print only if verbose."""
    if verbose:
        print(*args, **kwargs)


def compute_sha256(path, verbose=False):
    """Compute SHA256 checksum of a file with optional progress bar."""
    filesize = os.path.getsize(path)
    h = hashlib.sha256()

    if verbose and tqdm:
        pbar = tqdm(total=filesize, unit="B", unit_scale=True, desc=f"hash {os.path.basename(path)}")
    else:
        pbar = None

    chunk_size = 8 * 1024 * 1024
    with open(path, "rb") as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            h.update(chunk)
            if pbar:
                pbar.update(len(chunk))

    if pbar:
        pbar.close()

    return h.hexdigest()


def get_owner(stat_result):
    """Get username from stat, fall back to uid."""
    try:
        return pwd.getpwuid(stat_result.st_uid).pw_name
    except Exception:
        return str(stat_result.st_uid)


def build_inventory(root_dir, verbose=False):
    """Walk directory and compute inventory."""
    file_records = []
    file_paths = []

    file_list = []
    for dirpath, _, filenames in os.walk(root_dir):
        for name in filenames:
            file_list.append(os.path.join(dirpath, name))

    iterator = file_list
    if verbose and tqdm:
        iterator = tqdm(file_list, desc="Inventory", unit="files")

    root_dir = os.path.abspath(root_dir)

    for path in iterator:
        relpath = os.path.relpath(path, root_dir)
        st = os.stat(path)

        sha = compute_sha256(path, verbose=verbose)

        record = {
            "relative_path": relpath,
            "absolute_path": path,
            "size_bytes": st.st_size,
            "ctime": datetime.fromtimestamp(st.st_ctime).isoformat(),
            "owner": get_owner(st),
            "sha256": sha,
        }
        file_records.append(record)
        file_paths.append(path)

    inventory = {
        "inventory_id": str(uuid.uuid4()),
        "root_dir": root_dir,
        "created_at": datetime.utcnow().isoformat() + "Z",
        "total_files": len(file_records),
        "total_bytes": sum(r["size_bytes"] for r in file_records),
        "files": file_records,
    }

    return inventory, file_paths


def create_tar(root_dir, file_paths, scratch_dir=None, tar_compression=None, verbose=False):
    """Tar up files with optional progress bar."""
    root_dir = os.path.abspath(root_dir)
    base_name = os.path.basename(root_dir.rstrip(os.sep))
    suffix = ".tar.gz" if tar_compression == "gz" else ".tar"
    if scratch_dir is None:
        scratch_dir = tempfile.gettempdir()

    os.makedirs(scratch_dir, exist_ok=True)
    tar_path = os.path.join(scratch_dir, f"{base_name}_{uuid.uuid4().hex}{suffix}")

    mode = "w:gz" if tar_compression == "gz" else "w"

    iterator = file_paths
    if verbose and tqdm:
        iterator = tqdm(file_paths, desc="Tar", unit="files")

    with tarfile.open(tar_path, mode) as tar:
        for path in iterator:
            relpath = os.path.relpath(path, root_dir)
            tar.add(path, arcname=relpath)

    return tar_path


# ---------- S3 + Progress ----------

class ProgressCallback:
    """S3 upload progress callback."""

    def __init__(self, filename, verbose=False):
        self.verbose = verbose
        self._size = os.path.getsize(filename)
        self._seen = 0
        if verbose and tqdm:
            self._pbar = tqdm(
                total=self._size,
                unit="B",
                unit_scale=True,
                desc="Upload"
            )
        else:
            self._pbar = None

    def __call__(self, bytes_amount):
        self._seen += bytes_amount
        if self._pbar:
            self._pbar.update(bytes_amount)

    def close(self):
        if self._pbar:
            self._pbar.close()


def get_s3_client(profile=None, endpoint_url=None):
    if profile:
        session = boto3.session.Session(profile_name=profile)
    else:
        session = boto3.session.Session()
    if endpoint_url:
        return session.client("s3", endpoint_url=endpoint_url)
    return session.client("s3")


def upload_to_s3(tar_path, bucket, key, storage_class, s3_client, verbose=False):
    extra_args = {"StorageClass": storage_class}

    vprint(verbose, f"Uploading {tar_path} to s3://{bucket}/{key} ...")

    progress = ProgressCallback(tar_path, verbose=verbose)

    with open(tar_path, "rb") as f:
        s3_client.upload_fileobj(
            f,
            bucket,
            key,
            ExtraArgs=extra_args,
            Callback=progress
        )

    progress.close()


def verify_s3_object(tar_path, bucket, key, s3_client, verbose=False):
    local_size = os.path.getsize(tar_path)
    resp = s3_client.head_object(Bucket=bucket, Key=key)
    remote_size = resp["ContentLength"]

    if local_size != remote_size:
        raise RuntimeError(f"Verification failed: local size {local_size} != S3 size {remote_size}")

    vprint(verbose, f"Verified upload of {local_size} bytes.")


# ---------- Inventory Writing ----------

def write_inventory_file(inventory, bucket, key, tar_path, inventory_path, verbose=False):
    inv = dict(inventory)
    inv["archive"] = {
        "s3_bucket": bucket,
        "s3_key": key,
        "tar_size_bytes": os.path.getsize(tar_path),
        "tar_path_at_creation": os.path.abspath(tar_path),
    }

    os.makedirs(os.path.dirname(inventory_path), exist_ok=True)

    with open(inventory_path, "w") as f:
        json.dump(inv, f, indent=2, sort_keys=True)

    vprint(verbose, f"Wrote inventory file {inventory_path}")


# ---------- Main ----------

def main():
    parser = argparse.ArgumentParser(description="Archive a directory tree to S3.")
    parser.add_argument("directory")
    parser.add_argument("bucket")
    parser.add_argument("object_path")
    parser.add_argument("--storage-class", default="STANDARD")
    parser.add_argument("--scratch-dir", default=None)
    parser.add_argument("--compression", choices=["none", "gz"], default="none")
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--profile", default=None)
    parser.add_argument("--endpoint-url", default=None)
    parser.add_argument("--verbose", action="store_true")

    args = parser.parse_args()
    verbose = args.verbose

    root_dir = os.path.abspath(args.directory)

    if not os.path.isdir(root_dir):
        print(f"ERROR: {root_dir} is not a directory", file=sys.stderr)
        sys.exit(1)

    vprint(verbose, "Building inventory...")
    inventory, file_paths = build_inventory(root_dir, verbose=verbose)

    compression = None if args.compression == "none" else "gz"

    vprint(verbose, "Creating tar archive...")
    tar_path = create_tar(
        root_dir, file_paths,
        scratch_dir=args.scratch_dir,
        tar_compression=compression,
        verbose=verbose
    )

    base = os.path.basename(root_dir.rstrip(os.sep))
    tar_suffix = ".tar.gz" if compression == "gz" else ".tar"
    tar_key_name = base + tar_suffix

    opath = args.object_path.strip("/")
    s3_key = f"{opath}/{tar_key_name}" if opath else tar_key_name

    s3_client = get_s3_client(args.profile, args.endpoint_url)

    upload_to_s3(tar_path, args.bucket, s3_key, args.storage_class, s3_client, verbose=verbose)

    verify_s3_object(tar_path, args.bucket, s3_key, s3_client, verbose=verbose)

    if not args.force:
        resp = input(f"Upload verified. DELETE directory {root_dir}? Type 'yes': ")
        if resp.lower() != "yes":
            print("Aborting. Directory left intact.")
            sys.exit(0)

    vprint(verbose, f"Removing directory {root_dir}")
    shutil.rmtree(root_dir)

    parent = os.path.dirname(root_dir)
    invname = f"{base}.inventory.{inventory['inventory_id']}.json"
    invpath = os.path.join(parent, invname)

    write_inventory_file(inventory, args.bucket, s3_key, tar_path, invpath, verbose=verbose)

    vprint(verbose, f"Removing temporary tar {tar_path}")
    try:
        os.remove(tar_path)
    except OSError:
        pass

    vprint(verbose, "Done.")


if __name__ == "__main__":
    main()
