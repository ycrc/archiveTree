# archiveTree

Tools for archiving a directory tree to remote storage and restoring it
later, with two interchangeable backends:

- **S3** (`archive_to_s3.py` / `restore_from_s3.py`), including Glacier /
  Deep Archive support.
- **Globus** (`archive_to_globus.py` / `restore_from_globus.py`), for
  destinations only reachable via a Globus collection.

Both backends work the same way conceptually:

1. `archive_to_*.py` walks a directory, computes a SHA256 checksum and
   metadata for every file, packs small files into tar bundles (large files
   are transferred individually), uploads/transfers everything, writes a
   JSON **inventory** file describing what went where, and then (with
   confirmation) deletes the original directory.
2. `restore_from_*.py` reads that inventory file and downloads/transfers
   the data back, optionally verifying checksums against the inventory.

The inventory file is the single source of truth for a restore — keep it
somewhere safe (it's also written back to the remote storage itself as a
copy).

## Requirements

- Python 3.9+
- `boto3` (for the S3 tools)
- `globus-sdk` (for the Globus tools)
- `tqdm` (optional; enables progress bars with `--verbose`)

Both S3 and Globus dependencies are available in the `archiver` conda
environment on this system.

## Inventory file format

Every inventory JSON has this shape:

```jsonc
{
  "inventory_id": "<uuid>",       // == archive.archive_id
  "root_dir": "/original/absolute/path",
  "created_at": "2026-08-11T12:00:00Z",
  "total_files": 18,
  "total_bytes": 21002296,
  "files": [
    {
      "relative_path": "file1",
      "absolute_path": "/original/absolute/path/file1",
      "size_bytes": 1234,
      "ctime": "...",
      "owner": "netid",
      "sha256": "...",
      "is_symlink": false,
      "object_id": "tar-000000",     // which archived object holds this file
      "object_type": "tar",          // "file" or "tar"
      "object_key": "..."            // S3 key or Globus collection-relative path
    },
    ...
  ],
  "archive": {
    // S3 backend:
    "s3_bucket": "...",
    // Globus backend:
    "backend": "globus",
    "globus_dest_collection": "<uuid>",
    "globus_dest_path": "...",
    "transfer_task_ids": ["<uuid>", ...],

    "archive_id": "<uuid>",
    "objects": [
      {"id": "tar-000000", "type": "tar", "file_count": 18, ...},
      {"id": "file-000000", "type": "file", "relative_path": "...", ...}
    ]
  }
}
```

Symlinks are recorded specially: `is_symlink: true`, and `sha256` is the
hash of the link target string (not file contents) — the symlink itself is
recreated on restore via tar extraction, not by reading `symlink_target`
directly.

---

## S3 backend

### `archive_to_s3.py`

```
archive_to_s3.py [options] directory bucket object_path
```

| Argument | Description |
|---|---|
| `directory` | Directory tree to archive. |
| `bucket` | S3 bucket name. |
| `object_path` | Base S3 prefix under which archive objects are stored. |
| `--storage-class` | S3 storage class (`STANDARD`, `STANDARD_IA`, `ONEZONE_IA`, `INTELLIGENT_TIERING`, `GLACIER`, `GLACIER_IR`, `DEEP_ARCHIVE`). Default `STANDARD`. |
| `--scratch-dir` | Where local tars are built (default: system temp). |
| `--compression {none,gz}` | Tar compression. Default `none`. |
| `--delete` | Delete the source directory tree after a successful archive. Default: keep it. |
| `--profile` | AWS profile name. |
| `--endpoint-url` | Custom S3-compatible endpoint. |
| `--size-cutoff` | Files bigger than this (bytes) are uploaded individually. Default `1e9`. |
| `--size-grouping` | Target tar-group size in bytes. Default `1e10`. |
| `--max-workers` | Parallel hashing/upload workers. Default `4`. |
| `--dry-run` | Preview the plan; no uploads, tars, or deletes. |
| `--inventory-dir` | Where to write the local inventory JSON (default: parent of `directory`). |
| `--no-summary` | Suppress the final summary. |
| `--verbose` | Progress messages / progress bars. |

Example:

```bash
python3 archive_to_s3.py --profile ycrcbjornson --verbose \
    --storage-class DEEP_ARCHIVE --size-cutoff 1000000 \
    /path/to/mydata mybucket archive-prefix
```

The inventory is written next to `directory` (or `--inventory-dir`) as
`<dirname>.inventory.<uuid>.json.gz` (gzip-compressed to save space), and a
copy is uploaded to `s3://bucket/{prefix}/{dirname}/{archive_id}/inventory/`.
All the tools in this repo read inventories via a shared loader that
transparently handles both gzip-compressed and older plain-text inventory
files, so nothing needs to be decompressed by hand.

### `restore_from_s3.py`

```
restore_from_s3.py [options] inventory_file
```

| Argument | Description |
|---|---|
| `inventory_file` | Inventory JSON from `archive_to_s3.py`. |
| `--profile` / `--endpoint-url` | Same as above. |
| `--scratch-dir` | Where downloaded tars land before extraction. |
| `--restore-dir` | Restore target (default: the original `root_dir` recorded in the inventory). |
| `--overwrite` | Allow restoring into a non-empty directory. |
| `--only-path PATH` | Restore only this relative path (repeatable). |
| `--only-prefix PREFIX` | Restore only paths starting with this prefix (repeatable). |
| `--verify-checksums` | Re-hash restored files and compare to the inventory. |
| `--summary-csv PATH` | Write a CSV summary (`relative_path,full_path,size_bytes,verify_status`). |
| `--keep-tar` | Don't delete downloaded tars after restore. |
| `--dry-run` | Preview the plan; also reports Glacier/Deep Archive status. |
| `--auto-request-restore` | Submit Glacier restore requests for cold objects, then exit — rerun once they're ready. |
| `--restore-days` | Days to keep a Glacier restore. Default `7`. |
| `--restore-tier {Bulk,Standard,Expedited}` | Glacier restore speed. Default `Standard`. |
| `--max-workers` | Parallel download/verify workers. Default `4`. |
| `--verbose` | Progress messages / progress bars. |

Example:

```bash
python3 restore_from_s3.py --profile ycrcbjornson \
    --restore-dir /path/to/restore --verify-checksums \
    --summary-csv restore.csv mydata.inventory.<uuid>.json.gz
```

If any archived object is in `GLACIER`/`DEEP_ARCHIVE`/`GLACIER_IR` and not
yet restored, the script prints each object's status and exits without
downloading anything — pass `--auto-request-restore` to submit the restore
requests, wait for AWS to complete them (can take hours, depending on
`--restore-tier`), then rerun the same command.

### `list_object_status.py`

Check Glacier/Deep Archive restore status for every object in an
inventory without doing a restore:

```
list_object_status.py [--profile PROFILE] [--endpoint-url URL] \
    [--verbose] [--only-status {ready,cold,restoring,error}] inventory_file
```

---

## Globus backend

Use this when your destination storage is only reachable via a Globus
collection rather than S3.

### One-time setup

1. **Register a Globus app.** Go to
   https://app.globus.org/settings/developers, create a project if you
   don't have one, then register an app. When asked what kind of app it
   is, choose the option along the lines of *"OAuth public clients or
   native applications that are distributed to users, and thus cannot
   manage a client secret"* — **not** the client-credentials/service-account
   option. Copy the app's **Client UUID** (not the project's UUID).

2. **Find your collection UUIDs and mount paths.** You need:
   - The Globus collection that maps to *this* machine's filesystem (used
     as the transfer source when archiving, and the destination when
     restoring), and the local absolute path corresponding to that
     collection's root.
   - The Globus collection where archives will be stored, and a base path
     within it.

   To find the right local mount path for a collection: browse it in the
   Globus web File Manager (https://app.globus.org/file-manager) and
   compare the paths you see there to real paths on this filesystem. Get
   this wrong and transfers fail with `500 Path not allowed` errors from
   the GridFTP server — see Troubleshooting below.

3. **Fill in a config file** so you don't have to pass all of this on
   every command line. Copy the example and edit it:

   ```bash
   cp archive_globus.cfg.example ~/.archive_globus.cfg
   ```

   ```ini
   [globus]
   client_id = <native-app-client-uuid>
   token_cache = ~/.globus_archive_tokens.json
   source_collection = <uuid>       # collection mapped to this machine
   source_mount = /                 # local path == that collection's root
   dest_collection = <uuid>         # where archives are stored
   dest_path = /some/real/path      # base path within dest_collection
   ```

   Every value can also be set via an environment variable
   (`GLOBUS_ARCHIVE_CLIENT_ID`, `GLOBUS_ARCHIVE_SOURCE_COLLECTION`,
   `GLOBUS_ARCHIVE_SOURCE_MOUNT`, `GLOBUS_ARCHIVE_DEST_COLLECTION`,
   `GLOBUS_ARCHIVE_DEST_PATH`, `GLOBUS_ARCHIVE_TOKEN_CACHE`) or a CLI flag,
   in that precedence order: **CLI flag > env var > config file**.

4. **First login.** The first time you run either script (outside
   `--dry-run`), it prints a URL — open it, log in, and paste the code
   back at the prompt. A refresh token is cached at `--token-cache`
   (default `~/.globus_archive_tokens.json`, mode `0600`), so subsequent
   runs are non-interactive. Use `--globus-logout` to revoke and delete
   the cached token (e.g. to switch identities).

### `archive_to_globus.py`

```
archive_to_globus.py [options] directory
```

Same size-cutoff/tar-grouping behavior as `archive_to_s3.py`, but:

- Transfers happen as one Globus Transfer task per run (or a few, batched
  via `--max-items-per-task` for very large archives), not one call per
  file — Globus tasks are async and server-managed. Progress is reported
  at the task level (`--verbose`, polled every `--poll-interval` seconds),
  not per-file.
- `--scratch-dir` (where tars are built) and the inventory output
  directory must both be reachable via the source collection, i.e. under
  `--source-mount` — the script checks this up front and errors clearly
  if not.
- Local tars are deleted only after the *whole* transfer task succeeds
  (not incrementally per-tar, since there's one task).
- There's no `--storage-class` (Globus collections don't have storage
  classes) and no `--profile`/`--endpoint-url` (S3-only concepts).

| Argument | Description |
|---|---|
| `directory` | Directory tree to archive (optional only with `--globus-logout`). |
| `--dest-collection` | Destination (archive) Globus collection UUID. |
| `--dest-path` | Base path within the destination collection. |
| `--source-collection` | Collection mapped to this machine's filesystem. |
| `--source-mount` | Local path corresponding to `--source-collection`'s root. |
| `--client-id` | Globus Native App client ID. |
| `--token-cache` | Cached-token file path. |
| `--config-file` | Config file path (default `~/.archive_globus.cfg`). |
| `--globus-logout` | Revoke and delete cached tokens, then exit. |
| `--scratch-dir` | Local tar-build directory (must be under `--source-mount`). |
| `--compression {none,gz}` | Tar compression. Default `none`. |
| `--delete` | Delete the source directory tree after a successful archive. Default: keep it. |
| `--size-cutoff` | Files bigger than this (bytes) transfer individually. Default `1e9`. |
| `--size-grouping` | Target tar-group size in bytes. Default `1e10`. |
| `--max-workers` | Parallel *local tar-build* workers (transfer parallelism is handled by Globus). Default `4`. |
| `--dry-run` | Preview the plan; no tars, transfers, or deletes. Works without Globus credentials. |
| `--inventory-dir` | Where to write the local inventory JSON. |
| `--no-summary` | Suppress the final summary. |
| `--no-verify-checksum-transfer` | Disable Globus's built-in transfer checksum verification (on by default; additive to the SHA256 already recorded in the inventory). |
| `--poll-interval` | Seconds between task-status polls when `--verbose`. Default `15`. |
| `--max-items-per-task` | Split into sequential batched tasks above this many objects. Default `10000`. |
| `--verbose` | Progress messages. |

Example:

```bash
python3 archive_to_globus.py --verbose /path/to/mydata
# (collection UUIDs / mount / client-id come from ~/.archive_globus.cfg)
```

### `restore_from_globus.py`

```
restore_from_globus.py [options] inventory_file
```

Same shape as `restore_from_s3.py`, minus all Glacier-style
preflight/restore-request logic (a Globus collection is not tiered
storage, so there's no `--auto-request-restore`/`--restore-days`/
`--restore-tier`).

| Argument | Description |
|---|---|
| `inventory_file` | Inventory JSON from `archive_to_globus.py` (optional only with `--globus-logout`). |
| `--archive-collection` | Override for the collection holding archived objects (default: read from the inventory itself). |
| `--dest-collection` | Collection mapped to this machine's filesystem (the restore target). |
| `--dest-mount` | Local path corresponding to `--dest-collection`'s root. |
| `--client-id` / `--token-cache` / `--config-file` / `--globus-logout` | Same as the archive script. |
| `--scratch-dir` | Where downloaded tars land before extraction (must be under `--dest-mount`). |
| `--restore-dir` | Restore target (default: the original `root_dir` from the inventory). |
| `--overwrite` | Allow restoring into a non-empty directory. |
| `--only-path` / `--only-prefix` | Restore a subset, same as the S3 script. |
| `--verify-checksums` | Re-hash restored files and compare to the inventory. |
| `--summary-csv` | Write a CSV summary. |
| `--keep-tar` | Don't delete downloaded tars after restore. |
| `--dry-run` | Preview the plan. Works without Globus credentials. |
| `--max-workers` | Parallel *local extraction/verify* workers. Default `4`. |
| `--poll-interval` | Seconds between task-status polls when `--verbose`. Default `15`. |
| `--max-items-per-task` | Split into sequential batched tasks above this many objects. Default `10000`. |
| `--verbose` | Progress messages. |

Example:

```bash
python3 restore_from_globus.py --restore-dir /path/to/restore \
    --verify-checksums --summary-csv restore.csv --verbose \
    mydata.inventory.<uuid>.json.gz
```

`--dest-collection`/`--dest-mount` default from the same
`GLOBUS_ARCHIVE_SOURCE_COLLECTION`/`GLOBUS_ARCHIVE_SOURCE_MOUNT` config
values used by the archive script's `--source-collection`/
`--source-mount` — it's the same physical machine/collection on both
sides of a round trip.

### Troubleshooting

**`Invalid client_id parameter value`** at the login URL:
- Check you copied the **App's** Client UUID, not the **Project's** UUID —
  easy to mix up in the Globus Developer Console.
- Confirm the app was registered as a public/native client ("cannot
  manage a client secret"), not a client-credentials/service account.

**`Invalid endpoint name '<uuid> # some comment': Invalid username`** when
submitting a transfer:
- A stray inline comment in the config file leaked into the value (e.g.
  `dest_collection = <uuid>  # NESE Tape`). The config parser strips both
  whole-line and inline `#` comments, so this shouldn't happen — if you
  still see it, check your `archive_common`/`globus_config.py` version is
  current.

**`500 Command failed : Path not allowed` (FTPServerError)** during a
transfer:
- This is a Globus Connect Server path-restriction rejection, not a Unix
  permission error. It means `--source-mount`/`--dest-mount` doesn't
  match what that collection's root actually corresponds to.
- Browse the collection in the Globus web File Manager and compare the
  paths shown there to real paths on disk to figure out the correct
  mount. For a collection that exposes the whole filesystem, this is
  often `/`, not a deeper prefix like `/gpfs/gibbs`.
- `--dest-path` (archive script) / paths in the inventory are used
  directly as collection-relative paths, not translated through a mount —
  if your collection root is `/`, a real absolute path like
  `/gpfs/gibbs/project/.../archive` is exactly right for `--dest-path`.

**Consent errors mid-transfer:** the scripts automatically re-run the
login flow requesting the additional scope and retry once; if it still
fails, run `--globus-logout` and log in again from scratch.
