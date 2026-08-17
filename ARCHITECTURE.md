# Architecture

This document explains how archiveTree works internally: how a directory tree
becomes a handful of remote objects plus one inventory file, and how that
inventory is later used to restore all or part of the tree. It's written for
someone modifying or extending the code, not for end users — see `README.md`
for usage.

## Module map

| File | Role |
|---|---|
| `archive_common.py` | Transport-agnostic core: inventory building, tar create/extract, checksum verification, size partitioning/grouping, inventory JSON read/write. Imports neither `boto3` nor `globus_sdk`. |
| `archive_to_s3.py` / `restore_from_s3.py` | S3 backend (via `boto3`). |
| `archive_to_globus.py` / `restore_from_globus.py` | Globus Transfer backend. |
| `globus_auth.py` | Native App OAuth login flow + token cache, shared by the two Globus scripts. |
| `globus_config.py` | CLI flag / env var / config-file precedence resolution for Globus settings. |
| `globus_transfer.py` | `TransferData` construction, submission, and polling; local-path ↔ collection-relative-path mapping. |
| `list_object_status.py` | Reports each S3 object's storage class / Glacier restore status for an inventory. |
| `browse_inventory.py` | Interactive curses TUI for browsing an inventory and generating a restore script. |

Keeping `archive_common.py` free of backend SDK imports is deliberate: it
means the inventory format, tar logic, and checksum logic are backend-agnostic
by construction, not just by convention, and it lets tools like
`browse_inventory.py` operate on an inventory without either SDK installed.

## The inventory file

Every archive produces exactly one inventory: a JSON document that is the
sole record of which S3 object or Globus path each original file ended up
in. **It is the only thing that makes a restore possible** — losing it means
the archived objects are just opaque blobs (short of manually reconstructing
paths from the object keys).

### On disk

- Written by `write_inventory_file()` (`archive_common.py:549`), named
  `<dirname>.inventory.<archive_id>.json.gz` by the two archive scripts.
- Gzip-compressed by default (`gzip_output=True`). A copy is also uploaded/
  transferred to the backend itself, under `.../inventory/` in the same
  archive prefix — but that's a convenience/DR copy; the operative copy is
  the local one you pass to `restore_from_*.py`.
- Read via `load_inventory_file()` (`archive_common.py:594`), which detects
  gzip by magic bytes (`\x1f\x8b`), not by filename, so it transparently
  reads both new `.json.gz` files and the many pre-existing plain-text
  `.json` inventories written before compression was added.

### Top-level schema

```jsonc
{
  "inventory_id": "<uuid>",           // == archive.archive_id
  "root_dir": "/abs/path/to/tree",    // original location, used as restore default
  "created_at": "2026-08-17T12:00:00Z",
  "total_files": 189,
  "total_bytes": 119191588,
  "format_version": 3,
  "files": [ /* per-file records */ ],
  "directories": [ /* per-directory rollups */ ],
  "archive": { /* backend-specific: object list + destination info */ }
}
```

`files`, `directories`, `format_version` are added by `build_inventory()` and
`write_inventory_file()` respectively; `archive` is assembled by
`write_inventory_file()` from whatever `backend_meta` the caller passes in
(see [S3 vs Globus](#s3-vs-globus-differences) below).

### Per-file record (`files[]`)

Built by `_checksum_one()` (`archive_common.py:80`), one per regular file or
symlink under `root_dir`:

```jsonc
{
  "relative_path": "data/raw/sample.csv",
  "absolute_path": "/abs/path/to/tree/data/raw/sample.csv",
  "size_bytes": 512000,
  "ctime": "2026-08-10T09:00:00",
  "owner": "rdb9",
  "sha256": "...",
  "is_symlink": false
  // "symlink_target": "..."   -- present only if is_symlink is true
}
```

After the archive script assigns each file to an object, three more keys are
mutated onto the same dict in place: `object_id`, `object_type`
(`"file"` or `"tar"`), `object_key` (the S3 key or Globus path of the object
holding this file's bytes).

Checksums: for a regular file, `sha256` is the hash of its contents. For a
symlink, `sha256` is the hash of the **link target string**, not any file
content — this is what `verify_restored_files()` re-checks after restore.
Broken symlinks are recorded successfully (the target string is hashed
regardless of whether it resolves). Sockets, FIFOs, and device files are
silently skipped with a warning (`archive_common.py:96`) since they can't be
meaningfully archived.

### Per-directory rollup (`directories[]`)

Added in format_version 3. One entry per directory in the tree, **including
empty ones** (collected from every `os.walk()` iteration, not inferred from
file paths — `archive_common.py:150`):

```jsonc
{ "relative_path": "data/raw", "file_count": 20, "total_bytes": 9773629 }
```

`file_count`/`total_bytes` are recursive — everything under that directory,
not just its direct children. The root directory itself is the entry with
`relative_path == ""`; it always equals the top-level `total_files`/
`total_bytes`. This exists purely so tools like `browse_inventory.py` can
show directory sizes without re-scanning the full `files` list on every
navigation — it's a precomputed `du`, done once at archive time.

### `archive` section and object records

`archive.objects` is a flat list describing every S3/Globus object the
archive produced. Two shapes:

```jsonc
// a large file, stored as its own object
{ "id": "file-000000", "type": "file", "s3_key": "...", "size_bytes": 62914560,
  "storage_class": "STANDARD", "relative_path": "big_dataset.bin" }

// a tar bundling many small files into one object
{ "id": "tar-000002", "type": "tar", "s3_key": "...", "size_bytes": 5120111,
  "storage_class": "STANDARD", "file_count": 20, "group_index": 0 }
```

(Globus objects use `"globus_path"` instead of `"s3_key"` and have no
`storage_class`.) Note the shared `object_counter` in both archive scripts:
large-file jobs are numbered first (`file-000000`, `file-000001`, ...), then
tar-group jobs continue the *same* counter (`tar-000002`, `tar-000003`, ...)
— the numeric suffix is not restarted per type.

`archive` also carries backend-specific fields merged in by
`write_inventory_file`'s `backend_meta` argument:

- **S3**: `{"s3_bucket": bucket}`
- **Globus**: `{"backend": "globus", "globus_dest_collection": ..., "globus_dest_path": ..., "transfer_task_ids": [...]}`

`restore_from_globus.py` uses the presence of `archive.backend == "globus"`
as its own guard against being pointed at an S3 inventory
(`restore_from_globus.py:178`); the S3 side is identified implicitly by the
presence of `s3_bucket` instead of an explicit tag.

### `format_version`

A single top-level int, checked by `check_inventory_version()`
(`archive_common.py:531`) right after loading, before anything else touches
the file:

- **1** — no `format_version` key at all (every inventory written before
  this scheme existed; absence is treated as implicit version 1).
- **2** — `format_version` key added, no schema change otherwise.
- **3** — current. Adds `directories[]`.

`SUPPORTED_INVENTORY_VERSIONS` is the allow-list readers accept;
`CURRENT_INVENTORY_VERSION` is what new writes are stamped with. To add a
new version: bump `CURRENT_INVENTORY_VERSION`, add it to
`SUPPORTED_INVENTORY_VERSIONS`, and — since old and new versions must keep
working side by side (there is a lot of format-1/2 data already archived
that will never be rewritten) — make the schema change additive wherever
possible, and branch on `inventory.get("format_version", 1)` in the specific
code that needs to know, rather than assuming the current shape.

## Archive flow

Both `archive_to_s3.py` and `archive_to_globus.py` follow the same sequence;
only the upload/transfer mechanics differ.

1. **`build_inventory(root_dir)`** (`archive_common.py:138`) walks the tree
   once, computing every file's checksum in parallel (`ThreadPoolExecutor`,
   `--max-workers`), then rolls up per-directory stats. Returns the inventory
   dict described above, with `files[]` in deterministic `os.walk()` order.

2. **`partition_by_size(files, size_cutoff)`** splits records into
   `large_files` (> `--size-cutoff`, default 1 GB) and `small_files`.

3. **`group_small_files(small_files, size_grouping)`** greedily bins the
   small files into groups of ~`--size-grouping` bytes each (default 10 GB) —
   simple running-total bin packing, not size-balanced, just threshold-based
   (`archive_common.py:237`).

4. A flat **job list** is built: one `"file"` job per large file, one
   `"tar"` job per small-file group, with sequential `object_id`s.

5. **Upload/transfer** (this is where the two backends diverge):

   - **S3** (`archive_to_s3.py:317` `process_job`): each job — file or tar —
     is handled by one worker in a single `ThreadPoolExecutor`. A `"tar"`
     job calls `create_tar()` to build the tar *and* uploads it, all inside
     that one worker; `upload_file_to_s3()` uploads then immediately does a
     `head_object` to verify the remote size and record the actual storage
     class. Each object is a synchronous `boto3` call.

   - **Globus** (`archive_to_globus.py`): transfer tasks are async and
     server-managed, so the flow is split into two decoupled passes.
     First, all tar groups are built *locally* in parallel
     (`build_tar_job`, its own `ThreadPoolExecutor` pass, `:277`) — pure
     local CPU/disk work, no network yet. Then every job (large files +
     built tars) is added as an item to one or more `TransferData` tasks
     (`globus_transfer.batches()`, `--max-items-per-task`, default 10000)
     and submitted via `globus_transfer.submit_and_wait()`, which polls
     `task_wait()` until the task completes (`:339`).

   Either way, each file's record gets `object_id`/`object_type`/
   `object_key` mutated onto it in place as jobs complete. This is safe
   without locks because every record is only ever written by the one
   worker that owns it — mutating "the file record" (a dict already in
   `inventory["files"]`) via a reference held by the tar's job, not a copy.

6. **`write_inventory_file()`** writes the completed inventory (now full of
   object assignments) to disk, then that same file is uploaded/transferred
   to the backend as a normal object under `.../inventory/`.

7. **Optional delete**: only if `--delete` was passed does `shutil.rmtree()`
   run on the source tree. This ordering — upload everything, write and
   upload the inventory, *then* delete — means a crash mid-archive never
   loses data: worst case is some orphaned remote objects with no inventory
   pointing at them (harmless, just wasted space) or a fully-valid inventory
   with the source tree still sitting right next to it (safe to retry
   deletion by hand).

Both scripts support `--dry-run`, which stops after step 4 and prints a
summary (file/object counts, bytes) without creating tars, uploading, or
deleting anything.

## Restore flow

`restore_from_s3.py` and `restore_from_globus.py` again share the same
shape, diverging on transfer mechanics.

1. **Load + validate**: `load_inventory_file()` then
   `check_inventory_version()`, then a backend sanity check (S3 requires
   `archive.s3_bucket`; Globus requires `archive.backend == "globus"`).

2. **`select_relpaths()`** (`archive_common.py:334`) resolves `--only-path`
   (exact match, repeatable) and `--only-prefix` (repeatable) against
   `files[]`. With neither flag, everything is selected. **Note:**
   `--only-prefix` matching is a plain `str.startswith()` — not
   path-boundary aware — so a prefix of `logs` would also match a sibling
   directory named `logs2`. `browse_inventory.py` works around this itself
   by always emitting prefixes with a trailing `/`; anything calling
   `select_relpaths()` directly should do the same.

3. **Group selected files by object**: for each selected relpath, look up
   its `object_id` and bucket relpaths into `object_to_relpaths: dict[object_id, set[relpath]]`.
   This is the key data structure — it's what turns "restore these 5 files"
   into "download these 2 objects." If several selected files share a tar
   object, that object is only downloaded/transferred once.

4. **`--dry-run`** prints exactly this plan — selected file count, total
   bytes, and for each involved object: its id, type, key, own size, and
   how many/how many bytes of the *selection* live in it — without touching
   the network.

5. **Fetch + extract** (divergent):

   - **S3** (`restore_from_s3.py`): before downloading anything,
     `preflight_check_objects()` HEADs every required object and classifies
     it `ready` / `cold` / `restoring` / `error` based on `StorageClass` and
     the `Restore` header. If anything isn't `ready`, it prints the full
     status table and exits — optionally (`--auto-request-restore`)
     submitting Glacier/Deep Archive restore requests first — rather than
     starting a download that would fail partway through. Once everything
     is `ready`, one `ThreadPoolExecutor` worker per object either downloads
     a `"file"`-type object straight to its final destination path, or
     downloads a `"tar"`-type object to a scratch dir and calls
     `extract_tar(..., selected_relpaths=rels)` — which extracts *only* the
     files actually wanted from that tar, even though the whole tar had to
     be downloaded to get them.

   - **Globus** (`restore_from_globus.py`): no preflight (a Globus
     collection isn't tiered storage). All needed objects are pulled in one
     or a few batched `TransferData` tasks to a scratch dir (or straight to
     the final path, for `"file"`-type objects) via
     `submit_and_wait()`. Only *after* the transfer task(s) finish does a
     separate local `ThreadPoolExecutor` pass extract the downloaded tars
     (`extract_job`) — transfer and extraction are fully decoupled, unlike
     the S3 path where each worker does both.

6. **Optional `--verify-checksums`**: `verify_restored_files()`
   (`archive_common.py:364`) re-hashes every restored file (or symlink
   target) and compares to the inventory's recorded `sha256`, raising on any
   `missing`/`checksum_mismatch`.

7. **Optional `--summary-csv`**: `write_summary_csv()` writes
   `relative_path,full_path,size_bytes,verify_status` for the restored
   subset.

### Why "restore 1 file" can mean "download 100"

This is the single most important thing to understand about the restore
path: **the unit of storage is the object, not the file.** A `"tar"` object
groups many small files together; restoring even one of them requires
downloading/transferring that entire object (`extract_tar` then throws away
everything except what was asked for). `browse_inventory.py`'s summary line
(`Restore: N files ... | Archive read: M objects ...`) and its `d` debug
view exist specifically to make this cost visible before you commit to a
restore — see below.

## S3 vs Globus differences

| | S3 | Globus |
|---|---|---|
| Transfer unit | Individual synchronous `boto3` calls, one per object | Batched async `TransferData` tasks (poll to completion) |
| Verification | App-level: `head_object` size check after each upload | Server-side: `verify_checksum=True` / `sync_level="checksum"` on the transfer itself |
| Tiered storage | Yes — Glacier/Deep Archive preflight + restore-request flow | No such concept; Globus collections aren't tiered |
| Auth | AWS profile / env creds (`boto3` default chain) | Native App OAuth device flow, cached refresh token (`globus_auth.py`) |
| Path model | Global flat key namespace (`s3_key`) | Collection-relative paths (`globus_path`); local paths must be mapped via `--source-mount`/`--dest-mount` (`globus_transfer.local_path_to_collection_relative()`) — everything you touch (source dir, scratch dir, inventory file, restore dir) must live under the configured mount, or `require_under_mount()` exits with an error |
| Config resolution | CLI flags only | CLI flag → env var → `~/.archive_globus.cfg` → error, via `globus_config.resolve()`/`require()` (`globus_config.py:31`) |

## The `browse_inventory.py` TUI

An ncdu/xdu-view-style curses browser built entirely on the inventory's
`files[]`/`directories[]` data — it never touches S3 or Globus, so it works
with or without either SDK installed.

- **Tree model**: `build_tree()` builds an in-memory `Node` tree once at
  startup from `directories[]` (internal nodes, carrying their precomputed
  rollup) and `files[]` (leaves). Navigation is just walking this tree; no
  data is re-scanned per keystroke.
- **Selection**: `App.marks` is a flat `dict[relpath] -> "dir"|"file"`.
  `compute_restore_selection()` resolves marks into the minimal
  `(dir_prefixes, file_paths, whole_tree)` needed to reproduce the
  selection, dropping anything nested under an already-marked directory. A
  synthetic `[ALL]` row (pinned to the top of the root listing) represents
  marking the root itself — the one case that can't be reached by marking a
  child from its parent's listing, since the root has no parent listing.
- **Restore vs. archive-read metrics**: `compute_summary()` and
  `compute_touched_objects()` both resolve the current selection down to the
  actual touched `object_id`s (mirroring `object_to_relpaths` from the
  restore scripts) to compute "what lands on disk" vs. "what has to be
  downloaded to get it" — the same distinction described above.
- **Script generation**: `restore_command_groups()` is the single source of
  truth for the restore command line, shared by `build_restore_script()`
  (the `w` key) and the `d` debug view's command preview, so the preview can
  never drift from what actually gets written.

## Concurrency model

Everything uses `ThreadPoolExecutor`, never `multiprocessing`. This is a
deliberate fit for the workload: checksum hashing releases the GIL during
the C-level hash update calls, and upload/download/transfer calls are
I/O-bound waiting on network or disk. Worker functions are written to only
mutate data they exclusively own (one file record, one job dict), so there's
no locking anywhere in the codebase — correctness relies on that invariant
holding, not on any synchronization primitive.

## Developing/testing without cloud credentials

`archive_common.py` has no `boto3`/`globus_sdk` dependency, so you can
exercise most of the interesting logic — inventory building, the rollup,
gzip round-tripping, `browse_inventory.py`'s tree/selection/script logic —
without any credentials at all:

```python
import archive_common as ac

inventory, _paths = ac.build_inventory("/path/to/some/test/tree", max_workers=4)
ac.write_inventory_file(inventory, {"s3_bucket": "fake-bucket"}, objects=[], inventory_path="test.inventory.json.gz")
```

To get a realistic `archive.objects` list (with actual tar-group
assignments) for testing something like `browse_inventory.py`'s
restore-vs-archive-read metrics, without performing any real upload, drive
the same partitioning functions the archive scripts use directly:

```python
files = inventory["files"]
large, small = ac.partition_by_size(files, size_cutoff=20_000_000)
groups = ac.group_small_files(small, size_grouping=5_000_000)
# then assign object_id/object_type/object_key onto each record yourself,
# and build a matching "objects" list, mirroring archive_to_s3.py's
# process_job() — see that function for the exact shape.
```

`load_inventory_file()` will happily read back whatever you write this way,
and every downstream consumer (restore scripts, `list_object_status.py`,
`browse_inventory.py`) operates purely on the in-memory dict from that point
on.
