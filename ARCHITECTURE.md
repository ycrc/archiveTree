# Architecture

This document explains how archiveTree works internally: how a directory tree
becomes a handful of remote objects plus one inventory file, and how that
inventory is later used to restore all or part of the tree. It's written for
someone modifying or extending the code, not for end users — see `README.md`
for usage.

## Module map

| File | Role |
|---|---|
| `archive_common.py` | Transport-agnostic core: inventory building, tar create/extract, checksum verification, size partitioning/grouping, inventory JSON read/write, `detect_backend()`. Imports neither `boto3` nor `globus_sdk`. |
| `archive_to_s3.py` / `restore_from_s3.py` | S3 backend (via `boto3`), with optional `[s3]`-section config-file support. |
| `archive_to_globus.py` / `restore_from_globus.py` | Globus Transfer backend. |
| `archive_to_local.py` / `restore_from_local.py` | Local filesystem backend: copies to/from a locally-mounted destination directory, with optional `[local]`-section config-file support. No SDK dependency at all. |
| `archive.py` / `restore.py` | Thin dispatchers: pick a backend (s3/globus/local) via `--backend`, a config file, or (restore only) the inventory's own `archive.backend` field, then call straight into the matching backend script's `run()`. See [Unified entrypoints](#unified-entrypoints-archivepy--restorepy). |
| `globus_auth.py` | Native App OAuth login flow + token cache, shared by the two Globus scripts. |
| `archive_config.py` | Generalized CLI-flag / env-var / config-file precedence resolution, over a multi-section INI file (`[archive]`/`[s3]`/`[globus]`/`[local]`). Shared by all backend scripts and both dispatchers. |
| `globus_config.py` | Thin backward-compatible wrapper around `archive_config.py`, scoped to the `[globus]` section — existing Globus scripts/config files need no changes. |
| `globus_transfer.py` | `TransferData` construction, submission, and polling; local-path ↔ collection-relative-path mapping. |
| `list_object_status.py` | Reports each S3 object's storage class / Glacier restore status for an inventory. |
| `browse_inventory.py` | Interactive curses TUI for browsing an inventory and generating a restore script. |

Keeping `archive_common.py` free of backend SDK imports is deliberate: it
means the inventory format, tar logic, and checksum logic are backend-agnostic
by construction, not just by convention, and it lets tools like
`browse_inventory.py` operate on an inventory without either SDK installed.

## The inventory file

Every archive produces exactly one inventory: a JSON document that is the
sole record of which S3 object, Globus path, or local filesystem path each
original file ended up in. **It is the only thing that makes a restore
possible** — losing it means the archived objects are just opaque blobs
(short of manually reconstructing paths from the object keys).

### On disk

- Written by `write_inventory_file()` (`archive_common.py:650`), named
  `<dirname>.inventory.<archive_id>.json.gz` by all three archive scripts.
- Gzip-compressed by default (`gzip_output=True`). A copy is also
  uploaded/transferred/copied to the backend itself, under `.../inventory/`
  in the same archive prefix — but that's a convenience/DR copy; the
  operative copy is the local one you pass to `restore_from_*.py`.
- Read via `load_inventory_file()` (`archive_common.py:695`), which detects
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
  "format_version": 1,
  "files": [ /* per-file records */ ],
  "directories": [ /* per-directory rollups */ ],
  "unreadable_files": [ /* files build_inventory() couldn't read */ ],
  "archive": { /* backend-specific: object list + destination info */ }
}
```

`files`, `directories`, `unreadable_files`, `format_version` are added by
`build_inventory()` and `write_inventory_file()` respectively; `archive` is
assembled by `write_inventory_file()` from whatever `backend_meta` the
caller passes in (see [Backend differences](#backend-differences) below).

### Per-file record (`files[]`)

Built by `_checksum_one()` (`archive_common.py:81`), one per regular file or
symlink under `root_dir`:

```jsonc
{
  "relative_path": "data/raw/sample.csv",
  "absolute_path": "/abs/path/to/tree/data/raw/sample.csv",
  "size_bytes": 512000,
  "ctime": "2026-08-10T09:00:00",
  "owner": "rdb9",
  "sha256": "...",
  "is_symlink": false,
  "mode": 420
  // "symlink_target": "..."   -- present only if is_symlink is true
}
```

After the archive script assigns each file to an object, three more keys are
mutated onto the same dict in place: `object_id`, `object_type`
(`"file"` or `"tar"`), `object_key` (the S3 key, Globus path, or local
filesystem path of the object holding this file's bytes).

Checksums: for a regular file, `sha256` is the hash of its contents. For a
symlink, `sha256` is the hash of the **link target string**, not any file
content — this is what `verify_restored_files()` re-checks after restore.
Broken symlinks are recorded successfully (the target string is hashed
regardless of whether it resolves). On restore, the symlink itself is
recreated via tar extraction (`tarfile`'s own symlink handling) — nothing
reads `symlink_target` directly to reconstruct it.

A symlink whose target is a directory is archived as a leaf entry, the
same as a symlink to a file — its target's contents are never walked into.
This needs explicit handling in `build_inventory()`
(`archive_common.py:158`): `os.walk()`'s `is_dir()` check follows symlinks,
so such a path is classified as a subdirectory and shows up in `dirnames`,
not `filenames`, even though `os.walk()` still (correctly, per
`followlinks=False`) never descends into it. Anything found in `dirnames`
that `os.path.islink()` is true for is added to the file-hashing work list
exactly like a `filenames` entry, rather than being left to fall through
unrecorded.

`mode` is `stat.S_IMODE(st.st_mode)` — the
POSIX permission bits (e.g. `420` decimal == `0o644`), captured from the
same `os.lstat()` call used for everything else in the record. `owner` is
purely informational (a username string from `pwd.getpwuid`); nothing on
the restore path ever applies it — restored files are simply owned by
whoever runs the restore. Directory permissions are never recorded at all.
See [Permission handling](#permission-handling) below for how `mode` is
(and isn't) restored.

Sockets, FIFOs, and device files are silently skipped with a warning
(`archive_common.py:100`) since they can't be meaningfully archived. Files
that exist but can't be *read* (permission denied, or any other `OSError`
while hashing) are handled differently — see
[Unreadable files](#unreadable-files-unreadable_files) below.

### Per-directory rollup (`directories[]`)

One entry per directory in the tree, **including
empty ones** (collected from every `os.walk()` iteration, not inferred from
file paths — `archive_common.py:158`):

```jsonc
{ "relative_path": "data/raw", "file_count": 20, "total_bytes": 9773629, "mode": 493 }
```

`file_count`/`total_bytes` are recursive — everything under that directory,
not just its direct children. The root directory itself is the entry with
`relative_path == ""`; it always equals the top-level `total_files`/
`total_bytes`. This exists purely so tools like `browse_inventory.py` can
show directory sizes without re-scanning the full `files` list on every
navigation — it's a precomputed `du`, done once at archive time.

`mode` (mirroring the per-file `mode` above) is `stat.S_IMODE(st.st_mode)`
for the directory itself, captured via `os.lstat(dirpath)` alongside the
same `os.walk()` pass
(`archive_common.py:164`). It's `null` if that `lstat()` call itself failed
(e.g. a race with something removing the directory mid-walk) — every reader
must treat `mode` as optional, same as the per-file field. See
[Permission handling](#permission-handling) for how it's used on restore.

### `archive` section and object records

`archive.objects` is a flat list describing every S3/Globus/local object the
archive produced. Two shapes:

```jsonc
// a large file, stored as its own object
{ "id": "file-000000", "type": "file", "s3_key": "...", "size_bytes": 62914560,
  "storage_class": "STANDARD", "relative_path": "big_dataset.bin" }

// a tar bundling many small files into one object
{ "id": "tar-000002", "type": "tar", "s3_key": "...", "size_bytes": 5120111,
  "storage_class": "STANDARD", "file_count": 20, "group_index": 0 }
```

(Globus objects use `"globus_path"` instead of `"s3_key"`; local objects use
`"local_path"` (relative to `local_dest_dir`) and additionally carry
`"sha256"` on tar-type objects when `--verify-checksum` was used. Neither
Globus nor local objects have `storage_class`.) Note the shared
`object_counter` in all three archive scripts: large-file jobs are numbered
first (`file-000000`, `file-000001`, ...), then tar-group jobs continue the
*same* counter (`tar-000002`, `tar-000003`, ...) — the numeric suffix is not
restarted per type.

`archive` also carries backend-specific fields merged in by
`write_inventory_file`'s `backend_meta` argument:

- **S3**: `{"backend": "s3", "s3_bucket": bucket}`
- **Globus**: `{"backend": "globus", "globus_dest_collection": ..., "globus_dest_path": ..., "transfer_task_ids": [...]}`
- **Local**: `{"backend": "local", "local_dest_dir": dest_dir}`

All three restore scripts guard against being pointed at the wrong
backend's inventory: `restore_from_globus.py` requires
`archive.backend == "globus"` (`restore_from_globus.py:201`);
`restore_from_local.py` requires `archive.backend == "local"`
(`restore_from_local.py`, same style); `restore_from_s3.py` requires
`archive.backend` to be absent or `"s3"` (`restore_from_s3.py:571`) — the
"absent" case keeps inventories written before the `"backend"` key existed
working. `archive_common.detect_backend()` (`archive_common.py:397`)
implements the equivalent 3-way logic — `backend if backend in ("globus",
"local") else "s3"` — for callers (`browse_inventory.py`, `restore.py`)
that need to *pick* a backend before dispatching, rather than just
validating one after the fact.

### `format_version`

A single top-level int, checked by `check_inventory_version()`
(`archive_common.py:632`) right after loading, before anything else touches
the file. `CURRENT_INVENTORY_VERSION` is what new writes are stamped with;
`SUPPORTED_INVENTORY_VERSIONS` is the allow-list readers accept — currently
both are just `1`, with no prior schema to stay compatible with.

To add a new version in the future: bump `CURRENT_INVENTORY_VERSION`, add
it to `SUPPORTED_INVENTORY_VERSIONS`, and decide then whether the change
needs to be additive/backward-compatible with already-archived inventories
or whether older versions can simply be dropped from
`SUPPORTED_INVENTORY_VERSIONS`.

### Unreadable files (`unreadable_files[]`)

`_checksum_one()` (`archive_common.py:81`) catches `OSError` (e.g.
`PermissionError`) around the read/hash step of a regular file and returns
a skip reason instead of letting the exception propagate. `build_inventory()`
then does three things for each such file: prints an unconditional (not
`--verbose`-gated) warning to stderr, appends `{"relative_path": ...,
"reason": "unreadable: ..."}` to this list, and simply omits the file from
`files[]`/the directory rollup — the archive run continues and completes
normally with everything else.

### Permission handling

Every restored file's permission bits come from the `mode` its inventory
record recorded at archive time, applied explicitly — never inherited from
whatever the transport happened to produce:

- **All files**, whichever storage path they took:
  `restore_file_permissions()` (`archive_common.py`, shared by all three
  restore scripts) `os.chmod()`s each restored file to its recorded `mode`
  once all content is written. Symlinks are skipped — `os.chmod()` follows
  the link and would change the *target's* mode, and a symlink's own bits
  are meaningless on Linux.
- **Large files** (their own S3/Globus/local object) are *also* chmod'ed at
  download time by `download_file_from_s3()` (`restore_from_s3.py`), the
  post-transfer pass in `restore_from_globus.py:run()`, and
  `copy_file_from_local()` (`restore_from_local.py`). That's redundant with
  the pass above, but harmless, and keeps each object correct the moment
  it lands.
- **Small files** (bundled into a tar group) *used* to rely on `tarfile`
  reproducing `st_mode` as a side effect of `tar.extract()`. That stopped
  being dependable: where the interpreter supports extraction filters
  (PEP 706 — Python ≥3.12, ≥3.14 by default, and backported into some
  distributions' 3.9), the default filter **clears permission bits**, so
  `0664` came back as `0644` and setgid was dropped silently, while large
  files kept theirs — two files with identical modes restoring differently
  depending on which side of `--size-cutoff` they fell on. `--verify-checksums`
  never caught it, since it only compares content hashes. `extract_tar()`
  now pins `filter="fully_trusted"` where the parameter exists (these are
  archives this tool produced, and the default filter also rejects the
  absolute/escaping symlink targets archiveTree deliberately records), and
  `restore_file_permissions()` makes the final mode correct regardless of
  interpreter behavior.
- **Ownership** (uid/gid) is *not* restored by tar extraction in practice,
  since `tarfile.chown()` only attempts `os.chown()` when running as root.
- **Directories**: `restore_directory_permissions()` (`archive_common.py`,
  shared by all three restore scripts) runs once, after `restore_file_permissions()`
  — files first, so a directory whose recorded mode lacks write/execute
  can't block a chmod still to come inside it — in two passes over
  `directories[]`:
  1. **Create.** Tar extraction only creates the parent dirs its member
     files need, so a directory that was empty in the original tree (zero
     files anywhere in its subtree) never otherwise gets created. This
     pass `os.makedirs()`s any recorded directory that doesn't already
     exist under `restore_root` and is in scope of the restore: always, for
     a full restore, or (for a `--only-prefix` subset restore) only a
     directory at or under one of the given prefixes. An empty directory
     outside a `--only-prefix` subset is correctly left uncreated, same as
     any file outside that subset. `--only-path` alone brings no empty
     directory into scope, since it names individual files, not trees —
     which requires the function to receive **both** filters: knowing only
     about `--only-prefix` made a `--only-path`-only restore
     indistinguishable from an unfiltered one, and it would materialize
     the archived tree's entire directory skeleton around a single
     restored file.
  2. **Chmod.** `os.chmod()`s every directory that has a recorded `mode`
     *and* now exists under `restore_root` (either just created above, or
     already populated by file extraction), using the mode from its
     `directories[]` record, deepest-first (most path separators first) —
     restoring a restrictive parent mode (e.g. one missing the
     execute/search bit) before a still-to-be-touched child would make
     that child unreachable for its own `chmod()` call, so children are
     always done first. A directory with a `null` `mode` (its `lstat()`
     failed at archive time) is simply skipped.
- **Ownership** is recorded (the `owner` username string) but never
  restored by anything in this codebase, for either storage path or either
  files/directories.

## Data integrity and validation

This section ties together every check that stands between "wrote a byte"
and "user restores a corrupted file three years later." Each piece is
described in more depth elsewhere in this document (linked below); this is
the map.

1. **Preflight: is the destination even usable?** Before any inventorying
   or tar-building, each backend confirms its destination is reachable
   *and actually writable* — a real write-and-delete probe, not a
   permissions-bit check — so a bad bucket/collection/mount fails in
   seconds, not after hours of walking and hashing. See the **Preflight
   checks** paragraph at the top of [Archive flow](#archive-flow) for the
   per-backend mechanics (`probe_write_access()`, `operation_ls()`, etc.).

2. **Source-of-truth checksum, computed once, before packing.**
   `_checksum_one()` (`archive_common.py:81`) computes a SHA256 over every
   regular file's contents (or a symlink's target string) as
   `build_inventory()` walks the tree — this value is recorded in the
   `files[]` inventory record and never recomputed from the original at
   archive time again. It's the baseline everything downstream — both the
   archive-time upload checks and any later restore-time re-verification —
   is checked against.

3. **Upload/transfer verification, per backend.** Every object is checked
   immediately after it lands at the destination, before the run proceeds:
   - **S3**: `upload_file_to_s3()` always verifies destination size via
     `head_object`, and — when the endpoint supports it, per an up-front
     `probe_checksum_support()` capability probe — additionally compares a
     whole-object CRC64NVME checksum computed locally against the one S3
     computed server-side during upload. See
     [Upload checksum verification (S3)](#upload-checksum-verification-s3)
     for why CRC64NVME rather than the SHA256 already sitting in the
     inventory.
   - **Globus**: transfer tasks run with `verify_checksum=True`/
     `sync_level="checksum"` (on by default; `--no-verify-checksum-transfer`
     disables it) — Globus's own server-side, end-to-end checksum
     verification of the transfer itself, additive to the SHA256 already
     recorded in the inventory.
   - **Local**: `copy_file_to_local()` always compares destination size to
     source after every `shutil.copy2`, and, when `--verify-checksum` is
     passed, additionally re-hashes the destination with SHA256 and
     compares it to the source's inventory checksum (or, for a tar object,
     to a checksum computed from the freshly-built local tar just before
     the copy).

   A verification failure raises and aborts the run in all three
   backends — it never continues with unconfirmed data. See the closing
   paragraph of [Upload checksum verification (S3)](#upload-checksum-verification-s3)
   for the S3 case specifically; the Globus and local paths propagate the
   underlying `TransferAPIError`/mismatch the same way, uncaught.

4. **Delete only after everything is verified.** `--delete` (all three
   archive scripts) runs `shutil.rmtree()` on the source *only* after
   every object has been uploaded/transferred and verified, and the
   inventory itself has been written to disk and uploaded — see step 7 of
   [Archive flow](#archive-flow). This ordering means a mid-run crash can
   never produce a state where the source is gone but the archive isn't
   fully intact: worst case is wasted space (orphaned objects with no
   inventory pointing at them yet) or a redundant, still-intact source
   sitting next to a complete, valid archive.

5. **Object-level re-check at fetch time.** `verify_object_checksum()`
   (`archive_common.py`) compares a just-downloaded/located object against
   whatever whole-object checksum the inventory recorded for it, before its
   contents are trusted: `crc64nvme` for S3 objects (when the endpoint
   supported whole-object checksums), `sha256` for local tar objects (when
   the archive ran with `--verify-checksum`). Globus objects carry neither,
   since Globus verifies checksums in transit itself. This runs
   unconditionally — no flag — and is strictly cheaper and more precise
   than (5): it catches a damaged object at the object level, where the
   error can name it, rather than as an opaque extraction failure.

6. **Independent re-verification at restore time.** `--verify-checksums`
   (all three `restore_from_*.py` scripts) runs `verify_restored_files()`
   (`archive_common.py:435`) after restore completes: it re-hashes every
   restored file (or symlink target) from disk and compares it to the
   SHA256 recorded in the inventory at archive time, raising on any
   `missing` or `checksum_mismatch`. This is the one check that verifies
   the *entire* chain end-to-end — archive-time read, upload, storage,
   download, and extraction all have to have gone right for it to pass —
   independent of whatever backend-specific verification already happened
   at archive time. See step 7 of [Restore flow](#restore-flow).

Together, (2)+(3) guard the archive-time path (did the bytes that left
this machine arrive intact), while (5) and (6) guard the restore-time path
(did the bytes that come back match what was originally read). They're
independent checks — passing one doesn't imply the other — which is why
`--verify-checksums` on restore still has value even for a backend (like
Globus) that already verifies its transfers server-side: it also catches
corruption from extraction, filesystem, or storage-at-rest issues that
transfer-level verification can't see.

One thing none of these checks cover: **permission bits**. Every one of
them compares content, so a restore whose modes were silently altered
passes cleanly — which is exactly how the tar-extraction filter regression
described under [Permission handling](#permission-handling) went unnoticed.
Mode correctness rests on `restore_file_permissions()`/
`restore_directory_permissions()` applying what the inventory recorded, not
on verification catching a discrepancy afterward.

## `--verbose` configuration listing

`archive_common.print_config(verbose, label, settings)` prints a sorted
`key = value` listing under a `label:` header when `verbose` is true, and
is a no-op otherwise. Every `archive_to_*.py`/`restore_from_*.py` script
calls it once near the top of `run()`, after resolving every setting that
comes from more than one source (CLI flag / env var / config file), so the
listing always shows the value actually in effect, not just what was
passed on the command line. It's called *after* config resolution but
*before* anything that requires those values to be valid — printed even
on `--dry-run`, and even when a value is still `None` because it hasn't
been validated yet (`archive_config.require()`/`globus_config.require()`
run separately, after this).

For the two Globus scripts specifically, this required splitting
`globus_config.resolve()` (informational, no validation, safe to call
unconditionally) from `globus_config.require()` (the actual "fail if
missing" check): `source_collection`/`source_mount`/`dest_collection`
(`archive_to_globus.py`) and `dest_collection`/`dest_mount`
(`restore_from_globus.py`) are `resolve()`d unconditionally so the listing
reflects them even on a `--dry-run` (or, for restore, even before the
dry-run early exit), but are only `require()`d later, at the point they're
actually needed.

## Archive flow

`archive_to_s3.py`, `archive_to_globus.py`, and `archive_to_local.py` all
follow the same sequence; only the upload/transfer mechanics differ.

**Preflight checks** run first, before any inventorying or tar-building,
so a destination that can't actually be written to fails immediately
instead of after however long walking and hashing the whole tree took.
Skipped entirely on `--dry-run`, which never touches the destination:

- **S3**: `s3_client.head_bucket()` confirms the bucket exists and is
  reachable, then `probe_write_access()` (`archive_to_s3.py`) uploads and
  deletes a tiny throwaway object under `object_path` to verify actual
  `PutObject` permission — `head_bucket` alone only proves read-level
  access to the bucket, not write access to the target prefix.
- **Globus**: `transfer_client.operation_ls()` is called against both
  `--source-mount` on the source collection and the root of the
  destination collection, via `globus_transfer.call_with_auth_retry()`
  (which self-heals from the same `ConsentRequired`/
  `session_required_single_domain` errors `submit_and_wait()` handles —
  see [Globus login error recovery](#globus-login-error-recovery) below).
  The source-side check doubles as validation that `--source-mount` is
  actually a real, listable path on that collection — the single most
  common Globus setup mistake (see Troubleshooting in README.md). The
  `TransferClient` obtained here is reused for the rest of the run rather
  than re-fetched later.
- **Local**: `probe_write_access()` (`archive_to_local.py`) creates and
  removes a tiny throwaway file in `dest_dir`, rather than trusting
  `os.access()`'s permission-bit check alone — which can be misleading
  (e.g. running as root bypasses it) or miss real-world failure modes a
  plain stat can't see, like a read-only NFS export or a full
  filesystem/quota.

1. **`build_inventory(root_dir)`** (`archive_common.py:144`) walks the tree
   once, computing every file's checksum in parallel (`ThreadPoolExecutor`,
   `--max-workers`), then rolls up per-directory stats. Returns the inventory
   dict described above, with `files[]` in deterministic `os.walk()` order.

2. **`partition_by_size(files, size_cutoff)`** splits records into
   `large_files` (> `--size-cutoff`, default 1 GB) and `small_files`.

3. **`group_small_files(small_files, size_grouping)`** greedily bins the
   small files into groups of ~`--size-grouping` bytes each (default 10 GB) —
   simple running-total bin packing, not size-balanced, just threshold-based
   (`archive_common.py:279`).

4. A flat **job list** is built: one `"file"` job per large file, one
   `"tar"` job per small-file group, with sequential `object_id`s.

5. **Upload/transfer** (this is where the three backends diverge):

   - **S3** (`archive_to_s3.py:624` `process_job`): each job — file or tar —
     is handled by one worker in a single `ThreadPoolExecutor`. A `"tar"`
     job calls `create_tar()` to build the tar *and* uploads it, all inside
     that one worker; `upload_file_to_s3()` uploads then immediately does a
     `head_object` to verify the remote size and record the actual storage
     class, plus — when supported — a whole-object CRC64NVME checksum
     comparison (see [Upload checksum verification](#upload-checksum-verification-s3)
     below). Each object is a synchronous `boto3` call.

   - **Globus** (`archive_to_globus.py`): transfer tasks are async and
     server-managed, so the flow is split into two decoupled passes.
     First, all tar groups are built *locally* in parallel
     (`build_tar_job`, its own `ThreadPoolExecutor` pass, `:345`) — pure
     local CPU/disk work, no network yet. Then every job (large files +
     built tars) is added as an item to one or more `TransferData` tasks
     (`globus_transfer.batches_by_count_and_bytes()`, `--max-items-per-task`,
     default 10000) and submitted via `globus_transfer.submit_and_wait()`,
     which polls `task_wait()` until the task completes (`:422`).

   - **Local** (`archive_to_local.py:process_job`): structurally identical
     to S3's model — each job, file or tar, is handled synchronously by one
     worker in a single `ThreadPoolExecutor`; a `"tar"` job calls
     `create_tar()` then copies it. `copy_file_to_local()` copies via
     `shutil.copy2` and always verifies destination size against source;
     when `--verify-checksum` is passed it additionally re-hashes the
     destination with SHA256 and compares it to the checksum already on the
     file's inventory record (or, for tar objects, to a SHA256 computed from
     the freshly-built local tar just before the copy — there's no
     per-tar checksum otherwise). There's no CRC64NVME-style machinery here:
     that exists purely to work around S3's inability to return a genuine
     whole-object checksum for a multipart upload except via the CRC
     family; a same-machine filesystem copy has no such limitation, so a
     direct SHA256 comparison suffices.

   In all three cases, each file's record gets `object_id`/`object_type`/
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

## Globus login error recovery

`globus_transfer._relogin_for_transfer_error(err, ...)` centralizes
recovery from the two `TransferAPIError` kinds a Globus operation can hit
that are fixable by logging in again rather than being fatal:

- **`ConsentRequired`**: re-runs the interactive login with the
  additionally required scopes (`err.info.consent_required.required_scopes`).
- **`session_required_single_domain`**: a collection restricting access to
  identities from a specific institutional domain (e.g. `yale.edu`); the
  cached login predates `--login-domain` being set, or was never asked for
  that domain. Re-runs the interactive login requesting a session for the
  domain named in the error (or `--login-domain`, if given and the error
  lists more than one acceptable domain).

Either way it returns a fresh `TransferClient` built from the updated
token cache; any other error is re-raised as-is. `call_with_auth_retry(func,
transfer_client, ...)` wraps this into a generic "call this, and if it
hits a recoverable auth error, relogin and retry once" helper — used by
both `submit_and_wait()` (transfer submission) and the archive scripts'
preflight `operation_ls()` calls (see [Archive flow](#archive-flow)
above), so both get the same self-healing behavior instead of duplicating
the recovery logic.

## Upload checksum verification (S3)

Every `upload_file_to_s3()` call (`archive_to_s3.py:256`) verifies more than
just size when possible: it asks S3 to compute a whole-object CRC64NVME
checksum (`ChecksumAlgorithm=CRC64NVME`, `ChecksumType=FULL_OBJECT`) during
upload, reads it back via `head_object(ChecksumMode=ENABLED)`, and compares
it to a CRC64NVME computed locally over the same bytes just before upload
(`compute_crc64nvme_b64()`, `archive_to_s3.py:92`). This catches silent
corruption in transit that a size-only check would miss.

**Why CRC64NVME, not the SHA256 already in the inventory:** S3 only
supports whole-object checksums for *multipart* uploads with the CRC family
of algorithms (CRC32 / CRC32C / CRC64NVME). SHA256 (and SHA1) multipart
checksums are always *composite* — a hash of the per-part hashes, not a
hash of the whole object — so they can never be compared against a single
local hash of the whole file. Since almost everything this tool uploads
(large files, and especially tar groups) is well above s3transfer's default
multipart threshold, SHA256 verification would essentially never apply in
practice; CRC64NVME is the only algorithm S3 lets you get a genuine
whole-object checksum from for both single-part and multipart uploads.

**Capability probe:** before doing any real work, `probe_checksum_support()`
(`archive_to_s3.py:185`) uploads and verifies one tiny throwaway object,
deliberately forced through the *multipart* code path
(`TransferConfig(multipart_threshold=1, ...)`) even though the payload is
tiny — testing the single-part path would give a false positive, since a
plain `PutObject` is always a whole-object checksum regardless of
algorithm. This decides `strict_checksum` for the whole run.

**Graceful degradation** to size-only verification, with a printed warning,
happens in two cases:

- the `awscrt` package (needed to compute CRC64NVME locally — `boto3`
  itself also needs it to compute CRC64NVME on the wire, so if it's missing
  here it's missing there too) isn't installed;
- the probe determines the target endpoint doesn't support whole-object
  CRC64NVME checksums (e.g. an older or non-AWS S3-compatible service).

`--no-checksum-verify` skips the probe entirely and always uses size-only
verification. A genuine checksum *mismatch* during the real archive (as
opposed to the probe, which only ever downgrades to size-only) raises and
aborts the run before the source tree is deleted, same as any other upload
failure.

## Restore flow

`restore_from_s3.py`, `restore_from_globus.py`, and `restore_from_local.py`
again all share the same shape, diverging on transfer mechanics.

1. **Load + validate**: `load_inventory_file()` then
   `check_inventory_version()`, then a backend guard — S3 requires
   `archive.backend` to be absent or `"s3"` (`restore_from_s3.py:571`) and
   also requires `archive.s3_bucket`; Globus requires
   `archive.backend == "globus"` (`restore_from_globus.py:201`); local
   requires `archive.backend == "local"` and `archive.local_dest_dir` — before
   any script touches the objects themselves.

2. **`select_relpaths()`** resolves `--only-path` (exact match, repeatable)
   and `--only-prefix` (repeatable) against `files[]`. With neither flag,
   everything is selected. `--only-prefix` matching goes through
   `path_matches_prefix()`, which is **path-boundary aware**: a prefix of
   `logs` selects `logs` and everything under `logs/`, but not a sibling
   `logs2`. (It was a plain `str.startswith()` originally, which silently
   over-restored into any same-prefixed sibling; `browse_inventory.py`
   works around that by always emitting a trailing `/`, which remains
   correct — trailing slashes are ignored.)

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

   - **Local** (`restore_from_local.py`): no preflight either — objects
     under `local_dest_dir` are always immediately available, there's no
     tiered-storage concept. One `ThreadPoolExecutor` worker per object
     either `shutil.copy2`s a `"file"`-type object straight to its final
     destination path, or, for a `"tar"`-type object, calls
     `extract_tar(..., selected_relpaths=rels)` **directly against its
     location under `local_dest_dir`** — unlike S3/Globus, there is no
     scratch-dir download step at all, since the object is already a local
     file; copying it to `--scratch-dir` first would just be a redundant
     local-to-local copy.

   In all three cases, an object that carries a whole-object checksum in
   its inventory record (`crc64nvme` for S3, `sha256` for local tars — see
   [Data integrity and validation](#data-integrity-and-validation)) is
   re-checked by `verify_object_checksum()` the moment it's fetched, before
   anything reads its contents. A corrupt object is then reported as
   exactly that, naming the object id, instead of surfacing as a confusing
   tar-extraction failure or a pile of mismatched files much later.

6. **`restore_file_permissions()` then `restore_directory_permissions()`**
   (`archive_common.py`, unconditional, no flag): every restored file is
   chmod'ed to its recorded `mode`, then any recorded, in-scope directory
   that doesn't already exist is created (e.g. one that was empty in the
   original tree) and every directory with a recorded `mode` under
   `restore_root` is chmod'ed deepest-first. Files before directories, so a
   restrictive directory mode can't block a file chmod inside it. See
   [Permission handling](#permission-handling) above.

7. **Optional `--verify-checksums`**: `verify_restored_files()`
   (`archive_common.py:435`) re-hashes every restored file (or symlink
   target) and compares to the inventory's recorded `sha256`, raising on any
   `missing`/`checksum_mismatch`.

8. **Optional `--summary-csv`**: `write_summary_csv()` writes
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

## Unified entrypoints (`archive.py` / `restore.py`)

`archive.py` and `restore.py` are thin dispatchers, not a merged
implementation of the three backends — S3, Globus, and local have
fundamentally different transfer execution models (S3: synchronous
per-object calls in a `ThreadPoolExecutor`; Globus: one or a few async,
server-managed transfer tasks; local: synchronous per-object filesystem
copies in a `ThreadPoolExecutor`, structurally like S3 but with no network
transport at all), and collapsing them into one shared loop was
deliberately avoided (see [Backend differences](#backend-differences)
below). What they unify is just the *entrypoint*: pick a backend, then hand
off to that backend's existing, unmodified logic.

This works because all six backend scripts (`archive_to_s3.py`,
`archive_to_globus.py`, `archive_to_local.py`, `restore_from_s3.py`,
`restore_from_globus.py`, `restore_from_local.py`) are each split into
`build_arg_parser() -> argparse.ArgumentParser` and `run(args) -> None`,
with a thin `main() = run(build_arg_parser().parse_args())` kept for fully
backward-compatible standalone invocation. The dispatchers call
`build_arg_parser()` and `run()` directly — in-process function calls, not
a subprocess — so `archive.py --backend s3 ...` is behaviorally
indistinguishable from calling `archive_to_s3.py ...` directly with the
same flags.

**Backend resolution** (`archive_config.resolve_backend()`, called from
`restore.py`'s `main()`): `--backend` on the command line, else — for
`restore.py` only — auto-detected from the inventory file itself via
`archive_common.detect_backend()`, else the `backend` key in the config
file's `[archive]` section. Auto-detection deliberately outranks the
config default: the inventory already unambiguously records which backend
archived it, so a `backend = ...` left over from some other archive run
(a very likely scenario, since the same config file is shared across all
three backends) must never silently override that and send a restore into
the wrong backend's script — this was a real bug prior to this ordering,
where a stale config default beat the correct auto-detected backend with
no warning.

The auto-detect path is best-effort: `restore.py`'s
`_guess_inventory_file()` scans argv for the first token that isn't a flag
and isn't the value of a preceding one, using `_value_taking_flags()` (a
union, derived from all three backends' actual `build_arg_parser()`
output via their actions' `nargs`, of every flag that consumes a value —
not just `--config-file`, so a flag like `--restore-dir X` ahead of the
inventory path on the command line doesn't get mistaken for it) to know
which tokens to skip. If the guess is still wrong, or the file isn't a
readable inventory, detection just falls through to the config default (or
the "could not determine backend" error if there isn't one either) — it
never causes a *silent* wrong-backend dispatch, since each backend
script's own guard (see [Backend differences](#backend-differences) above)
still catches an inventory it didn't produce and names the correct script
to use instead. `archive.py` has no such fallback, since there's no
inventory file to inspect before archiving.

**Why the dispatchers hand-parse `--backend`/`--config-file` instead of
using `argparse.parse_known_args()`:** `extract_dispatch_flags()`
(`archive.py`) does plain string matching for exactly those two flags and
leaves everything else — including flags neither dispatcher has ever heard
of, and their values — completely untouched, in original order, for the
backend's own parser to consume. An `argparse.parse_known_args()`-based
peek was tried first and turned out to be unsafe: since that parser doesn't
know which unrecognized flags take a value, an invocation like
`archive.py --backend s3 --storage-class GLACIER mydir` could misclassify
`GLACIER` as the positional `directory` argument (it has no way to know
`--storage-class` consumes the next token). Plain string matching on the
two flags this module actually owns has no such ambiguity.

**Config file**: the same physical file used by the Globus scripts today
(default `~/.archive.cfg`, overridable via `--config-file` /
`GLOBUS_ARCHIVE_CONFIG`), generalized to hold multiple named INI sections —
`[archive]` (`backend = s3|globus|local`), `[s3]`
(`bucket`/`object_path`/`profile`/`endpoint_url`/`storage_class` for
archiving, `profile`/`endpoint_url` for restoring), `[local]` (`dest_dir`),
and the pre-existing `[globus]` section, unchanged. `archive_config.py`
generalizes `globus_config.py`'s old CLI-flag/env-var/config-file
precedence helpers to take a `section` argument; `globus_config.py` is now
a thin wrapper around it, scoped to `[globus]`, kept purely for backward
compatibility so existing Globus config files and call sites need zero
changes.

## Backend differences

| | S3 | Globus | Local |
|---|---|---|---|
| Transfer unit | Individual synchronous `boto3` calls, one per object | Batched async `TransferData` tasks (poll to completion) | Individual synchronous `shutil.copy2` calls, one per object |
| Verification (archive) | App-level: `head_object` size check after each upload, plus a whole-object CRC64NVME checksum comparison when supported (see [Upload checksum verification](#upload-checksum-verification-s3)) | Server-side: `verify_checksum=True` / `sync_level="checksum"` on the transfer itself | App-level: destination file size check after each copy, plus an opt-in (`--verify-checksum`) SHA256 comparison against the source's already-computed inventory checksum |
| Verification (restore) | Size check, plus `verify_object_checksum()` against the recorded `crc64nvme` when the object has one | Size/checksum verified server-side by the transfer itself; no per-object checksum is recorded to re-check | Size check, plus `verify_object_checksum()` against the recorded `sha256` when the archive ran with `--verify-checksum` |
| Tiered storage | Yes — Glacier/Deep Archive preflight + restore-request flow. `GLACIER_IR` is *not* treated as cold: its objects are readable immediately and `RestoreObject` against them is rejected | No such concept; Globus collections aren't tiered | No such concept; local objects are always immediately available |
| Auth | AWS profile / env creds (`boto3` default chain) | Native App OAuth device flow, cached refresh token (`globus_auth.py`) | None — just filesystem permissions on `dest_dir` |
| Path model | Global flat key namespace (`s3_key`) | Collection-relative paths (`globus_path`); local paths must be mapped via `--source-mount`/`--dest-mount` (`globus_transfer.local_path_to_collection_relative()`) — everything you touch (source dir, scratch dir, inventory file, restore dir) must live under the configured mount, or `require_under_mount()` exits with an error | Ordinary filesystem paths (`local_path`, relative to `local_dest_dir`) — `dest_dir` must already be mounted and reachable; this tool never mounts anything itself |
| Restore of tar objects | Downloaded to `--scratch-dir`, then extracted | Downloaded (via transfer task) to `--scratch-dir`, then extracted | Extracted directly from its location under `local_dest_dir` — no scratch-dir copy at all, since the object is already local |
| Config resolution | CLI flag → `[s3]` config section → error, via `archive_config.resolve()`/`require()` (`archive_config.py:38`); no env-var step, and only `bucket`/`object_path`/`profile`/`endpoint_url`/`storage_class` are config-fillable | CLI flag → env var → `[globus]` config section → error, via `globus_config.resolve()`/`require()`, now a thin wrapper around `archive_config.py` (`globus_config.py:18`) | CLI flag → `[local]` config section → error, via `archive_config.resolve()`/`require()`; no env-var step, only `dest_dir` is config-fillable |

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

One exception to "each worker only touches data it owns": tar extraction
during a restore writes into a *shared* filesystem namespace — the restore
tree — not an exclusively-owned data structure, so two workers restoring
different tar objects that both need an as-yet-uncreated shared parent
directory (e.g. two tar groups that both contain files under `b/`) can
genuinely race on creating it. `tarfile.TarFile._extract_member()`'s
directory creation is a non-atomic check-then-create (`if not
os.path.exists(upperdirs): os.makedirs(upperdirs)`, no `exist_ok`) for a
member's parent directories, so the losing thread raises `FileExistsError`
even though nothing is actually wrong (directory-type members don't have
this problem — tarfile's own `makedir()` already catches
`FileExistsError`). `_extract_member()` in `archive_common.py` (not to be
confused with the stdlib method of the same name) wraps `tar.extract()`
and retries once on `FileExistsError` — safe because by the time the
exception fires, the directory has already been created by the winning
thread, so the retry's `exists()` check passes and it proceeds normally.
This is the one place in the codebase where concurrent workers can observe
each other's side effects, and a retry (not a lock) is enough to make it
safe.

## Developing/testing without cloud credentials

`archive_common.py` has no `boto3`/`globus_sdk` dependency, so you can
exercise most of the interesting logic — inventory building, the rollup,
gzip round-tripping, `browse_inventory.py`'s tree/selection/script logic —
without any credentials at all. `archive_to_local.py`/`restore_from_local.py`
take this further: they're a fully real backend (real copies, real
inventory, real restore) with no SDK dependency and no credentials needed
at all — just a writable directory to use as `dest_dir` — making them the
easiest way to exercise a genuine end-to-end archive/restore round trip
while developing, including the tar-grouping and multi-worker-restore paths.

```python
import archive_common as ac

inventory, _paths = ac.build_inventory("/path/to/some/test/tree", max_workers=4)
ac.write_inventory_file(inventory, {"backend": "s3", "s3_bucket": "fake-bucket"}, objects=[], inventory_path="test.inventory.json.gz")
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
