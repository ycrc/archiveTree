# archiveTree

Long-term archival of large directory trees, with a browsable inventory and
selective restore.

Deep storage tiers (S3 Glacier/Deep Archive, tape-backed Globus
collections, a cold NFS/Lustre mount) are cheap, but they're built for
storing whole objects, not for living, POSIX directory trees with millions
of small files. Uploading each file individually is slow and expensive at
scale, and once a tree is archived, most tools give you nothing better than
"pull the whole thing back" if you later need just one subdirectory.
archiveTree exists to close that gap: it packs a directory tree into
size-grouped tar bundles for efficient transfer, verifies every file's
checksum before it lets you delete the original, and records exactly what
went where in a JSON **inventory** file — so restoring later, in whole or
in part, doesn't depend on remembering anything.

That inventory is the other half of the point. It's a self-contained map
of the archived tree (paths, sizes, checksums, owners, permissions, and
which archive object holds each file) that `browse-inventory` turns into
an `ncdu`/`xdu`-style browser, months or years later, to find and restore
exactly the files you need without touching the rest. The same archive/restore
workflow works identically against S3, a Globus collection, or a plain
locally-mounted directory, so the backend is a config choice, not a
rewrite.

For how this works internally (inventory format, archive/restore flow,
backend differences), see [ARCHITECTURE.md](ARCHITECTURE.md).

---

## Data integrity

Every step of an archive run is built to fail loudly rather than silently
lose or corrupt data:

- **Destination checked before any work starts.** Each backend probes that
  the destination is actually reachable and writable — a real write probe,
  not just a permissions check — before spending any time walking or
  hashing the source tree, so a bad bucket/collection/mount fails in
  seconds, not after hours of work.
- **Every file is checksummed at the source.** While walking the tree,
  archiveTree computes a SHA256 of each file's contents (or, for a
  symlink, its target string) and records it in the inventory *before*
  anything is packed or transferred — this becomes the permanent baseline
  everything downstream is checked against.
- **Every upload/transfer is verified**, using whichever mechanism the
  backend actually supports: S3 compares a whole-object checksum computed
  on both ends (falling back to a size check if the endpoint can't do
  that); Globus verifies the transfer server-side by default; the local
  backend always checks destination size and can optionally re-hash and
  compare. A verification failure aborts the run rather than continuing
  with unconfirmed data.
- **The source is only deleted once everything else has succeeded.**
  `--delete` removes the original directory tree only after every object
  is uploaded and verified and the inventory itself is safely written and
  stored — so a crash mid-archive can, at worst, leave some wasted space
  behind, never a deleted source with no usable copy.
- **Fetched objects are re-checked before they're trusted.** On restore,
  every archive object that carries a whole-object checksum is re-verified
  the moment it's downloaded — before anything is extracted from it — so
  storage bit-rot or a truncated download is reported against the object
  that's actually damaged, not as a confusing failure later on.
- **Restores can be independently re-verified.** `--verify-checksums` on
  any restore re-hashes every restored file and compares it to the
  checksum recorded at archive time, catching corruption anywhere between
  the original archive and the restored copy — including issues the
  archive-time upload check couldn't have seen (extraction bugs, storage
  bit-rot, and the like).
- **Permissions are restored from the inventory, not guessed.** Every
  file's and directory's recorded POSIX mode is reapplied explicitly after
  a restore, so permission bits survive the round trip regardless of how
  the file was stored (bundled in a tar or as its own object) or which
  Python version does the extracting.

For the exact mechanics behind each of these — which function does what,
and why S3 needs a different checksum algorithm than the other two
backends — see
[Data integrity and validation](ARCHITECTURE.md#data-integrity-and-validation)
in ARCHITECTURE.md.

---

## Installation

**archiveTree is currently a private repository**, so every install below
goes over SSH and requires GitHub access to the `ycrc` organization. Work
through the prerequisites once, then pick whichever install matches how you
already manage Python.

### Prerequisites (once per user)

> **Read this section even if you already use GitHub over SSH.** `uv` and
> `pip` run `git` in a subprocess with no controlling terminal, so ssh
> cannot ask you anything. A setup that works fine when you type `git pull`
> by hand can still hang the installer forever. The requirements below are
> what make the install work *without any prompting*.

**1. Read access.** Your GitHub account needs read access to
`ycrc/archiveTree`. Ask a YCRC admin if the verification command at the end
of this section fails with a permissions error.

**2. An SSH key that can be used without a passphrase prompt.** This is the
one that catches people. If you have no GitHub key on this machine yet,
create one with no passphrase:

```bash
ssh-keygen -t ed25519 -C "$USER@$(hostname -s)-github" -f ~/.ssh/id_ed25519_github -N ""
cat ~/.ssh/id_ed25519_github.pub
```

Add that public key at GitHub → Settings → SSH and GPG keys → **New SSH
key**.

If you would rather use an existing passphrase-protected key, you must load
it into an agent *in the shell you install from*, otherwise the install
hangs (see [Troubleshooting](#troubleshooting-the-install) below):

```bash
eval "$(ssh-agent -s)"
ssh-add ~/.ssh/your_key       # type the passphrase once
```

**3. SAML SSO authorization.** If the `ycrc` organization enforces single
sign-on, the key must additionally be *authorized for the organization*:
GitHub → Settings → SSH and GPG keys → **Configure SSO** next to the key →
Authorize. Skipping this produces a generic permission-denied error that
never mentions SSO, so check it first if access fails.

**4. An `~/.ssh/config` entry.** Recommended, and necessary if you have
several keys — ssh offers them one at a time and GitHub drops the
connection after six failures, so a valid key further down the list may
never be reached:

```
Host github.com
    HostName github.com
    User git
    IdentityFile ~/.ssh/id_ed25519_github
    IdentitiesOnly yes
```

`User git` is not a placeholder — GitHub's SSH user is always the literal
string `git`, never your GitHub username. `IdentitiesOnly yes` stops ssh
from parading every key you own past the server.

**5. GitHub's host key in `known_hosts`.** With prompts unavailable, an
unknown host key aborts the install with `Host key verification failed`
instead of offering to accept the fingerprint:

```bash
ssh-keyscan github.com >> ~/.ssh/known_hosts
```

### Verify before installing

Run this *exact* command. `BatchMode=yes` forbids ssh from prompting for
anything, which is precisely the constraint the installer runs under — so
if this succeeds, the install will work:

```bash
GIT_SSH_COMMAND="ssh -o BatchMode=yes" git ls-remote git@github.com:ycrc/archiveTree.git
```

It should print a list of refs including `refs/tags/0.7.0`. Do not proceed
until it does; see below if it fails.

### uv (recommended)

[`uv tool install`](https://docs.astral.sh/uv/) puts the ten console
scripts on your `PATH` in a dedicated virtual environment:

```bash
uv tool install "archiveTree[checksums] @ git+ssh://git@github.com/ycrc/archiveTree@0.7.0"
```

### pipx

Same isolation, if you already use pipx:

```bash
pipx install "archiveTree[checksums] @ git+ssh://git@github.com/ycrc/archiveTree@0.7.0"
```

### pip

Works, but installs into whatever environment is active. Use a virtual
environment; see the warning below.

```bash
python3 -m venv ~/.venvs/archivetree
~/.venvs/archivetree/bin/pip install "archiveTree[checksums] @ git+ssh://git@github.com/ycrc/archiveTree@0.7.0"
```

### conda / mamba

There is no conda-forge package. Create an environment and install into it
with pip — the conda packages supply the compiled dependencies, pip supplies
archiveTree itself:

```bash
conda create -n archivetree -c conda-forge python=3.12 boto3 "globus-sdk<5" tqdm awscrt
conda activate archivetree
pip install "archiveTree @ git+ssh://git@github.com/ycrc/archiveTree@0.7.0"
```

(Omit `[checksums]` on the pip line here — conda already provided `awscrt`.)

### pixi (development)

For hacking on archiveTree itself, `pixi.toml` in this repo defines a
complete environment and installs the project in editable mode, so edits to
the `.py` files take effect immediately:

```bash
git clone git@github.com:ycrc/archiveTree.git
cd archiveTree
pixi shell          # console scripts are now on PATH
pixi run archive    # or run a task directly
```

### The `[checksums]` extra

`awscrt` is optional and ships as a compiled wheel, so it is not installed
by default. Adding `[checksums]` pulls it in and enables local CRC64NVME
computation, which lets `archive_to_s3.py` verify uploads against S3's
whole-object checksums before deleting source files, and lets the S3 restore
path re-check downloads against the inventory. Without it, archiveTree
falls back to size-only verification and prints a warning. Recommended if
you use the S3 backend with `--delete`.

> **A note on shared environments.** archiveTree currently installs its
> modules at the top level of `site-packages` under generic names —
> including `archive`, `restore`, `archive_config`, and `globus_config`.
> In an environment shared with other packages these names can collide, and
> a file named `archive.py` in your working directory will shadow
> archiveTree's. The isolated installs above (`uv tool`, `pipx`, a dedicated
> venv or conda env) avoid this entirely; prefer them over installing into a
> general-purpose environment.

### Getting an example config

The [Getting started](#getting-started) steps below start by copying
`archive.cfg.example`. That file lives in the source repository and is not
installed by pip/uv. Because the repository is private it cannot be fetched
anonymously over HTTPS, so pull it out of a shallow clone:

```bash
git clone --depth 1 --branch 0.7.0 git@github.com:ycrc/archiveTree.git /tmp/archiveTree-src
cp /tmp/archiveTree-src/archive.cfg.example ~/.archive.cfg
rm -rf /tmp/archiveTree-src
$EDITOR ~/.archive.cfg
```

If you already have a checkout (the pixi path above), just copy it from
there instead.

### Troubleshooting the install

**`uv tool install` hangs at `Resolving dependencies...` and never
finishes.** Almost always ssh waiting on a passphrase it cannot ask for.
The installer runs `git` with no controlling terminal, so ssh falls back to
`SSH_ASKPASS`; with `DISPLAY` set (X11 forwarding) it then blocks
indefinitely on a helper that cannot render a prompt, instead of failing.
Interrupt it and diagnose with the fail-fast form:

```bash
GIT_SSH_COMMAND="ssh -o BatchMode=yes" git ls-remote git@github.com:ycrc/archiveTree.git
```

`BatchMode=yes` turns the hang into an immediate error. Fix by loading the
key into an agent in this shell (`eval "$(ssh-agent -s)"; ssh-add
~/.ssh/your_key`) or by using a passphrase-free key, then reinstall. Note
that `GIT_TERMINAL_PROMPT=0`, which uv sets, only suppresses *HTTPS*
credential prompts — it does nothing about ssh.

**`Permission denied (publickey)`, but verbose output says `Server accepts
key`.** Run `ssh -v -o BatchMode=yes -T git@github.com`. If you see
`Server accepts key:` followed immediately by `No more authentication
methods to try`, GitHub recognised the key but ssh could not *sign* with
it — the private key is passphrase-protected and nothing can unlock it.
Same fix as above.

**`Permission denied (publickey)` with no key offered at all.** Check that
`~/.ssh/config` points `Host github.com` at the right `IdentityFile`, and
that it says `User git`. Confirm the key is the one GitHub has by comparing
`ssh-keygen -lf ~/.ssh/id_ed25519_github.pub` against the fingerprint shown
under GitHub → Settings → SSH and GPG keys.

**`ssh -T git@github.com` succeeds but `git ls-remote` is denied.** You
authenticated, but cannot read this repository. Either you lack access to
`ycrc/archiveTree`, or the key is not SSO-authorized for the organization
(prerequisite 3 above).

**`ls-remote` works but prints no `refs/tags/0.7.0`.** Auth is fine; that
tag does not exist on the remote. Check the repository's tags page and
substitute whichever tag is current.

**Testing a key while an agent is loaded.** Add `-o IdentityAgent=none` to
force ssh to use the key file rather than a cached agent identity —
otherwise a stale agent key can make a broken configuration look healthy.

**Stale state from an interrupted install.** Clear uv's cached checkout and
retry:

```bash
uv cache clean archivetree
```

---

## Getting started

This is the fast path: create a config file, archive a directory, browse
the result, and restore from it. Details on every option follow further
down.

**1. Create a config file.** Copy the combined example (it has a section
for every backend — S3, Globus, local — fill in only the one you're using)
and edit it:

```bash
cp archive.cfg.example ~/.archive.cfg
$EDITOR ~/.archive.cfg
```

See [`archive.cfg.example`](archive.cfg.example) for every setting,
inline-documented, and the [Configuration file](#configuration-file)
section below for precedence rules.

**2. Archive a directory**, using the `archive` wrapper. The backend comes
from `--backend`, or from `backend = ...` in the `[archive]` section of the
config file:

```bash
archive --backend local /path/to/mydata /mnt/archive_storage
```

This walks `/path/to/mydata`, hashes and tars up its files, copies/uploads
everything to the destination, and writes an inventory file next to the
source directory: `mydata.inventory.<archive_id>.json.gz`. Keep that file —
it's the only thing a restore needs. (With `--delete`, the source directory
is removed afterward, once every checksum has verified.)

**3. Browse the inventory and pick what to restore**, using
`browse-inventory`:

```bash
browse-inventory mydata.inventory.<archive_id>.json.gz
```

Navigate like a file manager, press `space` to mark files or whole
directories, then `w` to write a ready-to-run restore script for exactly
what's marked. See [The browser: `browse-inventory`](#the-browser-browse-inventory)
for the full keybinding reference.

**4. Restore.** Either submit the script `browse-inventory` wrote for you:

```sbatch 
./restore_<archive_id>.sh
```

or call `restore` directly for a full restore of everything in the
inventory:

```bash
restore mydata.inventory.<archive_id>.json.gz 
```

`restore` auto-detects the backend from the inventory file itself, so
`--backend`/config aren't needed here unless you want to override it.

---

## Requirements

- Python 3.10+

The [Installation](#installation) steps above handle everything below
automatically; this section is for reference, or for running the scripts
straight from a checkout without installing.

Installed by default:

- `boto3>=1.43` (for the S3 tools)
- `globus-sdk>=3.41,<5` (for the Globus tools; both the 3.x and 4.x lines
  are supported, and archiveTree adapts to the differences at runtime)
- `tqdm>=4.70` (enables progress bars with `--verbose`; every import site
  degrades gracefully to plain progress messages if it is absent, so
  `--no-deps` installs still work)

Optional, via the `[checksums]` extra:

- `awscrt>=0.23` (lets `archive_to_s3.py` verify uploads with S3's
  whole-object CRC64NVME checksum instead of falling back to size-only
  verification — see `--no-checksum-verify` below)

The local backend (`archive_to_local.py` / `restore_from_local.py`) needs
nothing beyond the Python standard library — it never imports `boto3` or
`globus_sdk`, so it works in any environment (and is a convenient way to
exercise most of this tool's logic without cloud credentials at all). The
two are installed unconditionally only to keep installation simple; if you
want a truly minimal local-only install, use `--no-deps`.

**`--verbose` on every `archive_to_*.py`/`restore_from_*.py` script (and
`archive`/`restore`, which forward it through)** prints a `Configuration:`
listing right at startup, before any real work begins: every setting that
went into this run, after resolving CLI flag / environment variable /
config file precedence — the actual bucket, `dest_dir`, Globus collection,
etc. in effect, not just what was passed on the command line. Useful for
confirming which value a setting actually resolved to, since several of
them can come from any of those three sources.

---

## Unified entrypoints: `archive`, `restore`, `browse-inventory`

These three commands (installed as console scripts by `pyproject.toml`; run
`python3 archive.py` / `restore.py` / `browse_inventory.py` directly if not
installed) are the recommended way to use archiveTree day to day. `archive`
and `restore` are thin dispatchers: they pick a backend, then hand every
other flag straight to that backend's own script (`archive_to_s3.py`,
`restore_from_globus.py`, etc.) untouched — so every flag documented later
in this README under [S3](#s3-backend), [Local](#local-backend), and
[Globus](#globus-backend) works exactly the same way through these wrappers.
Reach for the backend-specific scripts directly only when you don't want
the dispatch step, e.g. scripting against one backend exclusively.

### `archive`

```
archive [--backend {s3,globus,local}] [--config-file PATH] <backend-specific args...>
```

| Argument | Description |
|---|---|
| `--backend {s3,globus,local}` | Which backend to use. Optional if `backend` is set in the `[archive]` section of the config file. |
| `--config-file` | Config file path (default `~/.archive.cfg`). Also passed through to the backend script. |
| *(everything else)* | Forwarded as-is to the chosen backend's `archive_to_s3.py` / `archive_to_globus.py` / `archive_to_local.py` argument parser — see those sections below. |

If no backend is specified (neither `--backend` nor the config file) and
`-h`/`--help` isn't passed either, it exits with an error telling you how to
pick one. With `-h`/`--help` and no backend yet resolved, it prints a short
usage note instead (pass `--backend` first to see that backend's full flag
list).

Example:

```bash
archive --backend s3 --profile myprofile --verbose \
    --storage-class DEEP_ARCHIVE /path/to/mydata mybucket archive-prefix
```

### `restore`

```
restore [--backend {s3,globus,local}] [--config-file PATH] inventory_file <backend-specific args...>
```

| Argument | Description |
|---|---|
| `--backend {s3,globus,local}` | Which backend to use. Optional — see auto-detection below. |
| `--config-file` | Config file path (default `~/.archive.cfg`). Also passed through to the backend script. |
| *(everything else)* | Forwarded as-is to the chosen backend's `restore_from_s3.py` / `restore_from_globus.py` / `restore_from_local.py` argument parser — see those sections below. |

Backend resolution order: `--backend` flag, then **auto-detection** from
the inventory file itself (its `archive.backend` field) — the common case,
since almost every `restore` invocation already names an inventory file —
then, only if neither of those resolves one, `backend` in the `[archive]`
section of the config file. Auto-detection outranks the config default on
purpose: the inventory already unambiguously records which backend
archived it, so a leftover `backend = ...` left over from some other
archive run should never override that and silently send the restore into
the wrong backend's script. If none of those resolve a backend, it prints
an error (or, with `-h`/`--help`, a short usage note).

Example:

```bash
restore --restore-dir /path/to/restore --verify-checksums --verbose \
    mydata.inventory.<uuid>.json.gz
```

### The browser: `browse-inventory`

```
browse-inventory inventory_file
```

An interactive, full-screen (curses-based) browser for an inventory file —
think `ncdu`/`xdu`: navigate the archived tree, see per-directory file
counts and sizes without re-scanning anything, mark the files and/or
directories you want back, and write a ready-to-run restore script for
exactly that selection.

The root listing has a synthetic `[ALL]` entry at the top — marking it
selects the entire tree in one keystroke, equivalent to marking every
top-level entry. Marking a directory selects its whole subtree; you don't
need to also mark anything inside it (descendants of a marked directory are
shown with `+` instead of `*`, meaning "covered, not directly marked").

| Key | Action |
|---|---|
| `up`/`k`, `down`/`j` | Move the selection cursor. |
| `right`/`Enter` | Enter the highlighted directory. |
| `left`/`backspace` | Go up to the parent directory. |
| `space` | Toggle mark on the highlighted file/directory. |
| `s` | Cycle sort mode: name, size (desc/asc), file count (desc/asc). |
| `v` | Toggle whether the generated restore script passes `--verbose`. |
| `c` | Clear all marks. |
| `w` | Write a restore script for the currently marked items. |
| `d` | Debug view: list the archive objects (tars/individual files, with sizes) needed to satisfy the current selection, plus a preview of the restore command it would produce. |
| `x`/`Esc` | Exit (offers to write a restore script first if anything is marked). |

The header line shows a live summary as you mark things: how many files and
bytes will be *restored* (`Restore:`), versus how many distinct archive
objects and bytes must actually be *downloaded/transferred* to satisfy that
(`Archive read:`) — usually far fewer, since one tar object can supply many
restored files at once. The `d` debug view breaks that down object-by-object.

Pressing `w` (or answering `y` to the exit prompt) walks you through:

1. Restore to the **original** location recorded in the inventory, or a
   **new** one you type in.
2. Where to write the generated script (defaults to
   `restore_<inventory_id>.sh` in the current directory).

The script it writes is a `#!/usr/bin/env bash` file (with an `#SBATCH -c
4` header for convenience on Slurm clusters) that calls the right
`restore_from_*.py` for the inventory's backend, with `--only-prefix`/
`--only-path` flags for every marked directory/file (or no filters at all,
for a whole-tree selection), `--restore-dir` if you picked a new location,
`--verbose` if toggled on, and `--max-workers "$SLURM_CPUS_PER_TASK"`. It's
written executable (`chmod 755`) and ready to run or submit with `sbatch`
as-is.  

---

## Configuration file

All three backends share one config file location and format: a single INI
file, default `~/.archive.cfg`, overridable everywhere with
`--config-file PATH` (Globus additionally honors `$GLOBUS_ARCHIVE_CONFIG`).
One section per backend — `[s3]`, `[globus]`, `[local]` — plus `[archive]`
for the unified entrypoints' default backend choice. A single value can
also live in more than one config file's worth of sections at once; sections
you're not using are simply ignored.

**[`archive.cfg.example`](archive.cfg.example)** has every section, with
every key inline-documented — copy it and fill in what you need:

```bash
cp archive.cfg.example ~/.archive.cfg
```

Precedence for every value, highest first:

- **S3 / Local:** CLI flag > config file. (No per-field environment
  variables for these two backends.)
- **Globus:** CLI flag > environment variable > config file. Environment
  variable names are listed in `archive.cfg.example` next to each `[globus]`
  key (e.g. `GLOBUS_ARCHIVE_CLIENT_ID`).

If a required value is missing everywhere, every script exits with a clear
error naming exactly which value is missing and how to supply it (which
flag, env var, and config key).

---

## Inventory file format

Every archive produces exactly one inventory JSON file next to the source
directory — the sole record of what went where, and the only thing a
restore needs. For the full schema (per-file and per-directory records,
the backend-specific `archive` section, `format_version`, unreadable-file
tracking, and how permissions are restored), see
[ARCHITECTURE.md](ARCHITECTURE.md#the-inventory-file).

---

## S3 backend

Use this when your destination storage is an S3 bucket, including Glacier /
Deep Archive storage classes. `archive_to_s3.py` verifies the bucket is
reachable and actually writable (a real upload/delete probe, not just a
permissions check) before doing any inventorying or tar-building, so a bad
`--profile`/bucket/permission fails immediately instead of after however
long walking and hashing the whole tree took.

### `archive_to_s3.py`

```
archive_to_s3.py [options] directory [bucket] [object_path]
```

| Argument | Description |
|---|---|
| `directory` | Directory tree to archive. |
| `bucket` | S3 bucket name. Optional if `bucket` is set in the `[s3]` section of the config file. |
| `object_path` | Base S3 prefix under which archive objects are stored. Optional if `object_path` is set in the `[s3]` section of the config file. |
| `--storage-class` | S3 storage class (`STANDARD`, `STANDARD_IA`, `ONEZONE_IA`, `INTELLIGENT_TIERING`, `GLACIER`, `GLACIER_IR`, `DEEP_ARCHIVE`). Falls back to `storage_class` in the `[s3]` config section, then `STANDARD`. The flag always wins over the config file, including when you pass `STANDARD` explicitly. |
| `--scratch-dir` | Where local tars are built (default: system temp). |
| `--compression {none,gz}` | Tar compression. Default `none`. |
| `--delete` | Delete the source directory tree after a successful archive. Default: keep it. |
| `--profile` | AWS profile name. |
| `--endpoint-url` | Custom S3-compatible endpoint. |
| `--config-file` | Config file with an `[s3]` section supplying defaults for `bucket`/`object_path`/`profile`/`endpoint_url`/`storage_class` (default `~/.archive.cfg`). |
| `--no-checksum-verify` | Skip whole-object CRC64NVME checksum verification (and its startup capability probe); fall back to size-only verification. archiveTree auto-detects lack of support (missing `awscrt`, or an endpoint that doesn't support it) and falls back on its own — only needed if the probe itself is problematic for your endpoint. |
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
| `--profile` | AWS profile name from `~/.aws/credentials` or `~/.aws/config`. |
| `--endpoint-url` | Custom S3-compatible endpoint. |
| `--config-file` | Config file with an `[s3]` section supplying defaults for `profile`/`endpoint_url` (default `~/.archive.cfg`). |
| `--scratch-dir` | Where downloaded tars land before extraction. |
| `--restore-dir` | Restore target (default: the original `root_dir` recorded in the inventory). |
| `--overwrite` | Allow restoring into a non-empty directory. |
| `--only-path PATH` | Restore only this relative path (repeatable). |
| `--only-prefix PREFIX` | Restore only paths at or under this prefix (repeatable). Matched on path boundaries, so `logs` selects `logs/` but not a sibling `logs2`. |
| `--verify-checksums` | Re-hash restored files and compare to the inventory. |
| `--summary-csv PATH` | Write a CSV summary (`relative_path,full_path,size_bytes,verify_status`). |
| `--keep-tar` | Don't delete downloaded tars after restore. |
| `--dry-run` | Preview the plan; also reports Glacier/Deep Archive status. |
| `--auto-request-restore` | Submit Glacier restore requests for cold objects, then exit — rerun once they're ready. |
| `--restore-days` | Days to keep a Glacier restore. Default `7`. |
| `--restore-tier {Bulk,Standard,Expedited}` | Glacier restore speed. Default `Standard`. |
| `--max-workers` | Parallel download/verify workers. Default `4`. |
| `--verbose` | Progress messages / progress bars. |

Directory permissions recorded in the inventory are restored automatically
after file content is written; there's no separate flag for this.

Example:

```bash
python3 restore_from_s3.py --profile ycrcbjornson \
    --restore-dir /path/to/restore --verify-checksums \
    --summary-csv restore.csv mydata.inventory.<uuid>.json.gz
```

If any archived object is in `GLACIER`/`DEEP_ARCHIVE` and not yet restored,
the script prints each object's status and exits without downloading
anything — pass `--auto-request-restore` to submit the restore requests,
wait for AWS to complete them (can take hours, depending on
`--restore-tier`), then rerun the same command. `GLACIER_IR` (Glacier
Instant Retrieval) needs none of this: despite the name, its objects are
readable immediately, so they're treated as ready and downloaded directly.

### `list_object_status.py`

Check Glacier/Deep Archive restore status for every object in an
inventory without doing a restore:

```
list_object_status.py [--profile PROFILE] [--endpoint-url URL] \
    [--verbose] [--only-status {ready,cold,restoring,error}] inventory_file
```

---

## Local backend

Use this when your destination is a locally-mounted directory (e.g. an NFS
or Lustre mount point) rather than S3 or a Globus collection. No cloud
credentials or SDKs are required — `archive_to_local.py` /
`restore_from_local.py` only need the destination directory to already be
mounted and writable; this tool never mounts anything itself. This also
makes the local backend a convenient way to test archiveTree's core logic
end-to-end without any credentials at all. `archive_to_local.py` verifies
`dest_dir` is actually writable (a real write probe, not just a
permissions check) before doing any inventorying or tar-building, so an
unmounted or read-only destination fails immediately instead of after
however long walking and hashing the whole tree took.

It also refuses to run if `dest_dir` and the directory being archived
overlap in either direction. Unlike S3 and Globus, whose destinations
can't sit inside the source by construction, a local `dest_dir` is just
another path on the same filesystem — and archiving a tree into itself is
unrecoverable with `--delete`, which would remove the source *and* the
archive just written into it.

Fill in the `[local]` section of your config file (see
[Configuration file](#configuration-file) above) to avoid passing `dest_dir`
on every invocation:

```ini
[local]
dest_dir = /path/to/mount
```

### `archive_to_local.py`

```
archive_to_local.py [options] directory [dest_dir]
```

| Argument | Description |
|---|---|
| `directory` | Directory tree to archive. |
| `dest_dir` | Locally-mounted destination directory under which archive objects are stored. Optional if `dest_dir` is set in the `[local]` section of the config file. |
| `--scratch-dir` | Where local tars are built (default: system temp). |
| `--compression {none,gz}` | Tar compression. Default `none`. |
| `--delete` | Delete the source directory tree after a successful archive. Default: keep it. |
| `--config-file` | Config file path (default `~/.archive.cfg`), read for the `[local]` section's `dest_dir`. |
| `--verify-checksum` | After each copy, recompute SHA256 of the destination and compare it to the source checksum. Off by default: size-only verification (a plain filesystem copy has no network-transit integrity gap the way S3 uploads do, so this is opt-in rather than automatic). |
| `--size-cutoff` | Files bigger than this (bytes) are copied individually. Default `1e9`. |
| `--size-grouping` | Target tar-group size in bytes. Default `1e10`. |
| `--max-workers` | Parallel hashing/copy workers. Default `4`. |
| `--dry-run` | Preview the plan; no tars, copies, or deletes. |
| `--inventory-dir` | Where to write the local inventory JSON (default: parent of `directory`). |
| `--no-summary` | Suppress the final summary. |
| `--verbose` | Progress messages / progress bars. |

Example:

```bash
python3 archive_to_local.py --verbose --size-cutoff 1000000 \
    /path/to/mydata /mnt/archive_storage
```

The inventory is written next to `directory` (or `--inventory-dir`) as
`<dirname>.inventory.<uuid>.json.gz`, and a copy is placed under
`{dest_dir}/{dirname}/{archive_id}/inventory/`, mirroring the layout used
under `files/` and `groups/` for the archived objects themselves.

### `restore_from_local.py`

```
restore_from_local.py [options] inventory_file
```

Same shape as `restore_from_s3.py`, minus all Glacier-style
preflight/restore-request logic (a local filesystem has no tiered storage —
objects are always immediately available) and minus `--keep-tar` (nothing
temporary is ever created for a local tar object: it's extracted directly
from its location under `dest_dir` instead of being copied to
`--scratch-dir` first, since it's already local).

| Argument | Description |
|---|---|
| `inventory_file` | Inventory JSON from `archive_to_local.py`. |
| `--config-file` | Config file path (currently unused by this backend; kept for CLI consistency). |
| `--scratch-dir` | Unused by this backend (tar objects extract directly from `dest_dir`); kept for CLI consistency. |
| `--restore-dir` | Restore target (default: the original `root_dir` recorded in the inventory). |
| `--overwrite` | Allow restoring into a non-empty directory. |
| `--only-path PATH` | Restore only this relative path (repeatable). |
| `--only-prefix PREFIX` | Restore only paths at or under this prefix (repeatable). Matched on path boundaries, so `logs` selects `logs/` but not a sibling `logs2`. |
| `--verify-checksums` | Re-hash restored files and compare to the inventory. |
| `--summary-csv PATH` | Write a CSV summary (`relative_path,full_path,size_bytes,verify_status`). |
| `--dry-run` | Preview the plan; no copies, extraction, or writes. |
| `--max-workers` | Parallel copy/verify workers. Default `4`. |
| `--verbose` | Progress messages. |

Example:

```bash
python3 restore_from_local.py --restore-dir /path/to/restore \
    --verify-checksums --summary-csv restore.csv --verbose \
    mydata.inventory.<uuid>.json.gz
```

---

## Globus backend

Use this when your destination storage is reached via Globus.
`archive_to_globus.py` verifies both the source collection (at
`--source-mount`) and the destination collection are actually reachable —
including logging in, if that hasn't happened yet — before doing any
inventorying or tar-building, so a bad collection UUID, wrong
`--source-mount`, or login/permission problem fails immediately instead of
after however long walking and hashing the whole tree took.

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
   every command line. Copy the combined example and edit the `[globus]`
   section (see [Configuration file](#configuration-file) above):

   ```bash
   cp archive.cfg.example ~/.archive.cfg
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
   `GLOBUS_ARCHIVE_DEST_PATH`, `GLOBUS_ARCHIVE_TOKEN_CACHE`,
   `GLOBUS_ARCHIVE_LOGIN_DOMAIN`) or a CLI flag, in that precedence order:
   **CLI flag > env var > config file**.

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
  via `--max-items-per-task`/`--max-batch-bytes` for very large archives),
  not one call per file — Globus tasks are async and server-managed.
  Progress is reported at the task level (`--verbose`, polled every
  `--poll-interval` seconds), not per-file.
- `--scratch-dir` (where tars are built) and the inventory output
  directory must both be reachable via the source collection, i.e. under
  `--source-mount` — the script checks this up front and errors clearly
  if not.
- Local tars are deleted only after the whole batch's transfer task
  succeeds (not incrementally per-tar).
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
| `--login-domain` | Require the interactive login to use an identity from this domain (e.g. `yale.edu`), via `session_required_single_domain`. Only takes effect on a fresh login — run `--globus-logout` first if a token cache already exists. |
| `--config-file` | Config file path (default `~/.archive.cfg`). |
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
| `--max-batch-bytes` | Split into sequential batched tasks if the built tars for one batch (individually-transferred large files don't count) would exceed this many bytes; bounds peak local scratch-disk usage to roughly one batch's worth of tars. Default `1e11` (100GB). |
| `--verbose` | Progress messages. |

Example:

```bash
python3 archive_to_globus.py --verbose /path/to/mydata
# (collection UUIDs / mount / client-id come from ~/.archive.cfg)
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
| `--client-id` / `--token-cache` / `--login-domain` / `--config-file` / `--globus-logout` | Same as the archive script. |
| `--scratch-dir` | Where downloaded tars land before extraction (must be under `--dest-mount`; default: a subdirectory of `--restore-dir`). |
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
| `--max-batch-bytes` | Split into sequential batched download tasks if the tar objects for one batch (individually-transferred files don't count) would exceed this many bytes. Default `1e11` (100GB). |
| `--verbose` | Progress messages / progress bars. |

Directory permissions recorded in the inventory are restored automatically
after file content is written; there's no separate flag for this.

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

**`PermissionDenied` / `Login Failed` / `None of your authenticated
identities are from domains allowed by resource policies`** when
submitting a transfer:
- The destination or source collection restricts access to identities from
  a specific institutional domain (e.g. `yale.edu`) via a
  `session_required_single_domain` session policy — set `--login-domain`
  (or `login_domain` in the config file) so the interactive login actually
  requests a session tied to that domain; see
  [One-time setup](#one-time-setup) above. The scripts automatically retry
  once on this error by re-running the login with the domain named in the
  error response (or your `--login-domain` value, if you set one and the
  collection accepts more than one domain) — if it still fails, the
  identity you log in with genuinely isn't from an allowed domain.
- This is different from a stale cache silently ignoring `--login-domain`:
  that flag only takes effect on a *fresh* login, so if you're setting it
  for the first time on an account that already has a cached token, run
  `--globus-logout` first to force the next login to actually request it.
