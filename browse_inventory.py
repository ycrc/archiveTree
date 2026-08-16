#!/usr/bin/env python3
"""
Interactively browse an inventory file (ncdu/xdu-view-style) and generate a
restore script for a selected set of files and/or directory trees.

Requires an inventory with format_version >= 3, since it relies on the
per-directory rollup ("directories": file_count/total_bytes per directory,
see build_inventory() in archive_common.py) to show directory sizes without
re-scanning the full file list on every navigation.

Navigation:
  up/k, down/j        move selection
  right/Enter          enter directory
  left/backspace       go up to parent
  space                toggle mark on the highlighted file/directory
  M                    toggle mark on the directory you're currently inside
  s                    cycle sort mode (name, size-desc/asc, count-desc/asc)
  v                    toggle --verbose in the generated restore script
  c                    clear all marks
  w                    write a restore script for the marked items
  d                    debug: list the archive objects (and sizes) needed for the current selection
  x/Esc                exit (offers to write a restore script first if anything is marked)

Marking a directory selects its entire subtree; marking is not required on
descendants of an already-marked directory (they're covered automatically,
shown with a '+' instead of '*').
"""

import argparse
import curses
import json
import os
import shlex
import sys
from datetime import datetime

from archive_common import check_inventory_version

SORT_MODES = ["name", "size-desc", "size-asc", "count-desc", "count-asc"]


# ---------- Tree model ----------

class Node:
    __slots__ = ("name", "relpath", "is_dir", "size_bytes", "file_count", "is_symlink", "children")

    def __init__(self, name, relpath, is_dir):
        self.name = name
        self.relpath = relpath
        self.is_dir = is_dir
        self.size_bytes = 0
        self.file_count = 0
        self.is_symlink = False
        self.children = {} if is_dir else None


def build_tree(inventory):
    """Build an in-memory directory tree from inventory["directories"] + inventory["files"]."""
    directories = inventory.get("directories")
    if directories is None:
        raise ValueError("Inventory has no 'directories' rollup data (format_version < 3).")

    nodes = {}
    for d in directories:
        relpath = d["relative_path"]
        name = os.path.basename(relpath) if relpath else (os.path.basename(inventory.get("root_dir", "")) or "/")
        node = Node(name=name, relpath=relpath, is_dir=True)
        node.file_count = d.get("file_count", 0)
        node.size_bytes = d.get("total_bytes", 0)
        nodes[relpath] = node

    root = nodes.get("")
    if root is None:
        root = Node(name=os.path.basename(inventory.get("root_dir", "")) or "/", relpath="", is_dir=True)
        nodes[""] = root

    for relpath, node in nodes.items():
        if relpath == "":
            continue
        parent = nodes.get(os.path.dirname(relpath), root)
        parent.children[node.name] = node

    for rec in inventory.get("files", []):
        relpath = rec["relative_path"]
        parent = nodes.get(os.path.dirname(relpath), root)
        name = os.path.basename(relpath)
        fnode = Node(name=name, relpath=relpath, is_dir=False)
        fnode.size_bytes = rec.get("size_bytes", 0)
        fnode.file_count = 1
        fnode.is_symlink = rec.get("is_symlink", False)
        parent.children[name] = fnode

    return root


def sort_key_for(mode):
    if mode == "size-desc":
        return lambda n: -n.size_bytes
    if mode == "size-asc":
        return lambda n: n.size_bytes
    if mode == "count-desc":
        return lambda n: -n.file_count
    if mode == "count-asc":
        return lambda n: n.file_count
    return lambda n: (not n.is_dir, n.name.lower())  # "name": dirs first, then alphabetical


def format_size(n):
    size = float(n)
    for unit in ("B", "KiB", "MiB", "GiB", "TiB", "PiB"):
        if unit == "B":
            if size < 1024.0:
                return f"{int(size)} B"
        elif size < 1024.0 or unit == "PiB":
            return f"{size:.2f} {unit}"
        size /= 1024.0


# ---------- Selection / script generation ----------

def ancestor_marked(relpath, marks):
    """True if some ancestor directory of relpath (not relpath itself) is marked."""
    parent = os.path.dirname(relpath)
    while True:
        if marks.get(parent) == "dir":
            return True
        if parent == "":
            return False
        parent = os.path.dirname(parent)


def compute_restore_selection(marks):
    """
    Given marks: dict[relpath] -> "dir"|"file", return
    (dir_prefixes, file_paths, whole_tree), with marks that are redundant
    (nested under another marked directory) dropped.
    """
    if marks.get("") == "dir":
        return [], [], True

    dir_marks = sorted(rp for rp, kind in marks.items() if kind == "dir")
    file_marks = sorted(rp for rp, kind in marks.items() if kind == "file")

    def under_other_dir(relpath, dirs):
        for d in dirs:
            if relpath != d and (relpath == d or relpath.startswith(d + "/")):
                return True
        return False

    kept_dirs = [d for d in dir_marks if not under_other_dir(d, dir_marks)]
    kept_files = [f for f in file_marks if not under_other_dir(f, kept_dirs)]

    return kept_dirs, kept_files, False


def _resolve_selection(inventory, marks):
    """
    Internal: resolve marks to (restore_count, restore_bytes, touched_object_ids)
    for the current selection. Shared by compute_summary() and
    compute_touched_objects() so they can't disagree about what's selected.
    """
    if not marks:
        return 0, 0, set()

    file_records = inventory.get("files", [])
    dir_prefixes, file_paths, whole_tree = compute_restore_selection(marks)

    if whole_tree:
        restore_count = inventory.get("total_files", len(file_records))
        restore_bytes = inventory.get("total_bytes", sum(r.get("size_bytes", 0) for r in file_records))
        touched_ids = {obj["id"] for obj in (inventory.get("archive") or {}).get("objects", [])}
        return restore_count, restore_bytes, touched_ids

    prefix_tuple = tuple(d + "/" for d in dir_prefixes)
    file_set = set(file_paths)
    restore_count = 0
    restore_bytes = 0
    touched_ids = set()
    for rec in file_records:
        rel = rec["relative_path"]
        if rel in file_set or (prefix_tuple and rel.startswith(prefix_tuple)):
            restore_count += 1
            restore_bytes += rec.get("size_bytes", 0)
            oid = rec.get("object_id")
            if oid is not None:
                touched_ids.add(oid)
    return restore_count, restore_bytes, touched_ids


def compute_summary(inventory, marks):
    """
    Return {"restore_count", "restore_bytes", "archive_object_count", "archive_bytes"}
    for the current selection:

    - restore_count/restore_bytes: files that will actually be written to
      disk (the resolved, deduplicated selection from compute_restore_selection).
    - archive_object_count/archive_bytes: distinct archive objects (tar
      groups or standalone files, see archive_to_s3.py/archive_to_globus.py's
      small-file grouping) that must be downloaded to satisfy that restore,
      and their total stored size. Grouping means this is usually far fewer
      than restore_count/total file count, since one tar download can supply
      many restored files at once.
    """
    restore_count, restore_bytes, touched_ids = _resolve_selection(inventory, marks)
    objects_by_id = {obj["id"]: obj for obj in (inventory.get("archive") or {}).get("objects", [])}
    archive_bytes = sum(
        objects_by_id[oid].get("size_bytes", 0) for oid in touched_ids if oid in objects_by_id
    )

    return {
        "restore_count": restore_count,
        "restore_bytes": restore_bytes,
        "archive_object_count": len(touched_ids & objects_by_id.keys()),
        "archive_bytes": archive_bytes,
    }


def compute_touched_objects(inventory, marks):
    """
    Return the archive object dicts (id/type/key/size_bytes/...) that must be
    downloaded to satisfy the current selection, largest first. This is the
    per-object detail behind compute_summary()'s archive_object_count/bytes.
    """
    _, _, touched_ids = _resolve_selection(inventory, marks)
    objects_by_id = {obj["id"]: obj for obj in (inventory.get("archive") or {}).get("objects", [])}
    objs = [objects_by_id[oid] for oid in touched_ids if oid in objects_by_id]
    objs.sort(key=lambda o: -o.get("size_bytes", 0))
    return objs


def detect_backend(inventory):
    archive = inventory.get("archive") or {}
    return "globus" if archive.get("backend") == "globus" else "s3"


def build_restore_script(inventory_path, inventory, dir_prefixes, file_paths, whole_tree,
                         restore_dir, script_dir, verbose=True):
    backend = detect_backend(inventory)
    restore_script = "restore_from_globus.py" if backend == "globus" else "restore_from_s3.py"
    restore_script_path = os.path.join(script_dir, restore_script)

    lines = [
        "#!/usr/bin/env bash",
        f"# Generated by browse_inventory.py on {datetime.now().isoformat(timespec='seconds')}",
        f"# Inventory: {inventory_path}",
    ]
    if whole_tree:
        lines.append("# Selection: entire inventory")
    else:
        lines.append(f"# Selection: {len(dir_prefixes)} directory tree(s), {len(file_paths)} individual file(s)")
    lines.append("set -euo pipefail")
    lines.append("")

    cmd_groups = [
        f"{shlex.quote(sys.executable or 'python3')} {shlex.quote(restore_script_path)}",
        shlex.quote(inventory_path),
    ]
    if restore_dir:
        cmd_groups.append(f"--restore-dir {shlex.quote(restore_dir)}")
    for d in dir_prefixes:
        cmd_groups.append(f"--only-prefix {shlex.quote(d + '/')}")
    for f in file_paths:
        cmd_groups.append(f"--only-path {shlex.quote(f)}")
    if verbose:
        cmd_groups.append("--verbose")

    lines.append(" \\\n  ".join(cmd_groups))
    lines.append("")
    return "\n".join(lines)


# ---------- Curses UI ----------

def _prompt_line(stdscr, prompt, initial=""):
    height, width = stdscr.getmaxyx()
    buf = list(initial)
    curses.curs_set(1)
    try:
        while True:
            stdscr.move(height - 1, 0)
            stdscr.clrtoeol()
            text = prompt + "".join(buf)
            stdscr.addnstr(height - 1, 0, text, width - 1)
            stdscr.move(height - 1, min(len(text), width - 1))
            stdscr.refresh()
            ch = stdscr.getch()
            if ch in (curses.KEY_ENTER, 10, 13):
                return "".join(buf)
            if ch == 27:
                return None
            if ch in (curses.KEY_BACKSPACE, 127, 8):
                if buf:
                    buf.pop()
            elif 32 <= ch < 127:
                buf.append(chr(ch))
    finally:
        curses.curs_set(0)


def _prompt_char(stdscr, prompt, valid_chars):
    height, width = stdscr.getmaxyx()
    stdscr.move(height - 1, 0)
    stdscr.clrtoeol()
    stdscr.addnstr(height - 1, 0, prompt, width - 1)
    stdscr.refresh()
    while True:
        ch = stdscr.getch()
        if ch == 27:
            return None
        if 0 <= ch < 256:
            c = chr(ch).lower()
            if c in valid_chars:
                return c


def _show_debug_view(stdscr, objects):
    """Full-screen scrollable listing of the archive objects for the current selection."""
    total_bytes = sum(o.get("size_bytes", 0) for o in objects)
    idx = 0
    scroll = 0
    while True:
        stdscr.erase()
        height, width = stdscr.getmaxyx()

        header = f" Archive objects required for restore: {len(objects):,} object(s), {format_size(total_bytes)} total"
        stdscr.addnstr(0, 0, header[:width - 1], width - 1, curses.A_BOLD)

        list_height = max(1, height - 3)
        if objects:
            idx = max(0, min(idx, len(objects) - 1))
            if idx < scroll:
                scroll = idx
            elif idx >= scroll + list_height:
                scroll = idx - list_height + 1

        for row in range(list_height):
            i = scroll + row
            if i >= len(objects):
                break
            obj = objects[i]
            key = obj.get("s3_key") or obj.get("globus_path") or "?"
            otype = obj.get("type", "?")
            size_str = format_size(obj.get("size_bytes", 0))
            extra = f"  file_count={obj['file_count']}" if "file_count" in obj else ""
            line = f"{size_str:>10}  {otype:<5} {obj.get('id', '?'):<14} {key}{extra}"
            attr = curses.A_REVERSE if i == idx else curses.A_NORMAL
            stdscr.addnstr(row + 2, 0, line[:width - 1], width - 1, attr)

        if objects:
            status = f"{len(objects)} object(s) | q/Esc/d:back  jk/updown:scroll"
        else:
            status = "No objects required (nothing marked, or marks resolve to nothing). q/Esc/d:back"
        stdscr.addnstr(height - 1, 0, status[:width - 1], width - 1, curses.A_REVERSE)
        stdscr.refresh()

        ch = stdscr.getch()
        if ch in (ord("q"), ord("d"), 27):
            return
        if ch in (curses.KEY_UP, ord("k")) and objects:
            idx -= 1
        elif ch in (curses.KEY_DOWN, ord("j")) and objects:
            idx += 1


class App:
    def __init__(self, stdscr, inventory, inventory_path):
        self.stdscr = stdscr
        self.inventory = inventory
        self.inventory_path = inventory_path
        self.root = build_tree(inventory)
        self.path_stack = [self.root]
        self.cursor_stack = [0]
        self.scroll_stack = [0]
        self.sort_mode_idx = 0
        self.marks = {}
        self.message = ""
        self._summary_cache = None
        self.verbose_script = True

    @property
    def sort_mode(self):
        return SORT_MODES[self.sort_mode_idx]

    def get_summary(self):
        if self._summary_cache is None:
            self._summary_cache = compute_summary(self.inventory, self.marks)
        return self._summary_cache

    def current_children(self):
        node = self.path_stack[-1]
        return sorted(node.children.values(), key=sort_key_for(self.sort_mode))

    def run(self):
        curses.curs_set(0)
        self.stdscr.keypad(True)
        while True:
            self.render()
            ch = self.stdscr.getch()
            if ch in (ord("x"), 27):
                if self.request_exit():
                    break
                continue
            self.message = ""
            self.handle_key(ch)

    def request_exit(self):
        """Return True if the app should actually exit now."""
        if not self.marks:
            return True
        choice = _prompt_char(
            self.stdscr,
            f"Exit: write restore script for {len(self.marks)} marked item(s) first? "
            "[y/n, Esc=cancel exit]: ", "yn",
        )
        if choice is None:
            self.message = "Exit cancelled."
            return False
        if choice == "y":
            self.write_script_flow()
        return True

    def handle_key(self, ch):
        children = self.current_children()
        if ch in (curses.KEY_UP, ord("k")):
            self.move_cursor(-1, children)
        elif ch in (curses.KEY_DOWN, ord("j")):
            self.move_cursor(1, children)
        elif ch in (curses.KEY_RIGHT, curses.KEY_ENTER, 10, 13):
            self.enter_selected(children)
        elif ch in (curses.KEY_LEFT, curses.KEY_BACKSPACE, 127, 8):
            self.go_up()
        elif ch == ord(" "):
            self.toggle_mark(children)
        elif ch == ord("M"):
            self.toggle_mark_current_dir()
        elif ch == ord("s"):
            self.sort_mode_idx = (self.sort_mode_idx + 1) % len(SORT_MODES)
        elif ch == ord("v"):
            self.verbose_script = not self.verbose_script
        elif ch == ord("c"):
            self.marks = {}
            self._summary_cache = None
        elif ch == ord("w"):
            self.write_script_flow()
        elif ch == ord("d"):
            self.show_debug()

    def show_debug(self):
        objects = compute_touched_objects(self.inventory, self.marks)
        _show_debug_view(self.stdscr, objects)

    def move_cursor(self, delta, children):
        if not children:
            return
        idx = max(0, min(self.cursor_stack[-1] + delta, len(children) - 1))
        self.cursor_stack[-1] = idx
        self.fix_scroll(children)

    def fix_scroll(self, children):
        height, _ = self.stdscr.getmaxyx()
        list_height = max(1, height - 3)
        idx = self.cursor_stack[-1]
        scroll = self.scroll_stack[-1]
        if idx < scroll:
            scroll = idx
        elif idx >= scroll + list_height:
            scroll = idx - list_height + 1
        self.scroll_stack[-1] = scroll

    def enter_selected(self, children):
        if not children:
            return
        node = children[self.cursor_stack[-1]]
        if node.is_dir:
            self.path_stack.append(node)
            self.cursor_stack.append(0)
            self.scroll_stack.append(0)

    def go_up(self):
        if len(self.path_stack) > 1:
            self.path_stack.pop()
            self.cursor_stack.pop()
            self.scroll_stack.pop()

    def toggle_mark(self, children):
        if not children:
            return
        node = children[self.cursor_stack[-1]]
        if node.relpath in self.marks:
            del self.marks[node.relpath]
        else:
            self.marks[node.relpath] = "dir" if node.is_dir else "file"
        self._summary_cache = None

    def toggle_mark_current_dir(self):
        node = self.path_stack[-1]
        if node.relpath in self.marks:
            del self.marks[node.relpath]
            self._summary_cache = None
        else:
            self.marks[node.relpath] = "dir"
        self._summary_cache = None

    def write_script_flow(self):
        if not self.marks:
            self.message = "No items marked. Press space to mark files/directories first."
            return

        choice = _prompt_char(
            self.stdscr,
            "Restore to (o)riginal or (n)ew location?  [o/n, Esc=cancel]: ", "on",
        )
        if choice is None:
            self.message = "Cancelled."
            return

        restore_dir = None
        if choice == "n":
            new_path = _prompt_line(self.stdscr, "New restore location (absolute path): ")
            if not new_path:
                self.message = "Cancelled."
                return
            restore_dir = new_path

        default_script = os.path.join(os.getcwd(), "restore_selected.sh")
        script_path = _prompt_line(self.stdscr, "Write restore script to: ", initial=default_script)
        if not script_path:
            self.message = "Cancelled."
            return

        dir_prefixes, file_paths, whole_tree = compute_restore_selection(self.marks)
        script_dir = os.path.dirname(os.path.abspath(__file__))
        content = build_restore_script(
            self.inventory_path, self.inventory, dir_prefixes, file_paths, whole_tree,
            restore_dir, script_dir, verbose=self.verbose_script,
        )

        script_path = os.path.abspath(os.path.expanduser(script_path))
        with open(script_path, "w") as f:
            f.write(content)
        os.chmod(script_path, 0o755)

        summary = "entire inventory" if whole_tree else f"{len(dir_prefixes)} dir(s), {len(file_paths)} file(s)"
        self.message = f"Wrote restore script ({summary}) to {script_path}"

    def render(self):
        stdscr = self.stdscr
        stdscr.erase()
        height, width = stdscr.getmaxyx()

        node = self.path_stack[-1]
        breadcrumb = node.relpath if node.relpath else "/"
        marked_here = " [DIR MARKED]" if node.relpath in self.marks else ""
        verbose_state = "on" if self.verbose_script else "off"
        header = (
            f" {self.inventory_path}  ->  {breadcrumb}   "
            f"[marked: {len(self.marks)}]{marked_here}   [verbose: {verbose_state}]"
        )
        stdscr.addnstr(0, 0, header[:width - 1], width - 1, curses.A_BOLD)

        summary = self.get_summary()
        summary_line = (
            f" Restore: {summary['restore_count']:,} file(s), {format_size(summary['restore_bytes'])}"
            f"   |   Archive read: {summary['archive_object_count']:,} object(s), {format_size(summary['archive_bytes'])}"
        )
        stdscr.addnstr(1, 0, summary_line[:width - 1], width - 1)

        children = self.current_children()
        list_height = max(1, height - 3)
        scroll = self.scroll_stack[-1]
        cursor = self.cursor_stack[-1]

        for row in range(list_height):
            i = scroll + row
            if i >= len(children):
                break
            child = children[i]
            if child.relpath in self.marks:
                mark_char = "*"
            elif ancestor_marked(child.relpath, self.marks):
                mark_char = "+"
            else:
                mark_char = " "
            size_str = format_size(child.size_bytes)
            count_str = str(child.file_count) if child.is_dir else ""
            suffix = "/" if child.is_dir else ("@" if child.is_symlink else "")
            line = f"{mark_char} {size_str:>10} {count_str:>8}  {child.name}{suffix}"
            attr = curses.A_REVERSE if i == cursor else curses.A_NORMAL
            stdscr.addnstr(row + 2, 0, line[:width - 1], width - 1, attr)

        status = self.message or (
            f"{len(children)} entries | sort:{self.sort_mode} | "
            "x:exit  jk/updown:nav  Enter:open  left/bksp:up  space:mark  M:mark-this-dir  "
            "c:clear  s:sort  v:verbose  w:write script  d:debug objects"
        )
        stdscr.addnstr(height - 1, 0, status[:width - 1], width - 1, curses.A_REVERSE)
        stdscr.refresh()


def main():
    parser = argparse.ArgumentParser(
        description="Interactively browse an inventory file and generate a restore "
                    "script for a selected set of files/directories."
    )
    parser.add_argument("inventory_file", help="Path to an inventory JSON file (format_version >= 3).")
    args = parser.parse_args()

    inv_path = os.path.abspath(args.inventory_file)
    if not os.path.isfile(inv_path):
        print(f"ERROR: Inventory file not found: {inv_path}", file=sys.stderr)
        sys.exit(1)

    with open(inv_path, "r", encoding="utf-8") as f:
        inventory = json.load(f)

    version_error = check_inventory_version(inventory)
    if version_error:
        print(f"ERROR: {version_error}", file=sys.stderr)
        sys.exit(1)

    if "directories" not in inventory:
        print(
            "ERROR: This inventory has no 'directories' rollup data (format_version < 3). "
            "Re-archive with a newer version of archiveTree to browse it.",
            file=sys.stderr,
        )
        sys.exit(1)

    curses.wrapper(lambda stdscr: App(stdscr, inventory, inv_path).run())


if __name__ == "__main__":
    main()
