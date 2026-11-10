#!/usr/bin/env python3
"""Give every tracked file an mtime derived from its content, and every
directory one derived from its tree, so go test's result cache sees a change
exactly when the content changes.

`go test` caches a package's result keyed on the size and mtime of each file the
test opened, and of each directory it listed or stat'ed -- not on content. A
fresh checkout stamps all of them with the clone time, so every package whose
tests read a file under the module misses the cache on every run.

The mtime here is a function of the git object id: a file's blob id, a
directory's tree id. Two checkouts holding the same bytes agree on it whatever
their history, and different bytes give a different mtime even at the same
size. An earlier version used the time of the last commit that named a file in
`git log`. Merge commits name no files, so a pull request's merge checkout could
hold content that no commit had named, under the time and size main's copy had,
and go test reported main's cached result for a file it had never seen.

A stamp is BASE plus 28 bits of the id as seconds and 30 bits of it, mod 1e9,
as nanoseconds; go test's file hash includes both. Every stamp is years in the
past, so none falls in the two-second window go test refuses to cache.

What git does not describe is left alone: a file whose working copy differs
from the index or the index from HEAD, and every directory that holds anything
untracked (ignored files do not count) or modified, keep the mtimes they have.
A tracked symlink is stamped itself, not its target. Those are newer than any stamp and change when the
file does, so they can cost a cache hit, never produce a stale one.

Needs no history: it reads the index and HEAD's trees. Run from anywhere inside
the checkout.
"""
import os
import subprocess
import sys

BASE = 1_000_000_000  # 2001-09-09
SPAN = 1 << 28  # about 8.5 years of seconds


def stamp(oid):
    v = int(oid[:16], 16)
    return (BASE + (v >> 30) % SPAN) * 1_000_000_000 + (v & ((1 << 30) - 1)) % 1_000_000_000


def git(*args):
    return subprocess.check_output(["git", *args])


def main():
    root = git("rev-parse", "--show-toplevel").decode().strip()
    os.chdir(root)

    # Anything the index does not describe, and every directory above it.
    dirty = set()
    unlike_head = git("diff", "--cached", "--name-only", "-z").decode().split("\0")
    listed = git("ls-files", "-z", "--modified", "--deleted", "--others", "--exclude-standard", "--directory")
    for path in listed.decode().split("\0") + unlike_head:
        path = path.rstrip("/")
        if not path:
            continue
        dirty.add(path)
        d = os.path.dirname(path)
        while True:
            dirty.add(d)
            if not d:
                break
            d = os.path.dirname(d)

    files = 0
    for entry in git("ls-files", "-z", "--stage").decode().split("\0"):
        if not entry:
            continue
        meta, path = entry.split("\t", 1)
        mode, oid, _ = meta.split(" ")
        if mode not in ("100644", "100755", "120000") or path in dirty or not os.path.lexists(path):
            continue
        t = stamp(oid)
        os.utime(path, ns=(t, t), follow_symlinks=False)
        files += 1

    trees = {"": git("rev-parse", "HEAD^{tree}").decode().strip()}
    for entry in git("ls-tree", "-r", "-d", "-z", "HEAD").decode().split("\0"):
        if entry:
            meta, path = entry.split("\t", 1)
            trees[path] = meta.split(" ")[2]
    dirs = 0
    # Deepest first, though setting a directory's mtime touches nothing else.
    for path in sorted(trees, key=lambda p: -p.count("/")):
        if path in dirty or not os.path.isdir(path or "."):
            continue
        t = stamp(trees[path])
        os.utime(path or ".", ns=(t, t))
        dirs += 1
    print(f"restore-mtimes: {files} files, {dirs} dirs, {len(dirty)} paths left as they are", file=sys.stderr)


if __name__ == "__main__":
    main()
