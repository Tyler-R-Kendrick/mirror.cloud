#!/usr/bin/env python3
"""Give every tracked file the commit time of the last commit that touched it,
and every directory the newest time beneath it.

`go test` caches a package's result keyed on the size and mtime of each file
the test opened, and of each directory it listed or ran in -- not on content.
A fresh checkout stamps all of them with the clone time, so every package whose
tests read a file under the module misses the cache on every run (26 of 116 here,
95 s against 14 s). A last-commit time is stable across clones and changes
exactly when a file's content does, which a constant would not: an edit that kept
a file's size would then hit a stale result.

Needs history. In a shallow clone a file with no commit in sight gets HEAD's
time, which is still correct, just a miss on the next commit. Run from anywhere
inside the checkout.
"""
import os
import subprocess
import sys

root = subprocess.check_output(["git", "rev-parse", "--show-toplevel"], text=True).strip()
os.chdir(root)


def git(*args):
    return subprocess.check_output(["git", *args], text=True)


want = set(git("ls-files", "-z").split("\0")) - {""}
head = int(git("log", "-1", "--format=%ct"))
when = {}
cur = head
# Newest first, so the first commit to name a path is the last to touch it.
log = subprocess.Popen(
    ["git", "log", "--format=@%ct", "--name-only", "--no-renames", "-z"],
    stdout=subprocess.PIPE, text=True)
pending = set(want)
for token in log.stdout.read().split("\0"):
    for line in token.split("\n"):
        if line.startswith("@") and line[1:].isdigit():
            cur = int(line[1:])
        elif line in pending:
            when[line] = cur
            pending.discard(line)
    if not pending:
        break
log.wait()
for path in pending:
    when[path] = head

newest = {}
for path, t in when.items():
    if os.path.islink(path) or not os.path.exists(path):
        continue
    os.utime(path, (t, t))
    d = os.path.dirname(path)
    while True:
        newest[d] = max(newest.get(d, 0), t)
        if not d:
            break
        d = os.path.dirname(d)
for d, t in newest.items():
    os.utime(d or ".", (t, t))
print(f"restore-mtimes: {len(when)} files, {len(newest)} dirs, {len(pending)} without history", file=sys.stderr)
