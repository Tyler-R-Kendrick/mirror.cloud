#!/usr/bin/env bash
# Run the native fuzz targets in scripts/fuzz-targets.txt, 10000 executions each.
#
#   scripts/fuzz.sh              every target
#   scripts/fuzz.sh SHARD SHARDS only targets whose line number is SHARD mod SHARDS (CI matrix)
#
# FUZZ_TARGETS names another list, for testing this script.
#
# `go test -fuzz` with a name that matches nothing exits 0, and says so only
# sometimes (a warning in a package that has other fuzz functions, a plain "ok" in
# one that has none), so a renamed or misspelled target would stay green forever.
# A run that fuzzed prints "fuzz: elapsed:" lines; one that did not is a failure
# here, as is a shard that selected nothing.
set -euo pipefail
cd "$(dirname "$0")/.."
shard=${1:-0} shards=${2:-1}
list=${FUZZ_TARGETS:-scripts/fuzz-targets.txt}
n=0 ran=0
while read -r pkg name; do
  case "$pkg" in '' | '#'*) continue ;; esac
  (( n++ % shards == shard )) || continue
  out=$(go test "$pkg" -run '^$' -fuzz "^${name}\$" -fuzztime=10000x -parallel=4 2>&1) || {
    echo "$out"; echo "fuzz: $pkg $name failed" >&2; exit 1
  }
  if ! grep -q 'fuzz: elapsed:' <<<"$out"; then
    echo "$out"; echo "fuzz: $pkg has no fuzz target named $name" >&2; exit 1
  fi
  echo "ok $pkg $name"
  ran=$((ran + 1))
done < "$list"
(( ran > 0 )) || { echo "fuzz: shard $shard of $shards selected no target" >&2; exit 1; }
