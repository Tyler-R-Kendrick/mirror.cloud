#!/usr/bin/env bash
# Run the native fuzz targets in scripts/fuzz-targets.txt, 10000 executions each.
#
#   scripts/fuzz.sh              every target
#   scripts/fuzz.sh SHARD SHARDS only targets whose line number is SHARD mod SHARDS (CI matrix)
#
# FUZZ_TARGETS names another list, for testing this script. FUZZ_PKGS names a
# file of packages (./dir, one per line): only targets in those are fuzzed, and
# the shards split those. A pull request passes the packages its diff can reach
# (cmd/affected); when that holds no target, there is nothing to fuzz and the
# script says so and passes. main passes nothing and fuzzes every target.
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
pkgs=${FUZZ_PKGS:-}
n=0 ran=0
while read -r pkg name; do
  case "$pkg" in '' | '#'*) continue ;; esac
  if [[ -n "$pkgs" ]] && ! grep -qxF -- "$pkg" "$pkgs"; then continue; fi
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
if [[ -n "$pkgs" ]] && (( n == 0 )); then
  echo "fuzz: the affected packages hold no fuzz target"; exit 0
fi
(( ran > 0 )) || { echo "fuzz: shard $shard of $shards selected no target" >&2; exit 1; }
