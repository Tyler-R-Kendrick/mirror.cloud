#!/usr/bin/env python3
"""Print the packages of one shard, filled by measured cost.

    go list ./... | scripts/shard.py COSTS SHARD SHARDS

Reads packages (import paths or ./dirs) on stdin and prints this shard's, as
./dirs. Every package goes to exactly one shard: the most expensive first, each
to the shard with the least so far, ties to the lower index, so every job of a
matrix computes the same split. COSTS is a JSON file with a "seconds" map from
./dir to seconds; a package not in it costs the median.

    scripts/shard.py COSTS --count BUDGET MAX < packages

prints how many shards the packages need: one per BUDGET seconds, at least one
and at most MAX.
"""
import json
import statistics
import sys

MODULE = "github.com/tyler-r-kendrick/mirror.cloud"


def norm(p):
    p = p.strip()
    if p == MODULE:
        return "."
    if p.startswith(MODULE + "/"):
        return "./" + p[len(MODULE) + 1:]
    if p and not p.startswith("."):
        return "./" + p
    return p


def main(argv):
    costs = json.load(open(argv[1]))["seconds"]
    default = statistics.median(costs.values()) if costs else 1.0
    pkgs = sorted({norm(l) for l in sys.stdin if l.strip()})
    cost = lambda p: costs.get(p, default)
    if argv[2] == "--count":
        budget, most = float(argv[3]), int(argv[4])
        total = sum(cost(p) for p in pkgs)
        print(max(1, min(most, int(total // budget) + 1)))
        return 0
    shard, shards = int(argv[2]), int(argv[3])
    if shards < 1 or not 0 <= shard < shards:
        print(f"shard {shard} of {shards} is not a shard", file=sys.stderr)
        return 2
    load = [0.0] * shards
    mine = []
    for p in sorted(pkgs, key=lambda p: (-cost(p), p)):
        best = min(range(shards), key=lambda s: (load[s], s))
        load[best] += cost(p)
        if best == shard:
            mine.append(p)
    print("\n".join(sorted(mine)))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
