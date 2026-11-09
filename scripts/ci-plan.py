#!/usr/bin/env python3
"""Turn cmd/affected's JSON into the plan job's outputs ($GITHUB_OUTPUT lines).

    go run ./cmd/affected -json -files changed.txt > a.json
    scripts/ci-plan.py a.json >> "$GITHUB_OUTPUT"

packages   the selected packages, one per line (a multi-line output)
race       {"shards": [...], "count": n, "packages": k}: the race matrix
fuzz       {"shards": [...], "count": n, "targets": k}: the fuzz matrix
<gate>     true or false, for each path-gated job

A selection of everything (main, or a change cmd/affected cannot place) gets
the full matrices: four race shards and six fuzz shards. A smaller one gets as
many as its cost needs, and a matrix of one shard whose job only says it was
skipped when there is nothing to run (k is 0), so the job still reports.
"""
import importlib.util
import json
import math
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
RACE_SHARDS, FUZZ_SHARDS = 4, 6
RACE_BUDGET = 900.0  # seconds of measured race time per shard; a full shard is about this
FUZZ_PER_SHARD = 22  # a sixth of the targets


def load_shard():
    sys.dont_write_bytecode = True  # no __pycache__ in the checkout
    spec = importlib.util.spec_from_file_location("shard", os.path.join(HERE, "shard.py"))
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def plan(affected, targets_file, costs_file):
    shard = load_shard()
    pkgs = [p for p in affected["packages"] if p != "./internal/mutation"]
    targets = []
    with open(targets_file) as f:
        for line in f:
            fields = line.split()
            if fields and not fields[0].startswith("#"):
                targets.append(fields[0])
    if affected["all"]:
        race_count, fuzz_count, fuzz_n = RACE_SHARDS, FUZZ_SHARDS, len(targets)
    else:
        costs = json.load(open(costs_file))["seconds"]
        median = sorted(costs.values())[len(costs) // 2] if costs else 1.0
        total = sum(costs.get(shard.norm(p), median) for p in pkgs)
        race_count = max(1, min(RACE_SHARDS, int(total // RACE_BUDGET) + 1))
        chosen = set(pkgs)
        fuzz_n = sum(1 for t in targets if t in chosen)
        fuzz_count = max(1, min(FUZZ_SHARDS, math.ceil(fuzz_n / FUZZ_PER_SHARD)))
    out = {
        "race": {"shards": list(range(race_count)), "count": race_count, "packages": len(pkgs)},
        "fuzz": {"shards": list(range(fuzz_count)), "count": fuzz_count, "targets": fuzz_n},
    }
    lines = ["packages<<__AFFECTED__", *affected["packages"], "__AFFECTED__"]
    for name, value in out.items():
        lines.append(f"{name}={json.dumps(value)}")
    for gate, opened in sorted(affected.get("jobs", {}).items()):
        lines.append(f"{gate}={'true' if opened or affected['all'] else 'false'}")
    return lines


def main(argv):
    affected = json.load(open(argv[1]))
    lines = plan(affected, os.path.join(ROOT, "scripts", "fuzz-targets.txt"), os.path.join(ROOT, "test", "race-costs.json"))
    print("\n".join(lines))
    reason = affected.get("reason") or f"{len(affected['packages'])} packages"
    print(f"plan: {reason}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
