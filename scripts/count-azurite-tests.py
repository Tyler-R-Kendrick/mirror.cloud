#!/usr/bin/env python3
"""Reproduce the Azurite test census in specs/azure/azurite-inventory.json.

Given a clone of https://github.com/Azure/Azurite at the pinned ref, count
direct it()/test() calls the same way the inventory was built. Parametrized
cases are not expanded.

  python3 scripts/count-azurite-tests.py --azurite /path/to/Azurite
  python3 scripts/count-azurite-tests.py --azurite /path/to/Azurite --check
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

IT = re.compile(r"\b(?:it|it\.skip|it\.only|test|test\.skip)\s*\(")
ROOT = Path(__file__).resolve().parents[1]
INVENTORY = ROOT / "specs" / "azure" / "azurite-inventory.json"

EXCLUDE_SUBSTRINGS = (
    "/unit/",
    "memoryStore.unit.test.ts",
    "fsStore.test.ts",
    "sqlBlobMetadataStorePool.test.ts",
    "https.test.ts",
    "blobEnvironment.test.ts",
    "blobKeepAliveTimeout.test.ts",
    "startupErrorRecovery.test.ts",
    "upgradeRegression.test.ts",
    "utils.test.ts",
)


def excluded(path: Path) -> bool:
    s = str(path)
    return any(x in s for x in EXCLUDE_SUBSTRINGS)


def count_file(path: Path) -> int:
    return len(IT.findall(path.read_text(errors="replace")))


def census(azurite: Path) -> dict[str, int]:
    tests = azurite / "tests"
    out = {}
    for svc in ("blob", "queue", "table"):
        n = 0
        for p in (tests / svc).rglob("*.ts"):
            if excluded(p):
                continue
            n += count_file(p)
        out[svc] = n
    out["total"] = sum(out.values())
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--azurite", required=True, type=Path)
    ap.add_argument("--check", action="store_true")
    args = ap.parse_args()
    got = census(args.azurite)
    print(json.dumps(got, indent=2, sort_keys=True))
    if not args.check:
        return 0
    inv = json.loads(INVENTORY.read_text())
    want = {svc: inv["services"][svc]["tests"] for svc in ("blob", "queue", "table")}
    want["total"] = inv["totalTests"]
    if got != want:
        print("mismatch want", want, file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
