#!/usr/bin/env python3
"""Reproduce the emulate Vercel census in specs/vercel/emulate-inventory.json.

Given a clone of https://github.com/vercel-labs/emulate at the pinned ref,
count the Vercel service's route registrations and test functions the same
way the inventory was built. Routes are .get/.post/.put/.patch/.delete calls
with a literal leading-slash path under src/routes/ (middleware name
registrations are excluded); tests are direct it()/test() calls under
src/__tests__/, parametrized cases not expanded.

  python3 scripts/count-emulate-vercel.py --emulate /path/to/emulate
  python3 scripts/count-emulate-vercel.py --emulate /path/to/emulate --check
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

ROUTE = re.compile(r"\.(get|post|put|patch|delete)\(\s*[`\"'](/[^`\"']*)")
IT = re.compile(r"\b(?:it|it\.skip|it\.only|test|test\.skip)\s*\(")
ROOT = Path(__file__).resolve().parents[1]
INVENTORY = ROOT / "specs" / "vercel" / "emulate-inventory.json"


def census(emulate: Path) -> dict[str, int]:
    vercel = emulate / "packages" / "@emulators" / "vercel"
    routes = 0
    for f in sorted((vercel / "src" / "routes").glob("*.ts")):
        routes += len(ROUTE.findall(f.read_text(errors="replace")))
    tests = 0
    for f in sorted((vercel / "src" / "__tests__").glob("*.ts")):
        tests += len(IT.findall(f.read_text(errors="replace")))
    return {"routes": routes, "tests": tests}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--emulate", required=True, type=Path)
    ap.add_argument("--check", action="store_true")
    args = ap.parse_args()

    got = census(args.emulate)
    inv = json.loads(INVENTORY.read_text())
    want = {"routes": inv["routeCount"], "tests": inv["testCount"]}
    if args.check:
        if got != want:
            print(f"census {got} != inventory {want}; re-audit the ledger", file=sys.stderr)
            return 1
        print(f"census matches inventory: {got}")
        return 0
    print(json.dumps(got))
    return 0


if __name__ == "__main__":
    sys.exit(main())
