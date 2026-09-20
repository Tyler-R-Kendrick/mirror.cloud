#!/usr/bin/env python3
"""Reproduce an emulate service census in specs/<service>/emulate-inventory.json.

Given a clone of https://github.com/vercel-labs/emulate at the pinned ref,
count the service's route registrations and test functions the same way the
inventory was built. Routes are .get/.post/.put/.patch/.delete calls with a
literal leading-slash path under src/routes/; tests are direct it()/test()
calls under src/__tests__/, parametrized cases not expanded. One script takes
a service name so the next provider does not copy-paste a third census.

  python3 scripts/count-emulate.py --service vercel --emulate /path/to/emulate
  python3 scripts/count-emulate.py --service vercel --emulate /path/to/emulate --check
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


def census(emulate: Path, service: str) -> dict[str, int]:
    pkg = emulate / "packages" / "@emulators" / service
    routes = 0
    for f in sorted((pkg / "src" / "routes").glob("*.ts")):
        routes += len(ROUTE.findall(f.read_text(errors="replace")))
    tests = 0
    for f in sorted((pkg / "src" / "__tests__").glob("*.ts")):
        tests += len(IT.findall(f.read_text(errors="replace")))
    return {"routes": routes, "tests": tests}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--service", required=True)
    ap.add_argument("--emulate", required=True, type=Path)
    ap.add_argument("--check", action="store_true")
    args = ap.parse_args()

    got = census(args.emulate, args.service)
    inv = json.loads((ROOT / "specs" / args.service / "emulate-inventory.json").read_text())
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
