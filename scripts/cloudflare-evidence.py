#!/usr/bin/env python3
"""Emit docs/cloudflare-evidence.json from the CF acceptance table."""
from __future__ import annotations

import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
REPORT = ROOT / "docs" / "CLOUDFLARE_IMPLEMENTATION_REPORT.md"
OUT = ROOT / "docs" / "cloudflare-evidence.json"
ROW = re.compile(
    r"^\|\s*`?(CF-[A-Z0-9*/ -]+?)`?\s*\|\s*([^|]+)\|\s*(.+?)\s*\|\s*$"
)


def main() -> int:
    cells = []
    in_table = False
    for line in REPORT.read_text().splitlines():
        if line.startswith("| Case |"):
            in_table = True
            continue
        if not in_table:
            continue
        if line.startswith("|---"):
            continue
        if not line.startswith("|"):
            break
        m = ROW.match(line)
        if not m:
            continue
        case, status, evidence = (g.strip() for g in m.groups())
        cells.append(
            {
                "id": case,
                "status": status.replace("**", "").strip(),
                "evidence": evidence.replace("**", "").strip(),
            }
        )
    if not cells:
        print("cloudflare-evidence: no CF-* rows parsed", file=sys.stderr)
        return 1
    OUT.write_text(
        json.dumps(
            {
                "schema": "mirror.cloudflare.evidence/1",
                "source": "docs/CLOUDFLARE_IMPLEMENTATION_REPORT.md",
                "cells": cells,
            },
            indent=2,
        )
        + "\n"
    )
    print(f"wrote {OUT.relative_to(ROOT)} ({len(cells)} cells)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
