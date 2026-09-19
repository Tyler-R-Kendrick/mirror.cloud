#!/usr/bin/env python3
"""Capture the oracle's answers for the Vercel differential corpus.

Boots vercel-labs/emulate (the pinned vendor-authored oracle, see
specs/vercel/emulate-inventory.json) with `npx emulate --service vercel`,
replays test/behavior/vercel/oracle/corpus.json against it, and records
normalized answers to answers.json beside the corpus. test/behavior/vercel's
oracle_differential_test.go replays the same corpus against mirror and
compares, so the oracle's behavior gates mirror without CI needing node.

Re-run when the pin moves:

  go build -o /tmp/mirror-oracle-capture ./cmd/mirror  # not needed; oracle only
  python3 scripts/capture-vercel-oracle.py            # writes the answers file
"""

from __future__ import annotations

import json
import re
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CORPUS = ROOT / "test" / "behavior" / "vercel" / "oracle" / "corpus.json"
OUT = CORPUS.with_name("answers.json")
ORACLE = "http://localhost:4400"

# Ephemeral values differ per run and per implementation: prefixed ids and
# tokens, long hex draws, millisecond timestamps, ISO instants, and the
# absolute URLs both emulators hand out. Both sides are normalized with the
# same rewrites before comparison (the Go test mirrors these).
NORMALIZERS = [
    (re.compile(r'https?://[^\s"]+?/'), "http://HOST/"),
    (re.compile(r"\b(?:prj|dpl|team|ak|vc|dep)_[A-Za-z0-9]{8,}"), "ID"),
    (re.compile(r"vercel_api_[A-Za-z0-9_-]{16,}"), "TOKEN"),
    (re.compile(r"vercel_[A-Za-z0-9_-]{16,}"), "TOKEN"),
    (re.compile(r"\b[0-9a-f]{20,}\b"), "HEX"),
    (re.compile(r'"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9:.]+Z"'), '"TS"'),
    (re.compile(r"\b1[0-9]{12}\b"), "EPOCHMS"),
]

BLOB_TOKEN = "vercel_blob_rw_corpusstore_zz"
API_TOKEN = "test_token_admin"


def normalize(value):
    s = json.dumps(value, sort_keys=True)
    for rx, repl in NORMALIZERS:
        s = rx.sub(repl, s)
    return s


def resolve(node, answers):
    """Substitute {"$ref": "step.path"} from recorded answers."""
    if isinstance(node, dict):
        if set(node) == {"$ref"}:
            step, _, path = node["$ref"].partition(".")
            cur = answers[step]["body"]
            for part in path.split("."):
                if isinstance(cur, list):
                    cur = cur[int(part)]
                else:
                    cur = cur[part]
            return cur
        return {k: resolve(v, answers) for k, v in node.items()}
    if isinstance(node, list):
        return [resolve(v, answers) for v in node]
    if isinstance(node, str) and "$ref:" in node:
        # path segments: "...$ref:NAME.FIELD..."
        m = re.search(r"\$ref:([a-z0-9-]+)\.([a-zA-Z0-9_.]+)", node)
        if m:
            cur = answers[m.group(1)]["body"]
            for part in m.group(2).split("."):
                cur = cur[int(part)] if isinstance(cur, list) else cur[part]
            return node[: m.start()] + str(cur) + node[m.end():]
    return node


def main() -> int:
    corpus = json.loads(CORPUS.read_text())["steps"]
    answers = {}
    ordered = []
    for i, step in enumerate(corpus):
        name = step.get("name", f"step{i}")
        path = resolve(step["path"], answers)
        body = resolve(step.get("body"), answers)
        host = step.get("host", "api.vercel.com")
        headers = {"Authorization": "Bearer " + (BLOB_TOKEN if step.get("token") == "blob" else API_TOKEN)}
        data = None
        if body is not None:
            data = json.dumps(body).encode()
            headers["Content-Type"] = "application/json"
        elif step.get("bodyText"):
            data = step["bodyText"].encode()
            headers["Content-Type"] = "text/plain"
        req = urllib.request.Request(ORACLE + path, data=data, method=step["method"], headers=headers)
        try:
            with urllib.request.urlopen(req, timeout=10) as res:
                raw = res.read().decode()
                status = res.status
        except urllib.error.HTTPError as e:
            raw = e.read().decode()
            status = e.code
        try:
            parsed = json.loads(raw)
        except ValueError:
            parsed = raw
        answers[name] = {"status": status, "body": parsed}
        ordered.append({"name": name, "status": status, "raw": json.dumps(parsed, sort_keys=True), "normalized": normalize(parsed)})
        print(f"{name}: {status}", file=sys.stderr)

    OUT.write_text(json.dumps({"comment": "Captured from vercel-labs/emulate at the pin in specs/vercel/emulate-inventory.json by scripts/capture-vercel-oracle.py; normalized per the rules in that script.", "answers": ordered}, indent=2) + "\n")
    print(f"wrote {OUT}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    for attempt in range(30):
        try:
            urllib.request.urlopen(ORACLE + "/v2/user", timeout=2)
            break
        except urllib.error.HTTPError:
            break  # any HTTP answer means the oracle is listening
        except OSError:
            time.sleep(1)
    else:
        print("oracle not up; start it with: npx emulate --service vercel --port 4400", file=sys.stderr)
        sys.exit(1)
    sys.exit(main())
