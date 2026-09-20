#!/usr/bin/env python3
"""Capture an oracle's answers for a differential corpus.

Boots nothing itself: start vercel-labs/emulate (the pinned vendor-authored
oracle, see specs/<service>/emulate-inventory.json) first, e.g.
`npx emulate --service vercel --port 4400`, then replay
test/behavior/<service>/oracle/corpus.json against it and record normalized
answers to answers.json beside the corpus. The matching
oracle_*_test.go replays the same corpus against mirror and compares, so the
oracle's behavior gates mirror without CI needing node.

Re-run when the pin moves:

  python3 scripts/capture-oracle.py --service vercel   # writes the answers file
  python3 scripts/capture-oracle.py --service stripe   # writes the answers file

One script takes a service name so the next provider does not copy-paste a
third capture: per-service facts (port, auth tokens, normalizers) live in
SERVICES below, and the request mechanics are shared.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import time
import urllib.request
import urllib.error
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

# Ephemeral values differ per run and per implementation: prefixed ids and
# tokens, long hex draws, timestamps, and the absolute URLs both emulators
# hand out. Both sides are normalized with the same rewrites before comparison
# (each Go test mirrors its service's list).
URL_NORMALIZER = (re.compile(r'https?://[^\s"]+?/'), "http://HOST/")
EPOCHMS = (re.compile(r"\b1[0-9]{12}\b"), "EPOCHMS")
EPOCHS = (re.compile(r"\b1[0-9]{9}\b"), "EPOCH")
HEX20 = (re.compile(r"\b[0-9a-f]{20,}\b"), "HEX")
ISO = (re.compile(r'"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9:.]+Z"'), '"TS"')

SERVICES = {
    "vercel": {
        "port": 4400,
        "probe": "/v2/user",
        "default_token": "api",
        "tokens": {"api": "test_token_admin", "blob": "vercel_blob_rw_corpusstore_zz"},
        "normalizers": [
            URL_NORMALIZER,
            (re.compile(r"\b(?:prj|dpl|team|ak|vc|dep)_[A-Za-z0-9]{8,}"), "ID"),
            (re.compile(r"vercel_api_[A-Za-z0-9_-]{16,}"), "TOKEN"),
            (re.compile(r"vercel_[A-Za-z0-9_-]{16,}"), "TOKEN"),
            HEX20,
            ISO,
            EPOCHMS,
        ],
    },
    "stripe": {
        "port": 4401,
        "probe": "/v1/customers",
        "default_token": None,
        "tokens": {},
        "normalizers": [
            URL_NORMALIZER,
            (re.compile(r"\b(?:cus|prod|price|pi|ch|cs|cuss_secret)_[A-Za-z0-9_-]{8,}"), "ID"),
            HEX20,
            ISO,
            EPOCHS,
        ],
    },
}


def normalize(value, normalizers):
    # Compact separators: the Go replay marshals compact too, and the two
    # canonical forms must be byte-identical, not just semantically equal.
    s = json.dumps(value, sort_keys=True, separators=(",", ":"))
    for rx, repl in normalizers:
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


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def main(service: str) -> int:
    cfg = SERVICES[service]
    base = f"http://localhost:{cfg['port']}"
    corpus_path = ROOT / "test" / "behavior" / service / "oracle" / "corpus.json"
    out_path = corpus_path.with_name("answers.json")

    opener = urllib.request.build_opener(NoRedirect)
    corpus = json.loads(corpus_path.read_text())["steps"]
    answers = {}
    ordered = []
    for i, step in enumerate(corpus):
        name = step.get("name", f"step{i}")
        path = resolve(step["path"], answers)
        body = resolve(step.get("body"), answers)
        headers = {}
        tok = step.get("token", cfg["default_token"])
        if tok in cfg["tokens"]:
            headers["Authorization"] = "Bearer " + cfg["tokens"][tok]
        data = None
        if body is not None:
            data = json.dumps(body).encode()
            headers["Content-Type"] = "application/json"
        elif step.get("bodyText") is not None:
            data = step["bodyText"].encode()
            headers["Content-Type"] = step.get("contentType", "text/plain")
        req = urllib.request.Request(base + path, data=data, method=step["method"], headers=headers)
        try:
            with opener.open(req, timeout=10) as res:
                raw = res.read().decode()
                status = res.status
                headers_out = dict(res.headers.items())
        except urllib.error.HTTPError as e:
            raw = e.read().decode()
            status = e.code
            headers_out = dict(e.headers.items())
        try:
            parsed = json.loads(raw)
        except ValueError:
            parsed = raw
        answers[name] = {"status": status, "body": parsed}
        entry = {"name": name, "status": status, "raw": json.dumps(parsed, sort_keys=True),
                 "normalized": normalize(parsed, cfg["normalizers"])}
        if status in (301, 302, 303, 307, 308):
            for k, v in headers_out.items():
                if k.lower() == "location":
                    entry["location"] = v
                    break
            else:
                entry["location"] = ""
        ordered.append(entry)
        print(f"{name}: {status}", file=sys.stderr)

    out_path.write_text(json.dumps({"comment": f"Captured from vercel-labs/emulate at the pin in specs/{service}/emulate-inventory.json by scripts/capture-oracle.py --service {service}; normalized per the rules in that script.", "answers": ordered}, indent=2) + "\n")
    print(f"wrote {out_path}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--service", required=True, choices=list(SERVICES))
    args = ap.parse_args()
    cfg = SERVICES[args.service]
    base = f"http://localhost:{cfg['port']}"
    for _ in range(30):
        try:
            urllib.request.urlopen(base + cfg["probe"], timeout=2)
            break
        except urllib.error.HTTPError:
            break  # any HTTP answer means the oracle is listening
        except OSError:
            time.sleep(1)
    else:
        print(f"oracle not up; start it with: npx emulate --service {args.service} --port {cfg['port']}", file=sys.stderr)
        sys.exit(1)
    sys.exit(main(args.service))
