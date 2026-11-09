#!/usr/bin/env python3
"""Turn one `go test -json` stream into a readable log and a per-tier summary.

CI used to run the snapshot, chaos, BDD, contract and fuzz-seed suites as their
own steps, each re-running packages the unit run had just run. Now the unit run
is the only run, and this says afterwards how each of those tiers did, from the
same events.

Usage:
    go test -json ./... 2>&1 | scripts/test-tiers.py [TITLE]

It prints what plain `go test` would: one line per package, the full output of
every failing test and of every package that failed, and anything that was not
JSON (build errors, make's own lines) as it arrives. So `scripts/known-red.py`
reads its output like any other run. Then it prints the table, and appends it to
$GITHUB_STEP_SUMMARY when that is set.

A test lands in the first tier whose rule matches it, in the order of
test/tiers.json.
Exit status is 1 when any test or package failed, 0 otherwise.
"""
import json
import os
import re
import sys

MODULE = "github.com/tyler-r-kendrick/mirror.cloud/"

# (tier, package pattern, test-name pattern), from test/tiers.json, which says
# what each tier holds and which tests the stages run.
with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "test", "tiers.json")) as f:
    TIERS = [(t["name"], t.get("packages", ".*"), t.get("tests", ".*")) for t in json.load(f)["tiers"]]
RULES = [(name, re.compile(p), re.compile(t)) for name, p, t in TIERS]
ORDER = []
for name, _, _ in TIERS:
    if name not in ORDER:
        ORDER.append(name)


def tier(pkg, test):
    rel = pkg[len(MODULE):] if pkg.startswith(MODULE) else pkg
    for name, p, t in RULES:
        if p.search(rel) and t.search(test):
            return name
    return "unit"


def main():
    out = sys.stdout
    tests = {}  # (pkg, top-level test) -> pass|fail|skip
    test_out = {}  # (pkg, test) -> output lines
    pkg_out = {}  # pkg -> package-level output lines
    cached, failed_pkgs, seconds = set(), set(), {}
    failed = False
    for line in sys.stdin:
        try:
            ev = json.loads(line)
            if not isinstance(ev, dict):
                raise ValueError
        except ValueError:
            out.write(line)
            continue
        action = ev.get("Action", "")
        if action == "build-output":
            out.write(ev.get("Output", ""))
            continue
        if action == "build-fail":
            failed = True
            continue
        pkg, test, text = ev.get("Package", ""), ev.get("Test"), ev.get("Output")
        if test:
            key = (pkg, test)
            if action == "output":
                test_out.setdefault(key, []).append(text)
            elif action in ("pass", "fail", "skip"):
                lines = test_out.pop(key, [])
                if action == "fail":
                    out.writelines(lines)
                if "/" not in test:
                    tests[key] = action
            continue
        if action == "output":
            pkg_out.setdefault(pkg, []).append(text)
            if "(cached)" in text:
                cached.add(pkg)
        elif action in ("pass", "fail", "skip"):
            lines = pkg_out.pop(pkg, [])
            if action != "skip":  # a package with no test files
                seconds[pkg] = ev.get("Elapsed") or 0
            if action == "fail":
                failed = True
                failed_pkgs.add(pkg)
                out.writelines(lines)
            else:
                out.writelines(l for l in lines if re.match(r"(ok|\?)\s", l))
    # Whatever a killed run left unfinished is printed, not dropped.
    for lines in list(test_out.values()) + list(pkg_out.values()):
        out.writelines(lines)

    rows = {name: {"pkgs": set(), "pass": 0, "fail": 0, "skip": 0, "cached": 0} for name in ORDER}
    for (pkg, test), result in tests.items():
        row = rows[tier(pkg, test)]
        row["pkgs"].add(pkg)
        row[result] += 1
        if pkg in cached:
            row["cached"] += 1
        if result == "fail":
            failed = True
    for pkg in failed_pkgs:
        if not any((p == pkg and r == "fail") for (p, _), r in tests.items()):
            rows["unit"]["fail"] += 1
            rows["unit"]["pkgs"].add(pkg)

    head = "| tier | packages | passed | failed | skipped | from cache |"
    table = [head, "|---|---:|---:|---:|---:|---:|"]
    for name in ORDER:
        r = rows[name]
        table.append("| %s | %d | %d | %d | %d | %d |" % (
            name, len(r["pkgs"]), r["pass"], r["fail"], r["skip"], r["cached"]))
    ran = [p for p in seconds if p not in cached]
    table.append("")
    table.append("%d packages with tests, %d from cache; %.1fs in the %d that ran." % (
        len(seconds), len(seconds) - len(ran), sum(seconds[p] for p in ran), len(ran)))
    out.write("\n" + "\n".join(table) + "\n")
    summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary:
        title = sys.argv[1] if len(sys.argv) > 1 else "go test"
        with open(summary, "a") as f:
            f.write("### %s\n\n%s\n\n" % (title, "\n".join(table)))
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
