#!/usr/bin/env python3
"""Say which of a `go test` run's failures were expected and which were not.

A step that is already red absorbs the next failure without trace. That is not
a hypothetical: TestGenerateCatalogIdempotent was red on main for two merges
inside a step red for the ratchet, and a Firehose test failed once in the same
step and was invisible for the same reason. Neither was hidden cleverly. Both
were hidden by a colour that already meant `failing`.

So this changes nothing about what passes. Every entry in known-red.json is
still a failing test and still fails its step -- suppressing them would trade
one kind of blindness for another, and the ratchet entries in particular exist
to stay red until the packs they measure are gone. What it changes is that the
END of the step says whether the red is the one expected or a new one, BY NAME.
That is the difference between a gate and a colour.

Usage:
    go test ./... 2>&1 | tee out.txt; scripts/known-red.py out.txt
    scripts/known-red.py < out.txt

Exit status is 0 when every failure was declared (including when there were
none) and 1 when any was not.
"""
import json
import os
import re
import sys

# Not anchored at the start of the line. `go test` indents a subtest's result,
# and a log downloaded from GitHub Actions prefixes every line with a
# timestamp -- which an anchored pattern matches none of, so the first run of
# this against a real CI log reported "no new failures" about a log with
# twenty-four of them. A tool whose whole job is to say nothing new broke must
# not be able to say that about input it did not understand, which is what
# UNDERSTOOD below is for.
FAIL = re.compile(r"--- FAIL: (\S+)")
UNDERSTOOD = re.compile(r"(^|\s)(ok|FAIL|---|\?)\s")
RULE = "known-red: " + "=" * 62


def main(argv):
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    path = os.path.join(root, "known-red.json")
    try:
        with open(path) as f:
            declared = {e["test"]: e for e in json.load(f)["expected"]}
    except (OSError, ValueError, KeyError) as err:
        print("known-red: cannot read %s: %s" % (path, err), file=sys.stderr)
        return 2

    src = sys.stdin if len(argv) < 2 or argv[1] == "-" else open(argv[1], errors="replace")
    seen, unknown, understood = [], [], False
    for line in src:
        if not understood and UNDERSTOOD.search(line):
            understood = True
        m = FAIL.search(line)
        if not m:
            continue
        name = m.group(1)
        if name in seen or name in unknown:
            continue
        (seen if name in declared else unknown).append(name)

    # Silence is the one answer this must never give by accident. An empty
    # input, a file that is not a test run, a pipe that carried stderr
    # somewhere else -- each produces no failures, and "no new failures" about
    # any of them is a lie in the direction that costs the most.
    if not understood:
        print()
        print(RULE)
        print("known-red: this input carries no `go test` output at all -- no ok,")
        print("known-red: FAIL or --- lines. Reporting `no new failures` about it")
        print("known-red: would be exactly the silence this exists to prevent, so")
        print("known-red: it reports nothing instead. Check that the run was teed")
        print("known-red: with 2>&1 and that the file is not empty.")
        print(RULE)
        return 2

    # A parent whose only declared failure is a subtest fails because of it.
    # TestMutantsAreKilled is red because one of its mutants survived; naming
    # the parent in the list would excuse every other mutant it runs, so the
    # list names subtests and the parent is inferred here.
    parents = {t.split("/", 1)[0] for t in declared if "/" in t}
    unknown = [t for t in unknown
               if not (t in parents and any(s.startswith(t + "/") for s in seen))]

    print()
    print(RULE)
    if unknown:
        print("known-red: NEW FAILURES, not on the expected list:")
        for t in unknown:
            print("known-red:   %s" % t)
        print("known-red:")
        print("known-red: Diagnose these before reading the expected ones below.")
        print("known-red: If one is genuinely expected, add it to known-red.json")
        print("known-red: with a reason -- never to make a step quieter.")
    else:
        print("known-red: no new failures.")
    if seen:
        print("known-red:")
        print("known-red: expected, and still red on purpose:")
        for t in seen:
            print("known-red:   %s -- %s" % (t, declared[t]["why"]))
    missing = sorted(t for t in declared if t not in seen)
    if missing and not unknown:
        print("known-red:")
        print("known-red: on the expected list and NOT seen in this run. Either the")
        print("known-red: run did not cover them -- the mutation suite is sharded, so")
        print("known-red: a shard sees a quarter of it -- or they are green now and")
        print("known-red: the entry should be deleted:")
        for t in missing:
            print("known-red:   %s" % t)
    print(RULE)
    return 1 if unknown else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
