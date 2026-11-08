#!/usr/bin/env python3
"""Write a `go build -overlay` file that builds mirror with only the named services.

    scripts/slim-overlay.py OVERLAY.json [--cicd] SERVICE|@FILE ...
    go build -overlay OVERLAY.json -o mirror ./cmd/mirror

A service is its ID (aws.s3); @FILE reads IDs from a file, one per line, blank
lines and # comments ignored (profiles/core.txt). The binary then embeds the
models and bundles of those services only, links only their hand-written packs,
and starts and serves exactly them. Nothing in the source tree changes: the
overlay hides the other services' embedded files and swaps one import list, so
the full build is untouched and a typo here cannot leak into it.

What a smaller build gives up: a service left out answers nothing, and what in
a kept service calls one that was left out (S3 notifying a queue, SNS invoking
a function) fails at that call with the missing service's name. CI/CD executors
(CodeBuild, CodePipeline, ...) are left out unless --cicd.

The files the overlay points at are written beside it, so keep them together.
"""
import json
import os
import re
import sys

root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def dirname(service_id):
    """internal/generated and behavior name a service's directory the same way."""
    provider, _, rest = service_id.partition(".")
    return provider, re.sub(r"[^a-z0-9]", "", rest.lower())


def main(argv):
    args = [a for a in argv if a != "--cicd"]
    cicd = "--cicd" in argv
    if len(args) < 2:
        sys.exit(__doc__)
    out, wanted = os.path.abspath(args[0]), []
    for a in args[1:]:
        if a.startswith("@"):
            with open(a[1:]) as f:
                wanted += [l.split("#")[0].strip() for l in f if l.split("#")[0].strip()]
        else:
            wanted.append(a)
    keep = {dirname(s) for s in wanted}
    exists = lambda base, p, d: os.path.isdir(os.path.join(root, base, p, d))
    for s in wanted:
        p, d = dirname(s)
        if not (exists("behavior", p, d) or exists("internal/generated", p, d) or exists("internal/services", p, d)):
            sys.exit(f"slim-overlay: no service {s!r} here (looked for behavior/{p}/{d}, internal/generated/{p}/{d})")

    replace = {}
    for base in ("internal/generated", "behavior"):
        for provider in sorted(os.listdir(os.path.join(root, base))):
            pdir = os.path.join(root, base, provider)
            if not os.path.isdir(pdir):
                continue
            for d in sorted(os.listdir(pdir)):
                if (provider, d) in keep or not os.path.isdir(os.path.join(pdir, d)):
                    continue
                for here, _, files in os.walk(os.path.join(pdir, d)):
                    for f in files:
                        replace[os.path.join(here, f)] = ""  # "" hides the file from the build

    side = out + ".d"
    os.makedirs(side, exist_ok=True)

    def write(name, text):
        path = os.path.join(side, name)
        with open(path, "w") as f:
            f.write(text)
        return path

    # The one list of hand-written packs, cut to the kept services.
    allservices = os.path.join(root, "internal", "allservices", "allservices.go")
    lines = []
    for line in open(allservices).read().split("\n"):
        pack = re.search(r'internal/services/([a-z]+)/([a-z0-9]+)"', line)
        if pack and (pack.group(1), pack.group(2)) not in keep:
            continue
        if "cicd/register" in line and not cicd:
            continue
        lines.append(line)
    replace[allservices] = write("allservices.go", "\n".join(lines))

    # `*/*/ops/*.yaml` must match something or the build fails, and a handful of
    # services may have no split operations. A bundle with no service in it is
    # skipped by the loader.
    replace[os.path.join(root, "behavior", "aws", "placeholder", "ops", "placeholder.yaml")] = write(
        "placeholder.yaml", "# keeps the embed pattern matching when no kept service splits its operations\n")
    replace[os.path.join(root, "internal", "specboot", "slim.go")] = write(
        "slim.go", "package specboot\n\nfunc init() { slim = true }\n")

    with open(out, "w") as f:
        json.dump({"Replace": replace}, f)
    print(f"slim-overlay: {len(wanted)} services, {sum(1 for v in replace.values() if v == '')} embedded files hidden", file=sys.stderr)


main(sys.argv[1:])
