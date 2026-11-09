#!/usr/bin/env python3
"""Check out only the services you work on.

    git clone --filter=blob:none --no-checkout URL mirror.cloud && cd mirror.cloud
    git show HEAD:scripts/sparse.py | python3 - aws.s3 aws.sqs       # first time
    scripts/sparse.py aws.s3 aws.sqs @profiles/core.txt vercel.*     # later

Each argument is a service ID (`mirror services` lists them), @FILE (one ID per
line, # comments; profiles/core.txt), or PROVIDER.* for every service of one
provider. The checkout then holds everything that is not a service, plus, for
each service asked for:

  - its hand-written pack (internal/services), its model (internal/generated)
    and its bundle (behavior), and the one file that links the pack into the
    binary (internal/allservices/svc_*.go);
  - the packs it needs to build and to run its own tests, and theirs, from
    profiles/graph.txt (make graph): S3 notifies SNS, Lambda and EventBridge,
    so asking for aws.s3 brings those along, and nothing else;
  - for PROVIDER.* only, that provider's vendored specs and behavior suite.

A whole-repository suite whose tests import a service that was not brought
(internal/chaos, internal/conformance, test/behavior/aws, ...) is left out. The
tree then builds and tests with plain `go build ./...` and `go test ./...`: the
parts that name a service are one file or one directory each, so leaving it
out leaves nothing dangling. `go test` reports a package whose tests need a
service that is not here as "setup failed"; `make test-present` runs the rest.

    scripts/sparse.py --list aws.s3     show what would be checked out, change nothing
    scripts/sparse.py --all             back to the whole tree

Everything is read from the commit (--ref, default HEAD), never from the files
on disk, so it runs before anything is checked out and fetches no file it
leaves out.
"""
import re
import subprocess
import sys


def git(*args, ok=False):
    p = subprocess.run(["git", *args], capture_output=True, text=True)
    if p.returncode and not ok:
        sys.exit(f"sparse: git {' '.join(args)}: {p.stderr.strip()}")
    return p.stdout


def key_of(service_id):
    """internal/generated and behavior name a service's directory this way."""
    provider, _, rest = service_id.partition(".")
    return f"{provider}/{re.sub(r'[^a-z0-9]', '', rest.lower())}"


def main(argv):
    ref, listing, wanted = "HEAD", False, []
    it = iter(argv)
    for a in it:
        if a == "--all":
            git("sparse-checkout", "disable")
            return
        elif a == "--list":
            listing = True
        elif a == "--ref":
            ref = next(it)
        elif a in ("-h", "--help") or not argv:
            sys.exit(__doc__)
        elif a.startswith("@"):
            try:
                text = open(a[1:]).read()
            except OSError:
                text = git("show", f"{ref}:{a[1:]}")
            wanted += [l.split("#")[0].strip() for l in text.splitlines() if l.split("#")[0].strip()]
        else:
            wanted.append(a)
    if not wanted:
        sys.exit(__doc__)

    dirs = set(git("ls-tree", "-r", "-d", "--name-only", ref).split())
    modeled = {}  # provider/service -> True, for what has a model or a bundle
    for d in dirs:
        m = re.fullmatch(r"(?:internal/generated|behavior)/([a-z0-9]+)/([a-z0-9]+)", d)
        if m and m.group(1) != "none":
            modeled[f"{m.group(1)}/{m.group(2)}"] = True

    requested, whole = set(), set()
    for w in wanted:
        if w.endswith(".*"):
            whole.add(w[:-2])
            requested |= {k for k in modeled if k.startswith(w[:-1].replace(".", "/"))}
        elif key_of(w) in modeled:
            requested.add(key_of(w))
        else:
            sys.exit(f"sparse: no service {w!r} in {ref} (looked for internal/generated/{key_of(w)}); `mirror services` lists them")
    if whole - {k.split("/")[0] for k in requested}:
        sys.exit(f"sparse: no provider {sorted(whole - {k.split('/')[0] for k in requested})} in {ref}")

    prod, test = {}, {}
    for line in git("show", f"{ref}:profiles/graph.txt").splitlines():
        m = re.fullmatch(r"([^ :]+)( \[test\])?: (.*)", line)
        if m:
            (test if m.group(2) else prod).setdefault(m.group(1), set()).update(m.group(3).split())

    # What S needs: its production imports and, because its tests are to run, its
    # test imports; for each of those, production imports only (its own tests do
    # not run here).
    need, todo = set(), list(requested)
    while todo:
        k = todo.pop()
        if k in need:
            continue
        need.add(k)
        d = f"internal/services/{k}"
        todo += prod.get(d, set()) | (test.get(d, set()) if k in requested else set())

    exclude = ["internal/specboot/complete.go"]  # the tree is not complete
    include = ["behavior/none/placeholder"]  # keeps behavior's //go:embed pattern matching
    for k in sorted(need):
        include += [f"{base}/{k}" for base in ("internal/services", "internal/generated", "behavior") if f"{base}/{k}" in dirs]
    svc_files = git("ls-tree", "--name-only", ref, "internal/allservices/").split()
    include += [f for f in svc_files if f.rsplit("/", 1)[-1] in {f"svc_{k.replace('/', '_')}.go" for k in need}]
    providers = {k.split("/")[0] for k in need if k in modeled}
    for p in sorted(whole):
        include += [d for d in (f"specs/{p}",) if d in dirs]
    leave_out = []
    for d in sorted(set(prod) | set(test)):
        if d.startswith("internal/services/"):
            continue
        deps = prod.get(d, set()) | test.get(d, set())
        has_code = any(f.endswith(".go") and not f.endswith("_test.go")
                       for f in git("ls-tree", "--name-only", ref, d + "/").split())
        if not has_code and not deps <= need:
            leave_out.append(d)  # a suite that imports a service that is not here
    for p in sorted(whole):
        d = f"test/behavior/{p}"
        if d in dirs and d not in leave_out:
            include.append(d)
    exclude += leave_out

    patterns = ["/*", "!/internal/services/*/*/", "!/internal/generated/*/*/", "!/behavior/*/*/",
                "!/internal/allservices/svc_*.go", "!/specs/*/", "!/test/behavior/*/"]
    patterns += [f"!/{e}" + ("/" if e in dirs else "") for e in exclude]
    patterns += [f"/{i}" + ("/" if i in dirs else "") for i in include]

    sys.stderr.write(
        f"sparse: {len(requested)} services asked for, {len(need - requested)} brought along "
        f"({', '.join(sorted(need - requested)) or 'none'}); left out: "
        f"{', '.join(leave_out) or 'no suite'}\n")
    if listing:
        print("\n".join(patterns))
        return
    p = subprocess.run(["git", "sparse-checkout", "set", "--no-cone", "--stdin"], input="\n".join(patterns) + "\n",
                       text=True, capture_output=True)
    if p.returncode:
        sys.exit(f"sparse: {p.stderr.strip()}")
    if not git("ls-files", "-s", ok=True).strip() or not git("ls-files", "--sparse", "--", "go.mod", ok=True).strip():
        git("checkout", "-q", ref, ok=True)


main(sys.argv[1:])
