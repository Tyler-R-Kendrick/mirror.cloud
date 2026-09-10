#!/usr/bin/env python3
"""Resolve every service in specs/mirror.set to one upstream model path.

Resolution order, most trustworthy first:

1. The service ID the upstream model itself declares, from `mirrorgen -index`.
   This is derived by the same receivers that generate the models, so it
   cannot drift from them.
2. specs/aws-dirs.json "aliases": a reviewed exception map from our service ID
   to an upstream directory, for the two cases the index cannot cover — our ID
   differs from the model's endpointPrefix, or the prefix is shared by several
   upstream services (rds/docdb/neptune, apigateway v1/v2, elb v1/v2).
3. specs/aws-dirs.json "unavailable": services with no upstream model at all,
   listed with a reason. Reported and skipped rather than silently dropped.

Anything else is a hard error: exit non-zero, name the services, change
nothing. Silent skipping is how most of the declared set ended up with no
spec while the pipeline reported success.

Writes `service-id<TAB>path-relative-to-models/` for each resolved service.
"""

import argparse
import json
import os
import sys


def load_index(path):
    """service-id -> [model paths], from mirrorgen -index output.

    A declared ID can be claimed by several upstream models: RDS, DocumentDB
    and Neptune all declare endpointPrefix "rds", and Kinesis Analytics v1 and
    v2 both declare "kinesisanalytics". Such an ID identifies nothing, so the
    caller must treat it as unresolved rather than picking one arbitrarily —
    that is how DocumentDB silently became RDS.
    """
    out = {}
    with open(path, encoding="utf-8") as fh:
        for line in fh:
            line = line.rstrip("\n")
            if not line:
                continue
            sid, _, rel = line.partition("\t")
            out.setdefault(sid, []).append(rel)
    return out


def load_set(path):
    """Ordered service IDs declared in mirror.set."""
    ids = []
    with open(path, encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            ids.append(line.split()[0])
    return ids


def models_in_dir(models_root, directory):
    """Model JSONs under models/<directory>/, sorted for determinism."""
    base = os.path.join(models_root, directory)
    found = []
    for dirpath, _dirnames, filenames in os.walk(base):
        for name in sorted(filenames):
            if name.endswith(".json"):
                rel = os.path.relpath(os.path.join(dirpath, name), models_root)
                found.append(rel.replace(os.sep, "/"))
    return sorted(found)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--index", required=True, help="mirrorgen -index output")
    ap.add_argument("--dirs", required=True, help="specs/aws-dirs.json")
    ap.add_argument("--set", required=True, dest="set_path", help="specs/mirror.set")
    ap.add_argument("--models", required=True, help="upstream models/ root")
    args = ap.parse_args()

    index = load_index(args.index)
    with open(args.dirs, encoding="utf-8") as fh:
        dirs = json.load(fh)
    aliases = dirs.get("aliases", {})
    unavailable = dirs.get("unavailable", {})

    resolved = []
    skipped = []
    unresolved = []
    stale_aliases = []

    for sid in load_set(args.set_path):
        if not sid.startswith("aws."):
            continue  # non-AWS providers are fetched separately

        paths = index.get(sid, [])
        if len(paths) == 1 and sid not in aliases:
            resolved.append((sid, paths[0]))
            continue
        if len(paths) > 1 and sid not in aliases:
            unresolved.append(
                f"{sid}: {len(paths)} upstream models declare this ID "
                f"({', '.join(p.split('/')[0] for p in sorted(paths))}); "
                "add an alias naming the right directory"
            )
            continue

        if sid in aliases:
            candidates = models_in_dir(args.models, aliases[sid])
            if not candidates:
                unresolved.append(
                    f"{sid}: alias points at models/{aliases[sid]}/, which holds no model"
                )
            else:
                # One model per service directory upstream; if a directory ever
                # holds several, take the lexically last, which is the newest
                # dated API version.
                resolved.append((sid, candidates[-1]))
            continue

        if sid in unavailable:
            skipped.append(f"{sid}: {unavailable[sid]}")
            continue

        unresolved.append(f"{sid}: not declared by any upstream model and no alias")

    # An alias that the index now resolves on its own is dead weight; report it
    # so the exception map shrinks as upstream IDs stabilise.
    for sid in aliases:
        if len(index.get(sid, [])) == 1:
            stale_aliases.append(sid)

    for line in skipped:
        print(f"specs-sync: no upstream spec — {line}", file=sys.stderr)
    for sid in stale_aliases:
        print(
            f"specs-sync: alias for {sid} is now redundant (the index resolves it); "
            "drop it from specs/aws-dirs.json",
            file=sys.stderr,
        )

    if unresolved:
        print(
            f"specs-sync: {len(unresolved)} service(s) in {args.set_path} could not be "
            "resolved to an upstream model. Add an entry to specs/aws-dirs.json "
            '("aliases" for a renamed or prefix-shared service, "unavailable" with a '
            "reason if AWS publishes no model), or remove the service from mirror.set:",
            file=sys.stderr,
        )
        for line in unresolved:
            print(f"  {line}", file=sys.stderr)
        return 1

    by_path = {}
    for sid, rel in resolved:
        by_path.setdefault(rel, []).append(sid)
    collisions = {rel: ids for rel, ids in by_path.items() if len(ids) > 1}
    if collisions:
        print(
            "specs-sync: several services resolved to the same upstream model; "
            "each model backs exactly one service:",
            file=sys.stderr,
        )
        for rel, ids in sorted(collisions.items()):
            print(f"  {rel} <- {', '.join(sorted(ids))}", file=sys.stderr)
        return 1

    for sid, rel in sorted(resolved):
        print(f"{sid}\t{rel}")
    print(
        f"specs-sync: resolved {len(resolved)} service(s), "
        f"{len(skipped)} with no upstream spec",
        file=sys.stderr,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
