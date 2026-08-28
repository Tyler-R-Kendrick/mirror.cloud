#!/usr/bin/env bash
# Fetch pinned provider specs into specs/ and rewrite specs/mirror.lock.
#
# Service IDs are resolved by asking `mirrorgen -index` what each upstream
# model declares, not by guessing directory names: the receivers that derive
# IDs are the same ones that generate the models, so the mapping cannot drift.
# specs/aws-dirs.json carries the reviewed exceptions (our ID differs from the
# model's endpointPrefix, or the prefix is not unique upstream).
#
# Unresolved services are fatal. A previous version warned and continued,
# which is how most of the declared set ended up with no spec at all.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

AWS_REPO="${AWS_REPO:-https://github.com/aws/api-models-aws}"
AWS_REF="${AWS_REF:-main}"
GCS_URL='https://storage.googleapis.com/$discovery/rest?version=v1'
LOCK="$ROOT/specs/mirror.lock"
SET="$ROOT/specs/mirror.set"
DIRS="$ROOT/specs/aws-dirs.json"

die() {
  echo "specs-sync: $*" >&2
  exit 1
}

need_cmd() {
  command -v "$1" >/dev/null 2>&1 || die "missing required command: $1"
}

need_cmd git
need_cmd sha256sum
need_cmd python3
need_cmd go

fetch() {
  if command -v curl >/dev/null 2>&1; then
    curl -fsSL --retry 2 --max-time 60 "$1"
  elif command -v wget >/dev/null 2>&1; then
    wget -qO- --timeout=60 "$1"
  else
    die "need curl or wget"
  fi
}

mkdir -p specs/aws specs/gcp

want_gcp=0
want_aws=0
if [[ -f "$SET" ]]; then
  while read -r id _rest; do
    [[ -z "$id" || "$id" == \#* ]] && continue
    case "$id" in
      gcp.storage) want_gcp=1 ;;
      aws.*) want_aws=1 ;;
    esac
  done < "$SET"
fi

TMP="$(mktemp -d "${TMPDIR:-/tmp}/mirror-specs.XXXXXX")"
cleanup() { rm -rf "$TMP"; }
trap cleanup EXIT

AWS_SHA=""
copied=()

if ((want_aws)); then
  echo "specs-sync: cloning $AWS_REPO@$AWS_REF (shallow)…" >&2
  if ! git clone --depth 1 --filter=blob:none --sparse --branch "$AWS_REF" "$AWS_REPO" "$TMP/aws" >/dev/null 2>"$TMP/git.err"; then
    # Some git builds reject --filter; retry a plain shallow clone.
    if ! git clone --depth 1 --branch "$AWS_REF" "$AWS_REPO" "$TMP/aws" >/dev/null 2>>"$TMP/git.err"; then
      die "git clone failed: $(tr '\n' ' ' < "$TMP/git.err")"
    fi
  else
    # Every model is needed: the index asks each one which service it is.
    git -C "$TMP/aws" sparse-checkout set --no-cone 'models/**' >/dev/null 2>>"$TMP/git.err" \
      || echo "specs-sync: sparse-checkout failed; using full tree" >&2
  fi

  AWS_SHA="$(git -C "$TMP/aws" rev-parse HEAD)"
  echo "specs-sync: aws pin $AWS_SHA" >&2

  echo "specs-sync: indexing upstream models…" >&2
  go run ./cmd/mirrorgen -index "$TMP/aws/models" > "$TMP/index.tsv" 2>"$TMP/index.err" \
    || die "mirrorgen -index failed: $(tr '\n' ' ' < "$TMP/index.err")"
  echo "specs-sync: indexed $(wc -l < "$TMP/index.tsv") upstream model(s)" >&2

  python3 "$ROOT/scripts/resolve_specs.py" \
    --index "$TMP/index.tsv" --dirs "$DIRS" --set "$SET" --models "$TMP/aws/models" \
    > "$TMP/resolved.tsv" || die "could not resolve every service in $SET"

  : > "$TMP/copied.tsv"
  while IFS=$'\t' read -r sid rel; do
    [[ -z "$rel" ]] && continue
    src="$TMP/aws/models/$rel"
    dest="$ROOT/specs/aws/$rel"
    mkdir -p "$(dirname "$dest")"
    cp -f "$src" "$dest"
    printf '%s\t%s\n' "$sid" "aws/$rel" >> "$TMP/copied.tsv"
    copied+=("aws/$rel")
  done < "$TMP/resolved.tsv"
  echo "specs-sync: copied ${#copied[@]} aws model(s)" >&2
fi

gcs_etag=""
if ((want_gcp)); then
  echo "specs-sync: fetching GCS discovery JSON…" >&2
  if ! fetch "$GCS_URL" > "$TMP/storage.json"; then
    die "failed to fetch $GCS_URL"
  fi
  mkdir -p "$ROOT/specs/gcp"
  cp -f "$TMP/storage.json" "$ROOT/specs/gcp/storage.json"
  # ETag is optional; the hash is the lock.
  gcs_etag="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
fi

ingested="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

touch "$TMP/copied.tsv"
python3 - "$LOCK" "$AWS_REPO" "$AWS_SHA" "$GCS_URL" "$ingested" "$gcs_etag" "$TMP/copied.tsv" <<'PY'
import json, os, sys, hashlib

lock_path = sys.argv[1]
aws_repo, aws_sha, gcs_url, ingested, gcs_etag = sys.argv[2:7]
pairs_path = sys.argv[7]
root = os.path.dirname(os.path.dirname(lock_path))

def sha256(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 16), b""):
            h.update(chunk)
    return h.hexdigest()

files = []
with open(pairs_path, encoding="utf-8") as fh:
    for line in fh:
        line = line.rstrip("\n")
        if not line:
            continue
        service_id, _, rel = line.partition("\t")
        path = os.path.join(root, "specs", rel)
        files.append({
            # serviceId is the canonical mirror.cloud ID for this model. It is
            # recorded here because the ID a model declares (its endpointPrefix)
            # is neither always ours nor always unique upstream; mirrorgen reads
            # it back so generation and serving agree on one identity.
            "serviceId": service_id,
            "source": aws_repo,
            "ref": aws_sha,
            "path": rel,
            "sha256": sha256(path),
            "ingested": ingested,
        })
gcs = os.path.join(root, "specs", "gcp", "storage.json")
if os.path.isfile(gcs):
    files.append({
        "serviceId": "gcp.storage",
        "source": gcs_url,
        "ref": gcs_etag,
        "path": "gcp/storage.json",
        "sha256": sha256(gcs),
        "ingested": ingested,
    })
files.sort(key=lambda x: x["path"])
doc = {
    "schemaVersion": "1",
    "comment": "Lockfile for vendored provider specs. Fields per file: source, ref, path, sha256, ingested. make specs-sync is the only writer.",
    "pin": {
        "aws": {"source": aws_repo, "ref": aws_sha},
        "gcp.storage": {"source": gcs_url, "ref": gcs_etag},
    },
    "files": files,
}
os.makedirs(os.path.dirname(lock_path), exist_ok=True)
with open(lock_path, "w") as f:
    json.dump(doc, f, indent=2)
    f.write("\n")
print("specs-sync: wrote %s (%d files)" % (lock_path, len(files)), file=sys.stderr)
PY
