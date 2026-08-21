#!/usr/bin/env bash
# Fetch pinned provider specs into specs/ and rewrite specs/mirror.lock.
# Network failure prints a clear error and leaves the bootstrap catalog in place.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

AWS_REPO="${AWS_REPO:-https://github.com/aws/api-models-aws}"
AWS_REF="${AWS_REF:-main}"
GCS_URL='https://storage.googleapis.com/$discovery/rest?version=v1'
LOCK="$ROOT/specs/mirror.lock"
SET="$ROOT/specs/mirror.set"

# sdk-id directory names in api-models-aws (verified: models/<sdk-id>/service/<version>/*.json)
declare -A AWS_DIR=(
  [s3]=s3
  [dynamodb]=dynamodb
  [sqs]=sqs
  [sns]=sns
  [sts]=sts
  [iam]=iam
  [ssm]=ssm
  [secretsmanager]=secrets-manager
  [cloudwatch]=cloudwatch
  [logs]=cloudwatch-logs
  [kms]=kms
  [kinesis]=kinesis
  [events]=eventbridge
  [ecr]=ecr
  [ecs]=ecs
  [eks]=eks
  [elasticache]=elasticache
  [rds]=rds
  [redshift]=redshift
  [cloudformation]=cloudformation
  [apigateway]=api-gateway
  [lambda]=lambda
  [route53]=route-53
  [acm]=acm
  [elbv2]=elastic-load-balancing-v2
  [autoscaling]=auto-scaling
  [applicationautoscaling]=application-auto-scaling
  [resourcegroupstaggingapi]=resource-groups-tagging-api
)

die() {
  echo "specs-sync: $*" >&2
  echo "specs-sync: leaving bootstrap catalog as fallback (specs/mirror.lock files=[])." >&2
  exit 1
}

need_cmd() {
  command -v "$1" >/dev/null 2>&1 || die "missing required command: $1"
}

need_cmd git
need_cmd sha256sum

fetch() {
  if command -v curl >/dev/null 2>&1; then
    curl -fsSL --retry 2 --max-time 60 "$1"
  elif command -v wget >/dev/null 2>&1; then
    wget -qO- --timeout=60 "$1"
  else
    die "need curl or wget"
  fi
}

sha256_of() {
  sha256sum "$1" | awk '{print $1}'
}

mkdir -p specs/aws specs/gcp

aws_ids=()
want_gcp=0
if [[ -f "$SET" ]]; then
  while read -r id _rest; do
    [[ -z "$id" || "$id" == \#* ]] && continue
    case "$id" in
      gcp.storage) want_gcp=1 ;;
      aws.*) aws_ids+=("${id#aws.}") ;;
    esac
  done < "$SET"
fi

sparse=()
for id in "${aws_ids[@]+"${aws_ids[@]}"}"; do
  dir="${AWS_DIR[$id]:-}"
  if [[ -z "$dir" ]]; then
    dir="$id"
  fi
  sparse+=("models/$dir")
done

TMP="$(mktemp -d "${TMPDIR:-/tmp}/mirror-specs.XXXXXX")"
cleanup() { rm -rf "$TMP"; }
trap cleanup EXIT

echo "specs-sync: cloning $AWS_REPO@$AWS_REF (shallow)…" >&2
if ! git clone --depth 1 --filter=blob:none --sparse --branch "$AWS_REF" "$AWS_REPO" "$TMP/aws" >/dev/null 2>"$TMP/git.err"; then
  # Some git builds reject --filter; retry a plain shallow clone.
  if ! git clone --depth 1 --branch "$AWS_REF" "$AWS_REPO" "$TMP/aws" >/dev/null 2>>"$TMP/git.err"; then
    die "git clone failed: $(tr '\n' ' ' < "$TMP/git.err")"
  fi
else
  if ((${#sparse[@]} > 0)); then
    git -C "$TMP/aws" sparse-checkout set --no-cone "${sparse[@]}" >/dev/null 2>>"$TMP/git.err" \
      || echo "specs-sync: sparse-checkout failed; using full tree" >&2
  fi
fi

AWS_SHA="$(git -C "$TMP/aws" rev-parse HEAD)"
echo "specs-sync: aws pin $AWS_SHA" >&2

# Copy each requested service JSON, preserving upstream relative path under specs/aws/.
copied=()
for id in "${aws_ids[@]+"${aws_ids[@]}"}"; do
  dir="${AWS_DIR[$id]:-$id}"
  src_dir="$TMP/aws/models/$dir"
  if [[ ! -d "$src_dir" ]]; then
    echo "specs-sync: warning: no models/$dir in pin (skip $id)" >&2
    continue
  fi
  while IFS= read -r -d '' f; do
    rel="${f#"$TMP/aws/"}"
    dest="$ROOT/specs/aws/$rel"
    mkdir -p "$(dirname "$dest")"
    cp -f "$f" "$dest"
    copied+=("$rel")
  done < <(find "$src_dir" -name '*.json' -print0 | sort -z)
done

gcs_etag=""
if ((want_gcp)); then
  echo "specs-sync: fetching GCS discovery JSON…" >&2
  if ! fetch "$GCS_URL" > "$TMP/storage.json"; then
    die "failed to fetch $GCS_URL"
  fi
  mkdir -p "$ROOT/specs/gcp"
  cp -f "$TMP/storage.json" "$ROOT/specs/gcp/storage.json"
  # ETag is optional; hash is the lock.
  gcs_etag="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
fi

ingested="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

python3 - "$LOCK" "$AWS_REPO" "$AWS_SHA" "$GCS_URL" "$ingested" "$gcs_etag" "${copied[@]+"${copied[@]}"}" <<'PY'
import json, os, sys, hashlib

lock_path = sys.argv[1]
aws_repo, aws_sha, gcs_url, ingested, gcs_etag = sys.argv[2:7]
copied = sys.argv[7:]
root = os.path.dirname(os.path.dirname(lock_path))

def sha256(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 16), b""):
            h.update(chunk)
    return h.hexdigest()

files = []
for rel in copied:
    path = os.path.join(root, "specs", "aws", rel)
    files.append({
        "source": aws_repo,
        "ref": aws_sha,
        "path": rel,
        "sha256": sha256(path),
        "ingested": ingested,
    })
gcs = os.path.join(root, "specs", "gcp", "storage.json")
if os.path.isfile(gcs):
    files.append({
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
