#!/usr/bin/env bash
# Fail unless a container image is what its release says it is.
#
#   scripts/image-check.sh IMAGE full|core VERSION ARCHIVE
#
# IMAGE is a local name or a registry reference (name@sha256:...), run on this
# machine's architecture; ARCHIVE is the release archive for that architecture and
# variant. The image must report VERSION, serve the services its variant lists (the
# full one, more than a hundred) and hold byte for byte the binary the archive does.
# The release workflow runs it on the image it built, and again on the copy it
# pulled back from the registry before that copy is promoted.
set -euo pipefail

image=$1 variant=$2 version=$3 archive=$4
profile="$(dirname "$0")/../profiles/core.txt"
fail() { echo "::error::$image: $*"; exit 1; }

[ "$(docker run --rm "$image" version)" = "$version" ] || fail "does not report $version"

got=$(docker run --rm "$image" services | cut -f1 | sort | tr '\n' ' ')
if [ "$variant" = core ]; then
  want=$(grep -v '^#' "$profile" | sort | tr '\n' ' ')
  [ "$got" = "$want" ] || fail "serves: $got; its profile lists: $want"
else
  [ "$(wc -w <<< "$got")" -gt 100 ] || fail "the full image serves only: $got"
fi

id=$(docker create "$image")
trap 'docker rm -f "$id" > /dev/null' EXIT
in_image=$(docker cp "$id":/mirror - | tar -xO | sha256sum)
in_archive=$(tar -xzOf "$archive" mirror | sha256sum)
[ "$in_image" = "$in_archive" ] || fail "its binary is not the one in $archive"
