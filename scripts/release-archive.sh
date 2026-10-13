#!/usr/bin/env bash
# Build the release archives, and the checksum file over them.
#
#   export SOURCE_DATE_EPOCH="$(git log -1 --format=%ct)"   # the tagged commit's time
#   scripts/release-archive.sh archive TAG GOOS GOARCH OUTDIR
#   scripts/release-archive.sh pack    TAG GOOS GOARCH BINARY OUTDIR
#   scripts/release-archive.sh sums    DIR
#
# archive builds ./cmd/mirror for GOOS/GOARCH and packs it; pack packs a binary
# that is already built (what archive does second, and how the zip layout is
# tested without a Windows build); sums writes DIR/SHA256SUMS over the archives
# in DIR. .github/workflows/release.yml runs archive once per platform and sums
# once, and docs/RELEASING.md says what the outputs are and how to verify them.
#
# An archive is mirror_<TAG>_<GOOS>_<GOARCH>.tar.gz (.zip for windows) holding
# exactly two files, LICENSE and the binary (mirror, or mirror.exe). The same
# commit, toolchain and archive tools give the same bytes:
#
#   - the binary: CGO_ENABLED=0, -trimpath, -buildvcs=false (no checkout state,
#     no clock in it), -s -w, and the version set by the linker to TAG without
#     its leading v -- the form `mirror version` already prints (0.1.0-dev);
#   - the tar: members sorted by name, every mtime SOURCE_DATE_EPOCH, owner and
#     group 0 with no names, ustar headers (no pax, so no timestamps of ours),
#     modes 0755 for the binary and 0644 for LICENSE;
#   - the gzip: -n, so no file name and no time in its header;
#   - the zip: -X (no extra attributes), mtimes set to SOURCE_DATE_EPOCH and
#     read in UTC, members in the order given.
#
# SOURCE_DATE_EPOCH is required rather than guessed so that two runs cannot
# differ because one of them was on another commit. GNU tar and gzip and
# Info-ZIP zip are required, which is to say a Linux machine; the Go toolchain
# is part of the input (the release workflow pins an exact version, and
# GOTOOLCHAIN=local keeps this script from switching to another one).
set -euo pipefail

MODULE=github.com/tyler-r-kendrick/mirror.cloud
TAG_RE='^v[0-9][0-9A-Za-z._+-]*$'
ROOT=$(cd "$(dirname "$0")/.." && pwd)
WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT

die() { echo "release-archive: $*" >&2; exit 2; }

usage() {
  cat >&2 <<'EOF'
usage: SOURCE_DATE_EPOCH=<unix time> scripts/release-archive.sh
         archive TAG GOOS GOARCH OUTDIR
         pack    TAG GOOS GOARCH BINARY OUTDIR
         sums    DIR
EOF
  exit 2
}

# Everything that ends up in a file name, an environment variable or a linker
# flag is checked first: the tag comes from a pushed tag name or a workflow input.
check_target() {
  [[ $1 =~ $TAG_RE ]] || die "tag $1 does not match $TAG_RE"
  [[ $2 =~ ^[a-z0-9]+$ && $3 =~ ^[a-z0-9]+$ ]] || die "bad GOOS/GOARCH: $2/$3"
  [[ ${SOURCE_DATE_EPOCH:-} =~ ^[0-9]+$ ]] || die "SOURCE_DATE_EPOCH must be a Unix time, e.g. \$(git log -1 --format=%ct)"
}

binary_name() { if [ "$1" = windows ]; then echo mirror.exe; else echo mirror; fi; }

cmd_archive() {
  [ $# -eq 4 ] || usage
  check_target "$1" "$2" "$3"
  local tag=$1 goos=$2 goarch=$3 out=$4 bin
  bin=$WORK/build/$(binary_name "$goos")
  (
    cd "$ROOT"
    export GOTOOLCHAIN=local GOWORK=off
    CGO_ENABLED=0 GOOS=$goos GOARCH=$goarch "${GO:-go}" build \
      -trimpath -buildvcs=false \
      -ldflags "-s -w -X ${MODULE}/internal/runtime.Version=${tag#v}" \
      -o "$bin" ./cmd/mirror
  )
  cmd_pack "$tag" "$goos" "$goarch" "$bin" "$out"
}

cmd_pack() {
  [ $# -eq 5 ] || usage
  check_target "$1" "$2" "$3"
  local tag=$1 goos=$2 goarch=$3 binary=$4 out bin stage name archive
  [ -f "$binary" ] || die "no such binary: $binary"
  mkdir -p "$5"
  out=$(cd "$5" && pwd)
  bin=$(binary_name "$goos")
  name=mirror_${tag}_${goos}_${goarch}
  stage=$WORK/stage
  rm -rf "$stage"
  mkdir -p "$stage"

  cp "$binary" "$stage/$bin"
  cp "$ROOT/LICENSE" "$stage/LICENSE"
  chmod 0755 "$stage/$bin"
  chmod 0644 "$stage/LICENSE"
  touch -d "@${SOURCE_DATE_EPOCH}" "$stage/$bin" "$stage/LICENSE"

  if [ "$goos" = windows ]; then
    archive=$name.zip
    rm -f "$out/$archive"
    (cd "$stage" && TZ=UTC zip -X -9 -q "$out/$archive" LICENSE "$bin")
  else
    archive=$name.tar.gz
    (cd "$stage" && LC_ALL=C tar --format=ustar --sort=name --mtime="@${SOURCE_DATE_EPOCH}" \
      --owner=0 --group=0 --numeric-owner -cf - LICENSE "$bin") | gzip -n -9 > "$out/$archive"
  fi
  (cd "$out" && sha256sum "$archive")
}

cmd_sums() {
  [ $# -eq 1 ] || usage
  [ -d "$1" ] || die "no such directory: $1"
  cd "$1"
  shopt -s nullglob
  local files=(mirror_*.tar.gz mirror_*.zip)
  [ ${#files[@]} -gt 0 ] || die "no mirror_*.tar.gz or mirror_*.zip in $1"
  printf '%s\n' "${files[@]}" | LC_ALL=C sort | xargs -d '\n' sha256sum > SHA256SUMS
  sha256sum --check --quiet SHA256SUMS
  cat SHA256SUMS
}

[ $# -ge 1 ] || usage
sub=$1
shift
case $sub in
  archive) cmd_archive "$@" ;;
  pack)    cmd_pack "$@" ;;
  sums)    cmd_sums "$@" ;;
  *)       usage ;;
esac
