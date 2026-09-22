#!/usr/bin/env bash
# Download and verify the pinned celld release into tools/celld-runtime/bin/.
set -euo pipefail
root=$(cd "$(dirname "$0")/.." && pwd)
pin="$root/tools/celld-runtime/PIN.json"
dir="$root/tools/celld-runtime"
mkdir -p "$dir/bin"
arch=$(uname -m)
case "$arch" in
  aarch64|arm64) key=linux-aarch64 ;;
  x86_64|amd64) key=linux-amd64 ;;
  *) echo "provision-celld: unsupported arch $arch" >&2; exit 2 ;;
esac
url=$(python3 -c "import json,sys; print(json.load(open(sys.argv[1]))['artifacts'][sys.argv[2]]['url'])" "$pin" "$key")
gzip_want=$(python3 -c "import json,sys; print(json.load(open(sys.argv[1]))['artifacts'][sys.argv[2]]['gzip_sha256'])" "$pin" "$key")
bin_want=$(python3 -c "import json,sys; print(json.load(open(sys.argv[1]))['artifacts'][sys.argv[2]]['bin_sha256'])" "$pin" "$key")
tmp=$(mktemp)
trap 'rm -f "$tmp"' EXIT
curl -fsSL -o "$tmp" "$url"
got=$(sha256sum "$tmp" | awk '{print $1}')
if [[ "$got" != "$gzip_want" ]]; then
  echo "provision-celld: gzip digest $got != $gzip_want" >&2
  exit 1
fi
gunzip -c "$tmp" > "$dir/bin/celld"
chmod +x "$dir/bin/celld"
gotb=$(sha256sum "$dir/bin/celld" | awk '{print $1}')
if [[ "$gotb" != "$bin_want" ]]; then
  echo "provision-celld: bin digest $gotb != $bin_want" >&2
  exit 1
fi
ver=$("$dir/bin/celld" --version | head -1)
echo "provision-celld: ok $ver ($key)"
