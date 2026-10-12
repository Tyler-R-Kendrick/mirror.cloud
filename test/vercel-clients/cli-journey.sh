#!/usr/bin/env bash
# The deploy path through Vercel's own CLI, against a running mirror.
#
#   MIRROR_URL=http://127.0.0.1:4566 MIRROR_VERCEL_CLIENT_DIR=/path/to/toolchain \
#     test/vercel-clients/cli-journey.sh
#
# MIRROR_VERCEL_CLIENT_DIR holds node_modules with the `vercel` package
# installed. The CLI is pointed at the emulator with --api; the token reaches it
# in the environment of this process only, never in a file. The emulator takes any
# token; for a real deploy, hand the CLI its token through a secrets broker.
set -euo pipefail

: "${MIRROR_VERCEL_CLIENT_DIR:?set MIRROR_VERCEL_CLIENT_DIR to a directory with the vercel package installed}"
VC="$MIRROR_VERCEL_CLIENT_DIR/node_modules/.bin/vercel"
[ -x "$VC" ] || { echo "no vercel binary at $VC" >&2; exit 2; }
API="${MIRROR_URL:-http://127.0.0.1:4566}"
export VERCEL_TOKEN="${VERCEL_TOKEN:-mirror-dev-token}"
NAME="${CLI_PROJECT:-cli-journey}"
vc() { "$VC" --api "$API" "$@"; }

work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT
cd "$work"
printf '<!doctype html><h1>cli</h1>\n' > index.html

vc whoami
vc project add "$NAME"
vc link --yes --project "$NAME"
printf 'https://api.example.test' | vc env add API_URL production
vc env ls
url="$(vc deploy --prod --yes)"
echo "deployed: $url"
vc inspect "$url"
vc alias set "$url" cli.example.test
vc ls
vc remove "$NAME" --yes
echo "cli journey ok against $API"
