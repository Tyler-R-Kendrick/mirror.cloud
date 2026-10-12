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
#
# Run against vercel 63.1.2. Two things the CLI itself insists on, found by running
# it: a token may contain only letters, digits and underscores (`--token` is
# rejected for any other character, "mirror-dev-token" included), and when it
# believes an AI agent is driving it prints a JSON document to stdout where a person
# gets the deployment's URL, so the agent switches in its environment are cleared
# and the URL is read from either form.
#
# Not walked, because the emulator cannot serve them: `env pull`, `pull` and `build`
# (docs/VERCEL.md, "Real clients"). `link` tries an env pull of its own and only
# warns when it fails.
set -euo pipefail

: "${MIRROR_VERCEL_CLIENT_DIR:?set MIRROR_VERCEL_CLIENT_DIR to a directory with the vercel package installed}"
VC="$MIRROR_VERCEL_CLIENT_DIR/node_modules/.bin/vercel"
[ -x "$VC" ] || { echo "no vercel binary at $VC" >&2; exit 2; }
API="${MIRROR_URL:-http://127.0.0.1:4566}"
export VERCEL_TOKEN="${VERCEL_TOKEN:-mirror_dev_token}"
export VERCEL_TELEMETRY_DISABLED=1 NO_UPDATE_NOTIFIER=1
NAME="${CLI_PROJECT:-cli-journey}"
# The switches @vercel/detect-agent reads. Cleared, the CLI behaves as it does in a terminal.
AGENT_VARS=(AI_AGENT ANTIGRAVITY_AGENT AUGMENT_AGENT CLAUDECODE CLAUDE_CODE CLAUDE_CODE_IS_COWORK CODEX_CI CODEX_SANDBOX
  CODEX_THREAD_ID COPILOT_ALLOW_ALL COPILOT_GITHUB_TOKEN COPILOT_MODEL CURSOR_AGENT CURSOR_EXTENSION_HOST_ROLE CURSOR_TRACE_ID
  GEMINI_CLI OPENCODE OPENCODE_CLIENT REPL_ID)
for v in "${AGENT_VARS[@]}"; do unset "$v"; done
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
out="$(vc deploy --prod --yes)"
# A person's terminal gets the URL; a JSON document comes back when the CLI took the run for an agent's.
url="$(printf '%s' "$out" | node -e 'let s="";process.stdin.on("data",d=>s+=d).on("end",()=>{s=s.trim();try{console.log(JSON.parse(s).deployment.url)}catch{console.log(s.split("\n")[0])}})')"
case "$url" in https://*.vercel.app) ;; *) echo "no deployment URL in: $out" >&2; exit 1 ;; esac
echo "deployed: $url"
vc inspect "$url"
# A *.vercel.app hostname needs no domain of the account's, which a custom one does.
vc alias set "$url" "$NAME-alias.vercel.app"
vc alias ls
vc ls
vc project ls
vc remove "$NAME" --yes
# `vercel remove` calls a 404 on its DELETE "already removed", so ask the API whether the project is really gone.
status="$(curl -s -o /dev/null -w '%{http_code}' -H "Authorization: Bearer $VERCEL_TOKEN" "$API/v9/projects/$NAME")"
[ "$status" = 404 ] || { echo "project $NAME is still there after 'vercel remove' (GET answered $status)" >&2; exit 1; }
echo "cli journey ok against $API"
