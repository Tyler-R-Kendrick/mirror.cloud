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
# Not walked: `build`, which needs the framework builders the toolchain does not
# carry (docs/VERCEL.md, "Real clients").
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

# `env pull` and `pull` read GET /v3/env/pull/{project}/{target}. The CLI stores a
# production variable as a secret unless told otherwise, and a secret is the one
# thing a pull cannot give back: it writes the placeholder, never the value.
# The value is on the command line, and the CLI still reads a piped stdin to its end, so give it none.
envadd() { vc env add "$@" < /dev/null; }
envadd LOG_LEVEL production --value info --type config --yes
envadd LOG_LEVEL development --value debug --type config --yes
envadd FEATURE_FLAG preview --value on --git-branch feature --type config --yes
envadd DB_PASSWORD production --value vtok_dummy_marker_0123456789abcdef --sensitive --yes
vc env pull .env.development --yes
vc env pull .env.production --environment production --yes
vc env pull .env.feature --environment preview --git-branch feature --yes
expect_line() { grep -qxF "$2" "$1" || { echo "$1 lacks the line $2:" >&2; cat "$1" >&2; exit 1; }; }
reject_text() { ! grep -qF "$2" "$1" || { echo "$1 holds $2:" >&2; cat "$1" >&2; exit 1; }; }
expect_line .env.development 'LOG_LEVEL="debug"'
expect_line .env.production 'LOG_LEVEL="info"'
expect_line .env.production 'DB_PASSWORD="[SENSITIVE]"'
reject_text .env.production vtok_dummy_marker
reject_text .env.production debug
expect_line .env.feature 'FEATURE_FLAG="on"'
# `pull` is the same read, written under .vercel with the project's settings beside it.
vc pull --yes --environment production
expect_line .vercel/.env.production.local 'LOG_LEVEL="info"'
reject_text .vercel/.env.production.local vtok_dummy_marker
grep -q '"projectId"' .vercel/project.json || { echo ".vercel/project.json has no projectId:" >&2; cat .vercel/project.json >&2; exit 1; }
# Domains. `domains add <domain>` adds one to the account (POST /v7/domains), and
# `domains add <domain> <project>` posts the older POST /projects/{project}/alias,
# which answers 409 ALIAS_DOMAIN_EXIST naming the holder when another project has
# the host, and which --force answers by taking it off the holder first. `inspect`
# and `rm` list the projects under a domain (GET /v1/domains/{domain}/project-domains)
# and `rm` deletes at DELETE /v3/domains/{domain}. (The CLI is not piped into grep:
# it aborts on the closed pipe.)
vcn() { vc "$@" < /dev/null; }
fail() { echo "$*" >&2; exit 1; }
vcn domains add example.dev
dup="$(vcn domains add example.dev 2>&1)" && fail "adding example.dev twice succeeded: $dup"
case "$dup" in *"already exists"*) ;; *) fail "no 'already exists' for a second add: $dup" ;; esac
vcn domains add "www.example.dev" "$NAME"
vcn project add "$NAME-other"
taken="$(vcn domains add www.example.dev "$NAME-other" 2>&1)" && fail "another project took a host $NAME holds: $taken"
case "$taken" in *"already assigned"*) ;; *) fail "no 'already assigned' for a host another project holds: $taken" ;; esac
vcn domains add www.example.dev "$NAME-other" --force
listed="$(vcn domains ls 2>&1)"
case "$listed" in *example.dev*) ;; *) fail "domains ls does not list example.dev: $listed" ;; esac
inspected="$(vcn domains inspect example.dev 2>&1)"
case "$inspected" in *"$NAME-other"*"www.example.dev"*) ;; *) fail "domains inspect does not show $NAME-other holding www.example.dev: $inspected" ;; esac
# A host typed in mixed case. The CLI finds its host in the answer with ===, so the answer
# has to keep the spelling that was posted ("did not return a matching alias" otherwise).
vcn domains add "WWW2.Example.Dev" "$NAME"

out="$(vc deploy --prod --yes)"
# A person's terminal gets the URL; a JSON document comes back when the CLI took the run for an agent's.
url="$(printf '%s' "$out" | node -e 'let s="";process.stdin.on("data",d=>s+=d).on("end",()=>{s=s.trim();try{console.log(JSON.parse(s).deployment.url)}catch{console.log(s.split("\n")[0])}})')"
case "$url" in https://*.vercel.app) ;; *) echo "no deployment URL in: $out" >&2; exit 1 ;; esac
echo "deployed: $url"
# A preview deployment (no --prod) is the CLI's default; it finishes when the deployment is READY and aliasAssigned.
preview="$(vc deploy --yes)"
preview="${preview%%$'\n'*}"
case "$preview" in https://*.vercel.app) echo "previewed: $preview" ;; *) echo "no preview URL in: $preview" >&2; exit 1 ;; esac
vc inspect "$url"
# A *.vercel.app hostname needs no domain of the account's, which a custom one does.
vc alias set "$url" "$NAME-alias.vercel.app"
vc alias ls
# A hostname is the account's to alias only once it holds the domain. The CLI looks the
# domain up itself (GET /v5/domains/{host}) and goes to the registrar for a name the account
# lacks, which is not served, so the refusal of an unheld name is held by the Go tests and
# the SDK journey; here the account holds example.dev and the CLI aliases it.
vc alias set "$url" example.dev
vc alias rm example.dev --yes
vcn domains rm example.dev --yes
listed="$(vcn domains ls 2>&1)"
case "$listed" in *example.dev*) fail "example.dev is still listed after domains rm: $listed" ;; esac
vc ls
vc project ls
vc alias rm "$NAME-alias.vercel.app" --yes
vc remove "$NAME" --yes
vc remove "$NAME-other" --yes
# `vercel remove` calls a 404 on its DELETE "already removed", so ask the API whether the project is really gone.
status="$(curl -s -o /dev/null -w '%{http_code}' -H "Authorization: Bearer $VERCEL_TOKEN" "$API/v9/projects/$NAME")"
[ "$status" = 404 ] || { echo "project $NAME is still there after 'vercel remove' (GET answered $status)" >&2; exit 1; }
echo "cli journey ok against $API"
