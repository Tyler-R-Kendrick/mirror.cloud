#!/usr/bin/env bash
# The deploy path of a Vercel client, walked with curl against a running mirror.
#
#   mirror up vercel.api &            # or: mirror up vercel.api aws.s3
#   scripts/vercel-acceptance.sh      # MIRROR_URL=http://127.0.0.1:4566 by default
#
# It makes a project at the two versions clients create them at (v9 and v11),
# sets a variable, uploads a file by its SHA-1, deploys it, waits for READY,
# aliases the deployment, reads it back and deletes the project. Every step
# asserts on the body it gets; the first one that is wrong stops the run with the
# body in front of you. Needs curl and jq.
#
# The emulator takes any credential. For a deploy against the real API, hand the
# token to the command through a secrets broker rather than writing it into this
# environment or an env file.
set -euo pipefail

BASE="${MIRROR_URL:-http://127.0.0.1:4566}"
TOKEN="${VERCEL_TOKEN:-mirror-dev-token}"
NAME="${ACCEPT_PROJECT:-accept-app}"
LEGACY="${NAME}-v9"
WAIT_SECONDS="${ACCEPT_WAIT_SECONDS:-30}"

body="$(mktemp)"
trap 'rm -f "$body"' EXIT

# call METHOD PATH [JSON] -> sets $status, leaves the body in $body.
call() {
  local method="$1" path="$2" data="${3:-}" args=()
  args=(-sS -o "$body" -w '%{http_code}' -X "$method" -H "Authorization: Bearer $TOKEN")
  if [ -n "$data" ]; then args+=(-H 'Content-Type: application/json' --data "$data"); fi
  status="$(curl "${args[@]}" "$BASE$path")"
}

fail() { echo "FAIL: $*" >&2; echo "--- last body ---" >&2; cat "$body" >&2; echo >&2; exit 1; }

# expect STATUS JQ_ASSERTION DESCRIPTION
expect() {
  local want="$1" assertion="$2" what="$3"
  [ "$status" = "$want" ] || fail "$what: status $status, want $want"
  jq -e "$assertion" "$body" >/dev/null || fail "$what: body does not satisfy $assertion"
  echo "ok   $what"
}

cleanup() {
  call DELETE "/v9/projects/$NAME" >/dev/null 2>&1 || true
  call DELETE "/v9/projects/$LEGACY" >/dev/null 2>&1 || true
}
cleanup

# 1. A project, at the version the CLI creates them at and at the document's.
call POST /v9/projects "{\"name\":\"$LEGACY\"}"
expect 200 ".name == \"$LEGACY\" and (.id | startswith(\"prj_\")) and .nodeVersion != null" "create project at v9"
call POST /v11/projects "{\"name\":\"$NAME\",\"framework\":\"vite\"}"
expect 200 ".name == \"$NAME\" and .framework == \"vite\" and .accountId != null" "create project at v11"
PROJECT_ID="$(jq -r .id "$body")"
call GET /v10/projects
expect 200 "[.projects[].name] | index(\"$NAME\") != null and index(\"$LEGACY\") != null" "both are one store: the listing at v10 has them"

# 2. A variable.
call POST "/v10/projects/$PROJECT_ID/env" '{"key":"API_URL","value":"https://api.example.test","type":"plain","target":["production","preview"]}'
expect 201 '.created.key == "API_URL" and .created.type == "plain" and (.failed | length) == 0' "set a variable"
call POST "/v10/projects/$PROJECT_ID/env" '{"key":"API_URL","value":"again","type":"plain","target":["production"]}'
expect 409 '.error.code == "ENV_CONFLICT" and .error.envVarKey == "API_URL"' "the same variable again is a conflict"
call GET "/v9/projects/$NAME/env"
expect 200 '.envs | length == 1' "the variable is listed at v9 by project name"

# 3. A file, named by the SHA-1 of its bytes.
CONTENT='<!doctype html><title>accept</title><h1>deployed</h1>'
SHA="$(printf '%s' "$CONTENT" | sha1sum | cut -d' ' -f1)"
SIZE="${#CONTENT}"
status="$(curl -sS -o "$body" -w '%{http_code}' -X POST \
  -H "Authorization: Bearer $TOKEN" -H "x-vercel-digest: $SHA" -H "Content-Length: $SIZE" \
  -H 'Content-Type: application/octet-stream' --data-binary "$CONTENT" "$BASE/v2/files")"
[ "$status" = 200 ] || fail "upload file: status $status"
echo "ok   upload the file by digest $SHA"

# 4. A deployment that refers to it. Naming a file nobody uploaded is refused.
call POST /v13/deployments '{"name":"'"$NAME"'","project":"'"$NAME"'","files":[{"file":"index.html","sha":"0000000000000000000000000000000000000000","size":3}]}'
expect 400 '.error.code == "missing_files" and (.error.missing | length) == 1' "a file that was not uploaded is named as missing"
call POST /v13/deployments '{"name":"'"$NAME"'","project":"'"$NAME"'","target":"production","files":[{"file":"index.html","sha":"'"$SHA"'","size":'"$SIZE"'}],"meta":{"commit":"abc123"}}'
expect 200 '.readyState == "QUEUED" and (.id | startswith("dpl_")) and .url != null and .projectId == "'"$PROJECT_ID"'" and .meta.commit == "abc123"' "create the deployment"
DEPLOYMENT_ID="$(jq -r .id "$body")"
DEPLOYMENT_URL="$(jq -r .url "$body")"

# 5. Poll to READY, the way every client does.
deadline=$((SECONDS + WAIT_SECONDS))
state=""
while [ "$SECONDS" -lt "$deadline" ]; do
  call GET "/v13/deployments/$DEPLOYMENT_ID"
  state="$(jq -r .readyState "$body")"
  case "$state" in
    READY) break ;;
    ERROR | CANCELED) fail "the deployment ended $state" ;;
  esac
  sleep 1
done
[ "$state" = READY ] || fail "the deployment is still $state after ${WAIT_SECONDS}s"
expect 200 '.readyState == "READY" and .ready != null and .aliasAssigned == true' "poll to READY"

# 6. An alias, then the deployment read back by id and by its URL.
call POST "/v2/deployments/$DEPLOYMENT_ID/aliases" '{"alias":"accept.example.test"}'
expect 200 '.alias == "accept.example.test" and (.uid | length) > 0 and .created != null' "alias the deployment"
call GET "/v13/deployments/$DEPLOYMENT_ID"
expect 200 '(.alias | index("accept.example.test")) != null' "the deployment names its alias"
call GET "/v13/deployments/$DEPLOYMENT_URL"
expect 200 ".id == \"$DEPLOYMENT_ID\"" "read it back by its URL"
call GET /v4/aliases/accept.example.test
expect 200 ".deploymentId == \"$DEPLOYMENT_ID\"" "read the alias by its hostname"
call GET "/v7/deployments?projectId=$PROJECT_ID&limit=5"
expect 200 '.deployments | length >= 1' "list the project's deployments"

# 7. A project is deleted with what hangs off it.
call DELETE "/v9/projects/$NAME"
[ "$status" = 204 ] || fail "delete the project: status $status"
echo "ok   delete the project"
call GET "/v13/deployments/$DEPLOYMENT_ID"
[ "$status" = 404 ] || fail "the deployment outlived its project: status $status"
echo "ok   its deployment went with it"
call DELETE "/v9/projects/$LEGACY" >/dev/null 2>&1 || true

echo
echo "accepted: $BASE answered the deploy path of a Vercel client"
