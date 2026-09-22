#!/usr/bin/env bash
# Live Cloudflare API probe: compares account namespace listing shape against
# mirror's native cloudflare.api. Requires CLOUDFLARE_API_TOKEN + CLOUDFLARE_ACCOUNT_ID.
set -euo pipefail
: "${CLOUDFLARE_API_TOKEN:?set CLOUDFLARE_API_TOKEN}"
: "${CLOUDFLARE_ACCOUNT_ID:?set CLOUDFLARE_ACCOUNT_ID}"
url="https://api.cloudflare.com/client/v4/accounts/${CLOUDFLARE_ACCOUNT_ID}/storage/kv/namespaces?per_page=5"
echo "cloudflare-probe: GET $url"
code=$(curl -fsS -o /tmp/cf-probe.json -w '%{http_code}' \
  -H "Authorization: Bearer ${CLOUDFLARE_API_TOKEN}" \
  -H "Content-Type: application/json" \
  "$url")
echo "cloudflare-probe: status $code"
python3 - <<'PY'
import json,sys
body=json.load(open("/tmp/cf-probe.json"))
assert body.get("success") is True, body
assert "result" in body and isinstance(body["result"], list), body
# each namespace has id+title like mirror's create/list answer
for ns in body["result"][:5]:
    assert "id" in ns and "title" in ns, ns
print("cloudflare-probe: ok", len(body["result"]), "namespaces; shape matches mirror list fields (id,title)")
PY
