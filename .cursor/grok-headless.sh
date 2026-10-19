#!/usr/bin/env bash
# Run Grok Build on the subscription login (~/.grok/auth.json). Never use XAI_API_KEY.
# Unset API keys so a runtime secret cannot shadow the subscription login.
# Usage: bash .cursor/grok-headless.sh -p "..." -m grok-4.7 --always-approve --output-format json
set -euo pipefail
exec env -u XAI_API_KEY -u GROK_CODE_XAI_API_KEY -u GROK_DEPLOYMENT_KEY grok "$@"
