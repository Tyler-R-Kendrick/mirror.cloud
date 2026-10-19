#!/usr/bin/env bash
# Install Tyler's Grok Build subscription login for Cursor Cloud Agents.
# Copied from Tyler-R-Kendrick/OpenSesame scripts/dev/grok-install-subscription-auth.sh.
# Copies the Cursor store file to ~/.grok/auth.json. Prints status only, never contents.
# Grok Build uses the subscription login, never XAI_API_KEY.
set -euo pipefail

GROK_SUBSCRIPTION_AUTH_SOURCE="${GROK_SUBSCRIPTION_AUTH_SOURCE:-/cursor/stores/bot/grok/auth.json}"
dest="${HOME}/.grok/auth.json"

if [[ -e "$GROK_SUBSCRIPTION_AUTH_SOURCE" ]]; then
  printf '%s\n' "grok subscription auth source: present"
else
  printf '%s\n' "grok subscription auth source: absent"
fi

if [[ -f "$GROK_SUBSCRIPTION_AUTH_SOURCE" && -r "$GROK_SUBSCRIPTION_AUTH_SOURCE" ]]; then
  mkdir -p "${HOME}/.grok"
  cp "$GROK_SUBSCRIPTION_AUTH_SOURCE" "$dest"
  chmod 600 "$dest"
  printf '%s\n' "grok subscription auth install: ran"
else
  printf '%s\n' "grok subscription auth install: skipped"
fi

if [[ -e "$dest" ]]; then
  printf '%s\n' "~/.grok/auth.json: present"
  exit 0
fi

printf '%s\n' "~/.grok/auth.json: absent"
exit 1
