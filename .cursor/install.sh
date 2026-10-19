#!/usr/bin/env bash
# Idempotent Cursor Cloud Agent bootstrap (see .cursor/environment.json). No secrets; no daemons.
set -euo pipefail

# Grok Build subscription auth. Non-fatal when the Cursor store is absent (local dev).
if ! bash .cursor/grok-auth.sh; then
  printf '%s\n' "grok subscription auth install skipped (store unavailable)"
fi
command -v grok >/dev/null 2>&1 && grok --version || printf '%s\n' "grok: not on PATH"

# Project dependencies. Non-fatal so a dependency hiccup never blocks the agent VM.
if ! { go mod download; }; then
  printf '%s\n' "dependency install failed; rerun it inside the agent"
fi
