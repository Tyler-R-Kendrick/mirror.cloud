# Cloudflare runtime helper (Miniflare)

Optional Node helper for mirror's Miniflare execution profile. Not required for native Cloudflare KV.

## Pin

| Package    | Version        | Notes                                      |
|------------|----------------|--------------------------------------------|
| miniflare  | `3.20250718.3` | workers-sdk Miniflare 3.x (`legacy` line) |
| workerd    | `1.20250718.0` | transitive via miniflare                   |

Pinned in `package.json` / `package-lock.json`. Runtime uses `node_modules` only — no `npx`, no download at `Session.Start`.

## Setup

```bash
cd tools/cloudflare-runtime
npm ci
```

Node ≥18. CI/local mirror may use `/home/codex/.local/bin/node` (v26 ok).

## Live Go test

```bash
cd tools/cloudflare-runtime && npm ci
# either:
go test -tags=miniflare ./internal/execution/miniflare/
# or:
MIRROR_MINIFLARE=1 go test ./internal/execution/miniflare/ -run Live
```

## Protocol

Versioned JSON lines on stdin/stdout (`v: 1`). Ops: `start`, `kv_put`, `kv_get`, `worker_fetch`, `stop`. Control channel never carries JS to eval; Worker script is fixed `worker.mjs`.
