# Cloudflare compatibility — implementation report (wave 1)

**Branch:** `feat/cloudflare-compat-execution`
**Base:** `83b62d7d` (main after #408/#409)
**Date:** 2026-09-21

## Baseline

- Inspected SHA in the prompt (`56362b7`) is superseded; work started from current main.
- PR #409 merged (title guard on create). PR #408 merged (Vercel). Open PR #411 left untouched.
- Unrelated untracked anti-slop/TS files preserved, not committed.

## Delivered

### Native KV repairs (`behavior/cloudflare/api/service.yaml`)

- Title create/rename: reject absent/null/empty/non-string without CEL 500 (`type(x) == type('')`).
- Values stored base64; reads `b64decode` for exact bytes (NUL / high bytes).
- `expiration` / `expiration_ttl` → `expires_at`; reads/lists omit expired keys (`spi.Clock`).
- Key list: prefix, limit, cursor (lexicographic); metadata when present.
- Namespace delete cascades parent-scoped entries.
- Multipart `value`+`metadata` parsed in REST/JSON codec (generic multipart, not CF-named branch).

### Engine

- `ListSpec.Limit` + compile/validate; post-filter page cap in `runList`.

### Aliases

- `cloudflare`, `cloudflare.kv`, `cloudflare.api` → `cloudflare.api`; profile `cloudflare-core` enables `cloudflare.api`.
- Tests: `internal/runtime/cloudflare_alias_test.go`.

### Capability seam

- `internal/spi/capability.go` — descriptors, errors, `CapabilitySession`.
- `internal/execution/capability` — generic dispatcher + fake backends.
- `internal/execution/process` — scrubbed spawn, readiness, idempotent Close.
- `internal/execution/miniflare` — stub session; Call/Start → unavailable unless preprovisioned (no npx/download).

### Docs / evidence

- `docs/CLOUDFLARE.md`, `docs/INTERFACE_NOTES.md` (compute scope extension).
- `docs/cloudflare-capability.manifest.json` + `internal/check/cloudflare_capability_test.go`.

## Commands run (pass)

```text
go test ./test/behavior/cloudflare/ ./internal/spine/ ./internal/proto/aws/restjson/ \
  ./internal/engine/ ./internal/bir/ ./internal/runtime/ ./internal/execution/... \
  ./internal/check/ -count=1
```

## Explicit gaps (not claimed done)

| Item | Status |
|---|---|
| Live Miniflare Workers/KV/D1/R2/DO/Queue/Workflow | unavailable — stub only |
| Live celld Worker/SQLite-DO | not started |
| Spec surface widen (Workers/D1/R2/Queues/Workflows ops) | not started |
| CF-KV-COHERENCE / CF-SNAPSHOT / CF-OFFLINE live | not-run in manifest |
| Real-cloud probe | unmeasured |

## Review counterexamples checked

- REST vs Worker split-brain: N/A until Miniflare binds same authority.
- Null title → 500: fixed (typed guard).
- Store lock across Worker I/O: documented; no live Worker yet.
- Offline claim: native path only; optional runtimes must not download at Start (tested).
- Snapshot without backend data: Miniflare snapshot not implemented; not claimed.
- Manifest does not mark miniflare/celld as pass.
EOF

## Wave 2 — optional runtimes

### Miniflare

- Pin: `tools/cloudflare-runtime` → `miniflare@3.20250718.3`
- Helper: JSON-lines v1 protocol (`helper.mjs` + `worker.mjs`)
- Live: `go test -tags=miniflare ./internal/execution/miniflare` → TestLiveKVPutGet, TestLiveWorkerFetchThenKVGet **pass**
- CF-KV-COHERENCE marked pass (Worker↔KV). Public REST bridge still pending.

### celld

- Pin: `v0.5.1` aarch64 linux, sha256 in `tools/celld-runtime/pin.json`
- `TestStartRecordsIdentity` pass; Call still CapErrUnsupported
- CF-DO-RESTART remains unavailable (example observed locally with esbuild; not CI-owned)

### Still open

Workers public upload API, D1/R2/Queues/Workflows B-IR surface, offline tripwires, coordinated snapshots, REST↔Miniflare authority bridge.
