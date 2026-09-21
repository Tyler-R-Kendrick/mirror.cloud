# cloudflare-celld (minimal)

SQLite Durable Object counter from denoland/celld `examples/counter` (v0.5.1).

## Needs

1. Pinned binary: `tools/celld-runtime/bin/celld` (see that README).
2. **esbuild** on `PATH`, or `CELLD_ESBUILD=/path/to/esbuild`.

Without esbuild, `celld dev` fails at bundle time.

## Local DO persist / restart probe

```bash
export PATH="/path/to/esbuild/dir:$PATH"   # or CELLD_ESBUILD
CELLD=../../tools/celld-runtime/bin/celld
rm -rf .celld
$CELLD dev . --port 19876 &
# curl http://127.0.0.1:19876/?name=probe   # n increments
# kill; restart; curl again — n continues if SQLite DO state persisted
```

Observed on linux aarch64 with celld 0.5.1: counter continued across `celld dev` restart.

This example is evidence for a future CF-DO-RESTART wire-up. It does **not** by itself flip `docs/cloudflare-capability.manifest.json`.
