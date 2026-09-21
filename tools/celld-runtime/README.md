# celld runtime pin

Optional [denoland/celld](https://github.com/denoland/celld) binary for experimental Workers / SQLite Durable Objects.

**License:** Apache-2.0 (upstream). See `pin.json` `license_note`.

## Pin (this host)

| Field | Value |
| --- | --- |
| Release | `v0.5.1` |
| Asset | `celld-aarch64-unknown-linux-gnu.gz` |
| Asset sha256 | `375c7cd9446d61e6ee2c263d6be792650153ffd7cd562b2c4b6b644776b4494e` |
| Binary sha256 | `2a2c853c803f694c104f6b32968bf1601f7dab006bb52eebcca14ce9726c7224` |
| `--version` | `celld 0.5.1` |

Committed: `pin.json`, `*.sha256`, this README. Binary under `bin/` is gitignored (repo-root `bin/` rule; ~64MiB).

## Fetch

```bash
cd tools/celld-runtime
mkdir -p bin
curl -fL -o bin/celld-aarch64-unknown-linux-gnu.gz \
  "$(node -pe 'JSON.parse(require("fs").readFileSync("pin.json","utf8")).asset.url')"
sha256sum -c celld-aarch64-unknown-linux-gnu.gz.sha256
gunzip -c bin/celld-aarch64-unknown-linux-gnu.gz > bin/celld
chmod +x bin/celld
sha256sum -c celld.sha256
./bin/celld --version   # expect: celld 0.5.1
```

## Gaps (honest)

1. **Identity only in Go adapter.** `internal/execution/celld` Start runs `--version` and records identity. It does **not** advertise Worker/DO/KV capability descriptors or claim CF-DO-RESTART.
2. **`celld dev` needs esbuild.** Without `esbuild` on `PATH` (or `CELLD_ESBUILD`), bundling fails: `Error: esbuild not found`. Identity Start still works.
3. **Local DO restart observed, not ledgered.** With esbuild available, `examples/cloudflare-celld` (`celld dev`) kept SQLite DO counter state across process restart (`n` continued). `docs/cloudflare-capability.manifest.json` case `CF-DO-RESTART` remains `unavailable` until a CI/integration owner wires evidence — do not treat this pin as that claim.
4. **Production `celld` needs a fleet bucket** (`--bucket s3://…` / `gs://` / `az://`). Dev mode uses a local object store under `.celld/dev` only.
