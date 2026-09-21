# Cloudflare on mirror.cloud

Profiles are distinct. Do not read “supported” as all three.

| Profile | What it is | Dependencies |
|---|---|---|
| `native` | Public KV REST (`cloudflare.api`) on the Go engine + B-IR | None (no Node, workerd, celld, Docker) |
| `miniflare` | Workers execution + coherent KV/D1/R2/DO/Queue/Workflow via Miniflare/`workerd` | Explicit install; optional |
| `celld` | Experimental Workers/SQLite DO via Deno celld | Explicit pin; optional; trusted code only |

## Native (default)

```bash
mirror up cloudflare
# aliases: cloudflare.kv → cloudflare.api; profile cloudflare-core
```

Service ID is always `cloudflare.api`. KV namespaces/keys live in mirror's Store. Controlled clock drives `expiration` / `expiration_ttl`.

Verified native cases (see `test/behavior/cloudflare`): CF-BOOT-NATIVE, CF-ALIASES, CF-TITLE, CF-KV-BINARY, CF-KV-TIME, CF-KV-PAGE, CF-DELETE, CF-KV-MULTIPART.

## Miniflare / celld

Not yet a green live integration in this branch wave. The capability seam (`spi.CapabilitySession`) and process-supervision path are the attach points. Until a live CI job records Miniflare/celld versions and passes CF-KV-COHERENCE / CF-DO-RESTART, those profiles are **unavailable**, not silently mocked.

## Trust and offline

Native path never phones home. Optional runtimes must be preprovisioned; startup must not `npx`/git-fetch. Offline/enforced egress is a tested policy when those profiles are enabled—not an adjective.

## Clock

Native TTL uses `spi.Clock`. External runtime timers stay external; advancing mirror's clock must not claim success for Miniflare/celld alarms.
