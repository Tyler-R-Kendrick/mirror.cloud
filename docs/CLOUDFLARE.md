# Cloudflare on mirror

mirror serves Cloudflare's Workers KV REST surface as a
specification-generated API compatibility layer, and optionally runs your
own Worker application code against the same authoritative resources using
Miniflare (`workerd`). This document states exactly which of those is which,
what has been verified, and how to run each path.

## The two profiles are different claims

| | `native` (default) | `miniflare` (optional) |
|---|---|---|
| What runs your API requests | mirror's B-IR bundle; KV entry bytes via `execute`→`native-kv` Store | same bundle; KV entry bytes via `execute`→Miniflare when that Executor is selected |
| What runs your application code | nothing — there is no application runtime | Miniflare → real `workerd` in a supervised helper process |
| Extra dependencies | none (single Go binary) | Node ≥ 22 + `npm ci` in `tools/cloudflare-runtime` |
| Internet required at runtime | no | no (see Offline) |
| Clock | mirror's controlled `spi.Clock` for native records | backend's own clock for anything inside workerd |
| Trust model | your own requests to your own process | your own Worker code, treated as trusted local code |

"Supported" never silently means the other row. A native deployment never
starts Node or `workerd`. A miniflare deployment that cannot start says so
with a diagnostic naming the missing capability; it does not fall back to a
mock or to real Cloudflare.

## Native profile — what is served

Service id `cloudflare.api` (aliases `cloudflare`, legacy `cloudflare.kv`;
profile `cloudflare-core`). Ten operations of the generated model are served
by the B-IR bundle; the rest of the model answers mock-tier, or `501
MirrorNotImplemented` under `--strict`:

- namespace create / list / get / rename / remove (remove cascades the
  namespace's keys),
- key write with metadata (multipart form parsed: `value` and `metadata`
  are separated; metadata capped at Cloudflare's published 1024 bytes),
- key read / delete (raw bytes, exact round-trip including invalid UTF-8),
- key listing (prefix filtering, truthful count, empty cursor — the
  generated model declares no pagination trait for this operation, so
  `limit`/`cursor` are accepted and ignored, stated as a quirk in
  `behavior/cloudflare/api/service.yaml`),
- metadata read (`.../metadata/{key}`).

Behavior repairs versus the historical pack: explicit/`null`/wrong-typed
titles answer the declared `10007` client error instead of an accidental
CEL `500`; `expiration`/`expiration_ttl` are stored and expired keys read
and list as missing under the controlled clock; multipart writes store the
value, not the MIME envelope; deleting a namespace deletes its keys.

Verified by: `make test-bdd` (`test/behavior/cloudflare`), the Cloudflare
spine tests, and `make equivalence` (historical recordings unchanged for
unchanged behavior; repairs are documented in the bundle's quirks, not
hidden by re-recording).

### Run it

```bash
go build ./cmd/mirror
./mirror up cloudflare            # or: cloudflare.kv (legacy), cloudflare.api,
                                  #     --profile cloudflare-core
./mirror support-matrix           # generated support table (docs/SUPPORT.md)
```

`--strict` turns every mock-tier answer into `501 MirrorNotImplemented`.
`--strict` never falls back to a mock, to another backend, or to real
Cloudflare.

## Miniflare profile — running your Worker

The miniflare profile supervises a helper process
(`tools/cloudflare-runtime/session.mjs`) that hosts Miniflare/`workerd` and
speaks a private, versioned, bearer-authenticated, loopback-only control
protocol. mirror owns the child lifecycle: explicit spawn with a scrubbed
environment, bounded readiness, process-group reaping on close.

### Provision once (needs Internet)

```bash
cd tools/cloudflare-runtime && npm ci && cd ../..
make test-cloudflare-miniflare    # required live suite; FAILS if backend absent
```

`npm ci` installs exactly `miniflare@5.20260921.0-alpha` per the committed
`package-lock.json` (integrity hashes included), which brings the pinned
`workerd` binary for your platform. Nothing is downloaded at session start;
an unprovisioned directory is refused by `MiniflareAvailable` before any
process spawns.

### What the live suite proves (executed, not aspirational)

- a real `workerd` process starts, becomes ready over authenticated
  loopback, and reports its pinned backend identity;
- **KV coherence both ways through ONE namespace**: a control-path write is
  read by the Worker's binding, and a Worker write is read by the control
  path — including byte-exact round-trip of invalid UTF-8;
- prefix listing, missing keys, absent namespaces, and the closed action
  set (an `eval` action does not exist and answers `unsupported`);
- a binding graph requesting remote bindings is refused before Miniflare
  starts (offline is admission, not a hope);
- re-apply advances the generation and preserves namespace data
  (`setOptions` on the live instance, verified against the pinned package);
- quiesce refuses new application work with a distinct class; close reaps
  the helper (double-close safe); a bad environment fails before spawn and
  leaks nothing.

### What the Miniflare profile does NOT claim

- **Not production isolation.** A V8 isolate in a local `workerd` process
  is not a hostile-tenant sandbox. Trusted local applications only.
- **Not Cloudflare's fleet.** `workerd` shares runtime code with
  Cloudflare; it does not reproduce global distribution, rate limits,
  eventual consistency windows, or support guarantees.
- **Not the backend's clock.** Expiration, alarms and timers inside
  workerd run on the backend's real clock. Advancing mirror's controlled
  clock must not and does not report success for them.
- **Bounded Miniflare control set.** Verified: `kv.*`, `worker.dispatch`,
  `snapshot`/`restore`, `offline.probe`, apply/quiesce/close. D1/R2/Queues
  run under the **celld** profile (`make test-cloudflare-celld`), not by
  pretending Miniflare is celld.
- **Snapshot scope.** Portable KV snapshot/restore is supported via the
  control protocol (`SnapshotKV`/`RestoreKV`). Re-apply also preserves live
  state. Do not copy a live helper's working directory as a substitute.

### Offline runtime

Once provisioned, a session makes no outbound requests: the helper binds
loopback on an ephemeral port with a per-session `crypto/rand` token,
remote bindings are refused at apply, node telemetry is disabled in the
scrubbed environment, and `cf:false` pins local request data. Ordinary CI
runs no npm install and no session; `make test-cloudflare-miniflare` is a
separate, explicitly provisioned job.

### Trust model and the control channel

The public mirror API keeps accepting dummy credentials as before. The
helper's control channel does not: every request needs the bearer token
generated at spawn (401 without it), must come from loopback (403
otherwise), and can only invoke the closed action set on namespaces the
session's own apply registered. There is no arbitrary JavaScript
evaluation, no arbitrary file or module loading, no shell, and no
caller-chosen upstream destination. The token lives in memory and in the
child's scrubbed environment only — never in a snapshot or a journal.

## Troubleshooting

| Symptom | Meaning |
|---|---|
| `miniflare: node not resolvable` | Node ≥ 22 not installed/visible; the native profile is unaffected |
| `miniflare: missing ... node_modules` | run `npm ci` in `tools/cloudflare-runtime` (needs network once) |
| `make test-cloudflare-miniflare` fails with `environment unavailable` | the required suite refuses to pass without the backend — provision it; this is not a skip |
| helper `401` from mirror's own client | token mismatch — restart the session; tokens are per-spawn |
| `unknown action` on `/v1/call` | the action set is closed by design; see the capability list above |
| `501 MirrorNotImplemented` under `--strict` | operation outside the served bundle; drop `--strict` to see mock-tier (labeled) answers |

## Evidence map

| Claim | Where it is proved |
|---|---|
| Native repairs, aliases, strict, account scoping, TTL, multipart | `test/behavior/cloudflare` (`make test-bdd`) |
| Historical behavior preserved where unchanged | `make equivalence` |
| Generic capability contract, two fake backends | `internal/execution/execution_test.go` |
| Real workerd session, coherence, binary, lifecycle | `make test-cloudflare-miniflare` |
| Support matrix matches served operations | `TestSupportMatrixMatchesDocs` / `mirror support-matrix` |
