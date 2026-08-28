# Extending mirror.cloud

## Add a receiver

Implement `receiver.Receiver` (`Name`, `Detect`, `Ingest`) under `internal/receiver/<cloud>/<format>`. Return `[]model.Service`. Fusion merges fragments; do not special-case a provider in `internal/model`.

Worked path: `internal/receiver/aws/smithy` (AWS Smithy JSON AST) and `internal/receiver/gcp/discovery` (Google Discovery).

## Add a service

Services are data, not Go. A new service is a `specs/mirror.set` entry and a Behavior IR bundle:

1. Add the service to `specs/mirror.set`, then `make specs-sync && make generate` — the generated model with its shapes lands in `internal/generated/<cloud>/<service>/` and is committed.
2. Write `behavior/<cloud>/<service>/service.yaml` against the schema in [`BEHAVIOR_IR.md`](./BEHAVIOR_IR.md): resources with identity and record projection, an error table, and per-operation `reads` / `require` / `effects` / `output`.
3. That is all. `internal/bundled` registers every bundle it finds, so there is no package to create, no `init()` to write, and no import to add. `internal/engine` serves it, validating requests against the generated shapes.
4. Unit-test through `bundled.New(id, spitest.Deps(t))` by constructing `spi.Request` values. Do not import the edge or codecs.

Worked path: `behavior/aws/shield/service.yaml`.

A bundle is validated against its model at load time and every bundle is built in CI (`make equivalence`), so a bundle that references an operation the model does not have, or projects a member that is not in the output shape, fails the build rather than the first request.

## Add a behavior pack (legacy; the ratchet forbids new ones)

The 150 remaining packs under `internal/services/` predate the engine and are being extracted service by service. `internal/check`'s ratchet fails CI on any new pack directory, any increase in `case` labels, service LOC, inline `&spi.Fault{}` sites, or `registry.Register` call sites — so this path is closed by construction. If a service genuinely cannot be expressed in B-IR, the answer is a named, versioned primitive referenced from data, not a new pack; see [`BEHAVIOR_IR.md`](./BEHAVIOR_IR.md) §6.

To extract an existing pack, follow the protocol in [`MASTER_PROMPT_V2.md`](./MASTER_PROMPT_V2.md) §3: record the pack's answers into `internal/equivalence/traces/<service>.json`, write the bundle, get `make equivalence` green, delete the pack in the same commit, and lower the baseline with `make ratchet-update`.

## Add a codec

Implement `proto.Codec` under `internal/proto/<cloud>/<protocol>` and register it in `internal/edge`. Codecs are the allowed hand-written protocol exception. Shared `proto.Codec` stays in `internal/proto`.

Worked path: `internal/proto/aws/awsjson`.

## Add a provider

Cloud-specific code lives under a directory named for that cloud (`aws`, `gcp`, next one you add). Shared spine (`internal/model`, `internal/spi`, `internal/edge`, `internal/runtime`) stays provider-neutral.

1. `specs/<cloud>/` for vendored specs; `internal/generated/<cloud>/<service>/` for generated dispatch.
2. Receiver at `internal/receiver/<cloud>/<format>`.
3. Protocol constant already exists for AWS and GCP; add one only if the wire protocol is new, under `internal/proto/<cloud>/`.
4. Behavior bundles at `behavior/<cloud>/<service>/` — the cross-check that `internal/model` and `internal/engine` stayed provider-neutral is that adding a cloud adds no Go outside a receiver and, if its wire format is new, a codec.
