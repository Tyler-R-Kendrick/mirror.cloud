# Extending mirror.cloud

## Add a receiver

Implement `receiver.Receiver` (`Name`, `Detect`, `Ingest`) under `internal/receiver/<cloud>/<format>`. Return `[]model.Service`. Fusion merges fragments; do not special-case a provider in `internal/model`.

Worked path: `internal/receiver/aws/smithy` (AWS Smithy JSON AST) and `internal/receiver/gcp/discovery` (Google Discovery).

## Add a behavior pack

1. New package `internal/services/<cloud>/<name>` depending only on `internal/spi` and `internal/model`.
2. `init()` calls `registry.Register` with a `Factory`.
3. Blank-import the package from `cmd/mirror`.
4. List emulate-tier operations in `Operations()`; anything missing is mock or `MirrorNotImplemented`.
5. Unit-test against `spitest.Deps(t)` by constructing `spi.Request` values. Do not import the edge or codecs.

Worked path: `internal/services/aws/s3`.

## Add a codec

Implement `proto.Codec` under `internal/proto/<cloud>/<protocol>` and register it in `internal/edge`. Codecs are the allowed hand-written protocol exception. Shared `proto.Codec` stays in `internal/proto`.

Worked path: `internal/proto/aws/awsjson`.

## Add a provider

Cloud-specific code lives under a directory named for that cloud (`aws`, `gcp`, next one you add). Shared spine (`internal/model`, `internal/spi`, `internal/edge`, `internal/runtime`) stays provider-neutral.

1. `specs/<cloud>/` for vendored specs; `internal/generated/<cloud>/<service>/` for generated dispatch.
2. Receiver at `internal/receiver/<cloud>/<format>`.
3. Protocol constant already exists for AWS and GCP; add one only if the wire protocol is new, under `internal/proto/<cloud>/`.
4. Emulate-tier pack at `internal/services/<cloud>/<service>/` — GCS is the cross-check that `internal/model` stayed provider-neutral.
