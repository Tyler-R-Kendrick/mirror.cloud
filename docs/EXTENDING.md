# Extending mirror.cloud

## Add a receiver

Implement `receiver.Receiver` (`Name`, `Detect`, `Ingest`) under `internal/receiver/<format>`. Return `[]model.Service`. Fusion merges fragments; do not special-case a provider in `internal/model`.

Worked path: `internal/receiver/smithy` (AWS Smithy JSON AST) and `internal/receiver/discovery` (Google Discovery).

## Add a behavior pack

1. New package `internal/services/<name>` depending only on `internal/spi` and `internal/model`.
2. `init()` calls `registry.Register` with a `Factory`.
3. Blank-import the package from `cmd/mirror`.
4. List emulate-tier operations in `Operations()`; anything missing is mock or `MirrorNotImplemented`.
5. Unit-test against `spitest.Deps(t)` by constructing `spi.Request` values. Do not import the edge or codecs.

Worked path: `internal/services/s3`.

## Add a codec

Implement `proto.Codec` under `internal/proto/<protocol>` and register it in `internal/edge`. Codecs are the allowed hand-written protocol exception.

Worked path: `internal/proto/awsjson`.

## Add a provider

1. Receiver for that provider's spec format.
2. Protocol constant already exists for AWS and GCP; add one only if the wire protocol is new.
3. One emulate-tier pack as the cross-check that `internal/model` stayed provider-neutral (GCS is that check today).
