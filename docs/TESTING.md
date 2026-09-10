# Testing

The test suite keeps each failure mode independently runnable:

| Type | Command | Coverage |
|---|---|---|
| Atomic unit | `make test-unit` | Package-level state, parsing, routing, and error semantics |
| Contract | `make test-contract` | Every protocol codec plus real AWS SDK S3, DynamoDB transaction, and SQS round trips |
| Snapshot / characterization | `make test-snapshot` | Catalog, support matrix, mock determinism, and spec diffs via `internal/golden` |
| Chaos | `make test-chaos` | Concurrent writes and transaction tokens, account isolation, injected blob failure, clock jumps, and subscriber panics |
| BDD / functional | `make test-bdd` | Booted HTTP S3/STS/Vercel/Cloudflare/Hostinger/GCS/Azure Blob/DigitalOcean behavior and Terraform read paths |
| Fuzz | `make test-fuzz` | AWS chunk framing, SigV4 identity, REST URI routing, every DynamoDB lifecycle/data/PartiQL/stream/transaction fuzzer, S3 state and checksums, Step Functions JSONPath, GCS paths and object bytes, Vercel routes/KV, Cloudflare routes/KV, Hostinger routes/DNS, Azure Blob routes/bytes, and DigitalOcean routes/create |
| Mutation | `make test-mutation` | selected routing, auth, protocol, state, queue, stream, event, compute, scheduler, pipes, lifecycle, storage, Vercel, Cloudflare, Hostinger, GCS, Azure Blob, and DigitalOcean mutants; all must be killed |

`internal/golden` is the stdlib-only Verify equivalent. Set `UPDATE_GOLDEN=1` only when intentionally accepting a reviewed snapshot.

`make test-coverage` merges atomic package and cross-package integration profiles and enforces an 80% whole-module floor, including generated and command packages. Mutation tests run separately because they contribute no production statements. `make test-race` runs the race detector separately because the normal build is CGO-free.

See [PARITY.md](PARITY.md) for the pinned LocalStack behavioral traceability denominator and current audited percentage; line coverage and operation routing are reported separately there.
