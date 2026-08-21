# Testing

The test suite keeps each failure mode independently runnable:

| Type | Command | Coverage |
|---|---|---|
| Atomic unit | `make test-unit` | Package-level state, parsing, routing, and error semantics |
| Contract | `make test-contract` | Every protocol codec plus real AWS SDK S3, DynamoDB, and SQS round trips |
| Snapshot / characterization | `make test-snapshot` | Catalog, support matrix, mock determinism, and spec diffs via `internal/golden` |
| Chaos | `make test-chaos` | Concurrent writes, account isolation, injected blob failure, clock jumps, and subscriber panics |
| BDD / functional | `make test-bdd` | Booted HTTP S3/STS behavior and Terraform read paths |
| Fuzz | `make test-fuzz` | AWS chunk framing, SigV4 identity, DynamoDB expressions/updates, and GCS paths |
| Mutation | `make test-mutation` | 134 selected routing, auth, protocol, state, queue, stream, event, compute, scheduler, pipes, lifecycle, and storage mutants; all must be killed |

`internal/golden` is the stdlib-only Verify equivalent. Set `UPDATE_GOLDEN=1` only when intentionally accepting a reviewed snapshot.

`make test-coverage` enforces the current 60% whole-module floor, including generated and command packages. `make test-race` runs the race detector separately because the normal build is CGO-free.
