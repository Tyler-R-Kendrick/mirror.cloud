# Testing

The test suite keeps each failure mode independently runnable:

| Type | Command | Coverage |
|---|---|---|
| Atomic unit | `make test-unit` | Package-level state, parsing, routing, and error semantics |
| Contract | `make test-contract` | Every protocol codec plus real AWS SDK S3, DynamoDB transaction, and SQS round trips |
| Snapshot / characterization | `make test-snapshot` | Catalog, support matrix, mock determinism, and spec diffs via `internal/golden` |
| Chaos | `make test-chaos` | Concurrent writes and transaction tokens, account isolation, injected blob failure, clock jumps, and subscriber panics |
| BDD / functional | `make test-bdd` | Booted HTTP S3/STS/Vercel/Cloudflare/Hostinger/GCS/Azure Blob/DigitalOcean/Hetzner/Railway/Fly Machines behavior and Terraform read paths |
| Fuzz | `make test-fuzz` | AWS chunk framing, SigV4 identity, REST URI routing, every DynamoDB lifecycle/data/PartiQL/stream/transaction fuzzer, S3 state and checksums, Step Functions JSONPath, GCS paths and object bytes, Vercel routes/KV, Cloudflare routes/KV, Azure Blob routes/bytes, DigitalOcean routes/create, and Railway GraphQL routes/create |
| Mutation | `make test-mutation` | selected routing, auth, protocol, state, queue, stream, event, compute, scheduler, pipes, lifecycle, storage, Vercel, Cloudflare, GCS, Azure Blob, DigitalOcean, and Railway mutants, plus the Hostinger, Hetzner and Fly Machines fault envelopes; all must be killed |

`internal/golden` is the stdlib-only Verify equivalent. Set `UPDATE_GOLDEN=1` only when intentionally accepting a reviewed snapshot.

`make test-coverage` runs every package `make test-unit` runs, once, with `-coverpkg=./...`, so a statement counts as covered when any test reaches it, and enforces an 80% whole-module floor, including command packages (`internal/generated` is left out of the total). Mutation tests run separately because they contribute no production statements. `make test-race` runs the race detector separately because the normal build is CGO-free.

See [PARITY.md](PARITY.md) for the pinned LocalStack behavioral traceability denominator and current audited percentage; line coverage and operation routing are reported separately there.

## Known-red triage

`known-red.json` is optional. When present, `scripts/known-red.py` classifies a `go test` log into declared vs new failures by test name. When absent (current main), every failure is new — there is no permitted-red set. An empty `expected` list is illegal; delete the file instead (`internal/check.TestKnownRedIsDeclaredHonestly`).

## How CI runs these

`.github/workflows/ci.yml` runs the tiers above as parallel jobs (`check`, `unit`, `suites`, `coverage`, `fuzz` in six shards, `race`, `mutation`, `image`), so a run takes as long as its slowest job, not the sum. `race` runs four shards and `mutation` twelve. `image` builds the Dockerfile with a service list and checks the image serves exactly that list. The `test` job only reports whether `check`, `unit`, `suites`, `coverage`, `fuzz`, `race` and `image` passed, and keeps the name the single job had.

- **Cache.** `.github/actions/go` restores a Go build and test-result cache. Jobs that build the same way share one (`test`); coverage, race, fuzz and mutation each keep their own. Only pushes to `main` write it, one job per cache, so a pull request never changes what another run restores. `go test` keys a cached result on the size and mtime of every file the test opened, so the action also runs `scripts/restore-mtimes.py`, which sets each file's mtime to the time of the last commit that touched it. Both are needed: without the second, a fresh checkout invalidates every package that reads a file.
- **Cached results.** Every run, `main`'s included, reuses a package's cached result when nothing it depends on changed: `unit` passes no `-count` and no `-cover`, either of which stops `go test` caching, and the flags match between `main` and pull requests. `coverage` cannot be cached, so it is the uncached run of every test, on every pull request and every push to `main`. A test that depends on something `go test` does not track (the clock, the network, a port) still runs uncached there, and `.github/workflows/nightly.yml` reruns `unit`, `coverage` and `race` with `-count=1` every night.
- **Fuzzing.** `scripts/fuzz.sh` runs `scripts/fuzz-targets.txt`, one target per line. It fails on a target that does not exist, which `go test -fuzz` does not. A `Fuzz` function missing from the list still runs its seed corpus but is never fuzzed; 68 of 195 were missing when the list moved out of the Makefile.
