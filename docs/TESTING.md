# Testing

The test suite keeps each failure mode independently runnable:

| Type | Command | Coverage |
|---|---|---|
| Atomic unit | `make test-unit` | Every package of the module once: package-level state, parsing, routing, and error semantics, and every tier below that lives in the module |
| Contract | `make test-contract` | Every protocol codec plus real AWS SDK S3, DynamoDB transaction, and SQS round trips (`make test-sdk` runs the `test/sdk/go` module alone) |
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

`.github/workflows/ci.yml` runs the tiers above as parallel jobs (`plan`, `check`, `unit`, `coverage`, `fuzz`, `race`, `mutation`, `image`, `generated-models`, `cloudflare-miniflare`), so a run takes as long as its slowest job, not the sum. The `test` job only reports whether `plan`, `check`, `unit`, `coverage`, `fuzz`, `race`, `image`, `generated-models` and `cloudflare-miniflare` passed (a job skipped as unaffected passes; one skipped because something it needs failed does not), and keeps the name the single job had.

### Promotion stages

What runs depends on where a change is. `test/tiers.json` says it per job, and `test/ci` checks that every job of the workflows has an entry there.

| Stage | Scope | What runs |
|---|---|---|
| Pull request | affected and diff-scoped | `plan` lists the files the pull request changes (`git diff HEAD^1 HEAD` on its merge commit) and runs `cmd/affected`, which selects every package whose test binary links a changed package (`go list -test -deps`), plus the packages whose tests read a changed file at run time (rules in `cmd/affected/affected.go`). A path it cannot place, `go.mod` or `go.sum` selects everything. `unit` runs every package and reads main's cached result for each one whose inputs did not change. `coverage` and `check` run in full. `race` runs the selected packages whose tests link changed code (a package selected only because a test reads a changed file is left to `unit`, whose cache tracks that file) with `-race -short`, in as many shards as their measured race time needs. `fuzz` runs the targets in those same packages. `mutation` runs the mutants on changed files, of changed tests, or whose own table entries changed (`internal/mutation/select_test.go`), or every mutant when `go.mod`, `go.sum` or the harness changed. `image`, `generated-models` and `cloudflare-miniflare` run when their inputs change and otherwise pass as skipped. |
| `main` | strict and full | Everything: every package under `-race` in four shards with no `-short`, every fuzz target in six, every mutant in twelve, every path-gated job. `unit` writes the result cache that pull requests read. |
| Nightly | uncached | `.github/workflows/nightly.yml` reruns `unit`, `coverage` and `race` with `-count=1`, for a test that depends on something `go test` does not track. |

- **What `-short` skips under race.** A test that parses a large input on one goroutine calls `racegate.SkipUnderRaceOnPullRequest` (`internal/racegate`). It skips only when the binary is built with `-race` and the run is `-short`, which is the pull-request race job and nothing else: the race detector costs ten to forty times as much there and needs two goroutines to find anything. `unit` runs those tests on every pull request without race; `main` and the nightly run them under race. Today these are the bundle and trace-replay gates of `internal/bundled` and `internal/equivalence`.
- **Shards by cost.** `make test-race` fills its shards by the per-package race times in `test/race-costs.json` (`scripts/shard.py`), and the mutation suite by the per-package mutant costs in `internal/mutation/costs_test.go`. Each mutation run reports how many kills were the 30 s timeout rather than a failing assertion, and what each package's mutants cost against the table (`MUTATION_REPORT`, added to the step summary).
- **Running a part locally.** `git diff --name-only origin/main | go run ./cmd/affected` prints what a pull request with your changes runs. `make test-mutation MUTATION_PKGS=./internal/engine` (or `MUTATION_FILES`, `MUTATION_NAMES`, or `MUTATION_DIFF` naming a `git diff -U0` file) runs part of the mutation table.

- **One run per package.** `unit` runs `make test-unit` once with `GOTESTFLAGS=-json` and pipes it through `scripts/test-tiers.py`, which prints the log plain `go test` would and then a table of the snapshot, chaos, BDD, contract, fuzz-seed, equivalence and unit tiers, from the same events. Those tiers used to be steps of their own that ran the same packages again. `extraction equivalence` stays a named step: `make equivalence` is the trace replay in `internal/equivalence`, whose result the step before cached, and the three build-and-register gates of `internal/bundled` (the rest of that package is the unit run's); `aws sdk round-trip` runs `test/sdk/go`, the one module `go list ./...` does not reach. `check` runs vet, gofmt and the ratchet.
- **Cache.** `.github/actions/go` restores a Go build and test-result cache. Jobs that build the same way share one (`test`); coverage, race, fuzz and mutation each keep their own. Only pushes to `main` write it, one job per cache, so a pull request never changes what another run restores. `go test` keys a cached result on the size and mtime of every file the test opened, so the action also runs `scripts/restore-mtimes.py`, which sets each file's mtime to the time of the last commit that touched it. Both are needed: without the second, a fresh checkout invalidates every package that reads a file.
- **Cached results.** Every run, `main`'s included, reuses a package's cached result when nothing it depends on changed: `unit` passes no `-count` and no `-cover`, either of which stops `go test` caching, and the flags match between `main` and pull requests (`-json` included; a result cached without it is not reused with it). `coverage` cannot be cached, so it is the uncached run of every test, on every pull request and every push to `main`. A test that depends on something `go test` does not track (the clock, the network, a port) still runs uncached there, and `.github/workflows/nightly.yml` reruns `unit`, `coverage` and `race` with `-count=1` every night.
- **Running it the way CI does.** `make test-unit GOTESTFLAGS=-json | python3 scripts/test-tiers.py` gives the same table locally. Run it twice: the second run reports every package from the cache.
- **Fuzzing.** `scripts/fuzz.sh` runs `scripts/fuzz-targets.txt`, one target per line. It fails on a target that does not exist, which `go test -fuzz` does not. A `Fuzz` function missing from the list still runs its seed corpus but is never fuzzed; 68 of 195 were missing when the list moved out of the Makefile.
