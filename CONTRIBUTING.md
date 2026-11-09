# Contributing

## Prerequisites

- Go 1.24, the version CI installs (`.github/workflows/ci.yml`)
- `make`
- git

## Clone and build

```bash
git clone https://github.com/tyler-r-kendrick/mirror.cloud.git
cd mirror.cloud
go build ./...
```

### Only the services you work on

Most of the repository is services: models, bundles, packs, vendored specs and
the suites that exercise them. To check out just the ones you touch, clone
without file contents and let `scripts/sparse.py` choose what to download:

```bash
git clone --filter=blob:none --no-checkout https://github.com/tyler-r-kendrick/mirror.cloud.git
cd mirror.cloud
git show HEAD:scripts/sparse.py | python3 - aws.s3 aws.sqs 'vercel.*'   # quoted: every Vercel service
go build ./... && make test-service SERVICES="aws.s3 aws.sqs"
```

| | full clone | `aws.s3` (and the seven services it needs) | `aws.dynamodb` |
|---|---|---|---|
| working tree | 67 MB, 1,469 files | 10 MB, 677 files | 8 MB, 549 files |
| `.git` | 38 MB | 5 MB | 5 MB |

The checkout holds all of the code that is not a service, and for each service
you ask for its pack, model and bundle, the services that one needs to build and
to run its own tests (S3 notifies SNS, Lambda and EventBridge, so those come
too), and nothing else. `scripts/sparse.py --list aws.s3` shows what that is
without changing anything; run it again with a different list to change the set,
and `scripts/sparse.py --all` returns to the whole tree. The set is fixed when you run it, so run it again after a `git pull` that gave one of your services a new dependency (the build says which package is missing). Plain `go build ./...`,
`go vet ./...` and `go test ./...` work in it, so editors do too. The one thing
it leaves out is a test that names a service that is not there (most of
`internal/runtime`, `internal/bundled`, `internal/edge` and the suites in `test/`
that span services): CI runs those on the full tree, and a `sparse` job proves
that every kind of set above still builds and passes. If you add a dependency
between services, `make graph` rebuilds `profiles/graph.txt`, which is what
decides what comes along; a test checks it is current.

## Test

```bash
make test-service SERVICES="aws.s3"   # just those services' own tests, in any tree
make test             # unit tests (includes determinism lint + import-graph checks)
make test-coverage    # unit tests with the coverage floor
make test-race        # race detector
gofmt -l .            # must print nothing
go vet ./...          # must be clean
```

`make vet` runs `go vet` and a `gofmt -l` check. CI enforces both, then the rest of the suite. See [docs/TESTING.md](docs/TESTING.md).

## Proposing changes

Fork the repository, branch from `main`, and open a small, focused pull request. CI must pass.

A change that would violate [`COVENANT.md`](COVENANT.md) or [`GOVERNANCE.md`](GOVERNANCE.md) is out of scope. [`COVENANT.md`](COVENANT.md) is product policy, not a code of conduct. Conduct is [`CODE_OF_CONDUCT.md`](CODE_OF_CONDUCT.md).

## Commit and pull request conventions

- Write a clear message that says what changed and why.
- Reference issues when relevant (`Fixes #123`).
- Keep the diff focused on one change.
- Fill in the pull request template.

## AI-assisted contributions

This project is built with coding agents under human direction and review. Humans decide scope and merge.

Disclose significant AI-generated content in the pull request. Commits made with an agent should credit the tool and model in the commit message or the pull request description.
