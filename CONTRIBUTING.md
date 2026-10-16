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

## Test

```bash
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
