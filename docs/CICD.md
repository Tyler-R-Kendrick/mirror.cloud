# CI/CD execution on mirror.cloud

Optional local job execution for CI/CD control planes. Default path is still B-IR admit + observe; process runs only when explicitly enabled.

## Profiles

| Path | What it is | Dependencies |
|---|---|---|
| Control plane (default) | CodeBuild B-IR CRUD; StartBuild admits `IN_PROGRESS` | None |
| Execute (opt-in) | AfterInvoke runs buildspec 0.2 in a local work dir | `MIRROR_CICD_EXECUTE=1` or `codebuild.Enable()` |

Fidelity header remains `mock|emulate|proxy`. Execute-mode is an opt-in side effect, not a fidelity value.

## Make targets

```bash
make test-cicd-core      # JobControl contract + lifecycle admit
make test-cicd-process   # buildspec 0.2 runner + codebuild hook package
make test-cicd-e2e       # full ./internal/execution/cicd/... (includes AWS-CB-ARTIFACT-BYTES)
```

Equivalent go invocations:

```bash
go test ./internal/execution/cicd/ ./internal/execution/cicd/lifecycle/ -count=1
go test ./internal/execution/cicd/buildspec/ ./internal/execution/cicd/codebuild/ -count=1
go test ./internal/execution/cicd/... -count=1
```

## CodeBuild buildspec 0.2 (local)

1. Blank-import `internal/execution/cicd/codebuild` so `RegisterAfterInvoke("aws.codebuild", "StartBuild", …)` runs.
2. Enable execution (`MIRROR_CICD_EXECUTE=1` or `Enable()`).
3. `StartBuild` with a project whose source carries a `version: 0.2` buildspec (or `buildspecOverride`).
4. Hook parses via `internal/execution/cicd/buildspec`, runs phase commands in a temp dir (no Docker/network), writes terminal status + artifact bytes to Store.

## ADV-EXEC-OFF

Without an executor registered/enabled, StartBuild must leave `buildStatus=IN_PROGRESS`. Fabricating `SUCCEEDED` is ADV-FALSE-SUCCESS. Covered in `TestAWS_CB_ARTIFACT_BYTES` after `Disable()`.

## Trust

Process execution runs caller-supplied buildspec commands on the host. Treat as trusted-dev only. Credential-free API surface unchanged; namespace scoping ≠ cloud auth.

## Cloudflare CI (`@cloudflare/ci` 0.2)

Distinct from Workers Builds / Pages GitHub App. This is Cloudflare's Workflows+Sandbox CI SDK ([cloudflare/ci](https://github.com/cloudflare/ci)), triggered by Artifacts `cf.artifacts.repo.pushed`.

Local profile (`internal/execution/cicd/cloudflareci`):

- Parse/map Artifacts push events → `CiParams`
- Run `ci.runner` / chained `deps.runner` graphs with `cache.inputs` content-addressed hits
- `Then` barrier models deploy-after-checks (failed check → deploy skipped, status `errored`)
- Trusted process stand-in for Sandbox — no Workflows durable steps, R2 pointers, or Containers

```bash
go test ./internal/execution/cicd/cloudflareci/ -run TestCF_CI_RUNNERS -count=1
```

Capability row: `internal/execution/cicd/cloudflareci/capability.json`.

## Additional bounded profiles

| Package | Fixture |
|---|---|
| `cicd/cfbuilds` | `CF-DELIVERY` Workers Builds stand-in |
| `cicd/cfpages` | `CF-PAGES-DELIVERY` |
| `cicd/vercel` | `VC-DELIVERY` |
| `cicd/azswa` | `AZ-SWA-PREVIEW` |
| `cicd/azacr` | `AZ-ACR-TASK` |
| `cicd/gcpapphost` | `GCP-APPHOST-DELIVERY` |
| `cicd/codepipeline` | `AWS-PIPE_CODEBUILD` |
| `cicd/delivery` | `HOST-*-DELIVERY` |

See `docs/CICD_IMPLEMENTATION_REPORT.md` for honesty bounds.
