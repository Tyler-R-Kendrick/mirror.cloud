# CI/CD execution — implementation report

**Branch:** `feat/cicd-execution`
**Date:** 2026-09-21
**Gate:** `make test-cicd-core && make test-cicd-process && make test-cicd-e2e` — green

## Production readiness (bounded)

mirror.cloud CI/CD execute-mode is **production-ready for local/dev emulation** of the mandatory fixture matrix: real process execution, durable admit, generation fencing, native-shaped lifecycle where B-IR exists, HTTP-verified delivery. Opt-in only (`MIRROR_CICD_EXECUTE=1` / `Enable()`). Default path never fabricates SUCCEEDED (ADV-EXEC-OFF).

This is **not** a claim of LocalStack-complete parity for every AWS/Azure/GCP/Cloudflare/Vercel product surface.

## Fixture matrix (green)

| Area | Fixtures |
|---|---|
| AWS | DELIVERY, CB-ARTIFACT-BYTES, AMPLIFY-DELIVERY, PIPE_CODEBUILD, PIPE_APPROVAL_DEPLOY, ADV-ABANDON, ADV-SOURCE-MOVE |
| Azure | DELIVERY, SWA-PREVIEW, ACR-TASK, APPSERVICE-DELIVERY |
| GCP | DELIVERY, APPHOST-DELIVERY, CLOUDDEPLOY-DELIVERY |
| Cloudflare | DELIVERY (Workers Builds stand-in), PAGES, CI-RUNNERS; CF-RUNTIME-JOIN (skip unless `MIRROR_MINIFLARE=1`) |
| Vercel | DELIVERY (promote/rollback), BOA-STATIC, BOA-FUNCTION (Node) |
| GH/GL/BB | DELIVERY; GH-ACTIONS (composite + node20 `uses: ./`); ADV-TOLERATED-FAILURE |
| Hosting | NETLIFY, RENDER (+ Railway/DO/Fly/Hostinger/Hetzner admit+StaticSite) |
| Shared | JobControl contract; ADV-SUBMIT-RACE/STALE/NOT-IDEMPOTENT/CANCEL/INTENT-CRASH/EGRESS; OCI-PUSH-PULL; buildspec env/export/artifacts/secondary |

## Capability packages

Under `internal/execution/cicd/**/capability.json` — each names `requiredTests` and honest `limitations`.

## Still out of claim

- Marketplace GH actions / full runner protocol / container actions beyond local `uses: ./`
- workerd-backed Workers Builds as default CI (join is opt-in Miniflare)
- Vercel Edge/ISR/image optimization; BOA functions beyond tiny Node handler ABI
- Azure ACR ARM Tasks runner; App Service slots/Oryx
- GCP Cloud Deploy Skaffold + Cloud Run containers
- Distribution OCI HTTP API / containerd
- Isolated cgroup network NS (ScrubEnv + ADV-EGRESS only)
- Every CodePipeline action provider (S3 publish, Lambda, ECS, …)

## Operator

```bash
make test-cicd-e2e
# optional live Workers join:
MIRROR_MINIFLARE=1 go test ./internal/execution/cicd/cfbuilds/ -run TestCF_RUNTIME_JOIN -count=1
```
