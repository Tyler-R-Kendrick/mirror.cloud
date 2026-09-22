# Interface notes

Objections to frozen interfaces in `docs/MASTER_PROMPT.md` §3 go here. They do not go in code.

## Bundle.Provider is singular

`model.Bundle.Provider` is one value, but v1 serves AWS and GCS from one process. The bootstrap catalog sets `ProviderAWS` even though it also contains `gcp.storage`. Service identity is the `Service.ID` / `Service.Protocol` pair; packs do not branch on `Bundle.Provider`. A mixed-provider Bundle field would be the right model, but the frozen struct is transcribed as written.

## Presign expiry has no field on Identity

`spi.Identity` has no expiry member. Presigned-URL expiry is signaled by appending `:expired` to `ARN` and checking `identity.Expired`. `Parse` takes the process clock (`now time.Time`) so it does not call `time.Now`.

## The served model is the generated model

`internal/specboot` derives the served bundle from `internal/generated` (S1 codegen: `cmd/mirrorgen`, `specs/`), served under the wire names SDKs use. The hand-built catalog it once booted from is gone; what no specification carries -- three AWS services with no published model, and a handful of data-plane operations -- is a short table in `specboot`.

## S1 — spec ingestion

- **AWS api-models-aws on-disk layout.** Verified against `github.com/aws/api-models-aws` on 2026-08-20: files live at `models/<sdk-id>/service/<version>/<sdk-id>-<version>.json`. The upstream README omits the `service/` segment. `scripts/specs-sync.sh` walks that tree rather than hardcoding a remembered layout; `internal/receiver/aws/smithy` is a shapes-map walker and does not assume a path.
- **`Operation` and `Service` have no `UnknownTraits`.** Unknown traits are recorded on `Shape` only. Operation- and service-level unknown traits (endpoint rule sets, auth, examples, …) are ignored after known traits are applied — never fatal. Adding a field would unfreeze §3.1.
- **`aws.protocols#awsQueryError` has no model cell.** It is treated as handled (not recorded as unknown) because it only affects query error wrapping in the codec, not the canonical model.
- **Multiple protocols on one service (SQS).** The model has a single `Protocol`. The Smithy receiver prefers restJson1 > restXml > awsJson1_1 > awsJson1_0 > awsQuery > ec2Query, so SQS becomes `awsJson1_0`. Dual-protocol dispatch stays an edge concern (§4.3).
- **Service IDs vs endpoint prefixes.** `aws.api#service.endpointPrefix` is the default ID suffix, with aliases `monitoring→cloudwatch`, `tagging→resourcegroupstaggingapi`, `elasticloadbalancing`+sdkId v2→`elbv2`, `application-autoscaling→applicationautoscaling`, so `specs/mirror.set` names match botocore-style IDs rather than raw prefixes.

## Optional local CI/CD execution (JobControl)

Historical v2 models CI/CD control planes as B-IR CRUD. Optional trusted-process execution is an additive seam: provider-neutral `JobControl` in `internal/execution/cicd`, plus engine `AfterInvoke` hooks that may advance an admitted job. Fidelity modes stay `mock|emulate|proxy` (`x-mirror-fidelity`); execute-mode is not a fourth fidelity tier.

Opt-in only: `MIRROR_CICD_EXECUTE=1`, or `codebuild.Enable()` in tests. Without an executor, StartBuild stays `IN_PROGRESS` — never silent `SUCCEEDED`. Credential-free covenant unchanged (dummy/`test` credentials still work). Namespace ≠ auth: Store account+region namespaces do not imply cloud identity.

No new provider Go CRUD packs for CodeBuild: control plane remains B-IR (`behavior/aws/codebuild/service.yaml`); process execution lives in the AfterInvoke hook package. See `docs/CICD.md`.
