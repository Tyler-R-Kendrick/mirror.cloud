# mirror.cloud

**Public/Private Cloud Emulation.**

Run cloud APIs locally. The ones you depend on, emulated. The ones you touch incidentally, mocked. The ones you must be exact about, proxied to the real thing and recorded. No account, no token, ever.

mirror.cloud generates cloud API emulators from the cloud providers' own published specifications — AWS Smithy models, Google Discovery documents — and serves them from a single static binary. Behavior is layered selectively on top of the generated protocol layer, at a fidelity that is always declared and never implied:

| Tier | What you get |
|---|---|
| `emulate` | Real semantics, hand-written, for the services people actually build on |
| `mock` | Generated from the spec: input validated against the real shape, response schema-valid and deterministic. So that one unsupported API call doesn't kill your test suite |
| `proxy` | Pass-through to the real cloud, with record, replay, and drift reporting |

Run one product or many — `mirror up s3` is one process, one port, sub-second.

## Status

v1 spine is in tree: frozen interfaces, edge + codecs, emulate-tier packs, mock synthesizer, CLI. Spec ingestion (`make specs-sync`) still vendors AWS Smithy / GCS Discovery; until that pin lands, the process boots from a hand-built catalog.

## Quick start

```bash
go build -o bin/mirror ./cmd/mirror
bin/mirror up s3
# another shell:
eval "$(bin/mirror env)"
aws --endpoint-url http://127.0.0.1:4566 s3 mb s3://demo
aws --endpoint-url http://127.0.0.1:4566 s3 cp README.md s3://demo/README.md
```

Dummy credentials `test`/`test` work. By default, so does anything else: signatures are parsed but not verified. Set `MIRROR_S3_VALIDATE_PRESIGNED_SIGNATURES=true` to verify S3 SigV2 and SigV4 requests plus single-chunk SigV4A/ECDSA presigned queries and Authorization headers against `test`/`test` and deterministic IAM/STS credentials, including issued temporary session tokens and LocalStack gateway-port Host aliases. Signed hexadecimal payload hashes, SigV4 streaming chunk and trailer signature chains, and supported trailing checksums are checked. SigV4A streaming chunks and trailers are not yet supported. Default listen: `http://127.0.0.1:4566`.

`bin/mirror up --profile aws-core` boots the emulate-tier AWS packs in docs/SUPPORT.md. Remaining ingested Smithy operations (if any) are mock-tier. This is not LocalStack-complete: no hypervisor, no real RDS/Redis/EKS, extra ops are named control-plane records. `bin/mirror up --all` serves mock-tier for everything else. `--strict` refuses mock. EC2 is VPC/subnet/SG/instance records on the ec2Query wire.

S3 terraform refresh reads (`GetBucketAcl`, CORS, encryption, …) return the empty “not configured” document the AWS provider tolerates — they are not silent write stubs. IAM evaluates Deny then Allow (default deny when the role has policies). SSM `SecureString` is reversible local encoding, not encryption. CloudFormation accepts JSON or YAML TemplateBody and a fixed resource-type set.

Hosted bind (`--bind 0.0.0.0:4566`) prints a banner: there is no authentication; do not expose the process to untrusted networks.

See [docs/SUPPORT.md](docs/SUPPORT.md) (generated; do not hand-edit), [docs/DAY2.md](docs/DAY2.md), [docs/EXTENDING.md](docs/EXTENDING.md).

## Documents

- **[docs/DIRECTION.md](docs/DIRECTION.md)** — the thesis, the market analysis behind it, and the scope decisions for v1.
- **[docs/MASTER_PROMPT.md](docs/MASTER_PROMPT.md)** — the complete, self-contained implementation specification: frozen interfaces, protocol reference, per-service fidelity table, subagent swarm contracts, and the definition of done.
- **[docs/CRITIQUE.md](docs/CRITIQUE.md)** — an adversarial review of both documents, plus positioning, iteration, and delivery advice.

## Covenants

These are binding project policy, decided at founding because they cannot be retrofitted:

- No capability will ever require an auth token, an account, or a license key.
- No telemetry, no phone-home, no usage counting.
- Scope is **spec-complete by generation, behavior-complete only where declared** — and the declaration is generated from the model, so it cannot drift into marketing.

## Development

```bash
npm install
uv tool install pre-commit   # official pre-commit (https://pre-commit.com)
npm run hooks:install
```

Hooks (pre-commit): trailing whitespace and file hygiene, Gitleaks, **anti-slop Oxlint**, and `tsc --noEmit`. Bypass only in an emergency with `SKIP_GIT_HOOKS=1`.

```bash
npm run lint           # anti-slop + Oxlint
npm run typecheck
npm run hooks:run      # run every hook against the tree
```

Licensed under [Apache-2.0](LICENSE).
