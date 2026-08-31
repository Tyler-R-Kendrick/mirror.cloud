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

## Where this fits

The local-AWS-emulator field is crowded and well served. What is not served, by anything currently in the field:

- **more than one cloud** — every incumbent is AWS-only;
- **running a single product standalone**, instead of a whole cloud on one port;
- **a proxy tier** that records real-cloud behavior, replays it, and reports drift — which is also how this project grades its own accuracy, rather than claiming fidelity from spec conformance alone;
- **spec-update diffs as a day-2 workflow**, so provider API drift becomes a reviewable pull request.

AWS is where the pipeline gets validated, because it has the best-published specs and the most mature SDKs to test against. It is not where the differentiation lives.

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

Dummy credentials `test`/`test` work. By default, so does anything else: signatures are parsed but not verified. Set `MIRROR_S3_VALIDATE_PRESIGNED_SIGNATURES=true` to verify S3 SigV2 and SigV4 presigned query signatures against `test`/`test` and deterministic IAM/STS credentials. Authorization-header signature verification is not yet supported. Default listen: `http://127.0.0.1:4566`.

`bin/mirror up --profile aws-core` boots the emulate-tier AWS packs in docs/SUPPORT.md. Remaining ingested Smithy operations (if any) are mock-tier. This is not LocalStack-complete: no hypervisor, no real RDS/Redis/EKS, extra ops are named control-plane records. `bin/mirror up --all` serves mock-tier for everything else. `--strict` refuses mock. EC2 is VPC/subnet/SG/instance records on the ec2Query wire.

S3 terraform refresh reads (`GetBucketAcl`, CORS, encryption, …) return the empty “not configured” document the AWS provider tolerates — they are not silent write stubs. IAM evaluates Deny then Allow (default deny when the role has policies). SSM `SecureString` is reversible local encoding, not encryption. CloudFormation accepts JSON or YAML TemplateBody and a fixed resource-type set.

Hosted bind (`--bind 0.0.0.0:4566`) prints a banner: there is no authentication; do not expose the process to untrusted networks.

See [docs/SUPPORT.md](docs/SUPPORT.md) (generated; do not hand-edit), [docs/DAY2.md](docs/DAY2.md), [docs/EXTENDING.md](docs/EXTENDING.md).

## Documents

- **[docs/DIRECTION.md](docs/DIRECTION.md)** — the thesis, the market analysis, the v1 scope decisions, and the v2 correction (§9): generate *behavior*, not just protocol.
- **[docs/BEHAVIOR_IR.md](docs/BEHAVIOR_IR.md)** — the Behavior IR: service semantics as versioned, provenance-tagged data (statecharts, CEL rules, effect vocabulary, budgeted primitives), executed by one generic engine.
- **[docs/PARITY_PIPELINE.md](docs/PARITY_PIPELINE.md)** — how ground truth is acquired: real-cloud probing, versioned behavior corpora, differential replay, per-operation grades, and the behavioral changelog for vendor changes nobody announced.
- **[docs/MASTER_PROMPT_V2.md](docs/MASTER_PROMPT_V2.md)** — the current execution contract: strangler extraction of the hand-written packs, frozen v2 interfaces, swarm contracts, and the anti-scope-drift CI gates.
- **[docs/MASTER_PROMPT.md](docs/MASTER_PROMPT.md)** — the v1 specification (still authoritative for the SPI, wire protocols, identity, and diagnostics).
- **[docs/CRITIQUE.md](docs/CRITIQUE.md)** — five adversarial passes, including the audit of the v1 implementation that triggered the v2 pivot.

## Covenants

These are binding project policy, decided at founding because they cannot be retrofitted:

- No capability will ever require an auth token, an account, or a license key.
- No telemetry, no phone-home, no usage counting.
- Scope is **spec-complete by generation, behavior-complete only where declared** — and the declaration is generated from the model, so it cannot drift into marketing.

## Development

```bash
go build ./...        # build everything
make test             # unit tests (includes determinism lint + import-graph checks)
make test-coverage    # unit tests with the coverage floor
make test-race        # race detector
```

`gofmt` and `go vet` must be clean; CI enforces both. See [docs/TESTING.md](docs/TESTING.md) for the full suite map (snapshot, chaos, BDD, fuzz, mutation).

Licensed under [Apache-2.0](LICENSE).
