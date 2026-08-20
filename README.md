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

Pre-implementation. The design is settled and written down; the code is not yet built.

## Documents

- **[docs/DIRECTION.md](docs/DIRECTION.md)** — the thesis, the market analysis behind it, and the scope decisions for v1.
- **[docs/MASTER_PROMPT.md](docs/MASTER_PROMPT.md)** — the complete, self-contained implementation specification: frozen interfaces, protocol reference, per-service fidelity table, subagent swarm contracts, and the definition of done.
- **[docs/CRITIQUE.md](docs/CRITIQUE.md)** — an adversarial review of both documents, plus positioning, iteration, and delivery advice.

## Covenants

These are binding project policy, decided at founding because they cannot be retrofitted:

- No capability will ever require an auth token, an account, or a license key.
- No telemetry, no phone-home, no usage counting.
- Scope is **spec-complete by generation, behavior-complete only where declared** — and the declaration is generated from the model, so it cannot drift into marketing.

Licensed under [Apache-2.0](LICENSE).
