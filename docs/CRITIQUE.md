# Adversarial Critique — attacking mirror.cloud's own plan

> This document exists to argue *against* [`DIRECTION.md`](./DIRECTION.md) and [`MASTER_PROMPT.md`](./MASTER_PROMPT.md). Findings that produced concrete changes are marked **→ folded in**. Findings that remain live risks are marked **→ open**. Nothing here is rhetorical; every objection is one a hostile reviewer would actually raise.

---

## Part 1 — Attacks on the thesis

### A1. "Generated emulators" oversells what generation actually buys

**The attack.** The pitch is "generate the emulator from the spec." But a spec describes *shape*, not *behavior*. Generation gets you routing, serialization, and validation — the boring 30%. Everything a developer actually cares about (does `ListObjectsV2` paginate correctly, does `UpdateItem` apply `if_not_exists`, does a message reappear after visibility timeout) is hand-written behavior that generation cannot touch. So the headline claim is architecturally true and experientially misleading.

**Verdict: the attack lands, and the fix is positioning, not architecture.** The honest framing is not "we generate emulators." It is:

> **One unsupported API call should not kill your test suite.**

That is the real, specific, felt pain. Today a developer using an emulator hits exactly one call the emulator doesn't have and is forced to abandon the emulator, hand-roll a stub, or skip the test. Generation means that call is *validated and answered* instead of a wall. That is a modest, true, and genuinely valuable claim. "300 services supported" is neither true nor survivable.

**→ folded in:** never advertise a raw service count; `docs/SUPPORT.md` reports emulate-tier and mock-tier separately, generated from the model so it cannot drift into marketing.

### A2. Mock tier will produce confident nonsense that ends up in assertions

**The attack.** A developer calls a mock-tier operation, sees a plausible response, writes `assert.Equal(t, "arn:aws:...:xyz", got.Arn)`, and ships. Six months later a spec re-pin changes the synthesized value and their suite breaks for reasons they cannot possibly diagnose. The feature actively manufactures future pain.

**Verdict: lands.** Determinism alone does not solve it — determinism makes the value stable *within* a spec version, not *across* one.

**→ folded in:** §4.10.6 requires synthesis seeded from shape structure and request only — never spec file ordering, trait ordering, or map iteration order — with a test that mutates a spec equivalently and asserts synthesized output is unchanged. Plus `x-mirror-fidelity: mock` on the wire, a first-use warning per operation, and `--strict` for CI.

**→ open:** a determined user can still assert on synthetic values. Documentation must say plainly: *assert on structure and status for mock-tier calls, never on values.*

### A3. Two providers is not "any cloud"

**The attack.** Eight AWS services plus one GCS service, with Azure explicitly deferred, does not demonstrate a provider-neutral architecture. It demonstrates an AWS emulator with a GCS feature.

**Verdict: partially lands.** One non-AWS service cannot *prove* neutrality, but it can *falsify* it — which is the only thing available at this size. The falsification is real: GCS uses a different spec format (Discovery, not Smithy), a different protocol shape, a different identity model (project, not account), and different self-referencing URL semantics. An AWS-shaped model breaks on all four.

**→ folded in:** §4.9 makes this an explicit, CI-enforced constraint — `internal/model` may contain no service-specific string literal — and states that if GCS needs a special case, the abstraction is wrong and gets fixed rather than special-cased.

**→ open:** honest positioning is "provider-neutral by construction, proven across two providers." Not "any cloud." Claim the second and the first reviewer to try Azure will call it vapor.

### A4. The differentiation may be too subtle to win on

**The attack.** A refugee from the LocalStack sunset is choosing today. Floci is MIT, drop-in, fast, has 20+ services and five figures of stars, and is already being adopted by downstream ecosystems. "We have a canonical behavioral model and three fidelity tiers" is an architecture argument aimed at a developer making a five-minute decision.

**Verdict: lands hard. This is the most serious strategic risk in the plan.** Kimi's own research says it explicitly: distribution and license beat capability. A better architecture that loses distribution loses.

**Consequences for delivery** (see Part 3): compete on the four things a developer can evaluate in five minutes — zero-config via `AWS_ENDPOINT_URL`, one command for one product, snapshots free, and the no-wall property — and do not compete on architecture. Also: **do not position against Floci.** Be compatible with the same conventions and the same port, and let the differentiation be additive rather than a migration argument.

### A5. "Compiler for cloud emulators" is an architect's tagline

**The attack.** It describes the implementation, not the benefit. No developer searches for it.

**Verdict: lands.** Keep it as the internal thesis; it is a good one. The external line should be the tier story, which is concrete and immediately legible:

> **Run cloud APIs locally. The ones you depend on, emulated. The ones you touch incidentally, mocked. The ones you must be exact about, proxied to the real thing and recorded. No account, no token, ever.**

---

## Part 2 — Attacks on the execution plan

### B1. The single most likely one-shot failure is DynamoDB expressions

A real lexer, parser, and evaluator for five expression grammars, with correct precedence and error handling, is a substantial project on its own. It is enumerated honestly in §4.8 rather than hand-waved, and isolated into its own package with its own test suite — but it remains the largest single risk.

**→ folded in:** coordination rule 9 (degradation policy) — reduce by dropping services, never by dropping the spine; and rule 5 permits honest demotion to mock tier over silent partial implementation.

### B2. Cold start, binary size, and "all services" were in direct contradiction

**The attack.** The original framing promised mock tier for the entire vendored spec set *and* a sub-2-second cold start *and* a small binary. Embedding hundreds of AWS service models satisfies none of those together.

**Verdict: lands — this was a genuine internal inconsistency.**

**→ folded in:** §4.14 — a curated generated set (emulate-tier services + GCS + ~20–30 commonly co-used services at mock tier), per-service lazy model embedding, `mirror spec add <service>` to extend on demand, and CI budgets on both binary size and cold start. `spec add` is a *better* promise than a large default binary: it makes "the wall is gone" true without making the default artifact heavy.

### B3. Spec fidelity ≠ SDK compatibility — the biggest source of "almost works"

**The attack.** Building from the spec produces something SDKs *nearly* work against. The gaps are all undocumented: `aws-chunked` framing, flexible checksums, `Expect: 100-continue`, timestamp format variance, empty-vs-absent semantics, self-referencing URLs, and retry behavior driven by error classification. A spec-driven implementation gets every one of these wrong by default, and `aws-chunked` in particular will silently corrupt every S3 upload.

**Verdict: lands hard. This was the most valuable finding of the critique pass.**

**→ folded in:** §4.12 enumerates all ten, assigns them to the edge/codec layer (S2) so they are solved once rather than per pack, and adds DoD items 22 and 25.

### B4. `terraform apply` fails on the read path, not the write path

**The attack.** DoD item 8 requires `terraform apply` and `destroy` to succeed. Refreshing a single `aws_s3_bucket` calls a dozen-plus read APIs (`GetBucketAcl`, `GetBucketPolicy`, `GetBucketCors`, `GetBucketWebsite`, `GetBucketLifecycleConfiguration`, `GetBucketReplication`, `GetBucketEncryption`, and more). If any returns 501, the apply fails. The §4.8 fidelity table listed almost none of them, so the plan as originally written **could not have satisfied its own definition of done.**

**Verdict: lands. This was a real, plan-invalidating defect.**

**→ folded in:** §4.13 requires every emulate-tier service to answer its entire refresh read-path with a valid "not configured" response rather than 501, requires the set to be enumerated empirically from provider debug logs into `test/terraform/READ_PATH.md`, and adds DoD item 21.

### B5. The "swarms" were serialized in disguise

**The attack.** Any plan that says "establish the foundation, then launch the service swarms in parallel" has a hard barrier at the foundation. Under a one-shot execution that barrier is most of the wall-clock.

**Verdict: lands.**

**→ folded in:** two mechanisms. First, §3 freezes every interface *in the prompt text*, so all fourteen swarms code against identical contracts from turn one without negotiating them. Second, S0 ships `internal/spitest` — reference in-memory implementations of every dependency — so behavior packs unit-test immediately without waiting for the real store. The decoupling rule (packs receive `Input map[string]any` and never import codecs or generated code) makes the fan-out real rather than nominal.

### B6. The user asked for hosted mode and the plan only described localhost

**The attack.** The requirement included hosting "real/fake/proxied cloud compatible APIs themselves." A localhost-only binary with a loopback bind does not serve a team.

**Verdict: lands — a straightforward scope miss.**

**→ folded in:** §4.15 — `--bind`, `--advertise-url`, optional TLS, account-per-developer tenancy reusing the existing namespacing, named shareable snapshots, and a startup banner stating there is no authentication. Critically: **the answer to "shared deployment needs auth" is network isolation, never an auth token.** Adding one would violate the covenant that is the project's main credibility asset.

### B7. Proxy tier is a credential-exfiltration surface

**The attack.** A feature that captures real cloud traffic to disk will eventually capture a session token into a file someone commits.

**→ folded in:** scrubbing at write time (not read time), covering `Authorization`, `X-Amz-Security-Token`, configured secret patterns, and any value present in the process environment; a test asserting no known-secret value survives; proxy off by default requiring an explicit flag *and* an explicit endpoint.

**→ open:** cassettes should default to a gitignored directory and `mirror doctor` should warn when a cassette directory is tracked by git. Worth adding during implementation.

---

## Part 3 — Rubber-duck: positioning, iteration, delivery

### Positioning

**Lead with the wall, not the architecture.** The one-line pitch is the tier story (A5). The one-paragraph pitch is: *emulated where it matters, mocked where it doesn't, proxied when you need the truth, and you can run just the one product you actually need.*

**Sequence the claims by evaluability.** A developer decides in five minutes. In descending order of five-minute impact:

1. `export AWS_ENDPOINT_URL=http://localhost:4566` and your existing code works — **zero code changes**. This is the strongest and most under-marketed property in the whole category, and it costs nothing to support because the SDKs already do it.
2. `mirror up s3` — one product, one process, sub-second.
3. The call your emulator doesn't have doesn't stop you.
4. Snapshots, free.
5. `mirror doctor` tells you why it isn't working instead of making you guess.

**Do not fight the incumbent challenger.** Floci is winning the drop-in-replacement race and that race is close to decided. Use the same default port and the same conventions, be a good ecosystem citizen, and differentiate additively. "Also works alongside what you already use" converts far better than "switch from the thing you just switched to."

**Publish the covenants before the code.** `COVENANT.md` and `GOVERNANCE.md` cost an afternoon and address the market's freshest trauma — a community that was gated overnight in March. This is the cheapest credibility available, and it is only cheap *now*: a covenant published at founding is a promise, one published after a funding round is a press release.

### Iteration

**The spine is the product; services are content.** Protect `spec → model → codegen → codec → edge → one pack → SDK round-trip → terraform apply`. Three services on a reproducible spine beat eight services on an unreproducible one, because the spine is what makes service nine cheap.

**Make the SDK the first consumer, not the last.** Every wire-level assumption in §4.12 is cheap on day one and expensive after eight packs are built on it. An `aws-sdk-go-v2` call against the running binary should be the first green test.

**Let the journal drive the roadmap.** Every `NotImplemented` is a logged, structured, aggregate-able signal of exactly what real users needed and did not get. That is a far better prioritization input than intuition, and no competitor in this space is collecting it — because collecting it usually requires telemetry, and here it is purely local and user-visible. `mirror doctor` can offer to print the top-N unimplemented calls so a user can file one useful issue instead of ten vague ones.

**Treat `mirror spec update` as the flagship day-2 demo.** "AWS shipped API changes; here is the reviewable diff of what changed in your emulator" is a capability nobody else in the category has, and it directly answers the question that kills emulators over time: *is this still accurate?*

### Delivery

**Distribution is the fight** — Kimi's strongest empirical finding, and the LocalStack sunset is its proof. In rough order of leverage:

1. **A single static binary on GitHub Releases**, plus `go install`. No Docker required to try it. Docker friction (Desktop licensing, Hub pull limits, API version breakage) is a recurring tax and containerless is a genuine differentiator, not a fallback.
2. **A Docker image**, and **per-service images** — `mirror-s3` as a distributable artifact is a distinctly better story than a monolith.
3. **A Testcontainers module.** This is how emulators actually get adopted: as one line in someone's test setup. Thin modules consuming configuration as data, never per-language reimplementations.
4. **A GitHub Action**, so CI adoption is one YAML block.
5. **The zero-config `AWS_ENDPOINT_URL` path in the first paragraph of the README**, above every other integration method.

**Make the README's quick start literally executable and test it in CI.** A quick start that has drifted is worse than none: it converts an interested developer into an annoyed one at the exact moment they were most willing.

**Ship the generated support matrix from day one, with the honest tier column visible.** Publishing "these eight are emulated, these thirty are mocked, everything else is one `spec add` away" builds more trust than any coverage claim, and it is the artifact that makes A1's honest positioning legible without a single word of marketing.
