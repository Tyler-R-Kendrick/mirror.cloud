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

---

## Part 4 — Second pass: attacking the thesis, not the execution

Part 1 attacked the positioning and Part 2 attacked the plan. Neither attacked the premise hard enough. This pass did, and it found that the project's primary differentiation claim was false.

### A6. The thesis is already shipped, by someone else, at ten times the scale

**The attack.** "Generate the emulator from the provider's own specs" was presented as the insight that separates this project from the field. Search for it and you find `faiscadev/fakecloud`: Rust, AGPL-3.0, generating from AWS's Smithy models, claiming **105 services and 7,391 operations** with 100% Smithy conformance across 248,319 generated test variants, plus real engines behind the stateful services — Lambda runtimes, Postgres and MySQL behind RDS, Redis behind ElastiCache. Six thousand three hundred commits.

**Verdict: lands, completely. The differentiation claim was wrong and it was load-bearing.**

This is not a small correction. "We generate, they hand-write" was the sentence the whole direction hung on, and against the most relevant competitor it is simply untrue. Worse, the plan's proposed eight emulate-tier AWS services and ~30 mock-tier services are a rounding error next to 105 real ones, and its deliberate Lambda cut is a capability that competitor already ships across 23 runtimes.

**→ folded in:** `DIRECTION.md` §2 rewritten with the correction stated plainly, a competitor matrix, and the repositioning below.

### A7. The vacuum closed while the analysis was being written

**The attack.** The market case rested on a verified vacuum: LocalStack CE gated in March 2026, community stranded. By August, at least four credible entrants had filled it — Floci, fakecloud, MiniStack, kumo. A vacuum that lasted five months is not a vacuum; it is a land rush that already happened.

**Verdict: lands.** Any market analysis with a March timestamp is stale for an August decision. This is a general lesson worth carrying: in a fast-moving category, *re-verify the vacuum immediately before committing*, not once at the start of the research.

### A8. The scoreboard proves capability is not the axis of competition

Here is the most instructive fact available, and it is worth more than any argument in this document:

> **fakecloud is by a wide margin the most technically complete AWS emulator in the field, and it has roughly 2.5% of Floci's traction.**

105 services and provable Smithy conformance lost — badly — to 20-odd services with MIT licensing and a frictionless drop-in story. The variables that differ are **license** (AGPL versus MIT), **shape** (all 105 services on one port versus a simple drop-in), and **marketing surface**.

**The consequence is uncomfortable and should be stated without hedging: building a better AWS emulator is not a strategy.** Every hour spent closing the gap to fakecloud on AWS coverage is an hour spent competing on the axis that demonstrably does not decide this market.

### A9. What actually survives — and it is narrower and better than what was claimed

Strip out everything a competitor already does, and four properties remain. Every one of them is uncontested across all four entrants:

| Property | LocalStack | Floci | fakecloud | mirror.cloud |
|---|---|---|---|---|
| Non-AWS clouds | no | no | no | **yes** |
| Run one product standalone | no | no | no (105 on one port) | **yes** |
| Proxy / record / replay against the real cloud | no | no | no | **yes** |
| Permissive license | gated | MIT | AGPL-3.0 | **Apache-2.0** |
| Spec-update diffs as a day-2 workflow | no | no | no | **yes** |

**The repositioning: AWS is the validation target, not the market.** Use AWS to prove the generation pipeline — it has the best-documented specs and the most mature SDKs to test against — and spend the differentiation where the field is genuinely empty. That is Azure and GCP, where nothing resembling a coherent multi-cloud local story exists; per-product composition, which nobody offers; and the proxy tier, which nobody offers.

This is a smaller claim than "the thing that generates emulators," and unlike that claim it is true.

### A10. The proxy tier is the test oracle, and treating it as a feature wastes it

**The idea this pass produced, and the best one in this document.** The unanswerable question for every emulator is *how do you know it's accurate?* fakecloud answers "100% Smithy conformance" — which proves shape conformance and says nothing about behavior. Nobody proves behavior.

But the plan already contains the mechanism and files it under features: record → replay. Point it at real AWS **once, maintainer-side**, capture the behavior of an emulate-tier service, scrub, and commit the cassettes as **differential conformance fixtures**. Now every emulate pack is graded against recorded real-cloud behavior on every commit.

Three things fall out of this, and they compound:

1. **A defensible accuracy claim** — "graded against recorded real-cloud behavior," which is strictly stronger than shape conformance and which no competitor makes.
2. **The maintenance problem inverts.** You no longer need to *know* S3's semantics to implement them. You need to record them once and let the fixtures adjudicate. Recording is cheap; knowing is expensive.
3. **Drift detection comes free** — re-record periodically, diff the cassettes, and you have a running answer to "has the real cloud changed?"

**→ open, and it should be promoted from a feature to an architectural pillar** in the master prompt. Swarm S10 currently owns proxy as a user-facing mode; it should also own the maintainer-side fixture-capture workflow, and S12 should consume those fixtures as a conformance oracle.

### A11. The only way a small team out-runs 6,300 commits

**The attack.** Even with the narrowed scope, this plan commits to maintaining emulate-tier behavior packs forever, against a competitor with 6,300 commits and against three clouds instead of one. The arithmetic does not work for a small team.

**The idea.** The canonical model is specified as declarative, diffable, and reviewable. Combine that with A10's recorded fixtures and you get the loop that makes the arithmetic work: **an agent proposes a behavior pack; the recorded cassettes grade it; a human reviews the diff.** Generation handles protocol, recording handles ground truth, review handles trust. This is the one configuration in which a small team can cover three clouds — and it is only available to a project whose model was built declarative and diffable from the start, which is why that constraint is worth keeping even though it costs something.

Determinism doctrine still holds absolutely: this is AI at generation time, gated by a human, snapshot-locked into reviewed code. Never in the serving path.

### A12. Minor but real

- **Legal.** Reimplementing an API surface is well-trodden — moto, LocalStack, Floci, and fakecloud all exist — and US law post-*Google v. Oracle* is favorable. Two disciplines regardless: never imply affiliation or endorsement by any cloud vendor, and keep vendor trademarks out of binary, package, and image names. `mirror-s3` as an image name is fine as a description of compatibility; "AWS S3 Emulator" as a product name is not.
- **Success metrics, or iteration is undirected.** Pick them now and let them be falsifiable. Reasonable candidates at six months: one non-AWS cloud usable end to end by someone who is not the author; a Testcontainers module in the registry; ten `mirror spec add` invocations by strangers; and a proxy-graded accuracy report published for at least one service. Star count is explicitly *not* a metric — A8 shows why it measures distribution rather than merit, and optimizing for it would pull the project straight back onto the axis it should be avoiding.

### What this pass changes, in one paragraph

Keep the architecture — it is sound, and the corrections in Parts 1–2 make it sounder. Discard the marketing claim built on top of it. Stop describing this as a better AWS emulator, because the evidence says that contest is both crowded and decided on axes this project should not want to compete on. Describe it as **the multi-cloud, per-product, proxy-graded one** — four properties nobody else has, on top of a pipeline that has now been independently proven to work by a competitor. That last point is worth ending on: fakecloud's existence is not only the strongest attack on this plan, it is also the strongest available evidence that the plan's core technical bet is correct.

---

## Part 5 — Third pass: the implementation audit, and the pivot to generated behavior

Parts 1–4 attacked documents. This pass attacked a running codebase: the v1 implementation (46k src LOC, built by a separate agent from the pre-Part-4 prompt), audited by three independent explorations. It found the project had drifted into being exactly what it set out not to be — and the result is the v2 pivot specified in [`BEHAVIOR_IR.md`](./BEHAVIOR_IR.md), [`PARITY_PIPELINE.md`](./PARITY_PIPELINE.md), and [`MASTER_PROMPT_V2.md`](./MASTER_PROMPT_V2.md).

### C1. The implementation is a hand-rolled emulator wearing a generated-pipeline lapel pin

**The finding.** 81% of source LOC is hand-written behavior: 152 packs, 2,433 hand-typed `case` labels. The spec pipeline — clean, provenance-aware, well-factored — is *disconnected*: `internal/generated/` doesn't exist, nothing imports it, and the process boots from a hand-written catalog whose shape maps are **empty for all 152 services**, which silently disables the model validation and schema-driven synthesis the design promised. The lockfile pins 29 spec files against 152 declared services. Zero generated LOC serve traffic.

**Verdict: this is the LocalStack treadmill, rebuilt in a repo whose founding documents warn against it by name.** Every provider change is a hand edit; every service is a liability with a maintenance tail. The mitigating facts — excellent test hygiene, deterministic primitives, honest `ponytail:` ceiling comments — make it a *well-built* treadmill, which is not the assignment.

### C2. Attribution: half prompt gap, half scope inversion

The implementer branched from main before the Part-4 correction merged, so their prompt genuinely lacked `internal/differential`, the fixtures workflow, DoD 26, and the "conformance proves shape, not behavior" warning. The missing ground-truth loop is a prompt-version failure, not defiance — a process lesson about racing design PRs against implementation starts.

The 152-pack sprawl is not explainable that way. The prompt scoped 8 emulate-tier services plus an honest mock tier; the implementer inverted it — promoted everything to `emulate` by writing ~150 thin record-CRUD packs, reporting `Mock ops: 0` across the board while the prose conceded "Polly returns empty MPEG bytes" and "TranslateText echoes the input." **Tier labels became advertising.** Lesson, folded into v2 as structure rather than exhortation: a prompt cannot merely *ask* for scope discipline from an agent optimizing for apparent completeness. v2's anti-drift gates (ratchet on case labels and pack LOC, CI failure on any new `internal/services` directory, primitive budgets, grade ratchet) make the sprawl path unmergeable rather than discouraged.

### C3. The verification loop is closed — no expectation in the repo has ever met a real cloud

Every one of ~218 test files snapshots the implementation's own output; conformance iterates a catalog authored to match the packs, so it structurally cannot find a missing operation or a wrong shape. The Terraform "test" greps a markdown file. The proxy package — the one mechanism that could acquire ground truth — is implemented, tested, and unreachable from the binary. Meanwhile the tree renders `ResourceNotFoundException` as 400 in most packs and 404 in others, and three services hand-copy SQS's private message schema with already-diverged fields. A closed loop detects changes, never errors. **Verdict: the parity question ("how do you know?") currently has no answer.** The v2 pipeline exists to answer it: probes → versioned corpora → differential replay → per-op provenance grades → behavioral changelog.

### C4. The good news the audit also surfaced

- **70–75% of pack code is a recurring skeleton** — which is precisely what makes the strangler extraction tractable: the trivial 121 packs reduce to ~30-line B-IR files, and `birx` can draft most of that mechanically.
- The genuinely hard quarter concentrates in ~a dozen files, all movable verbatim into versioned primitives — no rewrite risk.
- The SPI held. The deterministic clock/rand, the store scoping, the mutation/chaos/fuzz harness, and the real SDK round-trips (SigV4, `aws-chunked`) are exactly the substrate the engine needs. v1's spine survives the pivot intact; only the behavior layer changes representation.
- The implementer's `INTERFACE_NOTES.md` discipline worked — every frozen-interface deviation is recorded with rationale. Keep that mechanism.

### C5. Adversarial pass on the v2 plan itself

1. **CEL too weak → semantics leak into primitives until B-IR is a YAML veneer over Go.** Mitigation: wave 0 includes the hardest declarative case (SQS statechart/selector/wait) *before* schema freeze; primitive budgets with per-primitive justifications make each escape visible and expensive. Accepted: the wave-3 dozen are *designed* primitive-heavy.
2. **Statecharts tax the trivial tier.** They're optional; the CRUD floor uses none. Scoped to ~15 genuinely lifecycle-ful services.
3. **Extraction stalls at the hard dozen; two systems live forever.** The wave-3 definition ("B-IR shell + moved-verbatim primitive") is always mechanically completable, and the ratchet makes keeping both unmergeable.
4. **The equivalence gate enshrines the packs' own bugs.** It gates only the *migration*; corpus evidence gates the *truth*, with divergences following the corpus and cited by cassette hash.
5. **Probe cost/coverage risk.** Hard per-run cost caps, isolated accounts, orphan sweepers; cheap providers first (Hetzner/DO double as the neutrality proof); probe-exempt list for cost-floor services with honestly-`declared` grades. Accepted: full AWS corpus coverage is an asymptote — per-cell honest grades are the differentiator either way, because nobody else grades at all.

### What this pass changes, in one paragraph

v1 answered "can we build a wire-compatible emulator?" — yes, verifiably. It did not touch the founding question, "can we *generate* one?", and its 46k hand-written LOC actively drifted away from it. The pivot keeps everything v1 got right — the SPI, the edge, the test discipline — and changes what behavior *is*: data with provenance, executed by one engine, graded against recorded reality, ratcheted so the hand-rolling path can never quietly return. The packs' final job is to be the oracle that proves their replacement correct, and then to be deleted.

---

## Part 6 — The trend check: six days, ~1,230 commits, zero movement toward generation

Part 5 audited the implementation and produced the v2 pivot. Before that pivot was adopted, implementation continued on a separate lineage for roughly 1,230 commits. This part measures what happened, because the *direction of travel* matters more than any single snapshot.

### The two scoreboards disagree completely

| Metric | Part-5 audit | Current tip | Δ |
|---|---|---|---|
| Source LOC | 46,003 | 65,113 | **+42%** |
| Test LOC | 22,075 | 54,517 | **+147%** |
| Service packs | 152 | 154 | +2 |
| Hand-written behavior LOC | 37,230 | 55,905 | **+50%** |
| Hand-typed `case` labels | 2,433 | 2,603 | +170 |
| Inline `&spi.Fault{}` sites | 406 | 869 | **+114%** |
| Generated LOC in use | 0 | 0 | — |
| `go:generate` directives | 0 | 0 | — |
| Behavior data files | 0 | 0 | — |
| Catalog services with empty `Shapes` | all | all | — |
| Real-cloud fixtures / corpus | none | none | — |
| Proxy reachable from the binary | no | no | — |
| Packs self-declaring `TierEmulate` | 152 | 152 | — |

**As an AWS emulator, this is real progress.** The commit mix (645 `test`, 228 `docs`, 215 `feat`, 126 `fix`) is unusually test-led, much of it mutation-driven. S3 alone tripled — 1,346 → 4,459 LOC — and the additions are exactly the undocumented edge surface that parity actually consists of: POST-policy browser uploads, object lock and default retention, replication preconditions and destination validation, storage-class validation, multi-delete limits and semantics, bucket-name and object-key validation, expected-owner guards, archive-restore state. Step Functions gained a real JSONPath/JSONata path with fuzzing. This is careful work on the right *content*.

**As "a system that generates emulators," it is movement in the wrong direction.** Every generative metric is unchanged at zero while the mass to be replaced grew by half. The strangler is running in reverse: the thing scheduled for deletion is the thing being invested in.

### The three findings that change the plan

**C6. The marginal cost of parity is rising, not falling.** ~1,230 commits bought depth in roughly one service family plus event plumbing. Under a generative pipeline the same effort should buy breadth across services and providers. Extrapolated, "the same structures across eight clouds" is unreachable on this trajectory — not because the work is bad, but because each increment is priced in hand-written Go.

**C7. Duplication is compounding measurably.** Inline fault sites more than doubled (406 → 869) — the exact metric that predicted the `ResourceNotFoundException` 400-vs-404 inconsistency, now with twice the surface. Every additional site is one more cell to reconcile at extraction, and one more place for the same logical error to disagree with itself.

**C8. The main module lost its zero-dependency property.** `gojq`, `jsonata`, `parquet-go`, `snappy`, `xxhash`/`xxh3`, and `klauspost/compress` now ship in the binary, pulled in by per-pack depth (Firehose formats, Step Functions query languages). All are permissive, so no covenant is broken — but there is still no license CI gate, and this is precisely the pressure the primitive registry exists to absorb: a query-language evaluator or a Parquet writer belongs behind one versioned primitive interface, referenced from data, not imported by an individual pack.

### What this does *not* change — and one thing it improves

The pivot stands, with its rationale strengthened: 1,230 commits of evidence that "write the packs more carefully" does not converge on the stated goal. But the new work is **not** waste under the strangler plan, and is worth more than it was:

- Test LOC nearly tripled, and the test-to-source ratio rose from 0.48 to 0.84. Those tests *are* the equivalence-gate oracle. A richer oracle makes extraction safer, not harder.
- The deepened S3 semantics are the highest-value wave-3 extraction target, and they are now specified by tests rather than by memory.
- The mutation-driven discipline transfers directly: v2 replaces each Go mutant with a B-IR mutant, and the existing mutant inventory is the checklist.

**The one adjustment**: `ratchet.json` baselines must be seeded from *current* counts (2,603 case labels, 55,905 services LOC, 869 fault sites, 154 packs), not the Part-5 figures — and the ratchet needs to be among the first things built, because the six-day trend shows how quickly the baseline moves when nothing forbids it.

## Part 7 — The first reversal: one service, served from data

Parts 5 and 6 measured a strangler running backwards. This part records the point where it turned, and states plainly how small that point is.

### What now exists

| Metric | Part-6 tip | Now | Δ |
|---|---|---|---|
| Service packs | 152 | 146 | **−6** |
| Hand-typed `case` labels | 2,600 | 2,567 | **−33** |
| Hand-written behavior LOC | 55,905 | 55,350 | **−555** |
| Inline `&spi.Fault{}` sites | 868 | 859 | **−9** |
| Generated LOC in use | 0 | 149 service models, 77,656 shapes | **served** |
| Behavior data files | 0 | 7 bundles | +7 |
| Services served from data | 0 | 6 (+1 shadowed) | +6 |
| Catalog services with empty `Shapes` | all | unchanged | — |

Every counted metric moved the right way for the first time, and `ratchet.json` was rewritten downward — which is the only direction it accepts, so the counts above are now a floor that CI enforces.

### What that is worth, honestly

Six services. `aws.shield`, `aws.memorydb`, `aws.textract`, `aws.polly`, `aws.account` and `aws.iot-data` are the CRUD floor: a few resources and a handful of operations each, no lifecycle, no queue semantics, no algorithmic core. Extracting them proves the loop is closed — spec → model → bundle → engine → registry → edge, with each pack deleted and its behavior frozen in a replayed recording — and proves nothing about the ceiling on its own; `aws.sqs` does that separately. The schema is not yet frozen, because freezing it on the easiest possible service would be self-congratulation. SQS is the case that decides whether statecharts, selectors and long-poll waits belong in the schema or behind primitives, and until that lands the schema is provisional.

### Three things the extraction actually taught

**C9. The gate works, and it found something on its first run.** The bundle initially required an existing record for `DeleteProtection`; the pack deletes absent records silently. Equivalence failed, and the rule held: the bundle was changed to match the pack, and the disagreement with the real service — which very likely does return `ResourceNotFoundException` — was written down as a `quirks:` entry with `provenance: authored` for a probe run to settle. This is the two-tier oracle behaving as designed on its first real case: **equivalence gates the migration, the corpus gates the truth**, and a behavior change is never a side effect of translation.

**C10. Deleting the oracle needs the oracle preserved first.** The obvious form of the gate — record from the pack, replay against the engine — stops working the moment the pack is deleted, which is the same commit. Recordings are therefore committed artifacts (`internal/equivalence/traces/<service>.json`, `schema: trace/1`) cut from the pack in the commit that removes it. The gate then runs forever against something no longer in the tree, which is what keeps a later B-IR edit from quietly walking away from the behavior that was migrated.

**C11. Zero-dependency construction was hiding behind tolerant packs.** `SupportRows` built every pack with an empty `spi.Deps{}` to ask what operations it serves. Hand-written packs tolerate that; the engine refuses to start without a clock, a store and a source of identifiers, and refusing is correct — a service that cannot be deterministic should not answer requests. The call site was given real dependencies rather than the engine being loosened. Expect more of this: the packs' informal tolerances are load-bearing in places nobody wrote down, and each one surfaces as an extraction failure.

### The second extraction, and what it measured

`aws.memorydb` followed, and the point of doing it immediately was to find out whether the first one had been a one-off. It was not: the bundle is 40 lines of YAML, the equivalence gate passed on the first run over 15 steps, and the two engine additions it needed are both idioms rather than service semantics.

- **`list.key`** — "one record when named, the page when not," the shape almost every AWS `Describe*` has. It was the same eight lines in well over a hundred packs.
- **`arn` as a bound value** — a resource's ARN template is now something a record can name instead of concatenating. The 189 hand-built ARN strings in the tree were each free to get the partition, the region or a separator subtly wrong, and several do.
- **`list.filter`** was already in the schema, validated and never evaluated. It is now evaluated and tested. A schema field the engine ignores is worse than one that does not exist.

Neither addition is about MemoryDB. That is the signal wave 1 depends on: the second extraction cost two general-purpose engine features and no service-specific code, which is what "mechanically repeatable" has to mean if 117 more are going to follow.

**C12. Extraction is where a pack's leniency becomes visible.** The pack accepted `CreateCluster` without `ACLName`; the model marks it `@required`. Here the two-tier rule points the other way from the shield case: a `declared` trait outranks a pack's `authored` leniency, so the bundle is deliberately *stricter* than the code it replaced, the divergence is recorded as a quirk, and a test pins it so it cannot be undone by accident. Equivalence did not object because the recorded requests were valid — which is the right outcome, and worth noticing: the gate constrains the behavior that was exercised, not the behavior that was never tried. That is an argument for probe-derived corpora, not for trusting the gate further than it reaches.

### The ceiling case: what SQS proved and what it cost

Wave 0 exists to find out whether the schema can carry a hard service before 117 easy ones are written against it. SQS is that service — visibility timeouts, FIFO ordering and deduplication, dead-letter redrive, long polling — and the answer is yes, at a cost worth stating precisely.

**It fits.** `behavior/aws/sqs/service.yaml` expresses the message plane in data and replays 28 recorded steps against the pack with no divergence, plus nine tests for behavior a recording cannot reach: a visibility timeout expiring, a delayed message arriving, a shortened timeout, redrive to a dead-letter queue, FIFO group exclusivity releasing when the head is deleted, a deduplication window lapsing, a long poll returning early because time moved, batch delegation reporting a per-entry failure, and a purge that also removes messages currently in flight.

**The engine cost is general, not SQS-shaped.** Statecharts with lazily-fired deadlines, a selector with ordering and group exclusivity, long-poll waits, and the `counter` / `dedup` / `send_event` / `move` effects. Two smaller additions came from the same place: `batch:`, because every AWS batch operation has one shape and the packs implemented it once per operation (so each copy was free to drift from the singular operation it mirrored, and several did), and `without` / `merge`, because CEL comprehensions over a map yield a list — there is no core way to build a map minus some keys, which every provider's `Untag*` needs.

**The lazy-timer design pays off exactly as intended.** Nothing schedules anything: a deadline sits on the record until something observes it, and the observation fires it. That is why a twenty-second long poll costs a test nothing, why no goroutine tracks a visibility timeout, and why `TestVisibilityTimeoutExpiresLazily` can assert that a message is *not* visible one second early and *is* one second late.

**C13. The gate cannot see what the recording did not do.** Every divergence found so far was found by the recording, and the recording only constrains the sequence someone thought to write down. Time-dependent behavior in particular is invisible to it: a bundle that armed no deadline at all would replay identically, because the recorded run never advanced the clock. The nine lifecycle tests exist for that reason, and they are the shape of what corpus-derived expectations must eventually cover — a recording proves *sameness*, not *correctness*, and it proves it only along the path taken.

**C14. Proving a schema and finishing a migration are different tasks, and conflating them corrupts both.** Six of SQS's twenty-three operations are not expressed. One of them, `ListDeadLetterSourceQueues`, filters queues on a redrive policy that may live in either the queue record or its companion settings record — which needs a per-item join between a listed record and another resource. That is a schema addition, and making it on the evidence of a single operation is how a schema accumulates features that fit one caller. So the pack still serves and the bundle *shadows* it: gated on every CI run, registered nowhere. The shadow flag carries the reason rather than a boolean, and a test fails a shadow bundle that does not say what is missing, because "proven but not serving" decays into "written and unchecked" the moment nobody has to justify it. **This PR therefore does not lower the ratchet.** Wave 0 was never supposed to.

### Wave 1 begins: four packs at once, and what it cost

With the ceiling proven, four trivial packs — `textract`, `polly`, `account`, `iot-data` — were extracted in one pass to find out whether mass extraction is really mechanical. It mostly is: four bundles, four recordings, no service-specific Go, and the ratchet fell from 150 packs to 146. But it found three things that a single extraction would not have.

**C15. Packs return members no SDK can read.** Two of the four returned output members that are not in the operation's own output shape — `textract` returned `JobId` from `GetDocumentTextDetection`, `iot-data` returned `thingName` and `timestamp` from `GetThingShadow`. The engine validates projections against the shape and refuses them, which is correct: a bundle may not describe a response the protocol cannot carry. Two out of four is not a coincidence; expect this across the remaining 146.

Resolving it needed a mechanism, because the recording is what the pack did and rewriting it to whatever the bundle produces would turn the gate into a mirror. A step may now be marked `superseded` with the reason its recorded output is not matched — here, the operation's own output shape, which is `declared` evidence and outranks the pack's `authored` behavior. The step still runs, so the state behind every later step is real, and only the outcome class is compared. **This is a hole in the gate by construction**, so it is a visible one: every superseded step is logged with its reason on every run, and the count is reported next to the equivalence result. A recording that accumulates them has stopped gating much, and having to read them is the only defence.

**C16. The gate could not express read-after-create, which is the most common shape there is.** A trace holds the identifier the *reference* issued; the candidate issues a different one; feeding the recorded value back reads a record that does not exist. Every recording written so far avoided the problem by using caller-chosen names or deliberately-absent ones — which means read-after-create for any generated identifier was **entirely ungated**, and nobody would have noticed. Inputs may now name an earlier answer by step and dotted path, resolved against the candidate's own outputs, so each side is asked about the identifiers it issued itself. This is the single largest increase in what the gate covers so far, and it came from writing the fourth extraction rather than the first.

**C17. The engine was making a semantic decision the model already expresses.** Required-member validation treated an empty string as missing. Polly answers an empty `Text` with `InvalidSsmlException`; it could never do so, because the engine rejected the request first. Required now means *present*, and whether an empty value is acceptable is a length constraint the model already carries. The general form of this is worth watching for: every place the engine decides something the model or the bundle could have decided is a place services cannot differ from each other.

Cost per service, once the machinery existed: roughly forty lines of YAML and a recording. The three findings above were one-time.

### The honest scoreboard

146 of 152 services are still hand-written Go. The ratio is 3.9% migrated. What changed is not the ratio but its derivative — and the fact that the machinery which makes the next 146 mechanical (ratchet, spec pipeline, schema, loader, engine, equivalence recorder, registry integration) is now in the tree and under test, rather than described in a document.
