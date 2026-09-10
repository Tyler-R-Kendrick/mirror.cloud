# The Parity Pipeline — probing, corpora, and the behavioral changelog

> How mirror.cloud acquires ground truth about real cloud behavior, keeps it versioned, detects when vendors change things they never announced, and feeds the evidence back into the [Behavior IR](./BEHAVIOR_IR.md). This is the answer to the question every emulator dodges: *how do you know it's accurate?*

## 1. The problem this solves

Parity does not live in the happy path. It lives in the edge cases: which error wins when a request violates two rules at once; whether a name one character over the limit gets a 400 or a silent truncation; what a delete-then-get returns during the deletion window; which fields a real response carries that the spec never mentions. **None of this is documented, and all of it is what developers' error-handling code actually depends on.**

The v1 tree proved the failure mode: every test expectation was written from the implementer's beliefs about AWS, then the implementation was tested against those beliefs — a closed loop that can detect changes but never errors. Spec conformance doesn't break the loop either: it proves the *envelope* is right and says nothing about behavior (the closest competitor's "100% Smithy conformance" has exactly this ceiling).

The pipeline breaks the loop with recorded reality, acquired three ways:

| Evidence class | Source | What it's good for |
|---|---|---|
| `observed/recorded` | Proxy-captured traffic from real workloads | The paths people actually exercise |
| `observed/probed` | Generated probe suites run against real clouds | Systematic edge/error/limit coverage — the undocumented layer |
| `declared` | Vendored provider specs | Surface completeness, shapes, declared errors |

`authored` (a human or LLM wrote it down) is the honest floor, never presented as more.

## 2. The probe generator — `cmd/mirrorprobe`

Maintainer-side only, its own Go module (the main module stays zero-dependency), never run in user tests or PR CI.

**Inputs**: generated models (shapes, constraints, declared errors); a **producer–consumer graph** inferred RESTler-style — the output member of `Create*` feeds the same-named/typed input member of `Describe*`/`Delete*`; ARN-typed members matched by service — and, once a service has B-IR, its provenance map, so probing targets exactly the cells whose evidence is weakest.

**Probe classes**, each a generator over the graph:

- **chain** — minimal producer chains to materialize prerequisites (doubles as happy-path recording);
- **boundary** — `Constraints` min/max/pattern, exactly at and one past;
- **negative** — missing required member, invalid enum, nonexistent resource, malformed ARN;
- **ordering** — delete-then-get, double-create, use-while-deleting, wrong-state transitions;
- **error-precedence** — multiple violations in one request: which error wins. No spec documents this; every SDK retry policy depends on it;
- **limits** — batch sizes, page caps, name lengths, idempotency-token replay.

**Safety rails, non-negotiable and built into the harness**: a dedicated isolated account per provider; a permission boundary allowing only the target services; every resource named `mirrorprobe-<runid>-…`; a probe-side cost model with a **hard abort cap** per run plus provider-level budget alarms; a teardown pass and an orphan sweeper (anything with the prefix older than 24h is deleted at run start); scrubbing at write time via the existing `internal/proxy/scrub.go`. Services with unavoidable cost floors (Shield Advanced subscriptions, provisioned RDS/Redshift) are **probe-exempt**: their cells honestly stay `declared|authored` and the support matrix says so. Providers are sequenced by cost — Hetzner and DigitalOcean are near-free and double as the cross-provider proof; AWS free-tier-heavy services go first.

## 3. Corpora — versioned evidence

Recordings use **cassette v2**, an extension of the existing `internal/proxy` format (sorted records, atomic writes, write-time scrubbing): each record gains metadata (`probe-class:`, `service:`, `operation:`, `spec-sha:`, `session:`, `chain:`) and passes a **normalization pass** before write — account IDs, request IDs, timestamps, and probe-run prefixes become placeholders — so a re-probe of unchanged behavior produces a byte-identical cassette and a changed behavior produces a clean diff.

```
corpus/<provider>/<service>/<class>/<sha256-of-normalized-request>.cassette
corpus/index.json          # content-addressed manifest
```

The corpus is pinned by hash in `specs/mirror.lock` — recorded behavior is versioned evidence under exactly the same discipline as vendored specs. An emulator build therefore names both the spec snapshot and the behavior snapshot it claims parity with.

## 4. Change detection — the behavioral changelog

Two diff engines run on the same cadence:

- `internal/specdiff` (exists) — the **declared** layer: operations/shapes added, removed, changed.
- `cmd/corpusdiff` (new) — the **observed** layer: per-(service, operation, class) deltas in status, error code, error body, response shape between two corpus manifests.

The cross-reference is the product: **a corpus delta with no corresponding spec delta is an unannounced behavioral change.** It is emitted, mechanically, to `docs/behavior-changes/<provider>/<yyyy-mm>.md` with cassette hashes as citations — the "vendor changed something they never documented" record that exists nowhere else in the ecosystem. Emulator profiles can pin a snapshot (`--behavior-as-of`), so a team can hold last month's semantics while migrating.

## 5. Feeding evidence back into B-IR

- `cmd/birmine` mines corpora into B-IR **mechanically for the closed vocabulary only**: error-table rows (code/status/fault per trigger), limits, precedence orderings — emitted as reviewable PRs with `provenance: probed` and cassette references. Anything touching effects or statecharts is human/LLM-proposed and machine-gated, never machine-merged.
- `internal/differential` replays corpus request sequences against the engine and diffs normalized responses — the accuracy oracle that substantiates any fidelity claim.
- **The grade ratchet**: `docs/SUPPORT.md` is generated from per-cell provenance; a CI test compares against the committed baseline and fails any merge that lowers a grade. Parity claims can only strengthen.
- When evidence contradicts legacy behavior (v1 packs disagree with the real cloud — e.g. the tree's inconsistent 400-vs-404 for `ResourceNotFoundException`), **B-IR follows the corpus**, the legacy test expectation is updated citing the cassette hash, and the divergence is logged. Equivalence with the old pack gates the *migration*; the corpus gates the *truth*.

## 6. Borrowed oracles

Existing community conformance corpora are graded, not vendored: an adapter runs Ceph `s3-tests` (the de-facto S3 behavior suite, which itself flags where AWS doesn't enforce its own spec) against mirror and records the scoreboard alongside our own grades. Where a borrowed suite and our corpus disagree, the corpus wins and the disagreement is investigated — borrowed suites encode their authors' beliefs too.

## 7. What this yields, concretely

1. **An accuracy claim no competitor makes**: per-operation grades derived from recorded reality, not self-declared tiers.
2. **Maintenance economics inverted**: a behavior author no longer needs to *know* a service's semantics — they need a recording that adjudicates them. Recording is cheap; knowing is expensive. This is the only configuration in which a small team covers multiple clouds.
3. **Day-2 as a feature**: scheduled re-probe + corpus diff turns vendor drift into a reviewable artifact instead of a slowly-rotting emulator.
4. **The behavioral changelog** as a public good — useful even to people who never run the emulator, which is its own distribution channel.
