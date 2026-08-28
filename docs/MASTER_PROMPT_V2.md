# Master Instruction Prompt v2 — pivot mirror.cloud to generated behavior

> **Read this document, [`BEHAVIOR_IR.md`](./BEHAVIOR_IR.md), and [`PARITY_PIPELINE.md`](./PARITY_PIPELINE.md) as your complete specification.** All three are normative. Do not seek clarification from any conversation or person; where this prompt and the vendored specs disagree, the specs win and the disagreement is recorded in `docs/INTERFACE_NOTES.md`. The v1 prompt ([`MASTER_PROMPT.md`](./MASTER_PROMPT.md)) remains authoritative for everything it froze that v2 does not supersede: the SPI, the wire-protocol reference (§4.2–4.7, §4.12–4.15), identity, diagnostics, and the covenants.

---

## 0. Mission

v1 produced a working, well-tested, wire-compatible emulator — built the wrong way, and still growing that way. The verified state of this tree (measured at the current implementation tip; the parenthesized figures are the same metrics six days and ~1,230 commits earlier, so the trend is visible):

- **55,905 LOC of hand-written behavior** (was 37,230) across 154 service packs (was 152), dispatched by 2,603 hand-typed `case` labels (was 2,433). 70–75% of it is a recurring skeleton.
- **869 inline `&spi.Fault{}` construction sites** (was 406) — the duplication metric more than doubled while the design called for a single model-seeded error table.
- Generated code, `go:generate` directives, and behavior data files: **zero, unchanged**. The hand-written mass grew 50% in six days; the generative pipeline did not move.
- **The generation pipeline exists and is disconnected**: `internal/receiver → fusion → model → specdiff → mirrorgen` is clean, but `internal/generated/` is absent and gitignored, nothing imports it, and the process boots from `internal/catalog/catalog.go` — whose `Shapes` maps are **empty for all 152 services**, silently disabling model validation and schema-driven synthesis.
- `specs/mirror.set` declares 152 services; `specs/mirror.lock` pins 29 files; `scripts/specs-sync.sh` can fetch 28.
- **Nothing has ever been compared to a real cloud.** Conformance iterates a catalog authored to match the packs. All 152 packs self-declare `TierEmulate`.
- `internal/proxy` (record/replay/scrub/diff) is implemented, tested, and unreachable from the binary.

**v2's mission: invert the ratio.** Behavior becomes data (B-IR) executed by one generic engine; the 152 packs become the migration oracle and are deleted service-by-service; parity becomes a measured, evidence-graded property instead of a self-declared enum; and the whole pipeline becomes provider-neutral enough that the next cloud is a spec pin + a profile + B-IR, with **zero provider-specific Go**.

The end state, structurally enforced: **adding or changing a service never means writing Go.** It means a spec entry and behavior data — reviewed, graded, and regenerable.

### Non-goals

- Rewriting the hard interpreters (DynamoDB expressions, ASL, CloudFormation templates, IAM policy eval, the object-store core) in YAML. They move **verbatim** into versioned engine primitives. "Everything as rewrite rules" is a rejected tar pit.
- Real compute (Lambda runtimes, hypervisors), real broker protocol emulation, IAM beyond the existing evaluator.
- Probing from user machines or CI-on-PR. Probes are maintainer-side only, per `PARITY_PIPELINE.md` §2 safety rails.
- Breaking any covenant: no auth tokens, no telemetry, no phone-home, Apache-2.0, permissive deps only. The main module is **no longer zero-dependency** — it now carries `gojq`, `jsonata`, `parquet-go`, `snappy`, `xxhash`/`xxh3`, and `klauspost/compress` (all permissive, all pulled in by hand-written service depth). Add `cel-go` and a YAML parser for the engine; every dependency needs a license check in CI (§4-P1) and a line in `docs/INTERFACE_NOTES.md`. Several of these are candidates to become engine primitives rather than per-pack imports.

---

## 1. Absolute constraints (additions to v1 §1, which still applies)

1. **No new hand-written service packs, ever.** Enforced structurally (§5). Reducing scope means demoting a service's grade honestly, never writing Go for it.
2. **The engine refuses to boot a service whose `model.Service.Shapes` is empty.** This forces the spec pipeline to be connected before behavior serves — the empty-shapes catalog can never quietly become load-bearing again.
3. **CEL is pure.** No store access, no I/O, no randomness, no wall-clock — `now` is injected from `spi.Clock`; IDs come from effect `generate` specs consuming `spi.Rand`. The existing determinism lint (`internal/check`) must keep passing and must be extended to the engine.
4. **Effects are a closed vocabulary**: `create | put | patch | delete | counter | dedup | move | send_event | emit | primitive`. Adding a kind is an engine change with a test, never a per-service hack.
5. **Every behavioral cell carries provenance** (`declared | observed/recorded | observed/probed | authored`), and the grade ratchet (§5) forbids downgrades.
6. **Primitives are budgeted.** `ratchet.json` caps their count and per-primitive LOC; each ships a `JUSTIFICATION.md` naming why it cannot be B-IR.
7. **Never rewrite what you can move.** The v1 interpreters are correct-enough and mutation-tested; they migrate into `internal/engine/prims/` unchanged, and their existing tests move with them.
8. **A pack deletion requires a green equivalence gate** (§3) in the same PR. No exceptions, including "trivial" packs.

---

## 2. Frozen v2 interfaces

The v1 SPI (`internal/spi/spi.go`) is unchanged and remains frozen. New frozen surfaces — transcribe exactly; objections go to `docs/INTERFACE_NOTES.md`:

```go
// internal/bir — loaded, validated B-IR. Schema source of truth: the YAML
// grammar in docs/BEHAVIOR_IR.md §4–§5; the loader rejects anything that
// does not cross-check against the generated model.Service.
package bir

type Service struct {
    Schema     string               // "bir/1"
    ServiceID  string               // "aws.sqs"
    Provenance Provenance           // default for cells that don't override
    Resources  map[string]Resource
    Errors     map[string]ErrorDef  // ordered on marshal; keyed by logical name
    Limits     map[string]Limit
    Quirks     []Quirk
    Operations map[string]Operation
    Primitives map[string]PrimRef   // alias -> {Name, Version}
}

func Load(fsys fs.FS, svc *model.Service) (*Service, error) // validate + compile CEL

// internal/engine — the one generic pack.
package engine

func New(deps spi.Deps, ir *bir.Service, svc *model.Service) (spi.BehaviorPack, error)

// internal/engine/prims
package prims

type Func interface {
    Name() string
    Version() int
    Call(args ...any) (any, error)
}

type Behavior interface {
    Name() string
    Version() int
    Invoke(ctx context.Context, ec *EffectCtx, args map[string]any) (map[string]any, error)
}

// EffectCtx is a Behavior's ONLY state access: account/region-scoped
// Store/Blobs, Clock, Rand — with read/write sets journaled.
type EffectCtx struct { /* scoped accessors; see equivalence recorder */ }

func Register(p any) // Func or Behavior; called from init()

// internal/equivalence — the extraction gate.
package equivalence

// Record wraps spi.Deps and a Handler, capturing the (Request, Response|Fault)
// sequence produced by a pack's existing tests.
func Record(deps spi.Deps, h spi.Handler) (*Trace, spi.Deps, spi.Handler)

// Replay runs a Trace's requests against another Handler and diffs with token
// unification: the first occurrence of an unrecognized hex/uuid token binds a
// variable; later occurrences must match the binding. Structural mismatch fails.
func Replay(t *Trace, h spi.Handler) (*Diff, error)
```

**Engine evaluation order (normative)**: model validation (required members + `model.Constraints`) → resolve `reads:`/`let:` → ordered `require:` rules (first failure → error table → `spi.Fault`) → `select:` (lazy statechart timers fire first) → `wait:` (park on `Clock.After` + Bus wakeup topic published by create/move effects) → `effects:` in order, in `Collection.Txn` where single-collection, engine per-scope lock otherwise → shape-checked `output:` projection (member bindings, pagination tokens) → journal with tier and per-op grade.

**Cassette v2** extends the existing `internal/proxy` format per `PARITY_PIPELINE.md` §3 (metadata lines + normalization pass); the corpus layout and `specs/mirror.lock` pinning are normative from that document.

---

## 3. The extraction protocol (per service)

1. Generated model with non-empty shapes exists (S-SPEC output).
2. `cmd/birx` drafts B-IR from the pack's recognized idioms; residue is LLM-translated from pack source + tests. Drafts are proposals; the gate decides.
3. **Equivalence gate**, all in one CI job (`equivalence/<service>`, `make equivalence`):
   a. `equivalence.Record` over the pack's existing unit tests → `equivalence.Replay` against the engine-served B-IR — zero structural diffs. **Commit the recording**: the gate has to survive the pack it was recorded from, so in the deleting commit the pack's answers are frozen to `internal/equivalence/traces/<service>.json` (`schema: trace/1` — request, identity, and either output or the comparable part of the fault; the message is excluded because wording is not behavior) and `TestBundlesMatchRecordedPacks` replays it on every run thereafter;
   b. spine + conformance + emulate-effects + SDK + BDD suites green with `MIRROR_ENGINE_SHADOW` routing this service to the engine;
   c. every `internal/mutation` mutant touching the service replaced by a B-IR mutant (swap an error ref, drop a `require` rule, off-by-one a limit, flip a guard) that the suites kill.
4. Delete the Go pack in the same PR; ratchet baselines decrease. Do **not** invent a `bir` tier in `specs/mirror.set`: tier is the fidelity a user gets (`mock|emulate|proxy`), not how the service is implemented, and a bundled service delivers the same fidelity the pack did — `docs/SUPPORT.md` is expected to be byte-identical across an extraction, which is a useful check that nothing was lost. Which services are data-defined is already answered by the presence of `behavior/<provider>/<service>/`, and `bundled.ServiceIDs()` reads exactly that.
5. Provenance audit: every error row and limit either cites corpus evidence or is `authored`.

**Wave order.** Wave 0 (serial, schema-proving): `shield`, `memorydb`, `sqs`, and `sns` or `kms` — then **schema v1 and the engine API freeze**. `shield` and `memorydb` are done; each cost general-purpose engine additions (`list.key`, `list.filter`, `arn` as a bound value) and no service-specific Go, which is the property wave 1 depends on. `sqs` is the one that decides whether statecharts, selectors and long-poll waits belong in the schema or behind primitives, so the schema stays provisional until it lands. Wave 1 (maximally parallel): the ~117 remaining trivial packs, CRUD subset only, zero engine churn. Wave 2: the ~19 medium packs (may add `emit` routes and small primitives). Wave 3 (last): dynamodb, s3, gcs, iam, cloudformation, states, lambda — "extracted" means *B-IR shell + fat moved-verbatim primitive*: dispatch, errors, lifecycle, validation, projection in B-IR; the interpreter in `prims`. This definition is always mechanically completable.

**Two-tier oracle rule.** Equivalence-with-pack gates the migration. Corpus evidence gates the truth: where probes contradict a pack (e.g. the tree's 400-vs-404 inconsistency for `ResourceNotFoundException`), B-IR follows the corpus, the legacy expectation is updated citing the cassette hash, and the divergence lands in the behavioral changelog.

**Build-first, don't dedupe.** Do NOT refactor the 152 packs (no shared helper extraction, no `spipack` base) — they are scheduled for deletion; their idiom catalog becomes `birx`'s recognition grammar instead. The permanent shared machinery lands inside the engine: generic paginator (`model.Pagination` + `Collection.List`), `arn()` templating, central error table seeded from `model.ErrorTrait`, `core/objectstore` with declarative precondition tables, and event routing as data (SQS's B-IR defines the canonical enqueue; S3/SNS/EventBridge `emit` effects route via an ARN-pattern table — retiring the three drifted hand-copies of SQS's private message JSON).

---

## 4. Repairs to the current tree

**P0 — prerequisites, block all extraction:**
1. Reconnect spec ingestion end-to-end: derive the service→directory map from the pinned `aws/api-models-aws` tree (replacing the 28-entry hand map); extend `specs/mirror.lock` to every service in `mirror.set` or explicitly demote unspecced ones; lockfile-sha-verified fetch in CI; **commit `internal/generated/**` with real shapes**; un-skip `TestGenerateFromSpecsIdempotent`; add a byte-identical regeneration gate against the committed tree.
2. Retire catalog-empty-shapes: `internal/specboot` boots from committed `internal/generated`; `internal/catalog` shrinks to an explicit fallback list; CI: no served service may have `len(Shapes)==0`.
3. Neutrality check in `internal/check`: no service or provider names in `internal/model` or `internal/engine`.

**P1 — parallel, block "done":**
4. Wire the proxy tier: `--tier X=proxy` is currently parsed and dropped (`internal/registry/registry.go:72`); add `mirror record --fixtures`; make `mirror drift` call `proxy.Diff` instead of `bytes.Equal`.
5. CI gates missing from v1's DoD: staticcheck; license check; **`terraform apply`+`destroy` actually executed** against the booted binary (the current "terraform test" greps a markdown file); boto3 + AWS CLI v2 + `google-cloud-storage` smoke; cold-start and binary-size budgets; Docker build+smoke; full-stack same-seed byte-identical determinism test.
6. `docs/SUPPORT.md` becomes per-operation, provenance-derived grades — replacing the self-declared tier table that currently reports `emulate, Mock ops: 0` for all 152 services while its own prose concedes Polly returns empty bytes and TranslateText echoes input.
7. Mock-tier fixes (it remains the fallback): list member names from the model (not hardcoded `"Items"`), account/region-scope the CRUD map (currently a cross-tenant leak), remove the dead `p.crud[op.Name[:3]]` branch.

**P2:** per-account snapshots (`--account` currently discarded); `mirror doctor` journal top-N + git-tracked-cassette warning; `mirror spec sync|diff` wired to real implementations instead of printing strings.

---

## 5. Anti-scope-drift CI gates (non-negotiable, build these first)

The v1 implementer, given a prompt scoped to 8 emulate services + honest mock tier, hand-wrote 152 packs instead. v2 makes that path unmergeable:

1. **Ratchet — build this first.** `internal/check/ratchet_test.go` against a committed `ratchet.json` — `case "` label count in `internal/services`, `internal/services` file count and LOC, `&spi.Fault{` construction-site count, and `registry.Register` call sites may only decrease; a guard test fails if the baseline is edited upward. **Seed the baseline by measuring the tree at the moment you start** (at time of writing: 2,603 case labels, 55,905 services LOC, 869 fault sites, 154 packs) — do not copy figures from any document, including this one. The six days before this prompt added ~19k LOC of hand-written behavior and doubled the fault-site count; nothing structural forbade it. This gate is what makes the pivot real, so it lands before extraction, not after.
2. **No new packs**: any new directory under `internal/services/` fails CI. New services enter only as a `specs/` entry + `behavior/` YAML; a test diffs `mirror.set` tiers against the filesystem.
3. **Primitive budget**: plugin count and per-plugin LOC caps in `ratchet.json`; missing `JUSTIFICATION.md` fails.
4. **Grade ratchet**: SUPPORT.md grades may only improve.
5. **Shapes-non-empty** and **neutrality** checks (§4-P0).
6. **Equivalence job required**: a PR deleting a pack without a green `equivalence/<service>` job cannot merge.

---

## 6. Swarms

No phases. Every swarm codes from turn one against the frozen artifacts in §2 and the two normative docs. Only declared interface dependencies order anything.

| Swarm | Produces | Success criteria |
|---|---|---|
| **S-SPEC** | P0 items 1–2: reconnected ingestion, committed `internal/generated` with shapes, regen byte-identity CI | All served services have non-empty shapes; from-specs idempotency test runs (no skip) and passes |
| **S-BIR** | `internal/bir` loader/validator, `internal/behaviors` embed, golden fixtures (shield + sqs from `BEHAVIOR_IR.md`) | Fixtures round-trip; 20 seeded-invalid fixtures rejected with useful errors; CEL compile errors are load-time |
| **S-ENG** | `internal/engine/**` (celenv, effects, statechart, selector, project, paginate, prims) | shield+memorydb+sqs+sns served from B-IR pass spine/conformance/emulate-effects/SDK suites; long-poll works under controllable clock; `Bus.Subscribe` used |
| **S-XTOOL** | `cmd/birx`, `internal/equivalence`, B-IR mutation operators, `ratchet.json` + guard tests | Replay gate catches 10 seeded divergences; ratchets wired and red-then-green demonstrated |
| **S-XW1/2/3** | Extraction waves per §3 | Per-service equivalence gates green; packs deleted; ratchet counters strictly decrease |
| **S-OAPI** | `internal/receiver/openapi`, generic `internal/proto/httpjson` codec, `behavior/digitalocean/` + `behavior/hetzner/` profiles + core-resource B-IR | DO Droplet and Hetzner Server CRUD served with **zero provider Go**; real HTTP round-trips against both |
| **S-PROBE** | `cmd/mirrorprobe` (own module) per `PARITY_PIPELINE.md` §2 | Dry-run plans for 10 services; live run against 3 cheap services yields replayable corpus; cost cap + sweeper demonstrated |
| **S-CORPUS** | `cmd/corpusdiff`, `cmd/birmine`, `internal/differential`, grade ratchet, `docs/behavior-changes/` generator | Seeded corpus delta → changelog entry with cassette citations; differential replay green on wave-0 services |
| **S-VERIFY** | §4-P1 CI gates + §5 anti-drift gates | Every gate demonstrably red-then-green |
| **S-DOCS** | SUPPORT.md per-op grades, DAY2/EXTENDING rewrites ("add a service = spec + B-IR"), doctor/spec/drift wiring | SUPPORT.md generated + CI-asserted; every documented command executes |

## 7. Definition of Done

1. All v1 DoD items that remain applicable stay green (build/vet/gofmt/tests, SDK round-trips, isolation, snapshots, determinism, fidelity labeling, `--strict`).
2. Wave 0 complete: shield, memorydb, sqs, sns/kms served from B-IR by the engine; their Go packs deleted; equivalence gates green.
3. Waves 1–3 complete, or honestly reduced by *demoting services to mock tier and deleting their packs* — never by keeping hand-written Go. `internal/services` contains zero service packs at full completion.
4. `internal/generated` committed with non-empty shapes for every served service; regeneration byte-identical in CI.
5. DO + Hetzner core resources served with zero provider Go, via OpenAPI receiver + profiles + B-IR.
6. Probe harness demonstrated end-to-end on ≥3 cheap services; corpus committed and pinned; `internal/differential` green against it; at least one generated behavioral-changelog entry exists (seeded in test if no real drift occurred).
7. SUPPORT.md reports per-operation provenance grades; grade ratchet active.
8. All §5 gates active and demonstrated red-then-green.
9. Terraform apply/destroy, boto3, AWS CLI v2, google-cloud-storage, staticcheck, license, budgets, Docker smoke — all in CI and green.
10. `docs/INTERFACE_NOTES.md` records every deviation with rationale.

## 8. Coordination rules

1. Frozen means frozen: §2 signatures and the two normative docs. Objections are notes, not code.
2. The engine never imports a service pack, a codec, or the edge; packs (while they exist) never import the engine. CI-enforced.
3. **Degradation policy — protect the spine.** If work must shrink, the order of sacrifice is: wave 3 breadth → wave 2 → wave 1 breadth (demote + delete, never keep Go) → probe breadth. Never sacrificed: S-SPEC, wave 0, the equivalence tooling, the anti-drift gates. An engine that provably serves four services from data, with the gates that keep it that way, is a success; 152 packs with a YAML veneer is a failure regardless of coverage.
4. Every swarm ships its tests with its code; suites stay green continuously.
5. No wall-clock sleeps in tests; the controllable clock drives all timer behavior.
6. Commit messages and PRs describe behavior deltas, not code motion; a pack deletion PR names its equivalence run.
