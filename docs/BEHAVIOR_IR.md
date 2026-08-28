# Behavior IR (B-IR) — behavior as data

> The v2 architecture: service behavior expressed as versioned, diffable, provenance-tagged data, executed by one generic engine — replacing hand-written Go behavior packs. The normative execution contract for the implementing agent is [`MASTER_PROMPT_V2.md`](./MASTER_PROMPT_V2.md); this document is the schema's reference and rationale.

## 1. Why behavior must be data

The v1 tree hand-writes 100% of service behavior: 152 packs, 37,230 LOC, 2,433 hand-typed `case "Operation":` labels — while the spec-ingestion pipeline sits disconnected and the booted model carries empty shape maps. That shape of codebase is the LocalStack maintenance treadmill: every provider API change is a hand edit, every new service is a hand-written pack, and every error status is re-decided inline (the tree renders `ResourceNotFoundException` as HTTP 400 in most packs and 404 in others, because 406 separate `&spi.Fault{}` call sites each guessed).

Measured against the same tree, **70–75% of pack code is a recurring skeleton** — identical `col()`/`first()`/`listWrap()` helpers, 189 hand-concatenated ARNs, 121 sub-250-LOC CRUD packs that differ only in noun — and the genuinely service-unique quarter concentrates in about a dozen files. That distribution is the argument: the skeleton belongs in data executed by one engine; the dozen hard cores belong in named, versioned primitives referenced *from* data.

Three properties fall out of behavior-as-data, and each is unreachable from hand-written Go:

1. **Regenerability.** When a provider changes, the delta lands as a reviewable data diff — not an archaeology session across `switch` statements.
2. **Provenance.** Every behavioral cell (an error status, a limit, a state transition) carries where it came from: `declared` (spec), `observed` (recorded traffic or probe), or `authored` (a human/LLM guess). Parity claims become auditable instead of self-declared.
3. **Provider neutrality.** The same schema and engine serve an AWS queue, a GCS bucket, and a DigitalOcean droplet; per-provider vocabulary lives in the data and the codecs, never in the engine.

## 2. Formalisms: adopt, never invent

- **Statecharts** with SCXML/STATEMATE-class semantics for resource lifecycles (prior art: SCXML, XState, Sismic). Serialized as data; executed by one interpreter; timers are stored deadlines evaluated lazily at observation points on the owned `spi.Clock` — deterministic under the controllable clock, no background goroutines.
- **CEL** (Common Expression Language, `cel-go`) for every expression: guards, preconditions, projections, derivations. CEL is pure and non-Turing-complete by design. **CEL never touches the store, the clock beyond the injected `now`, or randomness** — reads are resolved by the engine before evaluation, and IDs come from effect `generate` specs consuming `spi.Rand`. This keeps the determinism lint honest and every rule statically analyzable.
- **A closed, typed effect vocabulary**: `create | put | patch | delete | counter | dedup | move | send_event | emit | primitive`. New effect kinds require an engine change — deliberately, so semantic pressure surfaces as a reviewed engine PR rather than silent DSL sprawl.
- **Explicitly rejected**: K-framework-style "all semantics as rewrite rules." Full executable-semantics frameworks are a research tar pit; the escape hatch for genuinely algorithmic behavior is the primitive registry (§6), which is budget-gated so it cannot quietly become the main path.

## 3. Layout

```
behavior/
  <provider>/profile.yaml            # provider profile: identity scheme, error envelope,
                                     #   endpoint/addressing conventions, pagination defaults
  <provider>/<service>/service.yaml  # header, resources, error table, limits, quirks
  <provider>/<service>/ops/*.yaml    # operation rules (small services: one file)
```

Embedded via `go:embed` in `internal/behaviors`; loaded and validated by `internal/bir`. **B-IR never redefines wire shapes.** Every output member, error shape, and pagination token must resolve against the generated `model.Service` from vendored specs — the loader fails otherwise, and the engine refuses to boot a service whose model has empty `Shapes`. This is a structural forcing function: the spec pipeline must be connected before any behavior serves.

## 4. Worked example — the CRUD floor

The 121 trivial control-plane packs reduce to this. `behavior/aws/shield/service.yaml`, replacing the 103-line Go pack:

```yaml
schema: bir/1
service: aws.shield
provenance: authored          # wave-0 default; probing upgrades individual cells

resources:
  protection:
    collection: shprot
    id:
      generate: { kind: hex, bytes: 8 }        # deps.Rand.Hex(8)
      input_members: [ProtectionId, Id]        # how callers name it on reads/deletes
    arn: "arn:{partition}:shield::{account}:protection/{id}"
    record:
      Id: id
      Name: input.Name
      ResourceArn: input.ResourceArn
  subscription:
    collection: shsub
    singleton: state

errors:
  NotFound:
    code: ResourceNotFoundException
    http: 400
    fault: client
    provenance: authored      # v1 pack said 400; a probe will confirm or correct → probed

operations:
  CreateProtection:
    effects: [ { create: { resource: protection } } ]
    output: { ProtectionId: id }
  DescribeProtection:
    reads: { rec: { resource: protection, key: id } }
    require:
      - { cond: rec_found, error: NotFound }
    output: { Protection: rec }
  ListProtections:
    list: { resource: protection, member: Protections, paginate: model }
  DeleteProtection:
    effects: [ { delete: { resource: protection, key: id } } ]
  CreateSubscription:
    effects: [ { put: { resource: subscription, record: { SubscriptionState: "'ACTIVE'" } } } ]
  GetSubscriptionState:
    reads: { rec: { resource: subscription } }
    output: { SubscriptionState: "rec_found ? rec.SubscriptionState : 'INACTIVE'" }
```

### `list` in full

```yaml
list:
  resource: cluster       # which resource to enumerate
  member: Clusters        # output member, checked against the output shape
  paginate: model         # bind tokens and page caps from the pagination trait
  key: "'ClusterName' in input ? string(input.ClusterName) : ''"
  filter: "item.Engine == 'valkey'"
```

`key` is the **describe-one-or-all** shape that almost every AWS `Describe*` has: a non-empty key narrows the answer to that one record, an empty key returns the page. A named record that does not exist yields an *empty list*, not a fault — an operation that must fault says so with `reads` plus `require`, and the difference is a per-service decision stated in the bundle. This is in the engine because it was the same eight lines in well over a hundred hand-written packs.

`filter` is a predicate over each candidate record, bound as `item`.

### Values an expression can name

Beyond `input`, `identity` and `now`, the bindings depend on where the expression sits, and the loader rejects anything out of scope:

| binding | where | what it is |
|---|---|---|
| `id` | anywhere | the resolved record key |
| `arn` | a resource with an `arn:` template | that template, expanded |
| `rec` | a read binding named `rec`; any operation with a write effect | the record read, or the record just written |
| `<name>`, `<name>_found` | operations with `reads:` | each read binding and whether it was there |
| `item` | `list.filter` | one candidate record |
| `event` | statechart transitions | the triggering event |
| `fx` | operations with effects | earlier effect results, by name |

`arn` deserves the emphasis: a resource's ARN template becomes a value the record names, rather than five string pieces concatenated at the call site. The tree it replaces has 189 hand-built ARN strings, each free to get the partition, the region or a separator subtly wrong — and several do.

Two details do real work. `list.paginate: model` binds tokens and page caps from `model.Pagination` + `spi.Collection.List(prefix, after, limit)` — which retrofits pagination onto the 66 list operations that v1 serves unbounded. And `member: Protections` is validated against the generated output shape, eliminating the entire class of bug where a synthesizer invents a member name (`"Items"`) that no SDK can read. Every read binding `x` also binds `x_found: bool`.

## 5. Worked example — the hard-case ceiling

SQS messages are the stress test: visibility timeouts, DLQ redrive, FIFO ordering and deduplication, long polling. `behavior/aws/sqs/ops/messages.yaml` (abridged to the load-bearing parts):

```yaml
schema: bir/1
service: aws.sqs

primitives:
  attr_md5: { name: aws.sqs/attr-md5, version: 1 }

resources:
  queue:
    collection: queues
    id:
      input_members: [QueueName]
      derive: "input.QueueName != '' ? input.QueueName : lastSegment(input.QueueUrl, '/')"
    views:                     # derived, cached CEL over the stored record
      fifo: "id.endsWith('.fifo')"
      redrive: "has(rec.attrs.RedrivePolicy) ? parseJSON(rec.attrs.RedrivePolicy) : null"
      visTimeout: "int(coalesce(rec.attrs.VisibilityTimeout, '30'))"

  message:
    collection: "msgs:{queue.id}"        # parent-scoped collection
    parent: queue
    id: { generate: { kind: hex, bytes: 16 } }
    key: handle
    statechart:
      initial: visible
      states:
        visible:
          on:
            RECEIVE:
              - guard: "queue.redrive != null && rec.receiveCount + 1 > int(queue.redrive.maxReceiveCount)"
                target: redriven
                actions:
                  - move:
                      to: { resource: message, queue: "queueFromArn(queue.redrive.deadLetterTargetArn)" }
                      set: { handle: { generate: {kind: hex, bytes: 64} },
                             origin: "coalesce(rec.origin, queue.id)",
                             receiveCount: rec.receiveCount }
                      state: visible
              - target: invisible
                actions:
                  - set: { receiveCount: "rec.receiveCount + 1" }
                  - deadline: { name: reappear, after: "seconds(event.visibilityTimeout)" }
            DELETE: { target: deleted }
        invisible:
          timers:
            - { deadline: reappear, target: visible }   # lazy: fires at next observation
          on:
            DELETE: { target: deleted }
            CHANGE_VISIBILITY:
              - target: invisible
                actions: [ { deadline: { name: reappear, after: "seconds(event.timeout)" } } ]
        redriven: { final: true }
        deleted:  { final: true }

operations:
  SendMessage:
    reads: { q: { resource: queue } }
    require:
      - { cond: q_found, error: QueueDoesNotExist }
      - { cond: "!q.fifo || input.MessageGroupId != ''", error: MissingParameter,
          message: "MessageGroupId required for FIFO queues" }
    let:
      dedupId: >
        coalesce(input.MessageDeduplicationId,
          q.fifo && q.rec.attrs.ContentBasedDeduplication == 'true' ? md5hex(input.MessageBody) : '')
    effects:
      - dedup:
          when: "dedupId != ''"
          table: "dedup:{queue.id}"
          key: dedupId
          ttl: 5m
          on_hit: { output: { MessageId: hit.id, MD5OfMessageBody: hit.md5 } }
      - create:
          resource: message
          record:
            body: input.MessageBody
            md5: md5hex(input.MessageBody)
            group: input.MessageGroupId
            attrs: input.MessageAttributes
            seq: { counter: "queues/{queue.id}/seq" }
            receiveCount: 0
            handle: { generate: { kind: hex, bytes: 64 } }
    output:
      MessageId: id
      MD5OfMessageBody: md5hex(input.MessageBody)
      MD5OfMessageAttributes: "has(input.MessageAttributes) ? prim.attr_md5(input.MessageAttributes) : null"
      SequenceNumber: "q.fifo ? string(fx.create.seq) : null"

  ReceiveMessage:
    reads: { q: { resource: queue } }
    require: [ { cond: q_found, error: QueueDoesNotExist } ]
    select:
      binding: msgs
      resource: message
      state: visible                # observation point: lazy timers fire here
      order_by: seq
      limit: "clamp(int(coalesce(input.MaxNumberOfMessages, 1)), 1, 10)"
      group:
        when: q.fifo
        by: group
        exclusive_in_flight: invisible   # skip groups with any member in 'invisible'
    wait:                           # long-poll: engine capability, not service code
      until: "msgs.size() > 0"
      timeout: "seconds(int(coalesce(input.WaitTimeSeconds, 0)))"
      on_timeout: { output: { Messages: [] } }
    effects:
      - send_event:
          foreach: msgs
          event: RECEIVE
          context: { visibilityTimeout: "int(coalesce(input.VisibilityTimeout, q.visTimeout))" }
    output:
      Messages: |
        msgs.map(m, {
          'MessageId': m.id, 'ReceiptHandle': m.handle, 'Body': m.body, 'MD5OfBody': m.md5,
          'Attributes': {'ApproximateReceiveCount': string(m.receiveCount)},
          'MessageAttributes': filterAttrs(m.attrs, input.MessageAttributeNames)
        })

  DeleteMessage:
    reads: { q: { resource: queue } }
    effects:
      - send_event: { resource: message, key: input.ReceiptHandle, event: DELETE, missing: ignore }
```

This transcribes the v1 pack's semantics faithfully (`internal/services/aws/sqs/sqs.go:208–348`) — including the redrive-checked-at-receive quirk and the 5-minute dedup window — and corrects it only where corpus evidence says otherwise.

What in SQS genuinely cannot be data, and where it goes instead:

1. **`aws.sqs/attr-md5`** — AWS's undocumented binary encoding for message-attribute MD5s. A ~40-LOC pure-function primitive.
2. **Long-poll `wait`** — a *generic engine capability*: park on `spi.Clock.After` plus a Bus wakeup topic that create/move effects publish to. This gives `spi.Bus.Subscribe` its first caller in the codebase and works under the controllable clock.
3. **FIFO exclusive-group selection** — the spec is data; the selector executor is generic engine code.
4. Real receipt-handle *content* encodes metadata; ours stays opaque-random — recorded as a quirk annotation, not code.

### What the ceiling case settled

The worked example above is the design. What the implementation settled, and where it differs:

- **A read binding *is* the record**, with the resource's `views` merged onto it. `q.fifo` is a view; `q.attrs` is a stored member. There is no `q.rec` wrapper. Views are recomputed on read and never persisted, and the loader rejects a view whose name collides with a stored member.
- **A record with a lifecycle carries `__state` and `__deadlines`.** Keeping them on the record makes lifecycle atomic with the data; the cost is two reserved names, which the loader enforces. Deadlines are absolute instants, so nothing counts them down.
- **`let` bindings resolve by dependency**, not by name order — YAML mapping order does not survive into a Go map, and an SQS send derives `dedupId` from `settings`, which sorts before it.
- **`batch:`** delegates an operation to a sibling once per entry. Every AWS batch operation has one shape, and the packs implemented it once per operation, so each copy could drift from the singular operation it mirrored.
- **`without(map, keys)` and `merge(a, b)`** are engine functions because CEL comprehensions over a map yield a list: there is no core way to build a map minus some keys, which every provider's `Untag*` needs.
- **`endpoint`** is the base URL the caller reached. A service that hands back URLs to itself has to echo it, or the client follows a link somewhere it cannot reach.
- **`shadow:`** marks a bundle proven but not yet serving: gated by the equivalence suite on every run, registered nowhere, with the pack still answering. Its value is the reason it is not serving, not a boolean — a shadow bundle that does not say what is missing is how a half-migration becomes permanent, so a test requires one.

## 6. Primitives — the budgeted escape hatch

```go
// Pure computation, exposed to CEL as prim.<alias>(...). No state access.
type Func interface {
    Name() string     // "aws.sqs/attr-md5", "aws.s3/etag", "token/base64json"
    Version() int
    Call(args ...any) (any, error)
}

// Stateful algorithmic semantics invoked as an effect or projection step.
// EffectCtx is the ONLY state access: scoped Store/Blobs/Clock/Rand,
// with read/write sets journaled for equivalence testing.
type Behavior interface {
    Name() string     // "aws.ddb/expr", "aws.iam/policy-eval", "core/objectstore"
    Version() int
    Invoke(ctx context.Context, ec *EffectCtx, args map[string]any) (map[string]any, error)
}
```

Initial roster (each moved verbatim from v1, not rewritten): `aws.ddb/expr` (the DynamoDB expression evaluator), `aws.iam/policy-eval` (the Deny-then-Allow authorizer core), `core/objectstore` (the S3/GCS shared object core, with per-service declarative precondition tables — `If-Match`→412, `ifGenerationMatch`→412), `aws.s3/etag` (including multipart `-N`), `aws.sqs/attr-md5`, `token/*` codecs, `aws.states/asl`, `aws.cfn/template`.

**The budget is the point.** `ratchet.json` caps primitive count and per-primitive LOC; every primitive carries a `JUSTIFICATION.md` naming why it cannot be B-IR. Without the budget, the primitive registry quietly becomes "hand-written packs with extra steps" — the K tar pit through the back door.

## 7. Provenance and grading

Every error-table row, limit, transition, and quirk carries provenance:

```
verified   > observed(recorded | probed) > declared > authored
```

- `declared` — stated by the vendored spec (`model.ErrorTrait`, `Constraints`, `Pagination`).
- `observed/recorded` — seen in captured real traffic (proxy cassettes).
- `observed/probed` — established by a maintainer-side probe run against the real cloud, cited by cassette hash.
- `authored` — a human or LLM wrote it down; the honest name for "we believe."

`docs/SUPPORT.md` becomes a **per-operation grade table generated from provenance** — replacing v1's self-declared `TierEmulate` on all 152 services, which measured nothing. A grade-ratchet CI test forbids merges that lower any grade. How corpora are captured, versioned, diffed into a behavioral changelog, and mined back into B-IR is specified in [`PARITY_PIPELINE.md`](./PARITY_PIPELINE.md).

## 8. Extraction from the v1 packs (strangler)

The 152 hand-written packs are not waste — they are the **migration oracle**:

1. `cmd/birx` mechanically drafts B-IR from the pack idioms (the CRUD skeleton, inline faults, `Rand.Hex(n)` id-gen, ARN concatenations) — ~90% complete for the 121 trivial packs. The remainder and all medium/hard services are LLM-translated from pack source + tests, **gated, never trusted**.
2. The **equivalence gate** must pass before a pack is deleted: trace-replay of the pack's recorded (Request, Response) sequences against the engine (with token unification for divergent `Rand` consumption order), the full suite matrix re-run in engine mode (`MIRROR_ENGINE_SHADOW`), and mutation retargeting (B-IR mutants replace the deleted Go mutants).

   The recording outlives the pack, which is what makes the gate permanent rather than a one-time ceremony. In the same commit that deletes a pack, its answers are frozen into `internal/equivalence/traces/<service>.json` (`schema: trace/1`) — request, identity, and either the output or the comparable part of the fault, with the message excluded because wording is not behavior. `TestBundlesMatchRecordedPacks` replays every recording against the bundled engine on every run, so a later edit to the B-IR that changes behavior fails against the pack that is no longer there to argue. Nil, `{}` and `[]` compare equal: a recording round-trips through JSON, and no codec distinguishes them.

   A recording is evidence about a *pack*, not about the real service — its `source` and `note` say so. When probed evidence contradicts it, the recording is re-cut deliberately with the cassette cited; the gate exists to make that a visible decision rather than a silent drift.
3. Wave order — **hard-first spike, easy-first mass**: wave 0 proves the schema ceiling (shield + memorydb + sqs + sns/kms) and then **freezes schema v1**; wave 1 mass-extracts the ~117 trivial packs in parallel with zero engine churn; wave 2 the ~19 medium; wave 3 the hard dozen, where "extracted" is defined as *B-IR shell + fat moved-verbatim primitive* — always mechanically completable, never "DynamoDB in YAML."
4. When probed evidence contradicts a pack, **B-IR follows the corpus**, the legacy expectation is updated citing the cassette hash, and the divergence lands in the behavioral changelog. Equivalence gates the migration; the corpus gates the truth.

## 9. Provider neutrality across the eight targets

All eight target providers publish machine-readable surface specs: AWS (Smithy, `aws/api-models-aws`), Azure (TypeSpec/OpenAPI, `Azure/azure-rest-api-specs`), GCP (Discovery), and — plain OpenAPI — Cloudflare (`cloudflare/api-schemas`), DigitalOcean (`digitalocean/openapi`), Vercel (`openapi.vercel.sh`), Hostinger (`hostinger/api`), Hetzner (`hcloud-openapi`). **One OpenAPI receiver covers six of eight.**

The long-tail providers are control-plane CRUD/lifecycle APIs (droplets, servers, DNS records, deployments) — precisely the B-IR floor tier, which is why DigitalOcean and Hetzner are the next neutrality proof: Droplet and Server CRUD served **with zero provider-specific Go**, from OpenAPI ingest + a provider profile + B-IR. Per-provider vocabulary (auth recognition, error envelope, pagination convention, self-referencing URL rules) lives in `behavior/<provider>/profile.yaml` and the codec layer; the engine contains no provider names, CI-enforced.
