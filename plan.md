# Program state: Azure parity done; Vercel emulate-oracle parity done (52/52 + differential); ratchet burn-down at the wave-3 wall

Export/import artifact for cross-session continuation.

## Landed (squash-merged to main)

- **#358** `9b9b9a78`: Azure Storage full parity ledger (806/806 Azurite trace
  rows, 59/59+11/11+12/12 swagger paths, B-IR extraction of azure.blobs/
  queue/table, SharedKey/SAS, CORS, batch).
- **#387** `1f8076ed`: last demux guess deleted (`demux_guesses` 0).
- **#388** `2353766d`: B-IR wave A — 12 packs extracted (swf, xray, cloudfront,
  elasticache, cognito-idp, monitoring, autoscaling, acm, logs, ecr, tagging),
  4 shadowed with named gaps (timestream, opensearch, s3tables, appsync).
- **#389** `b7236d3b`: vercel-labs/emulate pinned as the Vercel parity oracle
  (52 routes, 26 tests census; `TestVercelCensusDenominators`).
- **#391** `a0d25502`: vercel.api widened (teams get/patch/invite, deployment
  events/files/aliases/cancel, uploads, promote aliases, protection bypass;
  narrowed model 26→92; restjson header-member decode).
- **#393** `0007c84f`: B-IR wave B — eks (70 ops) + route53 (71) extracted,
  athena gated as shadow (StartQueryExecution needs the wave-3 primitive
  registry: cross-service reads of glue/s3 + DDL against s3tables).
- **#392** `52736434`: Vercel full emulate-oracle parity — 52/52 oracle
  routes served (vercel.blob data plane; auth supplement fused via the
  receiver's new `x-mirror-service`; OAuth flow incl. PKCE; two
  oracle-caught behavior fixes: cancel refuses READY, domains unverified
  until verified; differential corpus + capture script + replay test).
- **#394/#396/#397/#398/#399/#400/#402/#403**: wave C shadow bundles —
  apigateway (124 ops; ExecuteApi needs sibling-invoke+stream), kafka
  (firehose's Go-only Publish/Messages), scheduler (clock-driven worker),
  kinesis (bus emit + iterators), kms (crypto core), cloudformation
  (template engine + cross-service provisioning), redshift (COPY data
  plane), iam (NewAuthorizer + policy engine + cross-scope SCP reads).
  Each bundle is complete, gated by its recording, and names its takeover
  condition in `shadow:`.
- **#401** `949133d7`: secretsmanager fully extracted (pack deleted; 3
  needles retargeted to engine mutants, 2 deleted with reasons; engine
  `rollup` CEL helper; firehose/conformance/spine callers rerouted to
  bundled.Handler).

Nothing is open.
- **#395** `77fba2e7`: mutation suite sharded 8 ways (was timing out at 30m
  per shard on main) + bundled ServiceIDs/ShadowIDs cached (sync.OnceValues).

## Ratchet burn-down state

Baseline (pre-drift, only falls): packs 47, loc 45537, faults 754,
cases 1734, files 82. After waves A+B+C: **packs 33, loc 45834 (+297),
faults 896 (+142), cases 1465** — vs +5303/+188 at the start of this
program. Everything still counting is wave-3-blocked or owner-active:

- sqs (2027 LOC / 51 faults): unshadow needs resource-declared addressing
  members in input validation, then 153 needle retargets + 25 callers.
- Shadowed packs (timestream, opensearch, s3tables, appsync, kafka,
  scheduler, kinesis, kms, cloudformation, redshift, iam, apigateway):
  each bundle's `shadow:` names its takeover condition; all funnel into
  the wave-3 unlocks below.
- organizations/cloudcontrol: cross-scope writes / raw store reads.
- sns/states/firehose: hard tail. s3/dynamodb: owner-active (their PRs).
- elastictranscoder/qldb/lookoutmetrics: need authored specs (deprecated
  upstream).
- eventhttp/scheduleexpr: helpers counted but not packs — they die with
  the packs they serve, never by moving (C55).

When tree ≤ baseline on all six metrics: `go run ./cmd/ratchet -write`,
delete the two known-red.json entries, both ratchet tests go green.

## Vercel parity contract (frozen by TestVercelCensusDenominators)

- emulate oracle pinned afddfabb28f1 (specs/vercel/emulate-inventory.json):
  52 routes, 26 tests. Reproduce: scripts/count-emulate.py --service vercel --check.
- PARITY.md Vercel section is the ledger; serve-state must stay 52/52.
- Differential: corpus+answers committed at test/behavior/vercel/oracle/;
  re-capture with `npx emulate --service vercel --port 4400` +
  `python3 scripts/capture-oracle.py --service vercel` when the pin moves. Normalizer
  regexes exist in BOTH the script and oracle_differential_test.go — change
  them together.
- Document-vs-oracle conflicts resolve for the vendored document, with a
  corpus `note` on the step (teams nesting is the one case).

## Wave-3 unlocks (next session's keystone work)

1. **Engine primitive registry** — `primitives:` is in the BIR schema
   (bir.go PrimRef) and `prim()` fails loudly by design (compile.go). The
   shadowed/blocked packs need an effect-form (not CEL-pure) primitive that
   calls a sibling service synchronously and answers with its output or a
   stream:
   - apigateway `ExecuteApi` (#394 shadowed): resolve path → integration →
     `aws.lambda` Invoke with the synthesized proxy event → flatten to a
     stream answer. Live callers: pipes (execute-api enrichment) and the
     `_user_request_` edge route.
   - athena `StartQueryExecution` (#393 shadowed): reads glue GetTable + s3
     object bodies, evaluates the query (the "fat moved-verbatim primitive"
     of BEHAVIOR_IR wave 3).
   Also blocked on engine gaps, from wave A stop reports: cloudcontrol needs
   raw/non-object store reads + multi-source conditional list; organizations
   needs cross-scope store writes (`_mirror/global` orgmembers read by IAM's
   SCP authorizer) — a per-resource scope override also unblocks sts/s3.
2. **sqs unshadow** — its bundle is complete+gated (behavior/aws/sqs, 715
   lines); blocked on resource-declared addressing members satisfying
   model-required validation (the shadow comment in the bundle specifies it:
   QueueName must satisfy QueueUrl's requirement for the emulator's internal
   delivery paths). Then 153 mutation needles retarget to engine mutants
   (the ec2 pattern) and 25 callers reroute to bundled.Handler.
3. **elastictranscoder/qldb/lookoutmetrics** need authored specs (deprecated
   upstream; mirror.set already declares them; specs-sync says "3 with no
   upstream spec").

## Environment traps

- Go: `env GOCACHE=/tmp/gocache GOMODCACHE=/home/codex/.cache/packages/go-mod.pre-dev-controls GOTMPDIR=/tmp/gotmp /home/codex/.nix-profile/bin/go ...`
  (`mkdir -p` the /tmp dirs first; /tmp is wiped between sessions).
- Worktrees need GOFLAGS=-buildvcs=false.
- `make generate` fails on a wrapper flock — `go run ./cmd/mirrorgen`.
- specs/ changes need `bash scripts/specs-sync.sh` to re-lock.
- Commits: `PRE_COMMIT_ALLOW_NO_CONFIG=1 git commit`.
- Mutation suite: `-timeout 3600s` (measured up to 3008s).
- Squash-merge convention; deleting a stacked PR's base auto-closes the
  child — retarget children to main BEFORE merging the parent
  (gh pr edit <child> --base main), then rebase the child after.
- Owner merges concurrently — `git fetch origin main` first.
- Subagent quota is 5-hour-windowed; swarms can die mid-wave 403. Partial
  drafts without recordings are discarded, never merged (gated, never trusted).

## B-IR extraction recipe (proven over 16 extractions this program)

Templates: behavior/aws/{swf,dax,glacier}/service.yaml. Gates per pack:
TestEveryBundleBuilds + pack-side record_trace_test.go (replay trace against
the pack BEFORE deletion) + TestBundlesMatchRecordedPacks. Semantics that
bite: input_members first-present-wins (composite keys → derive-only); fx.*
only in effect scope (records with generated members go inline in the put);
resource-level record: re-evaluates per write (input-independent only); list
records must project to the declared item shape; describe may answer raw;
`rec` is the LAST write's record (order effects so the output's record is
last); reads run before effects (a patch's output reads rec, not the read
binding); CEL ternary branches must unify (dyn() around literals); flow
maps {} cannot hold block scalars (cond: > needs block form); engine enforces
model-required members first (trace steps must satisfy them; pack fallbacks
for absent required members are dead — quirk them); form bodies reach restjson
via r.PostForm (the demux pre-parses).
