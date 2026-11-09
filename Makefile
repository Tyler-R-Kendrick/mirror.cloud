BIN := bin
GO  := go
# Whether the suites below may reuse a cached result. CI passes COUNT= on pull
# requests, where main's cache is restored, and keeps the uncached default on main.
COUNT ?= -count=1
export CGO_ENABLED := 0

.PHONY: all build build-slim test-service graph test test-unit test-contract test-snapshot test-chaos test-bdd test-cicd-core test-cicd-process test-cicd-e2e cloudflare-evidence test-cloudflare-miniflare test-cloudflare-celld test-fuzz-seeds test-fuzz test-mutation test-mutation-shard test-race test-coverage vet fmt generate specs-sync specs-refresh ratchet ratchet-update equivalence known-red

all: build

build:
	mkdir -p $(BIN)
	$(GO) build -o $(BIN)/mirror ./cmd/mirror
	$(GO) build -o $(BIN)/awslocal ./cmd/awslocal
	$(GO) build -o $(BIN)/gcslocal ./cmd/gcslocal

# A build of only some services: make build-slim SERVICES=@profiles/core.txt, or
# SERVICES="aws.s3 aws.sqs". See scripts/slim-overlay.py.
build-slim:
	mkdir -p $(BIN)
	python3 scripts/slim-overlay.py $(BIN)/slim-overlay.json $(SERVICES)
	$(GO) build -trimpath -ldflags '-s -w' -overlay $(BIN)/slim-overlay.json -o $(BIN)/mirror-slim ./cmd/mirror

# The tests of the services you are working on and nothing else, in any tree:
#   make test-service SERVICES="aws.s3 vercel.*"
# A sparse checkout (scripts/sparse.py) holds those and what they need and no
# more. docs/TESTING.md says what this does not cover.
test-service:
	@test -n "$(SERVICES)" || { echo 'make test-service SERVICES="aws.s3 aws.sqs"  (PROVIDER.* for a whole provider)'; exit 2; }
	$(GO) test $(COUNT) $$(python3 scripts/sparse.py --packages $(SERVICES))

# profiles/graph.txt, which says what each pack needs; commit the result.
graph:
	python3 scripts/service-graph.py > profiles/graph.txt

test: test-unit test-contract

# The needle check is pulled out of the excluded mutation package on purpose:
# it is the one part of that suite that is fast, and a mutant whose needle has
# stopped matching is exactly what a refactor breaks and what waiting for the
# full suite reports half an hour late.
test-unit:
	$(GO) test $$($(GO) list ./... | grep -v '/internal/mutation$$')
	$(GO) test ./internal/mutation -run '^TestMutantNeedlesExist$$' -count=1

test-contract:
	$(GO) test ./internal/conformance ./internal/proto/... $(COUNT)
	cd test/sdk/go && $(GO) test ./... $(COUNT)

test-snapshot:
	$(GO) test ./internal/edge ./internal/identity ./internal/mock ./internal/proto/aws/restxml ./internal/runtime ./internal/specdiff $(COUNT)
	$(GO) test ./internal/services/aws/s3 -run 'Characterization$$|TestNamedBucketConfigurations$$|TestUploadPartCopyConditionsAndRange$$' $(COUNT)
	$(GO) test ./internal/services/aws/dynamodb -run 'Characterization$$' $(COUNT)
	$(GO) test ./test/behavior/aws -run 'Characterization$$' $(COUNT)
	$(GO) test ./internal/services/aws/states -run 'Characterization$$' $(COUNT)
	$(GO) test ./internal/services/gcp/gcs -run 'Characterization$$' $(COUNT)
	$(GO) test ./internal/bundled -run 'TestAzureBlobCharacterization$$' $(COUNT)

test-chaos:
	$(GO) test ./internal/chaos $(COUNT)

test-bdd:
	$(GO) test ./test/behavior/... ./test/terraform $(COUNT)

test-cicd-core:
	$(GO) test ./internal/execution/cicd/ ./internal/execution/cicd/lifecycle/ -count=1

test-cicd-process:
	$(GO) test ./internal/execution/cicd/buildspec/ ./internal/execution/cicd/codebuild/ ./internal/execution/cicd/cloudflareci/ ./internal/execution/cicd/cfbuilds/ ./internal/execution/cicd/cfpages/ ./internal/execution/cicd/vercel/ -count=1

test-cicd-e2e:
	$(GO) test ./internal/execution/cicd/... -count=1

# Required live Cloudflare runtime suite: real node + workerd through the
# pinned helper under tools/cloudflare-runtime. Built with -tags miniflare so
# the default unit path stays usable without optional runtimes; built WITH
# the tag, an unprovisioned environment FAILS (the test fatals, it never
# skips) -- this target must not report success when the backend is absent.
# Provision once: (cd tools/cloudflare-runtime && npm ci)
cloudflare-evidence:
	python3 scripts/cloudflare-evidence.py

test-cloudflare-miniflare:
	$(GO) test -tags miniflare ./internal/execution/ -run 'TestMiniflare' -count=1 -v
	$(GO) test -tags miniflare ./test/behavior/cloudflare/ -run 'TestCFKVCoherenceViaExecute' -count=1 -v

test-cloudflare-celld:
	$(GO) test -tags celld ./internal/execution/ -run 'TestCelld' -count=1 -v

test-fuzz-seeds:
	$(GO) test ./internal/edge ./internal/identity ./internal/proto/aws/httpuri ./internal/services/aws/dynamodb ./internal/services/aws/dynamodb/expr ./internal/services/aws/firehose ./internal/services/aws/s3 ./test/behavior/aws ./internal/services/aws/states ./internal/services/gcp/gcs ./internal/bundled ./internal/proto/aws/restjson ./internal/proto/aws/restxml ./internal/proto/graphql $(COUNT)

test-fuzz:
	bash scripts/fuzz.sh

# The timeout is set from measurement, not from hope. The suite runs every
# mutant against the full pack surface, so its cost tracks the emulator's
# size, and what it costs varies by more than 3x for reasons that have nothing
# to do with the code: runs of 463s, 702s and 1589s on the same developer
# machine, 1704s on a GitHub-hosted runner, and one runner exceeding 1800s
# outright. A budget set within a few percent of the observed maximum
# therefore reports which machine drew the job rather than whether anything
# broke, so this one carries roughly 2x headroom over the slowest passing run
# seen. Lower it when the suite gets cheaper, not to make a slow run fail
# sooner.
test-mutation:
	MIRROR_MUTATION_FULL=1 $(GO) test ./internal/mutation -count=1 -parallel 4 -timeout 3600s

# One slice of the mutation suite. CI runs these as a matrix because the cost
# is irreducible per mutant -- each needs its own compile of the mutated
# package -- so the only thing that shrinks the wall clock is running fewer of
# them per job. `make test-mutation` still runs all of them, which is what to
# use locally before pushing a change that touches the mutant table.
test-mutation-shard:
	MUTATION_SHARD=$(MUTATION_SHARD) MUTATION_SHARDS=$(MUTATION_SHARDS) \
	  $(GO) test ./internal/mutation -count=1 -parallel 4 -timeout 1800s

# RACE_SHARD of RACE_SHARDS takes every Nth package; CI runs four, because the
# race build of this suite is the longest single job there is.
RACE_SHARD ?= 0
RACE_SHARDS ?= 1
test-race:
	CGO_ENABLED=1 $(GO) test -race $$($(GO) list ./... | grep -v '/internal/mutation$$' | awk -v s=$(RACE_SHARD) -v n=$(RACE_SHARDS) 'NR % n == s')

# Say which of a run's failures were expected, by name, and which were not.
#
# It changes nothing about what passes. Every entry in known-red.json is still
# a failing test and still fails its step -- the ratchet ones exist to stay red
# until the packs they measure are gone, and suppressing them would trade one
# kind of blindness for another. What it changes is that the end of a red step
# says WHICH red, which is the difference between a gate and a colour: a step
# already red for the ratchet absorbed TestGenerateCatalogIdempotent for two
# merges and a Firehose flake for one, and neither was hidden cleverly.
#
# Reads a saved `go test` run, so a step tees its output and passes the file:
#   $(GO) test ./... 2>&1 | tee out.txt; $(MAKE) known-red RUN=out.txt
known-red:
	@python3 scripts/known-red.py $(RUN)

test-coverage:
	@packages="$$($(GO) list ./... | grep -v '/internal/mutation$$' | grep -v '/internal/generated/')"; $(GO) test $$packages -covermode=atomic -coverprofile=coverage-unit.out
	$(GO) test ./internal/chaos ./internal/conformance ./internal/runtime ./internal/spine ./test/behavior/... ./test/terraform \
		./internal/services/aws/apigateway ./internal/services/aws/athena ./internal/services/aws/cloudcontrol \
		./internal/services/aws/cloudformation ./internal/services/aws/ecs ./internal/services/aws/events ./internal/services/aws/firehose \
		./internal/services/aws/iam ./internal/services/aws/organizations ./internal/services/aws/pipes ./internal/services/aws/s3 \
		./internal/services/aws/scheduler ./internal/services/aws/sns ./internal/services/aws/states \
		-coverpkg=./... -covermode=atomic -coverprofile=coverage-integration.out
	@# internal/generated is mirrorgen output pinned by specs/mirror.lock; its
	@# correctness is asserted by regeneration byte-identity and the
	@# internal/check gates, not by covering generated accessors.
	@awk 'BEGIN { print "mode: atomic" } /^mode:/ { next } $$1 ~ /internal\/generated\// { next } { statements[$$1] = $$2; if ($$3 > 0) covered[$$1] = 1 } END { for (block in statements) print block, statements[block], covered[block] + 0 }' coverage-unit.out coverage-integration.out > coverage.out
	@$(RM) coverage-unit.out coverage-integration.out
	@pct=$$($(GO) tool cover -func=coverage.out | awk '/^total:/ {gsub("%", "", $$3); print $$3}'); awk -v got="$$pct" 'BEGIN { if (got < 80) { print "coverage " got "% is below 80%"; exit 1 } }'

ratchet:
	$(GO) test ./internal/check/ -run 'TestRatchet|TestNoNew' -count=1

ratchet-update:
	$(GO) run ./cmd/ratchet -write

# Replays every recorded pack trace against the bundle that replaced it, and
# checks that every bundle still builds and registers. A pack may only be
# deleted with this green.
equivalence:
	$(GO) test ./internal/equivalence/ ./internal/bundled/ -count=1

vet:
	$(GO) vet ./...
	@out=$$(gofmt -l $$(find . -name '*.go' -not -path './node_modules/*') || true); if [ -n "$$out" ]; then echo "$$out"; exit 1; fi

fmt:
	gofmt -w $$(find . -name '*.go' -not -path './node_modules/*')

generate:
	$(GO) run ./cmd/mirrorgen

# Fetches exactly what specs/mirror.lock pins, so the generated models follow
# from the lock on any machine at any time.
specs-sync:
	bash scripts/specs-sync.sh

# Moves the pins forward. Whatever changed upstream lands as a reviewable diff
# in the lock, under specs/ and in the regenerated models -- which is how an
# unannounced vendor change gets noticed, so it must be a deliberate act and
# never a side effect of a build.
#
# SERVICE scopes it. `make specs-refresh SERVICE=railway.graphql` moves that one
# pin and leaves every other document at its committed copy; SERVICE=aws moves
# the AWS pin, which is one commit covering every aws.* service. Without it,
# every pin moves.
#
# Scope it whenever the reason for the refresh is one service. Refreshing all of
# them to pick up a single schema change also re-pinned AWS and rewrote
# twenty-nine unrelated models into a pull request about one of them, and CI
# agreed with all of it: the models were regenerated consistently, so every
# check passed.
specs-refresh:
	SPECS_REFRESH=$(or $(SERVICE),1) bash scripts/specs-sync.sh
