BIN := bin
GO  := go
export CGO_ENABLED := 0

.PHONY: all build test test-unit test-contract test-snapshot test-chaos test-bdd test-fuzz-seeds test-fuzz test-mutation test-race test-coverage vet fmt generate specs-sync specs-refresh

all: build

build:
	mkdir -p $(BIN)
	$(GO) build -o $(BIN)/mirror ./cmd/mirror
	$(GO) build -o $(BIN)/awslocal ./cmd/awslocal
	$(GO) build -o $(BIN)/gcslocal ./cmd/gcslocal

test: test-unit test-contract

test-unit:
	$(GO) test $$($(GO) list ./... | grep -v '/internal/mutation$$')

test-contract:
	$(GO) test ./internal/conformance ./internal/proto/... -count=1
	cd test/sdk/go && $(GO) test ./... -count=1

test-snapshot:
	$(GO) test ./internal/catalog ./internal/mock ./internal/runtime ./internal/specdiff -count=1

test-chaos:
	$(GO) test ./internal/chaos -count=1

test-bdd:
	$(GO) test ./test/behavior/... ./test/terraform -count=1

test-fuzz-seeds:
	$(GO) test ./internal/edge ./internal/identity ./internal/services/aws/dynamodb/expr ./internal/services/aws/firehose ./internal/services/aws/s3 ./internal/services/aws/states ./internal/services/gcp/gcs -count=1

test-fuzz:
	$(GO) test ./internal/edge -run '^$$' -fuzz '^FuzzDeframeAWSChunked$$' -fuzztime=10s -parallel=4
	$(GO) test ./internal/identity -run '^$$' -fuzz '^FuzzParse$$' -fuzztime=10s -parallel=4
	$(GO) test ./internal/services/aws/dynamodb/expr -run '^$$' -fuzz '^FuzzEvalBool$$' -fuzztime=10s -parallel=4
	$(GO) test ./internal/services/aws/dynamodb/expr -run '^$$' -fuzz '^FuzzApplyUpdate$$' -fuzztime=10s -parallel=4
	$(GO) test ./internal/services/aws/firehose -run '^$$' -fuzz '^FuzzKPLDeaggregation$$' -fuzztime=10s -parallel=4
	$(GO) test ./internal/services/aws/s3 -run '^$$' -fuzz '^FuzzArchiveRestore$$' -fuzztime=10s -parallel=4
	$(GO) test ./internal/services/aws/s3 -run '^$$' -fuzz '^FuzzStorageClassValidation$$' -fuzztime=10s -parallel=4
	$(GO) test ./internal/services/aws/s3 -run '^$$' -fuzz '^FuzzObjectKeyLength$$' -fuzztime=10s -parallel=4
	$(GO) test ./internal/services/aws/s3 -run '^$$' -fuzz '^FuzzCreateBucketCollisions$$' -fuzztime=10s -parallel=4
	$(GO) test ./internal/services/aws/s3 -run '^$$' -fuzz '^FuzzDeleteBucketEmptiness$$' -fuzztime=10s -parallel=4
	$(GO) test ./internal/services/aws/s3 -run '^$$' -fuzz '^FuzzCreateBucketLocations$$' -fuzztime=10s -parallel=4
	$(GO) test ./internal/services/aws/s3 -run '^$$' -fuzz '^FuzzBucketVersioningState$$' -fuzztime=10s -parallel=4
	$(GO) test ./internal/services/aws/s3 -run '^$$' -fuzz '^FuzzBucketNames$$' -fuzztime=10s -parallel=4
	$(GO) test ./internal/services/aws/s3 -run '^$$' -fuzz '^FuzzAccountRegionalBucketNames$$' -fuzztime=10s -parallel=4
	$(GO) test ./internal/services/aws/s3 -run '^$$' -fuzz '^FuzzXXHashChecksums$$' -fuzztime=10s -parallel=4
	$(GO) test ./internal/services/aws/s3 -run '^$$' -fuzz '^FuzzListBucketsPagination$$' -fuzztime=10s -parallel=4
	$(GO) test ./internal/services/aws/s3 -run '^$$' -fuzz '^FuzzDeleteObjectVersionRestoration$$' -fuzztime=10s -parallel=4
	$(GO) test ./internal/services/aws/s3 -run '^$$' -fuzz '^FuzzDeleteObjectsVersionSemantics$$' -fuzztime=10s -parallel=4
	$(GO) test ./internal/services/aws/s3 -run '^$$' -fuzz '^FuzzReplicationVersions$$' -fuzztime=10s -parallel=4
	$(GO) test ./internal/services/aws/s3 -run '^$$' -fuzz '^FuzzReplicationConfigurationValidation$$' -fuzztime=10s -parallel=4
	$(GO) test ./internal/services/aws/s3 -run '^$$' -fuzz '^FuzzReplicationDestinations$$' -fuzztime=10s -parallel=4
	$(GO) test ./internal/services/aws/s3 -run '^$$' -fuzz '^FuzzPostObjectMultipart$$' -fuzztime=10s -parallel=4
	$(GO) test ./internal/services/aws/s3 -run '^$$' -fuzz '^FuzzPostObjectPolicy$$' -fuzztime=10s -parallel=4
	$(GO) test ./internal/services/aws/s3 -run '^$$' -fuzz '^FuzzPostObjectTagging$$' -fuzztime=10s -parallel=4
	$(GO) test ./internal/services/aws/s3 -run '^$$' -fuzz '^FuzzPostObjectExpires$$' -fuzztime=10s -parallel=4
	$(GO) test ./internal/services/aws/states -run '^$$' -fuzz '^FuzzJSONPath$$' -fuzztime=10s -parallel=4
	$(GO) test ./internal/services/gcp/gcs -run '^$$' -fuzz '^FuzzParsePath$$' -fuzztime=10s -parallel=4

# The timeout is set from measurement, not from hope. The suite runs every
# mutant against the full pack surface, so its cost tracks the emulator's
# size; a GitHub-hosted runner has been observed finishing it in 1704s and,
# on a slower draw, exceeding 1800s outright, against 463-702s on a developer
# machine. A budget within a few percent of the observed maximum reports
# runner speed rather than correctness, so this one carries roughly 2x
# headroom over the slowest passing run seen. Lower it when the suite gets
# cheaper, not to make a slow run fail sooner.
test-mutation:
	$(GO) test ./internal/mutation -count=1 -parallel 4 -timeout 3600s

test-race:
	CGO_ENABLED=1 $(GO) test -race $$($(GO) list ./... | grep -v '/internal/mutation$$')

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

# Moves the pins forward: AWS from its default branch, Google Discovery
# refetched. Whatever changed upstream lands as a reviewable diff in the lock,
# in specs/gcp/ and in the regenerated models -- which is how an unannounced
# vendor change gets noticed, so it must be a deliberate act and never a side
# effect of a build.
specs-refresh:
	SPECS_REFRESH=1 bash scripts/specs-sync.sh
