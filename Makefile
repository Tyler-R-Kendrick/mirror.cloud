BIN := bin
GO  := go
export CGO_ENABLED := 0

.PHONY: all build test test-unit test-contract test-snapshot test-chaos test-bdd test-fuzz-seeds test-fuzz test-mutation test-race test-coverage vet fmt generate specs-sync

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
	$(GO) test ./internal/catalog ./internal/edge ./internal/mock ./internal/proto/aws/restxml ./internal/runtime ./internal/specdiff -count=1
	$(GO) test ./internal/services/aws/s3 -run 'Characterization$$|TestNamedBucketConfigurations$$' -count=1

test-chaos:
	$(GO) test ./internal/chaos -count=1

test-bdd:
	$(GO) test ./test/behavior/... ./test/terraform -count=1

test-fuzz-seeds:
	$(GO) test ./internal/edge ./internal/identity ./internal/services/aws/dynamodb/expr ./internal/services/aws/firehose ./internal/services/aws/s3 ./internal/services/aws/states ./internal/services/gcp/gcs -count=1

test-fuzz:
	$(GO) test ./internal/edge -run '^$$' -fuzz '^FuzzDeframeAWSChunked$$' -fuzztime=10000x -parallel=4
	$(GO) test ./internal/edge -run '^$$' -fuzz '^FuzzS3ResponseEnvelope$$' -fuzztime=10000x -parallel=4
	$(GO) test ./internal/edge -run '^$$' -fuzz '^FuzzSignedGatewayHost$$' -fuzztime=10000x -parallel=4
	$(GO) test ./internal/identity -run '^$$' -fuzz '^FuzzParse$$' -fuzztime=10000x -parallel=4
	$(GO) test ./internal/identity -run '^$$' -fuzz '^FuzzVerifyS3PresignedV4$$' -fuzztime=10000x -parallel=4
	$(GO) test ./internal/identity -run '^$$' -fuzz '^FuzzVerifyS3AuthorizationV4$$' -fuzztime=10000x -parallel=4
	$(GO) test ./internal/identity -run '^$$' -fuzz '^FuzzVerifyS3PresignedV2$$' -fuzztime=10000x -parallel=4
	$(GO) test ./internal/identity -run '^$$' -fuzz '^FuzzVerifyS3SessionToken$$' -fuzztime=10000x -parallel=4
	$(GO) test ./internal/proto/aws/restxml -run '^$$' -fuzz '^FuzzEmptyResponseHeaders$$' -fuzztime=10000x -parallel=4
	$(GO) test ./internal/services/aws/dynamodb/expr -run '^$$' -fuzz '^FuzzEvalBool$$' -fuzztime=10000x -parallel=4
	$(GO) test ./internal/services/aws/dynamodb/expr -run '^$$' -fuzz '^FuzzApplyUpdate$$' -fuzztime=10000x -parallel=4
	$(GO) test ./internal/services/aws/firehose -run '^$$' -fuzz '^FuzzKPLDeaggregation$$' -fuzztime=10000x -parallel=4
	$(GO) test ./internal/services/aws/s3 -run '^$$' -fuzz '^FuzzArchiveRestore$$' -fuzztime=10000x -parallel=4
	$(GO) test ./internal/services/aws/s3 -run '^$$' -fuzz '^FuzzStorageClassValidation$$' -fuzztime=10000x -parallel=4
	$(GO) test ./internal/services/aws/s3 -run '^$$' -fuzz '^FuzzObjectKeyLength$$' -fuzztime=10000x -parallel=4
	$(GO) test ./internal/services/aws/s3 -run '^$$' -fuzz '^FuzzCreateBucketCollisions$$' -fuzztime=10000x -parallel=4
	$(GO) test ./internal/services/aws/s3 -run '^$$' -fuzz '^FuzzCreateBucketTags$$' -fuzztime=10000x -parallel=4
	$(GO) test ./internal/services/aws/s3 -run '^$$' -fuzz '^FuzzCreateBucketObjectOwnership$$' -fuzztime=10000x -parallel=4
	$(GO) test ./internal/services/aws/s3 -run '^$$' -fuzz '^FuzzBucketOwnershipControls$$' -fuzztime=10000x -parallel=4
	$(GO) test ./internal/services/aws/s3 -run '^$$' -fuzz '^FuzzPublicAccessBlock$$' -fuzztime=10000x -parallel=4
	$(GO) test ./internal/services/aws/s3 -run '^$$' -fuzz '^FuzzBucketRequestPayment$$' -fuzztime=10000x -parallel=4
	$(GO) test ./internal/services/aws/s3 -run '^$$' -fuzz '^FuzzBucketAccelerateConfiguration$$' -fuzztime=10000x -parallel=4
	$(GO) test ./internal/services/aws/s3 -run '^$$' -fuzz '^FuzzBucketLogging$$' -fuzztime=10000x -parallel=4
	$(GO) test ./internal/services/aws/s3 -run '^$$' -fuzz '^FuzzBucketCors$$' -fuzztime=10000x -parallel=4
	$(GO) test ./internal/services/aws/s3 -run '^$$' -fuzz '^FuzzBucketWebsite$$' -fuzztime=10000x -parallel=4
	$(GO) test ./internal/services/aws/s3 -run '^$$' -fuzz '^FuzzBucketLifecycle$$' -fuzztime=10000x -parallel=4
	$(GO) test ./internal/services/aws/s3 -run '^$$' -fuzz '^FuzzBucketPolicy$$' -fuzztime=10000x -parallel=4
	$(GO) test ./internal/services/aws/s3 -run '^$$' -fuzz '^FuzzBucketEncryption$$' -fuzztime=10000x -parallel=4
	$(GO) test ./internal/services/aws/s3 -run '^$$' -fuzz '^FuzzNamedBucketConfigurations$$' -fuzztime=10000x -parallel=4
	$(GO) test ./internal/services/aws/s3 -run '^$$' -fuzz '^FuzzACLConfigurations$$' -fuzztime=10000x -parallel=4
	$(GO) test ./internal/services/aws/s3 -run '^$$' -fuzz '^FuzzBucketNotifications$$' -fuzztime=10000x -parallel=4
	$(GO) test ./internal/services/aws/s3 -run '^$$' -fuzz '^FuzzDeleteBucketEmptiness$$' -fuzztime=10000x -parallel=4
	$(GO) test ./internal/services/aws/s3 -run '^$$' -fuzz '^FuzzCreateBucketLocations$$' -fuzztime=10000x -parallel=4
	$(GO) test ./internal/services/aws/s3 -run '^$$' -fuzz '^FuzzCrossRegionBucketResolution$$' -fuzztime=10000x -parallel=4
	$(GO) test ./internal/services/aws/s3 -run '^$$' -fuzz '^FuzzBucketVersioningState$$' -fuzztime=10000x -parallel=4
	$(GO) test ./internal/services/aws/s3 -run '^$$' -fuzz '^FuzzObjectLockDefaultRetention$$' -fuzztime=10000x -parallel=4
	$(GO) test ./internal/services/aws/s3 -run '^$$' -fuzz '^FuzzBucketNames$$' -fuzztime=10000x -parallel=4
	$(GO) test ./internal/services/aws/s3 -run '^$$' -fuzz '^FuzzAccountRegionalBucketNames$$' -fuzztime=10000x -parallel=4
	$(GO) test ./internal/services/aws/s3 -run '^$$' -fuzz '^FuzzXXHashChecksums$$' -fuzztime=10000x -parallel=4
	$(GO) test ./internal/services/aws/s3 -run '^$$' -fuzz '^FuzzListBucketsPagination$$' -fuzztime=10000x -parallel=4
	$(GO) test ./internal/services/aws/s3 -run '^$$' -fuzz '^FuzzDeleteObjectVersionRestoration$$' -fuzztime=10000x -parallel=4
	$(GO) test ./internal/services/aws/s3 -run '^$$' -fuzz '^FuzzDeleteObjectsVersionSemantics$$' -fuzztime=10000x -parallel=4
	$(GO) test ./internal/services/aws/s3 -run '^$$' -fuzz '^FuzzReplicationVersions$$' -fuzztime=10000x -parallel=4
	$(GO) test ./internal/services/aws/s3 -run '^$$' -fuzz '^FuzzReplicationConfigurationValidation$$' -fuzztime=10000x -parallel=4
	$(GO) test ./internal/services/aws/s3 -run '^$$' -fuzz '^FuzzReplicationDestinations$$' -fuzztime=10000x -parallel=4
	$(GO) test ./internal/services/aws/s3 -run '^$$' -fuzz '^FuzzPostObjectMultipart$$' -fuzztime=10000x -parallel=4
	$(GO) test ./internal/services/aws/s3 -run '^$$' -fuzz '^FuzzPostObjectPolicy$$' -fuzztime=10000x -parallel=4
	$(GO) test ./internal/services/aws/s3 -run '^$$' -fuzz '^FuzzPostObjectTagging$$' -fuzztime=10000x -parallel=4
	$(GO) test ./internal/services/aws/s3 -run '^$$' -fuzz '^FuzzPostObjectExpires$$' -fuzztime=10000x -parallel=4
	$(GO) test ./internal/services/aws/s3 -run '^$$' -fuzz '^FuzzPostObjectChecksums$$' -fuzztime=10000x -parallel=4
	$(GO) test ./internal/services/aws/s3 -run '^$$' -fuzz '^FuzzObjectServerSideEncryption$$' -fuzztime=10000x -parallel=4
	$(GO) test ./internal/services/aws/s3 -run '^$$' -fuzz '^FuzzGetObjectResponseOverrides$$' -fuzztime=10000x -parallel=4
	$(GO) test ./internal/services/aws/s3 -run '^$$' -fuzz '^FuzzUserMetadataRFC2047$$' -fuzztime=10000x -parallel=4
	$(GO) test ./internal/services/aws/s3 -run '^$$' -fuzz '^FuzzExplicitKMSKeyValidation$$' -fuzztime=10000x -parallel=4
	$(GO) test ./internal/services/aws/s3 -run '^$$' -fuzz '^FuzzObjectSSECustomerKey$$' -fuzztime=10000x -parallel=4
	$(GO) test ./internal/services/aws/s3 -run '^$$' -fuzz '^FuzzMultipartServerSideEncryption$$' -fuzztime=10000x -parallel=4
	$(GO) test ./internal/services/aws/s3 -run '^$$' -fuzz '^FuzzMultipartSSECustomerKey$$' -fuzztime=10000x -parallel=4
	$(GO) test ./internal/services/aws/s3 -run '^$$' -fuzz '^FuzzCopyObjectSSECustomerKeys$$' -fuzztime=10000x -parallel=4
	$(GO) test ./internal/services/aws/states -run '^$$' -fuzz '^FuzzJSONPath$$' -fuzztime=10000x -parallel=4
	$(GO) test ./internal/services/gcp/gcs -run '^$$' -fuzz '^FuzzParsePath$$' -fuzztime=10000x -parallel=4

test-mutation:
	$(GO) test ./internal/mutation -count=1 -parallel 4 -timeout 3600s

test-race:
	CGO_ENABLED=1 $(GO) test -race $$($(GO) list ./... | grep -v '/internal/mutation$$')

test-coverage:
	@packages="$$($(GO) list ./... | grep -v '/internal/mutation$$')"; $(GO) test $$packages -covermode=atomic -coverprofile=coverage-unit.out
	$(GO) test ./internal/chaos ./internal/conformance ./internal/runtime ./internal/spine ./test/behavior/... ./test/terraform \
		./internal/services/aws/apigateway ./internal/services/aws/athena ./internal/services/aws/cloudcontrol \
		./internal/services/aws/cloudformation ./internal/services/aws/ecs ./internal/services/aws/events ./internal/services/aws/firehose \
		./internal/services/aws/iam ./internal/services/aws/organizations ./internal/services/aws/pipes ./internal/services/aws/s3 \
		./internal/services/aws/scheduler ./internal/services/aws/sns ./internal/services/aws/states \
		-coverpkg=./... -covermode=atomic -coverprofile=coverage-integration.out
	@awk 'BEGIN { print "mode: atomic" } /^mode:/ { next } { statements[$$1] = $$2; if ($$3 > 0) covered[$$1] = 1 } END { for (block in statements) print block, statements[block], covered[block] + 0 }' coverage-unit.out coverage-integration.out > coverage.out
	@$(RM) coverage-unit.out coverage-integration.out
	@pct=$$($(GO) tool cover -func=coverage.out | awk '/^total:/ {gsub("%", "", $$3); print $$3}'); awk -v got="$$pct" 'BEGIN { if (got < 80) { print "coverage " got "% is below 80%"; exit 1 } }'

vet:
	$(GO) vet ./...
	@out=$$(gofmt -l $$(find . -name '*.go' -not -path './node_modules/*') || true); if [ -n "$$out" ]; then echo "$$out"; exit 1; fi

fmt:
	gofmt -w $$(find . -name '*.go' -not -path './node_modules/*')

generate:
	$(GO) run ./cmd/mirrorgen

specs-sync:
	bash scripts/specs-sync.sh
