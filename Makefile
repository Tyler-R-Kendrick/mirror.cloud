BIN := bin
GO  := go
export CGO_ENABLED := 0

.PHONY: all build test test-unit test-contract test-snapshot test-chaos test-bdd test-cicd-core test-cicd-process test-cicd-e2e cloudflare-evidence test-cloudflare-miniflare test-cloudflare-celld test-fuzz-seeds test-fuzz test-mutation test-mutation-shard test-race test-coverage vet fmt generate specs-sync specs-refresh ratchet ratchet-update equivalence known-red

all: build

build:
	mkdir -p $(BIN)
	$(GO) build -o $(BIN)/mirror ./cmd/mirror
	$(GO) build -o $(BIN)/awslocal ./cmd/awslocal
	$(GO) build -o $(BIN)/gcslocal ./cmd/gcslocal

test: test-unit test-contract

# The needle check is pulled out of the excluded mutation package on purpose:
# it is the one part of that suite that is fast, and a mutant whose needle has
# stopped matching is exactly what a refactor breaks and what waiting for the
# full suite reports half an hour late.
test-unit:
	$(GO) test $$($(GO) list ./... | grep -v '/internal/mutation$$')
	$(GO) test ./internal/mutation -run '^TestMutantNeedlesExist$$' -count=1

test-contract:
	$(GO) test ./internal/conformance ./internal/proto/... -count=1
	cd test/sdk/go && $(GO) test ./... -count=1

test-snapshot:
	$(GO) test ./internal/edge ./internal/identity ./internal/mock ./internal/proto/aws/restxml ./internal/runtime ./internal/specdiff -count=1
	$(GO) test ./internal/services/aws/s3 -run 'Characterization$$|TestNamedBucketConfigurations$$|TestUploadPartCopyConditionsAndRange$$' -count=1
	$(GO) test ./internal/services/aws/dynamodb -run 'Characterization$$' -count=1
	$(GO) test ./test/behavior/aws -run 'Characterization$$' -count=1
	$(GO) test ./internal/services/aws/states -run 'Characterization$$' -count=1
	$(GO) test ./internal/services/gcp/gcs -run 'Characterization$$' -count=1
	$(GO) test ./internal/bundled -run 'TestAzureBlobCharacterization$$' -count=1

test-chaos:
	$(GO) test ./internal/chaos -count=1

test-bdd:
	$(GO) test ./test/behavior/... ./test/terraform -count=1

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
	$(GO) test ./internal/edge ./internal/identity ./internal/proto/aws/httpuri ./internal/services/aws/dynamodb ./internal/services/aws/dynamodb/expr ./internal/services/aws/firehose ./internal/services/aws/s3 ./test/behavior/aws ./internal/services/aws/states ./internal/services/gcp/gcs ./internal/bundled ./internal/proto/aws/restjson ./internal/proto/aws/restxml ./internal/proto/graphql -count=1

test-fuzz:
	$(GO) test ./internal/edge -run '^$$' -fuzz '^FuzzDeframeAWSChunked$$' -fuzztime=10000x -parallel=4
	$(GO) test ./internal/edge -run '^$$' -fuzz '^FuzzS3AWSChunkedContentEncoding$$' -fuzztime=10000x -parallel=4
	$(GO) test ./internal/edge -run '^$$' -fuzz '^FuzzS3ResponseEnvelope$$' -fuzztime=10000x -parallel=4
	$(GO) test ./internal/edge -run '^$$' -fuzz '^FuzzSignedGatewayHost$$' -fuzztime=10000x -parallel=4
	$(GO) test ./internal/identity -run '^$$' -fuzz '^FuzzParse$$' -fuzztime=10000x -parallel=4
	$(GO) test ./internal/identity -run '^$$' -fuzz '^FuzzLocalhostRegion$$' -fuzztime=10000x -parallel=4
	$(GO) test ./internal/identity -run '^$$' -fuzz '^FuzzPresignedCredentialSyntax$$' -fuzztime=10000x -parallel=4
	$(GO) test ./internal/identity -run '^$$' -fuzz '^FuzzS3AuthorizationTimeFault$$' -fuzztime=10000x -parallel=4
	$(GO) test ./internal/identity -run '^$$' -fuzz '^FuzzS3AmzHeaderSigning$$' -fuzztime=10000x -parallel=4
	$(GO) test ./internal/identity -run '^$$' -fuzz '^FuzzVerifyS3PresignedV4$$' -fuzztime=10000x -parallel=4
	$(GO) test ./internal/identity -run '^$$' -fuzz '^FuzzVerifyS3AuthorizationV4$$' -fuzztime=10000x -parallel=4
	$(GO) test ./internal/identity -run '^$$' -fuzz '^FuzzVerifyS3V4A$$' -fuzztime=10000x -parallel=4
	$(GO) test ./internal/identity -run '^$$' -fuzz '^FuzzVerifyS3AuthorizationV2$$' -fuzztime=10000x -parallel=4
	$(GO) test ./internal/identity -run '^$$' -fuzz '^FuzzVerifyS3PostPolicy$$' -fuzztime=10000x -parallel=4
	$(GO) test ./internal/identity -run '^$$' -fuzz '^FuzzVerifyS3StreamingV4$$' -fuzztime=10000x -parallel=4
	$(GO) test ./internal/identity -run '^$$' -fuzz '^FuzzVerifyS3StreamingTrailerV4$$' -fuzztime=10000x -parallel=4
	$(GO) test ./internal/identity -run '^$$' -fuzz '^FuzzVerifyS3StreamingUnsignedTrailerV4$$' -fuzztime=10000x -parallel=4
	$(GO) test ./internal/identity -run '^$$' -fuzz '^FuzzVerifyS3StreamingV4A$$' -fuzztime=10000x -parallel=4
	$(GO) test ./internal/identity -run '^$$' -fuzz '^FuzzVerifyS3StreamingUnsignedTrailerV4A$$' -fuzztime=10000x -parallel=4
	$(GO) test ./internal/identity -run '^$$' -fuzz '^FuzzVerifyS3PresignedV2$$' -fuzztime=10000x -parallel=4
	$(GO) test ./internal/identity -run '^$$' -fuzz '^FuzzVerifyS3SessionToken$$' -fuzztime=10000x -parallel=4
	$(GO) test ./internal/proto/aws/httpuri -run '^$$' -fuzz '^FuzzParseAndMatch$$' -fuzztime=10000x -parallel=4
	$(GO) test ./internal/proto/aws/httpuri -run '^$$' -fuzz '^FuzzMatchAgainstARealService$$' -fuzztime=10000x -parallel=4
	$(GO) test ./internal/proto/aws/restxml -run '^$$' -fuzz '^FuzzEmptyResponseHeaders$$' -fuzztime=10000x -parallel=4
	$(GO) test ./internal/services/aws/dynamodb/expr -run '^$$' -fuzz '^FuzzEvalBool$$' -fuzztime=10000x -parallel=4
	$(GO) test ./internal/services/aws/dynamodb/expr -run '^$$' -fuzz '^FuzzApplyUpdate$$' -fuzztime=10000x -parallel=4
	$(GO) test ./internal/services/aws/dynamodb -run '^$$' -fuzz '^FuzzTableLifecycle$$' -fuzztime=10000x -parallel=4
	$(GO) test ./internal/services/aws/dynamodb -run '^$$' -fuzz '^FuzzDynamoDBBinaryValues$$' -fuzztime=10000x -parallel=4
	$(GO) test ./internal/services/aws/dynamodb -run '^$$' -fuzz '^FuzzDynamoDBTableClass$$' -fuzztime=10000x -parallel=4
	$(GO) test ./internal/services/aws/dynamodb -run '^$$' -fuzz '^FuzzDynamoDBTableMetadata$$' -fuzztime=10000x -parallel=4
	$(GO) test ./internal/services/aws/dynamodb -run '^$$' -fuzz '^FuzzDynamoDBDefaultSSE$$' -fuzztime=10000x -parallel=4
	$(GO) test ./internal/services/aws/dynamodb -run '^$$' -fuzz '^FuzzDynamoDBBackupInsights$$' -fuzztime=10000x -parallel=4
	$(GO) test ./internal/services/aws/dynamodb -run '^$$' -fuzz '^FuzzDynamoDBPartiQL$$' -fuzztime=10000x -parallel=4
	$(GO) test ./internal/services/aws/dynamodb -run '^$$' -fuzz '^FuzzDynamoDBStreamRecords$$' -fuzztime=10000x -parallel=4
	$(GO) test ./internal/services/aws/dynamodb -run '^$$' -fuzz '^FuzzDynamoDBKinesisDestination$$' -fuzztime=10000x -parallel=4
	$(GO) test ./internal/services/aws/dynamodb -run '^$$' -fuzz '^FuzzDynamoDBGlobalTable$$' -fuzztime=10000x -parallel=4
	$(GO) test ./internal/services/aws/dynamodb -run '^$$' -fuzz '^FuzzDynamoDBTransactions$$' -fuzztime=10000x -parallel=4
	$(GO) test ./test/behavior/aws -run '^$$' -fuzz '^FuzzListQueuesPagination$$' -fuzztime=10000x -parallel=4
	$(GO) test ./test/behavior/aws -run '^$$' -fuzz '^FuzzQueueMetadataAttributeSelection$$' -fuzztime=10000x -parallel=4
	$(GO) test ./test/behavior/aws -run '^$$' -fuzz '^FuzzQueueDeletionWindow$$' -fuzztime=10000x -parallel=4
	$(GO) test ./test/behavior/aws -run '^$$' -fuzz '^FuzzSendReceiveMessageDigest$$' -fuzztime=10000x -parallel=4
	$(GO) test ./test/behavior/aws -run '^$$' -fuzz '^FuzzReceiveMessageMaxNumber$$' -fuzztime=10000x -parallel=4
	$(GO) test ./test/behavior/aws -run '^$$' -fuzz '^FuzzEmptyReceiveOmitsMessages$$' -fuzztime=10000x -parallel=4
	$(GO) test ./test/behavior/aws -run '^$$' -fuzz '^FuzzReceiveMessageWaitTime$$' -fuzztime=10000x -parallel=4
	$(GO) test ./test/behavior/aws -run '^$$' -fuzz '^FuzzMessagesRemainQueueScoped$$' -fuzztime=10000x -parallel=4
	$(GO) test ./test/behavior/aws -run '^$$' -fuzz '^FuzzSendMessageBatchBodies$$' -fuzztime=10000x -parallel=4
	$(GO) test ./test/behavior/aws -run '^$$' -fuzz '^FuzzSendMessageBatchEntryCount$$' -fuzztime=10000x -parallel=4
	$(GO) test ./test/behavior/aws -run '^$$' -fuzz '^FuzzMessageSizeBoundary$$' -fuzztime=10000x -parallel=4
	$(GO) test ./test/behavior/aws -run '^$$' -fuzz '^FuzzSendMessageBatchSizeBoundary$$' -fuzztime=10000x -parallel=4
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
	$(GO) test ./internal/services/aws/s3 -run '^$$' -fuzz '^FuzzBucketCorsHTTP$$' -fuzztime=10000x -parallel=4
	$(GO) test ./internal/services/aws/s3 -run '^$$' -fuzz '^FuzzLocalStackCORSOrigins$$' -fuzztime=10000x -parallel=4
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
	$(GO) test ./internal/services/aws/s3 -run '^$$' -fuzz '^FuzzListObjectsPagination$$' -fuzztime=10000x -parallel=4
	$(GO) test ./internal/services/aws/s3 -run '^$$' -fuzz '^FuzzListEncodingType$$' -fuzztime=10000x -parallel=4
	$(GO) test ./internal/services/aws/s3 -run '^$$' -fuzz '^FuzzListObjectVersionsPagination$$' -fuzztime=10000x -parallel=4
	$(GO) test ./internal/services/aws/s3 -run '^$$' -fuzz '^FuzzListMultipartUploadsMarkers$$' -fuzztime=10000x -parallel=4
	$(GO) test ./internal/services/aws/s3 -run '^$$' -fuzz '^FuzzListPartsPagination$$' -fuzztime=10000x -parallel=4
	$(GO) test ./internal/services/aws/s3 -run '^$$' -fuzz '^FuzzNoSuchUploadFaults$$' -fuzztime=10000x -parallel=4
	$(GO) test ./internal/services/aws/s3 -run '^$$' -fuzz '^FuzzMultipartPartNumberFaults$$' -fuzztime=10000x -parallel=4
	$(GO) test ./internal/services/aws/s3 -run '^$$' -fuzz '^FuzzMultipartCompletionFaults$$' -fuzztime=10000x -parallel=4
	$(GO) test ./internal/services/aws/s3 -run '^$$' -fuzz '^FuzzUploadPartContentMD5$$' -fuzztime=10000x -parallel=4
	$(GO) test ./internal/services/aws/s3 -run '^$$' -fuzz '^FuzzUploadPartChecksumFaults$$' -fuzztime=10000x -parallel=4
	$(GO) test ./internal/services/aws/s3 -run '^$$' -fuzz '^FuzzUploadPartSSECustomerKeyFaults$$' -fuzztime=10000x -parallel=4
	$(GO) test ./internal/services/aws/s3 -run '^$$' -fuzz '^FuzzCompleteMultipartChecksumTypeFault$$' -fuzztime=10000x -parallel=4
	$(GO) test ./internal/services/aws/s3 -run '^$$' -fuzz '^FuzzCompleteMultipartPreconditionFaults$$' -fuzztime=10000x -parallel=4
	$(GO) test ./internal/services/aws/s3 -run '^$$' -fuzz '^FuzzWritePreconditionFaults$$' -fuzztime=10000x -parallel=4
	$(GO) test ./internal/services/aws/s3 -run '^$$' -fuzz '^FuzzWriteConditionFaultDetails$$' -fuzztime=10000x -parallel=4
	$(GO) test ./internal/services/aws/s3 -run '^$$' -fuzz '^FuzzWriteIfMatchRequiresSingleETag$$' -fuzztime=10000x -parallel=4
	$(GO) test ./internal/services/aws/s3 -run '^$$' -fuzz '^FuzzCompleteMultipartConditionalConflicts$$' -fuzztime=10000x -parallel=4
	$(GO) test ./internal/services/aws/s3 -run '^$$' -fuzz '^FuzzDeleteObjectVersionRestoration$$' -fuzztime=10000x -parallel=4
	$(GO) test ./internal/services/aws/s3 -run '^$$' -fuzz '^FuzzDeleteObjectMissingKeyVersionIsIdempotent$$' -fuzztime=10000x -parallel=4
	$(GO) test ./internal/services/aws/s3 -run '^$$' -fuzz '^FuzzDeleteObjectUnversionedMissingKeyVersions$$' -fuzztime=10000x -parallel=4
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
	$(GO) test ./internal/services/aws/s3 -run '^$$' -fuzz '^FuzzCopySourcePreconditions$$' -fuzztime=10000x -parallel=4
	$(GO) test ./internal/services/aws/s3 -run '^$$' -fuzz '^FuzzCopyObjectSSECustomerKeys$$' -fuzztime=10000x -parallel=4
	$(GO) test ./internal/services/aws/states -run '^$$' -fuzz '^FuzzJSONPath$$' -fuzztime=10000x -parallel=4
	$(GO) test ./internal/services/gcp/gcs -run '^$$' -fuzz '^FuzzParsePath$$' -fuzztime=10000x -parallel=4
	$(GO) test ./internal/services/gcp/gcs -run '^$$' -fuzz '^FuzzObjectBytes$$' -fuzztime=10000x -parallel=4
	$(GO) test ./internal/proto/aws/restjson -run '^$$' -fuzz '^FuzzVercelRoute$$' -fuzztime=10000x -parallel=4
	$(GO) test ./internal/proto/aws/restjson -run '^$$' -fuzz '^FuzzCloudflareRoute$$' -fuzztime=10000x -parallel=4
	$(GO) test ./internal/proto/aws/restxml -run '^$$' -fuzz '^FuzzAzureRoute$$' -fuzztime=10000x -parallel=4
	$(GO) test ./internal/bundled -run '^$$' -fuzz '^FuzzBlobBytes$$' -fuzztime=10000x -parallel=4
	$(GO) test ./internal/proto/aws/restjson -run '^$$' -fuzz '^FuzzDigitalOceanRoute$$' -fuzztime=10000x -parallel=4
	$(GO) test ./internal/proto/graphql -run '^$$' -fuzz '^FuzzCodec$$' -fuzztime=10000x -parallel=4

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

test-race:
	CGO_ENABLED=1 $(GO) test -race $$($(GO) list ./... | grep -v '/internal/mutation$$')

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
