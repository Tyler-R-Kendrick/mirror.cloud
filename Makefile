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
	$(GO) test ./...

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
	$(GO) test ./internal/edge ./internal/identity ./internal/services/aws/dynamodb/expr ./internal/services/gcp/gcs -count=1

test-fuzz:
	$(GO) test ./internal/edge -run '^$$' -fuzz '^FuzzDeframeAWSChunked$$' -fuzztime=10s -parallel=4
	$(GO) test ./internal/identity -run '^$$' -fuzz '^FuzzParse$$' -fuzztime=10s -parallel=4
	$(GO) test ./internal/services/aws/dynamodb/expr -run '^$$' -fuzz '^FuzzEvalBool$$' -fuzztime=10s -parallel=4
	$(GO) test ./internal/services/aws/dynamodb/expr -run '^$$' -fuzz '^FuzzApplyUpdate$$' -fuzztime=10s -parallel=4
	$(GO) test ./internal/services/gcp/gcs -run '^$$' -fuzz '^FuzzParsePath$$' -fuzztime=10s -parallel=4

test-mutation:
	$(GO) test ./internal/mutation -count=1 -parallel 4 -timeout 300s

test-race:
	CGO_ENABLED=1 $(GO) test -race $$($(GO) list ./... | grep -v '/internal/mutation$$')

test-coverage:
	$(GO) test ./... -covermode=atomic -coverprofile=coverage.out
	@pct=$$($(GO) tool cover -func=coverage.out | awk '/^total:/ {gsub("%", "", $$3); print $$3}'); awk -v got="$$pct" 'BEGIN { if (got < 60) { print "coverage " got "% is below 60%"; exit 1 } }'

vet:
	$(GO) vet ./...
	@out=$$(gofmt -l $$(find . -name '*.go' -not -path './node_modules/*') || true); if [ -n "$$out" ]; then echo "$$out"; exit 1; fi

fmt:
	gofmt -w $$(find . -name '*.go' -not -path './node_modules/*')

generate:
	$(GO) run ./cmd/mirrorgen

specs-sync:
	bash scripts/specs-sync.sh
