BIN := bin
GO  := go
export CGO_ENABLED := 0

.PHONY: all build test vet fmt generate specs-sync

all: build

build:
	mkdir -p $(BIN)
	$(GO) build -o $(BIN)/mirror ./cmd/mirror
	$(GO) build -o $(BIN)/awslocal ./cmd/awslocal
	$(GO) build -o $(BIN)/gcslocal ./cmd/gcslocal

test:
	$(GO) test ./...

test-fuzz:
	$(GO) test ./internal/edge -fuzz=FuzzDeframeAWSChunked -fuzztime=10s
	$(GO) test ./internal/services/dynamodb/expr -fuzz=FuzzEvalBool -fuzztime=10s

test-mutation:
	$(GO) test ./internal/mutation -count=1 -timeout 120s

vet:
	$(GO) vet ./...
	@out=$$(gofmt -l $$(find . -name '*.go' -not -path './node_modules/*') || true); if [ -n "$$out" ]; then echo "$$out"; exit 1; fi

fmt:
	gofmt -w $$(find . -name '*.go' -not -path './node_modules/*')

generate:
	$(GO) run ./cmd/mirrorgen --catalog

specs-sync:
	bash scripts/specs-sync.sh
