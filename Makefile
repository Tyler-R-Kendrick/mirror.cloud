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

vet:
	$(GO) vet ./...
	@out=$$(gofmt -l $$(find . -name '*.go' -not -path './node_modules/*') || true); if [ -n "$$out" ]; then echo "$$out"; exit 1; fi

fmt:
	gofmt -w $$(find . -name '*.go' -not -path './node_modules/*')

generate:
	$(GO) run ./cmd/mirrorgen --catalog

specs-sync:
	bash scripts/specs-sync.sh
