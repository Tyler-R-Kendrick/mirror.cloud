.PHONY: specs-sync generate test vet fmt

specs-sync:
	./scripts/specs-sync.sh

generate:
	go run ./cmd/mirrorgen

test:
	go test ./...

vet:
	go vet ./...

fmt:
	gofmt -w .
