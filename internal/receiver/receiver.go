// Package receiver defines spec-ingestion. v1 ships smithy and discovery
// implementations; the interface is unchanged for future pact/openapi/har.
package receiver

import (
	"context"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
)

// Receiver ingests one specification format and emits canonical model
// fragments. v1 ships smithy and discovery receivers. The interface exists
// unchanged for future pact/openapi/har receivers; do not narrow it to
// cloud-provider specs.
type Receiver interface {
	// Name is a stable identifier, e.g. "smithy", "discovery".
	Name() string
	// Detect reports whether this receiver can parse the file at path.
	Detect(path string, head []byte) bool
	// Ingest parses one source file into services plus their provenance.
	Ingest(ctx context.Context, src model.SourceRef, data []byte) ([]model.Service, error)
}
