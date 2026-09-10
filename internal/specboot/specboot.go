// Package specboot builds the process model: the catalog's list of services,
// with every wire fact taken from the models generated from the vendored
// specifications.
package specboot

import (
	"sync"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/catalog"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
)

var (
	once sync.Once
	got  *model.Bundle
)

// Bundle returns the model the runtime serves.
//
// It used to walk `specs/` and ingest whatever it found, falling back to the
// catalog when the directory was absent. That made the served model depend on
// local state: a checkout that had run `make specs-sync` served a different
// set of services from one that had not, and where a spec-derived ID differed
// from the catalog's -- `aws.models.lex` against `aws.lex-models`,
// `aws.api.sagemaker` against `aws.sagemaker` -- the bundle carried both, as
// two services sharing one endpoint.
//
// `internal/generated` is that same ingestion, run once, committed, and
// checked by CI to follow byte-for-byte from the pinned lock. Reading it
// instead makes the served model a property of the repository rather than of
// the machine, and there is nothing left for a second ingestion to add.
func Bundle() *model.Bundle {
	once.Do(func() {
		got = catalog.Bundle()
		adoptGenerated(got)
	})
	return got
}
