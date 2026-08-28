// Package bundled registers every Behavior IR bundle as a served service.
//
// This is the whole per-service Go cost of a data-defined service: none. A
// bundle under behavior/<provider>/<service>/ plus a generated model under
// internal/generated/ is a registered spi.BehaviorPack, because this package
// walks the two embedded sets and pairs them by service ID. Adding a service
// touches specs/ and behavior/ and nothing here.
//
// Registration happens in init, like the hand-written packs, so the edge is
// unchanged: it still resolves a service through the registry and cannot tell
// whether the answer came from Go or from YAML.
//
// A bundle whose model is missing, or which fails validation against that
// model, is a build-time fact rather than a runtime surprise: the factory
// returns the error and registry.New refuses to start. TestEveryBundleRegisters
// makes that failure visible in CI instead of at `mirror up`.
package bundled

import (
	"fmt"

	behaviors "github.com/tyler-r-kendrick/mirror.cloud/behavior"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/engine"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/generated"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func init() {
	for _, id := range ServiceIDs() {
		registry.Register(registry.Factory{
			ServiceID: id,
			Tier:      model.TierEmulate,
			New:       factory(id),
		})
	}
}

// ServiceIDs lists the services served from bundles, sorted.
func ServiceIDs() []string { return behaviors.ServiceIDs() }

// New builds the engine for one bundled service. Exported so tests and tools
// can construct a bundled service without going through the registry.
func New(id string, deps spi.Deps) (spi.BehaviorPack, error) {
	svc, err := generated.Model(id)
	if err != nil {
		return nil, fmt.Errorf("bundled %s: %w", id, err)
	}
	ir, err := behaviors.Load(id, svc)
	if err != nil {
		return nil, fmt.Errorf("bundled %s: %w", id, err)
	}
	return engine.New(deps, ir, svc)
}

// factory captures the ID so each registered entry builds its own service.
func factory(id string) func(spi.Deps) (spi.BehaviorPack, error) {
	return func(deps spi.Deps) (spi.BehaviorPack, error) { return New(id, deps) }
}
