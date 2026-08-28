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

// ServiceIDs lists the services actually served from bundles, sorted. A shadow
// bundle is proven but not yet serving, so it is not here; ShadowIDs lists
// those.
func ServiceIDs() []string {
	var out []string
	for _, id := range behaviors.ServiceIDs() {
		if reason, _ := shadowOf(id); reason == "" {
			out = append(out, id)
		}
	}
	return out
}

// ShadowIDs lists the bundles that are gated but not yet serving, each with
// the reason it is not.
func ShadowIDs() map[string]string {
	out := map[string]string{}
	for _, id := range behaviors.ServiceIDs() {
		if reason, err := shadowOf(id); err == nil && reason != "" {
			out[id] = reason
		}
	}
	return out
}

// shadowOf reports a bundle's shadow reason. Loading is cheap enough to do
// once per bundle at init, and a bundle that cannot load at all is caught by
// the factory rather than silently treated as shadowed.
func shadowOf(id string) (string, error) {
	svc, err := generated.Model(id)
	if err != nil {
		return "", err
	}
	ir, err := behaviors.Load(id, svc)
	if err != nil {
		return "", err
	}
	return ir.Shadow, nil
}

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
