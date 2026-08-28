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
	"context"
	"fmt"
	"sync"

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

// built caches one compiled engine per service, so the second and later calls
// for a service rebind dependencies instead of parsing and compiling the
// bundle again.
//
// Parsing, validating and compiling a bundle costs tens of milliseconds and
// yields something read-only; only the dependencies differ between callers.
// The cost matters because cross-service calls construct their target on
// demand: an EventBridge rule delivering to a queue, or a pipe polling one,
// asks for aws.sqs on every message.
var (
	builtMu sync.Mutex
	built   = map[string]*engine.Engine{}
)

// New builds the engine for one bundled service. Exported so tests and tools
// can construct a bundled service without going through the registry, and so
// one service can reach another the way the edge would.
func New(id string, deps spi.Deps) (spi.BehaviorPack, error) {
	builtMu.Lock()
	proto, ok := built[id]
	builtMu.Unlock()
	if ok {
		return proto.WithDeps(deps)
	}

	svc, err := generated.Model(id)
	if err != nil {
		return nil, fmt.Errorf("bundled %s: %w", id, err)
	}
	ir, err := behaviors.Load(id, svc)
	if err != nil {
		return nil, fmt.Errorf("bundled %s: %w", id, err)
	}
	e, err := engine.New(deps, ir, svc)
	if err != nil {
		return nil, err
	}
	// Two callers racing here both build and one cache entry wins, which costs
	// a duplicate compile and nothing else: an engine is read-only once built,
	// so either is as good as the other.
	builtMu.Lock()
	built[id] = e
	builtMu.Unlock()
	return e, nil
}

// Handler answers with the service serving id, carrying any construction
// failure into the call rather than to the caller.
//
// It exists for the delivery paths where one service reaches another as a
// single expression -- an EventBridge rule sending to a queue, a pipe polling
// one -- and where a build failure must not read as "delivered nothing". A
// caller that can act on the failure should use New instead.
func Handler(id string, deps spi.Deps) spi.BehaviorPack {
	p, err := New(id, deps)
	if err != nil {
		return broken{id: id, err: err}
	}
	return p
}

// broken stands in for a service that could not be built, so the failure is
// reported at the point of use instead of vanishing.
type broken struct {
	id  string
	err error
}

func (b broken) ServiceID() string    { return b.id }
func (b broken) Tier() model.Tier     { return model.TierEmulate }
func (b broken) Operations() []string { return nil }
func (b broken) Invoke(context.Context, *spi.Request) (*spi.Response, error) {
	return nil, b.err
}

// factory captures the ID so each registered entry builds its own service.
func factory(id string) func(spi.Deps) (spi.BehaviorPack, error) {
	return func(deps spi.Deps) (spi.BehaviorPack, error) { return New(id, deps) }
}
