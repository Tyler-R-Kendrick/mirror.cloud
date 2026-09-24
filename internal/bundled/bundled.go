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
	"slices"
	"sync"

	behaviors "github.com/tyler-r-kendrick/mirror.cloud/behavior"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/engine"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/generated"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/specboot"
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
//
// Both are computed once: the answer is a fact of the embedded behavior/ tree,
// which cannot change after process start, and recomputing it re-parses and
// re-validates every bundle (about half a second, paid per test binary --
// which is where a mutation shard's budget goes).
var serviceLists = sync.OnceValues(func() ([]string, map[string]string) {
	var serving []string
	shadow := map[string]string{}
	for _, id := range behaviors.ServiceIDs() {
		reason, err := shadowOf(id)
		if err == nil && reason != "" {
			shadow[id] = reason
			continue
		}
		serving = append(serving, id)
	}
	return serving, shadow
})

func ServiceIDs() []string {
	serving, _ := serviceLists()
	return serving
}

// ShadowIDs lists the bundles that are gated but not yet serving, each with
// the reason it is not.
func ShadowIDs() map[string]string {
	_, shadow := serviceLists()
	return shadow
}

// servedModel returns the model a bundle is built against: the one the runtime
// will serve it with.
//
// It used to call generated.Model(id) directly, which is the same shape of
// mistake #296 through #299 were about -- building a service against one
// description while the edge serves it through another. Here it was not a
// divergence but a ceiling. Four services are served under an ID the
// specification does not use, because the SDKs still send the older wire name:
// CloudWatch signs as `monitoring`, ELBv2 as `elasticloadbalancing`, and the
// catalog shortened two more. generated.Model has no entry under those names,
// so those four could never be extracted to bundles at all -- silently, since
// nothing tries until someone writes the YAML.
//
// specboot.Bundle() is the model the edge routes with, mapping included, so
// reading it here means a bundle is validated against exactly what will serve
// it. The operations there are the specification's unioned with any the
// catalog carried, which can only admit bundles that generated.Model would
// have refused; nothing that loaded before stops loading.
func servedModel(id string) (*model.Service, error) {
	if svc := specboot.Bundle().ServiceByID(id); svc != nil {
		return svc, nil
	}
	// A bundle with no service in the booted model has nothing to be served
	// as. generated.Model gives the better error, naming the missing model.
	return generated.Model(id)
}

// shadowOf reports a bundle's shadow reason. Loading is cheap enough to do
// once per bundle at init, and a bundle that cannot load at all is caught by
// the factory rather than silently treated as shadowed.
func shadowOf(id string) (string, error) {
	svc, err := servedModel(id)
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
	e, err := compiled(id, deps)
	if err != nil {
		return nil, err
	}
	var p spi.BehaviorPack = e
	if len(e.IR().Native) != 0 {
		p = hybrid{Engine: e, deps: deps}
	}
	if w := wraps[id]; w != nil && e.IR().Wrap != "" {
		p = wrapped{p, deps, w}
	}
	return p, nil
}

// Wrap runs around every request a bundle serves: it may rewrite the request
// before next and the response after it.
type Wrap func(ctx context.Context, deps spi.Deps, req *spi.Request, next func(context.Context, *spi.Request) (*spi.Response, error)) (*spi.Response, error)

// wraps is written only from package init, before any New runs.
var wraps = map[string]Wrap{}

// RegisterWrap supplies the Go a bundle declares under wrap:.
func RegisterWrap(id string, w Wrap) { wraps[id] = w }

type wrapped struct {
	spi.BehaviorPack
	deps spi.Deps
	wrap Wrap
}

func (w wrapped) Invoke(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	return w.wrap(ctx, w.deps, req, w.BehaviorPack.Invoke)
}

// NativeFunc serves one operation a bundle lists under native:.
type NativeFunc func(context.Context, spi.Deps, *spi.Request) (*spi.Response, error)

// natives is written only from package init, before any New runs.
var natives = map[string]NativeFunc{}

// RegisterNative supplies the Go for an operation a bundle declares native.
// Call it from init; the bundle's native: list is what makes it served.
func RegisterNative(id, op string, fn NativeFunc) { natives[id+"/"+op] = fn }

// Worker runs a service's background loop against deps and answers how to
// stop it.
type Worker func(spi.Deps) (stop func() error)

// workers is written only from package init, before any factory runs.
var workers = map[string]Worker{}

// RegisterWorker supplies the loop a bundle declares under worker:.
func RegisterWorker(id string, w Worker) { workers[id] = w }

// withWorker is a registered service whose loop runs while it does; the
// registry closes it on shutdown.
type withWorker struct {
	spi.BehaviorPack
	stop func() error
}

func (w withWorker) Close() error { return w.stop() }

// Unregistered lists every id/op a bundle declares native that no linked
// package registered, and every id/worker and id/wrap likewise. A binary missing one still serves the rest of that
// bundle -- CloudFormation reaches API Gateway's control plane without
// linking ExecuteApi -- so this is checked where every service is linked.
func Unregistered() []string {
	var missing []string
	for _, id := range ServiceIDs() {
		svc, err := servedModel(id)
		if err != nil {
			continue
		}
		if ir, err := behaviors.Load(id, svc); err == nil {
			for _, op := range ir.Native {
				if natives[id+"/"+op] == nil {
					missing = append(missing, id+"/"+op)
				}
			}
			if ir.Worker != "" && workers[id] == nil {
				missing = append(missing, id+"/worker")
			}
			if ir.Wrap != "" && wraps[id] == nil {
				missing = append(missing, id+"/wrap")
			}
		}
	}
	return missing
}

// hybrid is a bundle with native operations: the engine serves everything the
// bundle defines, and the listed operations go to Go against the same store.
type hybrid struct {
	*engine.Engine
	deps spi.Deps
}

func (h hybrid) Operations() []string {
	return append(h.Engine.Operations(), h.Engine.IR().Native...)
}

func (h hybrid) Invoke(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	if !slices.Contains(h.Engine.IR().Native, req.Operation) {
		return h.Engine.Invoke(ctx, req)
	}
	if fn := natives[h.ServiceID()+"/"+req.Operation]; fn != nil {
		return fn(ctx, h.deps, req)
	}
	return nil, fmt.Errorf("bundled %s: native %s has no RegisterNative; link its package", h.ServiceID(), req.Operation)
}

func compiled(id string, deps spi.Deps) (*engine.Engine, error) {
	builtMu.Lock()
	proto, ok := built[id]
	builtMu.Unlock()
	if ok {
		return proto.WithDeps(deps)
	}

	svc, err := servedModel(id)
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
	return func(deps spi.Deps) (spi.BehaviorPack, error) {
		p, err := New(id, deps)
		if err != nil || workers[id] == nil {
			return p, err
		}
		return withWorker{p, workers[id](deps)}, nil
	}
}
