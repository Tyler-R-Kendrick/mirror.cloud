package engine

import (
	"context"
	"sync"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

// AfterInvokeHook runs after a successful B-IR operation, outside the
// engine mutex and after durable effects are committed. Used for CI/CD
// dispatch (CodeBuild StartBuild → process executor) without embedding
// provider branches in eval.
type AfterInvokeHook func(ctx context.Context, deps spi.Deps, req *spi.Request, resp *spi.Response) error

var (
	afterMu    sync.RWMutex
	afterHooks = map[string][]AfterInvokeHook{} // "serviceID/Operation"
)

// RegisterAfterInvoke appends a post-success hook for serviceID+operation.
// Hooks run in registration order; a hook error fails the Invoke.
func RegisterAfterInvoke(serviceID, operation string, h AfterInvokeHook) {
	if h == nil {
		return
	}
	key := serviceID + "/" + operation
	afterMu.Lock()
	afterHooks[key] = append(afterHooks[key], h)
	afterMu.Unlock()
}

func runAfterInvoke(ctx context.Context, serviceID string, deps spi.Deps, req *spi.Request, resp *spi.Response) error {
	key := serviceID + "/" + req.Operation
	afterMu.RLock()
	hooks := append([]AfterInvokeHook(nil), afterHooks[key]...)
	afterMu.RUnlock()
	for _, h := range hooks {
		if err := h(ctx, deps, req, resp); err != nil {
			return err
		}
	}
	return nil
}
