package execution

import (
	"context"
	"fmt"
	"sync"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

// EnsuringMiniflare ensures every KV/D1/R2/queue name used by execute effects
// exists in the live Apply graph (and a tiny Worker binding) before the action
// runs. That closes the public REST → Worker same-resource bridge for
// long-lived sessions without requiring callers to Apply by hand.
type EnsuringMiniflare struct {
	Session *MiniflareSession
	Inner   spi.Executor
	mu      sync.Mutex
	kv      map[string]struct{}
	d1      map[string]struct{}
	r2      map[string]struct{}
	queues  map[string]struct{}
}

// Execute applies missing bindings then delegates.
func (e *EnsuringMiniflare) Execute(ctx context.Context, account, region, action string, args map[string]any) (map[string]any, error) {
	if err := e.ensure(ctx, action, args); err != nil {
		return nil, err
	}
	if e.Inner == nil {
		return nil, fmt.Errorf("execution: EnsuringMiniflare: no inner executor")
	}
	return e.Inner.Execute(ctx, account, region, action, args)
}

func (e *EnsuringMiniflare) ensure(ctx context.Context, action string, args map[string]any) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.kv == nil {
		e.kv, e.d1, e.r2, e.queues = map[string]struct{}{}, map[string]struct{}{}, map[string]struct{}{}, map[string]struct{}{}
	}
	changed := false
	add := func(set map[string]struct{}, name string) {
		if name == "" || name == "<nil>" {
			return
		}
		if _, ok := set[name]; ok {
			return
		}
		set[name] = struct{}{}
		changed = true
	}
	switch {
	case stringsHasPrefix(action, "kv."):
		ns, _ := args["namespace"].(string)
		add(e.kv, ns)
	case action == "d1.query" || action == "d1.exec":
		add(e.d1, fmt.Sprint(args["database"]))
	case stringsHasPrefix(action, "r2."):
		add(e.r2, fmt.Sprint(args["bucket"]))
	case action == "queue.send":
		add(e.queues, fmt.Sprint(args["queue"]))
	}
	if !changed {
		return nil
	}
	kvNames := mapKeys(e.kv)
	d1Names := mapKeys(e.d1)
	r2Names := mapKeys(e.r2)
	qNames := mapKeys(e.queues)
	script := ensureWorkerScript()
	worker := map[string]any{
		"name": "app", "script": script, "compatibilityDate": "2025-01-01",
		"kvNamespaces": kvNames, "d1Databases": d1Names, "r2Buckets": r2Names, "queueProducers": qNames,
	}
	graph := map[string]any{
		"workers": []any{worker}, "kvNamespaces": kvNames,
		"d1Databases": d1Names, "r2Buckets": r2Names, "queueProducers": qNames,
	}
	if _, err := e.Session.Apply(ctx, graph); err != nil {
		return fmt.Errorf("execution: ensure apply: %w", err)
	}
	return nil
}

func ensureWorkerScript() string {
	return "export default { async fetch(req, env) {\n" +
		"  const u = new URL(req.url);\n" +
		"  const name = u.searchParams.get('ns') || Object.keys(env).find(k => env[k] && env[k].get);\n" +
		"  const KV = name ? env[name] : null;\n" +
		"  if (!KV || !KV.get) return new Response('no-ns', {status:404});\n" +
		"  if (req.method === 'PUT') { await KV.put(u.searchParams.get('k'), await req.text()); return new Response('stored'); }\n" +
		"  const v = await KV.get(u.searchParams.get('k'));\n" +
		"  return new Response(v === null ? 'miss' : 'v=' + v);\n" +
		"}};"
}

func stringsHasPrefix(s, p string) bool {
	return len(s) >= len(p) && s[:len(p)] == p
}

func mapKeys(m map[string]struct{}) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
