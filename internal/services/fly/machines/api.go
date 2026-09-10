// Package machines is the emulate-tier Fly Machines API v1 apps+machines control plane.
package machines

import (
	"context"
	"encoding/json"
	"strings"
	"sync"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func init() {
	registry.Register(registry.Factory{ServiceID: "fly.machines", Tier: model.TierEmulate, New: func(d spi.Deps) (spi.BehaviorPack, error) {
		return New(d), nil
	}})
}

// Pack implements fly.machines.
type Pack struct {
	deps spi.Deps
	// ponytail: process-wide lock; per-account locks if concurrent create throughput matters.
	mu sync.Mutex
}

// New constructs the pack.
func New(d spi.Deps) *Pack { return &Pack{deps: d} }

func (p *Pack) ServiceID() string { return "fly.machines" }
func (p *Pack) Tier() model.Tier  { return model.TierEmulate }
func (p *Pack) Operations() []string {
	return []string{"CreateApp", "ListApps", "GetApp", "DeleteApp", "CreateMachine", "ListMachines", "GetMachine", "DeleteMachine"}
}

func (p *Pack) col(req *spi.Request, n string) spi.Collection {
	acct := req.Identity.Account
	if acct == "" {
		acct = "000000000000"
	}
	reg := req.Identity.Region
	if reg == "" {
		reg = "us-east-1"
	}
	return p.deps.Store.Scope(acct, reg).Collection(n)
}

func (p *Pack) Invoke(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if req.Input == nil {
		req.Input = map[string]any{}
	}
	if req.HTTP != nil {
		hydrate(req)
	}
	switch req.Operation {
	case "CreateApp":
		return p.createApp(ctx, req)
	case "ListApps":
		return p.listApps(ctx, req)
	case "GetApp":
		return p.getApp(ctx, req)
	case "DeleteApp":
		return p.deleteApp(ctx, req)
	case "CreateMachine":
		return p.createMachine(ctx, req)
	case "ListMachines":
		return p.listMachines(ctx, req)
	case "GetMachine":
		return p.getMachine(ctx, req)
	case "DeleteMachine":
		return p.deleteMachine(ctx, req)
	default:
		return nil, spi.NotImplemented("fly.machines", req.Operation, "emulate")
	}
}

func (p *Pack) createApp(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	name := str(req.Input["app_name"])
	if name == "" {
		name = str(req.Input["name"])
	}
	if name == "" {
		return nil, flyFault("invalid", "app name is required", 400)
	}
	if _, exists, _ := p.col(req, "flyapp").Get(ctx, name); exists {
		return nil, flyFault("taken", "app name already taken", 422)
	}
	org := str(req.Input["org_slug"])
	if org == "" {
		org = "personal"
	}
	id := p.deps.Rand.Hex(8)
	rec := map[string]any{
		"id": id, "name": name, "status": "pending",
		"organization": map[string]any{"name": org, "slug": org},
		"network":      str(req.Input["network"]),
		"created_at":   1,
	}
	if rec["network"] == "" {
		rec["network"] = "default"
	}
	_ = p.col(req, "flyapp").Put(ctx, name, mustJSON(rec))
	return &spi.Response{Status: 201, Output: map[string]any{"_wrap": "app", "app": map[string]any{"id": id, "created_at": rec["created_at"]}}}, nil
}

func (p *Pack) getApp(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	name := appName(req)
	b, ok, _ := p.col(req, "flyapp").Get(ctx, name)
	if !ok {
		return nil, flyFault("not_found", "app not found", 404)
	}
	var rec map[string]any
	_ = json.Unmarshal(b, &rec)
	return &spi.Response{Output: map[string]any{"_wrap": "app", "app": rec}}, nil
}

func (p *Pack) deleteApp(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	if _, err := p.getApp(ctx, req); err != nil {
		return nil, err
	}
	_ = p.col(req, "flyapp").Delete(ctx, appName(req))
	return &spi.Response{Status: 202}, nil
}

func (p *Pack) listApps(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	kvs, _, _ := p.col(req, "flyapp").List(ctx, "", "", 0)
	var items []any
	for _, kv := range kvs {
		var rec map[string]any
		if json.Unmarshal(kv.Value, &rec) == nil {
			items = append(items, map[string]any{
				"id": rec["id"], "name": rec["name"], "network": rec["network"],
				"machine_count": 0, "volume_count": 0,
			})
		}
	}
	if items == nil {
		items = []any{}
	}
	return &spi.Response{Output: map[string]any{"_list": items, "_wrap": "apps"}}, nil
}

func (p *Pack) createMachine(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	if _, err := p.getApp(ctx, req); err != nil {
		return nil, err
	}
	img := image(req)
	if img == "" {
		return nil, flyFault("invalid", "image is required", 400)
	}
	id := p.deps.Rand.Hex(8)
	name := str(req.Input["name"])
	if name == "" {
		name = id
	}
	rec := map[string]any{
		"id": id, "name": name, "state": "created", "region": str(req.Input["region"]),
		"app_name": appName(req), "config": map[string]any{"image": img},
	}
	_ = p.col(req, "flymach").Put(ctx, id, mustJSON(rec))
	return &spi.Response{Output: map[string]any{"_wrap": "machine", "machine": rec}}, nil
}

func (p *Pack) getMachine(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	id := str(req.Input["id"])
	b, ok, _ := p.col(req, "flymach").Get(ctx, id)
	if !ok {
		return nil, flyFault("not_found", "machine not found", 404)
	}
	var rec map[string]any
	_ = json.Unmarshal(b, &rec)
	return &spi.Response{Output: map[string]any{"_wrap": "machine", "machine": rec}}, nil
}

func (p *Pack) deleteMachine(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	if _, err := p.getMachine(ctx, req); err != nil {
		return nil, err
	}
	_ = p.col(req, "flymach").Delete(ctx, str(req.Input["id"]))
	return &spi.Response{Status: 200}, nil
}

func (p *Pack) listMachines(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	if _, err := p.getApp(ctx, req); err != nil {
		return nil, err
	}
	app := appName(req)
	kvs, _, _ := p.col(req, "flymach").List(ctx, "", "", 0)
	var items []any
	for _, kv := range kvs {
		var rec map[string]any
		if json.Unmarshal(kv.Value, &rec) == nil && str(rec["app_name"]) == app {
			items = append(items, rec)
		}
	}
	if items == nil {
		items = []any{}
	}
	return &spi.Response{Output: map[string]any{"_list": items, "_wrap": "machines"}}, nil
}

func hydrate(req *spi.Request) {
	parts := strings.Split(strings.Trim(req.HTTP.URL.Path, "/"), "/")
	if len(parts) >= 3 && parts[0] == "v1" && parts[1] == "apps" {
		if str(req.Input["app_name"]) == "" {
			req.Input["app_name"] = parts[2]
		}
		if len(parts) >= 5 && parts[3] == "machines" && str(req.Input["id"]) == "" {
			req.Input["id"] = parts[4]
		}
	}
	if cfg, ok := req.Input["config"].(map[string]any); ok {
		if str(req.Input["image"]) == "" {
			req.Input["image"] = str(cfg["image"])
		}
	}
}

func appName(req *spi.Request) string {
	if n := str(req.Input["app_name"]); n != "" {
		return n
	}
	return str(req.Input["name"])
}

func image(req *spi.Request) string {
	if img := str(req.Input["image"]); img != "" {
		return img
	}
	if cfg, ok := req.Input["config"].(map[string]any); ok {
		return str(cfg["image"])
	}
	return ""
}

func flyFault(code, msg string, status int) *spi.Fault {
	return &spi.Fault{Code: code, Message: msg, HTTPStatus: status, Fault: "client"}
}

func mustJSON(v any) []byte {
	b, _ := json.Marshal(v)
	return b
}

func str(v any) string {
	s, _ := v.(string)
	return s
}
