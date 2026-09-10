// Package v2 is the emulate-tier DigitalOcean API v2 droplets+domains control plane.
package v2

import (
	"context"
	"encoding/json"
	"strconv"
	"strings"
	"sync"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func init() {
	registry.Register(registry.Factory{ServiceID: "digitalocean.v2", Tier: model.TierEmulate, New: func(d spi.Deps) (spi.BehaviorPack, error) {
		return New(d), nil
	}})
}

// Pack implements digitalocean.v2.
type Pack struct {
	deps spi.Deps
	// ponytail: process-wide lock; per-account locks if concurrent create throughput matters.
	mu sync.Mutex
}

// New constructs the pack.
func New(d spi.Deps) *Pack { return &Pack{deps: d} }

func (p *Pack) ServiceID() string { return "digitalocean.v2" }
func (p *Pack) Tier() model.Tier  { return model.TierEmulate }
func (p *Pack) Operations() []string {
	return []string{"CreateDroplet", "ListDroplets", "GetDroplet", "DeleteDroplet", "CreateDomain", "ListDomains", "GetDomain", "DeleteDomain"}
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
	case "CreateDroplet":
		return p.createDroplet(ctx, req)
	case "ListDroplets":
		return p.listDroplets(ctx, req)
	case "GetDroplet":
		return p.getDroplet(ctx, req)
	case "DeleteDroplet":
		return p.deleteDroplet(ctx, req)
	case "CreateDomain":
		return p.createDomain(ctx, req)
	case "ListDomains":
		return p.listDomains(ctx, req)
	case "GetDomain":
		return p.getDomain(ctx, req)
	case "DeleteDomain":
		return p.deleteDomain(ctx, req)
	default:
		return nil, spi.NotImplemented("digitalocean.v2", req.Operation, "emulate")
	}
}

func (p *Pack) createDroplet(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	name := str(req.Input["name"])
	if name == "" {
		return nil, doFault("unprocessable_entity", "droplet name is required", 422)
	}
	id := p.deps.Rand.Intn(1<<30) + 1
	rec := map[string]any{
		"id": id, "name": name, "status": "active",
		"region": str(req.Input["region"]), "size": str(req.Input["size"]), "image": str(req.Input["image"]),
	}
	_ = p.col(req, "dodrop").Put(ctx, strconv.Itoa(id), mustJSON(rec))
	return &spi.Response{Output: map[string]any{"_wrap": "droplet", "droplet": rec}}, nil
}

func (p *Pack) listDroplets(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	return p.list(ctx, req, "dodrop", "droplets")
}

func (p *Pack) getDroplet(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	id := str(req.Input["id"])
	b, ok, _ := p.col(req, "dodrop").Get(ctx, id)
	if !ok {
		return nil, doFault("not_found", "The resource you were accessing could not be found.", 404)
	}
	var rec map[string]any
	_ = json.Unmarshal(b, &rec)
	return &spi.Response{Output: map[string]any{"_wrap": "droplet", "droplet": rec}}, nil
}

func (p *Pack) deleteDroplet(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	if _, err := p.getDroplet(ctx, req); err != nil {
		return nil, err
	}
	_ = p.col(req, "dodrop").Delete(ctx, str(req.Input["id"]))
	return &spi.Response{Status: 204}, nil
}

func (p *Pack) createDomain(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	name := strings.ToLower(str(req.Input["name"]))
	if name == "" {
		return nil, doFault("unprocessable_entity", "domain name is required", 422)
	}
	if _, exists, _ := p.col(req, "dodom").Get(ctx, name); exists {
		return nil, doFault("conflict", "name already exists", 409)
	}
	rec := map[string]any{"name": name, "ttl": 1800}
	_ = p.col(req, "dodom").Put(ctx, name, mustJSON(rec))
	return &spi.Response{Output: map[string]any{"_wrap": "domain", "domain": rec}}, nil
}

func (p *Pack) listDomains(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	return p.list(ctx, req, "dodom", "domains")
}

func (p *Pack) getDomain(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	name := strings.ToLower(str(req.Input["name"]))
	b, ok, _ := p.col(req, "dodom").Get(ctx, name)
	if !ok {
		return nil, doFault("not_found", "The resource you were accessing could not be found.", 404)
	}
	var rec map[string]any
	_ = json.Unmarshal(b, &rec)
	return &spi.Response{Output: map[string]any{"_wrap": "domain", "domain": rec}}, nil
}

func (p *Pack) deleteDomain(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	if _, err := p.getDomain(ctx, req); err != nil {
		return nil, err
	}
	_ = p.col(req, "dodom").Delete(ctx, strings.ToLower(str(req.Input["name"])))
	return &spi.Response{Status: 204}, nil
}

func (p *Pack) list(ctx context.Context, req *spi.Request, col, key string) (*spi.Response, error) {
	kvs, _, _ := p.col(req, col).List(ctx, "", "", 0)
	var items []any
	for _, kv := range kvs {
		var rec map[string]any
		if json.Unmarshal(kv.Value, &rec) == nil {
			items = append(items, rec)
		}
	}
	if items == nil {
		items = []any{}
	}
	return &spi.Response{Output: map[string]any{"_list": items, "_wrap": key}}, nil
}

func hydrate(req *spi.Request) {
	parts := strings.Split(strings.Trim(req.HTTP.URL.Path, "/"), "/")
	if len(parts) >= 3 && parts[0] == "v2" && parts[1] == "droplets" {
		req.Input["id"] = parts[2]
	}
	if len(parts) >= 3 && parts[0] == "v2" && parts[1] == "domains" {
		req.Input["name"] = parts[2]
	}
}

func doFault(code, msg string, status int) *spi.Fault {
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
