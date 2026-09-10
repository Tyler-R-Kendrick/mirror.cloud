// Package api is the emulate-tier Hostinger DNS/domains REST control plane.
package api

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
	registry.Register(registry.Factory{ServiceID: "hostinger.dns", Tier: model.TierEmulate, New: func(d spi.Deps) (spi.BehaviorPack, error) {
		return New(d), nil
	}})
}

// Pack implements hostinger.dns.
type Pack struct {
	deps spi.Deps
	// ponytail: process-wide lock; per-account locks if concurrent create throughput matters.
	mu sync.Mutex
}

// New constructs the pack.
func New(d spi.Deps) *Pack { return &Pack{deps: d} }

func (p *Pack) ServiceID() string { return "hostinger.dns" }
func (p *Pack) Tier() model.Tier  { return model.TierEmulate }
func (p *Pack) Operations() []string {
	return []string{"CreateDomain", "ListDomains", "GetDomain", "GetDNSRecords", "UpdateDNSRecords", "DeleteDNSRecords"}
}

func (p *Pack) col(req *spi.Request, n string) spi.Collection {
	return p.deps.Store.Scope(req.Identity.Account, req.Identity.Region).Collection(n)
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
	case "CreateDomain":
		return p.createDomain(ctx, req)
	case "ListDomains":
		return p.listDomains(ctx, req)
	case "GetDomain":
		return p.getDomain(ctx, req)
	case "GetDNSRecords":
		return p.getDNS(ctx, req)
	case "UpdateDNSRecords":
		return p.updateDNS(ctx, req)
	case "DeleteDNSRecords":
		return p.deleteDNS(ctx, req)
	default:
		return nil, spi.NotImplemented("hostinger.dns", req.Operation, "emulate")
	}
}

func (p *Pack) createDomain(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	domain := strings.ToLower(str(req.Input["domain"]))
	if domain == "" {
		return nil, hsFault("validation_error", "Domain is required", 422)
	}
	if _, exists, _ := p.col(req, "hsdom").Get(ctx, domain); exists {
		return nil, hsFault("conflict", "Domain already exists", 409)
	}
	rec := map[string]any{"domain": domain, "status": "active"}
	_ = p.col(req, "hsdom").Put(ctx, domain, mustJSON(rec))
	_ = p.col(req, "hszone").Put(ctx, domain, mustJSON([]any{}))
	return &spi.Response{Output: rec}, nil
}

func (p *Pack) listDomains(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	kvs, _, _ := p.col(req, "hsdom").List(ctx, "", "", 0)
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
	return &spi.Response{Output: map[string]any{"_list": items}}, nil
}

func (p *Pack) getDomain(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	domain := strings.ToLower(str(req.Input["domain"]))
	b, ok, _ := p.col(req, "hsdom").Get(ctx, domain)
	if !ok {
		return nil, hsFault("not_found", "Domain not found", 404)
	}
	var rec map[string]any
	_ = json.Unmarshal(b, &rec)
	return &spi.Response{Output: rec}, nil
}

func (p *Pack) getDNS(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	if _, err := p.getDomain(ctx, req); err != nil {
		return nil, err
	}
	domain := strings.ToLower(str(req.Input["domain"]))
	b, ok, _ := p.col(req, "hszone").Get(ctx, domain)
	if !ok {
		return &spi.Response{Output: map[string]any{"_list": []any{}}}, nil
	}
	var zone []any
	_ = json.Unmarshal(b, &zone)
	if zone == nil {
		zone = []any{}
	}
	return &spi.Response{Output: map[string]any{"_list": zone}}, nil
}

func (p *Pack) updateDNS(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	if _, err := p.getDomain(ctx, req); err != nil {
		return nil, err
	}
	zone, _ := req.Input["zone"].([]any)
	if zone == nil {
		return nil, hsFault("validation_error", "zone is required", 422)
	}
	domain := strings.ToLower(str(req.Input["domain"]))
	overwrite := truthy(req.Input["overwrite"])
	if !overwrite {
		b, ok, _ := p.col(req, "hszone").Get(ctx, domain)
		var existing []any
		if ok {
			_ = json.Unmarshal(b, &existing)
		}
		zone = append(existing, zone...)
	}
	_ = p.col(req, "hszone").Put(ctx, domain, mustJSON(zone))
	return &spi.Response{Output: map[string]any{"message": "Request accepted"}}, nil
}

func (p *Pack) deleteDNS(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	if _, err := p.getDomain(ctx, req); err != nil {
		return nil, err
	}
	domain := strings.ToLower(str(req.Input["domain"]))
	_ = p.col(req, "hszone").Put(ctx, domain, mustJSON([]any{}))
	return &spi.Response{Output: map[string]any{"message": "Request accepted"}}, nil
}

func hydrate(req *spi.Request) {
	parts := strings.Split(strings.Trim(req.HTTP.URL.Path, "/"), "/")
	if len(parts) >= 5 && parts[0] == "api" && parts[1] == "dns" && parts[3] == "zones" {
		req.Input["domain"] = parts[4]
	}
	if len(parts) >= 5 && parts[0] == "api" && parts[1] == "domains" && parts[3] == "portfolio" {
		req.Input["domain"] = parts[4]
	}
}

func hsFault(code, msg string, status int) *spi.Fault {
	return &spi.Fault{Code: code, Message: msg, HTTPStatus: status, Fault: "client"}
}

func mustJSON(v any) []byte {
	b, _ := json.Marshal(v)
	return b
}

func truthy(v any) bool {
	b, _ := v.(bool)
	return b
}

func str(v any) string {
	s, _ := v.(string)
	return s
}
