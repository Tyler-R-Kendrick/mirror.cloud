// Package api is the emulate-tier Cloudflare REST v4 KV control plane.
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
	registry.Register(registry.Factory{ServiceID: "cloudflare.kv", Tier: model.TierEmulate, New: func(d spi.Deps) (spi.BehaviorPack, error) {
		return New(d), nil
	}})
}

// Pack implements cloudflare.kv.
type Pack struct {
	deps spi.Deps
	// ponytail: process-wide lock; per-account locks if concurrent create throughput matters.
	mu sync.Mutex
}

// New constructs the pack.
func New(d spi.Deps) *Pack { return &Pack{deps: d} }

func (p *Pack) ServiceID() string { return "cloudflare.kv" }
func (p *Pack) Tier() model.Tier  { return model.TierEmulate }
func (p *Pack) Operations() []string {
	return []string{"CreateNamespace", "ListNamespaces", "GetNamespace", "PutValue", "GetValue", "DeleteValue"}
}

func (p *Pack) col(req *spi.Request, n string) spi.Collection {
	acct := str(req.Input["account_id"])
	if acct == "" {
		acct = req.Identity.Account
	}
	return p.deps.Store.Scope(acct, req.Identity.Region).Collection(n)
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
	case "CreateNamespace":
		return p.createNamespace(ctx, req)
	case "ListNamespaces":
		return p.listNamespaces(ctx, req)
	case "GetNamespace":
		return p.getNamespace(ctx, req)
	case "PutValue":
		return p.putValue(ctx, req)
	case "GetValue":
		return p.getValue(ctx, req)
	case "DeleteValue":
		return p.deleteValue(ctx, req)
	default:
		return nil, spi.NotImplemented("cloudflare.kv", req.Operation, "emulate")
	}
}

func (p *Pack) createNamespace(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	title := str(req.Input["title"])
	if title == "" {
		return nil, cfFault("10007", "Title is required", 400)
	}
	if _, exists, _ := p.col(req, "cfkvns").Get(ctx, "title:"+title); exists {
		return nil, cfFault("10014", "A namespace with this account ID and title already exists", 400)
	}
	id := p.deps.Rand.Hex(16)
	rec := map[string]any{"id": id, "title": title, "supports_url_encoding": true}
	_ = p.col(req, "cfkvns").Put(ctx, id, mustJSON(rec))
	_ = p.col(req, "cfkvns").Put(ctx, "title:"+title, []byte(id))
	return &spi.Response{Output: rec}, nil
}

func (p *Pack) listNamespaces(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	kvs, _, _ := p.col(req, "cfkvns").List(ctx, "", "", 0)
	var items []any
	for _, kv := range kvs {
		if strings.HasPrefix(kv.Key, "title:") {
			continue
		}
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

func (p *Pack) getNamespace(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	id := str(req.Input["namespace_id"])
	if id == "" {
		return nil, cfFault("10013", "Namespace not found", 404)
	}
	b, ok, _ := p.col(req, "cfkvns").Get(ctx, id)
	if !ok {
		return nil, cfFault("10013", "Namespace not found", 404)
	}
	var rec map[string]any
	_ = json.Unmarshal(b, &rec)
	return &spi.Response{Output: rec}, nil
}

func (p *Pack) requireNS(ctx context.Context, req *spi.Request) error {
	_, err := p.getNamespace(ctx, req)
	return err
}

func (p *Pack) values(req *spi.Request) spi.Collection {
	return p.col(req, "cfkv:"+str(req.Input["namespace_id"]))
}

func (p *Pack) putValue(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	if err := p.requireNS(ctx, req); err != nil {
		return nil, err
	}
	key := str(req.Input["key"])
	if key == "" {
		return nil, cfFault("10007", "Key is required", 400)
	}
	_ = p.values(req).Put(ctx, key, []byte(str(req.Input["value"])))
	return &spi.Response{Output: map[string]any{"_null": true}}, nil
}

func (p *Pack) getValue(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	if err := p.requireNS(ctx, req); err != nil {
		return nil, err
	}
	key := str(req.Input["key"])
	b, ok, _ := p.values(req).Get(ctx, key)
	if !ok {
		return nil, cfFault("10009", "key not found", 404)
	}
	return &spi.Response{Output: map[string]any{"_raw": string(b)}}, nil
}

func (p *Pack) deleteValue(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	if err := p.requireNS(ctx, req); err != nil {
		return nil, err
	}
	key := str(req.Input["key"])
	_, ok, _ := p.values(req).Get(ctx, key)
	if !ok {
		return nil, cfFault("10009", "key not found", 404)
	}
	_ = p.values(req).Delete(ctx, key)
	return &spi.Response{Output: map[string]any{"_null": true}}, nil
}

func hydrate(req *spi.Request) {
	parts := strings.Split(strings.Trim(req.HTTP.URL.Path, "/"), "/")
	if len(parts) >= 2 && parts[0] == "client" && parts[1] == "v4" {
		parts = parts[2:]
	}
	if len(parts) >= 2 && parts[0] == "accounts" {
		req.Input["account_id"] = parts[1]
	}
	if len(parts) >= 6 && parts[4] == "namespaces" {
		if len(parts) >= 6 {
			req.Input["namespace_id"] = parts[5]
		}
		if len(parts) >= 8 && parts[6] == "values" {
			req.Input["key"] = strings.Join(parts[7:], "/")
		}
	}
}

func cfFault(code, msg string, status int) *spi.Fault {
	return &spi.Fault{Code: code, Message: msg, HTTPStatus: status, Fault: "client"}
}

func mustJSON(v any) []byte {
	b, _ := json.Marshal(v)
	return b
}

func str(v any) string {
	switch t := v.(type) {
	case nil:
		return ""
	case string:
		return t
	default:
		return ""
	}
}
