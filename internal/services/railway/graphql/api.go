// Package graphql is the emulate-tier Railway GraphQL v2 project+service control plane.
package graphql

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
	registry.Register(registry.Factory{ServiceID: "railway.graphql", Tier: model.TierEmulate, New: func(d spi.Deps) (spi.BehaviorPack, error) {
		return New(d), nil
	}})
}

// Pack implements railway.graphql.
type Pack struct {
	deps spi.Deps
	// ponytail: process-wide lock; per-account locks if concurrent create throughput matters.
	mu sync.Mutex
}

// New constructs the pack.
func New(d spi.Deps) *Pack { return &Pack{deps: d} }

func (p *Pack) ServiceID() string { return "railway.graphql" }
func (p *Pack) Tier() model.Tier  { return model.TierEmulate }
func (p *Pack) Operations() []string {
	return []string{"projectCreate", "projects", "project", "projectDelete", "serviceCreate", "service", "serviceDelete"}
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
	hydrate(req)
	switch req.Operation {
	case "projectCreate":
		return p.projectCreate(ctx, req)
	case "projects":
		return p.projects(ctx, req)
	case "project":
		return p.project(ctx, req)
	case "projectDelete":
		return p.projectDelete(ctx, req)
	case "serviceCreate":
		return p.serviceCreate(ctx, req)
	case "service":
		return p.service(ctx, req)
	case "serviceDelete":
		return p.serviceDelete(ctx, req)
	default:
		return nil, spi.NotImplemented("railway.graphql", req.Operation, "emulate")
	}
}

func (p *Pack) projectCreate(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	name := str(req.Input["name"])
	if name == "" {
		return nil, rwFault("BAD_USER_INPUT", "project name is required", 200)
	}
	id := p.deps.Rand.Hex(8)
	rec := map[string]any{"id": id, "name": name}
	_ = p.col(req, "rwproj").Put(ctx, id, mustJSON(rec))
	return &spi.Response{Output: map[string]any{"_wrap": "projectCreate", "projectCreate": rec}}, nil
}

func (p *Pack) projects(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	kvs, _, _ := p.col(req, "rwproj").List(ctx, "", "", 0)
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
	return &spi.Response{Output: map[string]any{"_list": items, "_wrap": "projects"}}, nil
}

func (p *Pack) project(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	id := str(req.Input["id"])
	b, ok, _ := p.col(req, "rwproj").Get(ctx, id)
	if !ok {
		return nil, rwFault("NOT_FOUND", "Project not found", 200)
	}
	var rec map[string]any
	_ = json.Unmarshal(b, &rec)
	return &spi.Response{Output: map[string]any{"_wrap": "project", "project": rec}}, nil
}

func (p *Pack) projectDelete(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	got, err := p.project(ctx, req)
	if err != nil {
		return nil, err
	}
	_ = got
	_ = p.col(req, "rwproj").Delete(ctx, str(req.Input["id"]))
	return &spi.Response{Output: map[string]any{"_wrap": "projectDelete", "projectDelete": true}}, nil
}

func (p *Pack) serviceCreate(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	name := str(req.Input["name"])
	if name == "" {
		return nil, rwFault("BAD_USER_INPUT", "service name is required", 200)
	}
	pid := str(req.Input["projectId"])
	if pid == "" {
		pid = str(req.Input["projectID"])
	}
	if _, err := p.project(ctx, &spi.Request{Identity: req.Identity, Input: map[string]any{"id": pid}}); err != nil {
		return nil, err
	}
	id := p.deps.Rand.Hex(8)
	rec := map[string]any{"id": id, "name": name, "projectId": pid}
	_ = p.col(req, "rwsvc").Put(ctx, id, mustJSON(rec))
	return &spi.Response{Output: map[string]any{"_wrap": "serviceCreate", "serviceCreate": rec}}, nil
}

func (p *Pack) service(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	id := str(req.Input["id"])
	b, ok, _ := p.col(req, "rwsvc").Get(ctx, id)
	if !ok {
		return nil, rwFault("NOT_FOUND", "Service not found", 200)
	}
	var rec map[string]any
	_ = json.Unmarshal(b, &rec)
	return &spi.Response{Output: map[string]any{"_wrap": "service", "service": rec}}, nil
}

func (p *Pack) serviceDelete(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	got, err := p.service(ctx, req)
	if err != nil {
		return nil, err
	}
	_ = got
	_ = p.col(req, "rwsvc").Delete(ctx, str(req.Input["id"]))
	return &spi.Response{Output: map[string]any{"_wrap": "serviceDelete", "serviceDelete": true}}, nil
}

func hydrate(req *spi.Request) {
	if vars, ok := req.Input["variables"].(map[string]any); ok {
		for k, v := range vars {
			if str(req.Input[k]) == "" {
				req.Input[k] = v
			}
		}
		if in, ok := vars["input"].(map[string]any); ok {
			for k, v := range in {
				if str(req.Input[k]) == "" {
					req.Input[k] = v
				}
			}
		}
	}
	if in, ok := req.Input["input"].(map[string]any); ok {
		for k, v := range in {
			if str(req.Input[k]) == "" {
				req.Input[k] = v
			}
		}
	}
}

func rwFault(code, msg string, status int) *spi.Fault {
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

// FieldName maps a GraphQL query document to one core field. Longest names first.
func FieldName(q string) string {
	switch {
	case strings.Contains(q, "projectCreate"):
		return "projectCreate"
	case strings.Contains(q, "projectDelete"):
		return "projectDelete"
	case strings.Contains(q, "serviceCreate"):
		return "serviceCreate"
	case strings.Contains(q, "serviceDelete"):
		return "serviceDelete"
	case strings.Contains(q, "projects"):
		return "projects"
	case strings.Contains(q, "project"):
		return "project"
	case strings.Contains(q, "service"):
		return "service"
	default:
		return "Unknown"
	}
}
