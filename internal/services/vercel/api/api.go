// Package api is the emulate-tier Vercel REST API pack (projects, deployments, env, KV).
package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func init() {
	registry.Register(registry.Factory{ServiceID: "vercel.api", Tier: model.TierEmulate, New: func(d spi.Deps) (spi.BehaviorPack, error) {
		return New(d), nil
	}})
}

// Pack implements vercel.api.
type Pack struct {
	deps spi.Deps
	// ponytail: process-wide lock; per-account locks if concurrent create/KV throughput matters.
	mu sync.Mutex
}

// New constructs the pack.
func New(d spi.Deps) *Pack { return &Pack{deps: d} }

func (p *Pack) ServiceID() string { return "vercel.api" }
func (p *Pack) Tier() model.Tier  { return model.TierEmulate }
func (p *Pack) Operations() []string {
	return []string{
		"GetUser", "CreateProject", "ListProjects", "GetProject", "DeleteProject",
		"ListProjectEnv", "CreateProjectEnv", "DeleteProjectEnv",
		"ListProjectDomains", "AddProjectDomain",
		"CreateDeployment", "ListDeployments", "GetDeployment", "DeleteDeployment",
		"KvCommand",
	}
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
	case "GetUser":
		return &spi.Response{Output: map[string]any{
			"id": "usr_" + req.Identity.Account, "email": "test@mirror.cloud", "name": "Mirror", "username": "test",
		}}, nil
	case "CreateProject":
		return p.createProject(ctx, req)
	case "ListProjects":
		return p.listJSON(ctx, req, "vproj", "projects")
	case "GetProject":
		return p.getJSON(ctx, req, "vproj", str(req.Input["id"]))
	case "DeleteProject":
		return p.deleteProject(ctx, req)
	case "CreateProjectEnv":
		return p.createEnv(ctx, req)
	case "ListProjectEnv":
		return p.listPrefixed(ctx, req, "venv", p.projectID(ctx, req)+"/", "envs")
	case "DeleteProjectEnv":
		return p.deleteJSON(ctx, req, "venv", p.projectID(ctx, req)+"/"+str(req.Input["envId"]))
	case "AddProjectDomain":
		return p.addDomain(ctx, req)
	case "ListProjectDomains":
		return p.listPrefixed(ctx, req, "vdom", p.projectID(ctx, req)+"/", "domains")
	case "CreateDeployment":
		return p.createDeployment(ctx, req)
	case "ListDeployments":
		return p.listJSON(ctx, req, "vdeploy", "deployments")
	case "GetDeployment":
		return p.getJSON(ctx, req, "vdeploy", str(req.Input["id"]))
	case "DeleteDeployment":
		id := str(req.Input["id"])
		if _, err := p.getJSON(ctx, req, "vdeploy", id); err != nil {
			return nil, err
		}
		_ = p.col(req, "vdeploy").Delete(ctx, id)
		return &spi.Response{Output: map[string]any{"uid": id, "state": "DELETED"}}, nil
	case "KvCommand":
		return p.kv(ctx, req)
	default:
		return nil, spi.NotImplemented("vercel.api", req.Operation, "emulate")
	}
}

func (p *Pack) createProject(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	name := str(req.Input["name"])
	if name == "" {
		return nil, &spi.Fault{Code: "bad_request", Message: "Project name is required", HTTPStatus: 400, Fault: "client"}
	}
	if _, exists, _ := p.col(req, "vproj").Get(ctx, "name:"+name); exists {
		return nil, &spi.Fault{Code: "conflict", Message: "A project with the same name already exists.", HTTPStatus: 409, Fault: "client"}
	}
	id := "prj_" + p.deps.Rand.Hex(12)
	now := p.deps.Clock.Now().UnixMilli()
	rec := map[string]any{"id": id, "name": name, "accountId": req.Identity.Account, "createdAt": now, "updatedAt": now, "framework": req.Input["framework"]}
	_ = p.col(req, "vproj").Put(ctx, id, mustJSON(rec))
	_ = p.col(req, "vproj").Put(ctx, "name:"+name, []byte(id))
	return &spi.Response{Output: rec}, nil
}

func (p *Pack) createEnv(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	got, err := p.getJSON(ctx, req, "vproj", str(req.Input["id"]))
	if err != nil {
		return nil, err
	}
	pid := str(got.Output["id"])
	if str(req.Input["key"]) == "" {
		return nil, &spi.Fault{Code: "bad_request", Message: "Env key is required", HTTPStatus: 400, Fault: "client"}
	}
	eid := "env_" + p.deps.Rand.Hex(10)
	target := req.Input["target"]
	if target == nil {
		target = []any{"production", "preview", "development"}
	}
	rec := map[string]any{"id": eid, "key": str(req.Input["key"]), "value": str(req.Input["value"]), "type": first(req.Input["type"], "encrypted"), "target": target}
	_ = p.col(req, "venv").Put(ctx, pid+"/"+eid, mustJSON(rec))
	return &spi.Response{Output: rec}, nil
}

func (p *Pack) addDomain(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	got, err := p.getJSON(ctx, req, "vproj", str(req.Input["id"]))
	if err != nil {
		return nil, err
	}
	pid := str(got.Output["id"])
	name := str(req.Input["name"])
	if name == "" {
		return nil, &spi.Fault{Code: "bad_request", Message: "Domain name is required", HTTPStatus: 400, Fault: "client"}
	}
	now := p.deps.Clock.Now().UnixMilli()
	rec := map[string]any{"name": name, "apexName": name, "projectId": pid, "verified": true, "createdAt": now, "updatedAt": now}
	_ = p.col(req, "vdom").Put(ctx, pid+"/"+name, mustJSON(rec))
	return &spi.Response{Output: rec}, nil
}

func (p *Pack) createDeployment(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	name := str(req.Input["name"])
	if name == "" {
		return nil, &spi.Fault{Code: "bad_request", Message: "Deployment name is required", HTTPStatus: 400, Fault: "client"}
	}
	project := str(req.Input["project"])
	if project == "" {
		project = name
	}
	pid := project
	if !strings.HasPrefix(project, "prj_") {
		b, ok, _ := p.col(req, "vproj").Get(ctx, "name:"+project)
		if ok {
			pid = string(b)
		} else {
			created, err := p.createProject(ctx, &spi.Request{Identity: req.Identity, Input: map[string]any{"name": project}})
			if err != nil {
				return nil, err
			}
			pid = str(created.Output["id"])
		}
	}
	id := "dpl_" + p.deps.Rand.Hex(16)
	now := p.deps.Clock.Now().UnixMilli()
	host := name + "-" + p.deps.Rand.Hex(6) + ".vercel.app"
	rec := map[string]any{
		"id": id, "uid": id, "name": name, "url": host, "projectId": pid,
		"readyState": "READY", "state": "READY", "createdAt": now, "created": now, "type": "LAMBDAS",
	}
	_ = p.col(req, "vdeploy").Put(ctx, id, mustJSON(rec))
	return &spi.Response{Output: rec}, nil
}

func (p *Pack) kv(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	cmd, _ := req.Input["_redis"].([]any)
	if len(cmd) == 0 {
		return nil, &spi.Fault{Code: "bad_request", Message: "redis command required", HTTPStatus: 400, Fault: "client"}
	}
	op := strings.ToUpper(str(cmd[0]))
	col := p.col(req, "vkv")
	switch op {
	case "SET":
		if len(cmd) < 3 {
			return nil, &spi.Fault{Code: "bad_request", Message: "SET needs key and value", HTTPStatus: 400, Fault: "client"}
		}
		_ = col.Put(ctx, str(cmd[1]), []byte(str(cmd[2])))
		return &spi.Response{Output: map[string]any{"result": "OK"}}, nil
	case "GET":
		if len(cmd) < 2 {
			return nil, &spi.Fault{Code: "bad_request", Message: "GET needs key", HTTPStatus: 400, Fault: "client"}
		}
		b, ok, _ := col.Get(ctx, str(cmd[1]))
		if !ok {
			return &spi.Response{Output: map[string]any{"result": nil}}, nil
		}
		return &spi.Response{Output: map[string]any{"result": string(b)}}, nil
	case "DEL":
		if len(cmd) < 2 {
			return nil, &spi.Fault{Code: "bad_request", Message: "DEL needs key", HTTPStatus: 400, Fault: "client"}
		}
		_, ok, _ := col.Get(ctx, str(cmd[1]))
		_ = col.Delete(ctx, str(cmd[1]))
		n := 0
		if ok {
			n = 1
		}
		return &spi.Response{Output: map[string]any{"result": n}}, nil
	default:
		return nil, spi.NotImplemented("vercel.api", "KvCommand."+op, "emulate")
	}
}

func (p *Pack) getJSON(ctx context.Context, req *spi.Request, col, key string) (*spi.Response, error) {
	if key == "" {
		return nil, &spi.Fault{Code: "not_found", Message: "The requested resource was not found.", HTTPStatus: 404, Fault: "client"}
	}
	b, ok, _ := p.col(req, col).Get(ctx, key)
	if !ok && col == "vproj" {
		b, ok, _ = p.col(req, col).Get(ctx, "name:"+key)
		if ok {
			b, ok, _ = p.col(req, col).Get(ctx, string(b))
		}
	}
	if !ok {
		return nil, &spi.Fault{Code: "not_found", Message: "The requested resource was not found.", HTTPStatus: 404, Fault: "client"}
	}
	var rec map[string]any
	_ = json.Unmarshal(b, &rec)
	return &spi.Response{Output: rec}, nil
}

func (p *Pack) projectID(ctx context.Context, req *spi.Request) string {
	got, err := p.getJSON(ctx, req, "vproj", str(req.Input["id"]))
	if err != nil {
		return str(req.Input["id"])
	}
	return str(got.Output["id"])
}

func (p *Pack) deleteProject(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	got, err := p.getJSON(ctx, req, "vproj", str(req.Input["id"]))
	if err != nil {
		return nil, err
	}
	id, name := str(got.Output["id"]), str(got.Output["name"])
	_ = p.col(req, "vproj").Delete(ctx, id)
	if name != "" {
		_ = p.col(req, "vproj").Delete(ctx, "name:"+name)
	}
	return &spi.Response{Status: http.StatusOK, Output: map[string]any{}}, nil
}

func (p *Pack) deleteJSON(ctx context.Context, req *spi.Request, col, key string) (*spi.Response, error) {
	if _, err := p.getJSON(ctx, req, col, key); err != nil {
		return nil, err
	}
	_ = p.col(req, col).Delete(ctx, key)
	return &spi.Response{Status: http.StatusOK, Output: map[string]any{}}, nil
}

func (p *Pack) listJSON(ctx context.Context, req *spi.Request, col, field string) (*spi.Response, error) {
	kvs, _, _ := p.col(req, col).List(ctx, "", "", 0)
	var items []any
	for _, kv := range kvs {
		if strings.HasPrefix(kv.Key, "name:") {
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
	return &spi.Response{Output: map[string]any{field: items, "pagination": map[string]any{"count": len(items)}}}, nil
}

func (p *Pack) listPrefixed(ctx context.Context, req *spi.Request, col, prefix, field string) (*spi.Response, error) {
	kvs, _, _ := p.col(req, col).List(ctx, "", "", 0)
	var items []any
	for _, kv := range kvs {
		if !strings.HasPrefix(kv.Key, prefix) {
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
	return &spi.Response{Output: map[string]any{field: items}}, nil
}

func hydrate(req *spi.Request) {
	parts := strings.Split(strings.Trim(req.HTTP.URL.Path, "/"), "/")
	if len(parts) > 0 && len(parts[0]) >= 2 && parts[0][0] == 'v' && parts[0][1] >= '0' && parts[0][1] <= '9' {
		parts = parts[1:]
	}
	if len(parts) >= 2 && (parts[0] == "projects" || parts[0] == "deployments") && str(req.Input["id"]) == "" {
		req.Input["id"] = parts[1]
	}
	if len(parts) >= 4 && parts[2] == "env" {
		req.Input["envId"] = parts[3]
	}
}

func str(v any) string {
	switch t := v.(type) {
	case nil:
		return ""
	case string:
		return t
	default:
		return fmt.Sprint(t)
	}
}

func first(v any, fallback string) string {
	if s := str(v); s != "" {
		return s
	}
	return fallback
}

func mustJSON(v any) []byte {
	b, _ := json.Marshal(v)
	return b
}
