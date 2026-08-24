// Package codebuild stores projects and builds (no Docker/agent).
package codebuild

import (
	"context"
	"encoding/json"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func init() {
	registry.Register(registry.Factory{ServiceID: "aws.codebuild", Tier: model.TierEmulate, New: func(d spi.Deps) (spi.BehaviorPack, error) {
		return New(d), nil
	}})
}

// Pack implements CodeBuild-lite.
type Pack struct{ deps spi.Deps }

// New constructs the pack.
func New(d spi.Deps) *Pack { return &Pack{deps: d} }

func (p *Pack) ServiceID() string { return "aws.codebuild" }
func (p *Pack) Tier() model.Tier  { return model.TierEmulate }
func (p *Pack) Operations() []string {
	return []string{
		"CreateProject", "BatchGetProjects", "ListProjects", "UpdateProject", "DeleteProject",
		"StartBuild", "BatchGetBuilds", "ListBuilds", "StopBuild",
	}
}

func (p *Pack) col(req *spi.Request, n string) spi.Collection {
	return p.deps.Store.Scope(req.Identity.Account, req.Identity.Region).Collection(n)
}

func (p *Pack) Invoke(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	name := first(req.Input, "name", "Name", "projectName", "ProjectName")
	switch req.Operation {
	case "CreateProject", "UpdateProject":
		if name == "" {
			if in, ok := req.Input["project"].(map[string]any); ok {
				name = first(in, "name", "Name")
			}
		}
		if name == "" {
			name = first(req.Input, "name")
		}
		rec := map[string]any{"name": name, "arn": "arn:aws:codebuild:" + req.Identity.Region + ":" + req.Identity.Account + ":project/" + name, "source": req.Input["source"], "environment": req.Input["environment"]}
		if rec["name"] == "" {
			return nil, &spi.Fault{Code: "InvalidInputException", HTTPStatus: 400, Fault: "client"}
		}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "cbproj").Put(ctx, name, b)
		return &spi.Response{Output: map[string]any{"project": rec}}, nil
	case "BatchGetProjects":
		names := stringList(req.Input["names"])
		var items []any
		for _, n := range names {
			b, ok, _ := p.col(req, "cbproj").Get(ctx, n)
			if !ok {
				continue
			}
			var rec map[string]any
			_ = json.Unmarshal(b, &rec)
			items = append(items, rec)
		}
		return &spi.Response{Output: map[string]any{"projects": items}}, nil
	case "ListProjects":
		kvs, _, _ := p.col(req, "cbproj").List(ctx, "", "", 0)
		var names []any
		for _, kv := range kvs {
			names = append(names, kv.Key)
		}
		return &spi.Response{Output: map[string]any{"projects": names}}, nil
	case "DeleteProject":
		_ = p.col(req, "cbproj").Delete(ctx, name)
		return &spi.Response{Output: map[string]any{}}, nil
	case "StartBuild":
		id := name + ":" + p.deps.Rand.Hex(8)
		rec := map[string]any{"id": id, "projectName": name, "buildStatus": "SUCCEEDED"}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "cbbuild").Put(ctx, id, b)
		return &spi.Response{Output: map[string]any{"build": rec}}, nil
	case "BatchGetBuilds":
		ids := stringList(req.Input["ids"])
		var items []any
		for _, id := range ids {
			b, ok, _ := p.col(req, "cbbuild").Get(ctx, id)
			if !ok {
				continue
			}
			var rec map[string]any
			_ = json.Unmarshal(b, &rec)
			items = append(items, rec)
		}
		return &spi.Response{Output: map[string]any{"builds": items}}, nil
	case "ListBuilds":
		kvs, _, _ := p.col(req, "cbbuild").List(ctx, "", "", 0)
		var ids []any
		for _, kv := range kvs {
			ids = append(ids, kv.Key)
		}
		return &spi.Response{Output: map[string]any{"ids": ids}}, nil
	case "StopBuild":
		id := first(req.Input, "id")
		b, ok, _ := p.col(req, "cbbuild").Get(ctx, id)
		if ok {
			var rec map[string]any
			_ = json.Unmarshal(b, &rec)
			rec["buildStatus"] = "STOPPED"
			nb, _ := json.Marshal(rec)
			_ = p.col(req, "cbbuild").Put(ctx, id, nb)
			return &spi.Response{Output: map[string]any{"build": rec}}, nil
		}
		return &spi.Response{Output: map[string]any{}}, nil
	default:
		return nil, spi.NotImplemented("aws.codebuild", req.Operation, "emulate")
	}
}

func stringList(v any) []string {
	switch t := v.(type) {
	case []any:
		out := make([]string, 0, len(t))
		for _, x := range t {
			if s, ok := x.(string); ok {
				out = append(out, s)
			}
		}
		return out
	case []string:
		return t
	}
	return nil
}

func first(in map[string]any, keys ...string) string {
	for _, k := range keys {
		if s, ok := in[k].(string); ok && s != "" {
			return s
		}
	}
	return ""
}
