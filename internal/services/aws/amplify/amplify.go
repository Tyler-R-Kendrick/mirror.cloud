// Package amplify stores apps, branches, and jobs (no build/hosting).
package amplify

import (
	"context"
	"encoding/json"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func init() {
	registry.Register(registry.Factory{ServiceID: "aws.amplify", Tier: model.TierEmulate, New: func(d spi.Deps) (spi.BehaviorPack, error) {
		return New(d), nil
	}})
}

// Pack implements Amplify-lite.
type Pack struct{ deps spi.Deps }

// New constructs the pack.
func New(d spi.Deps) *Pack { return &Pack{deps: d} }

func (p *Pack) ServiceID() string { return "aws.amplify" }
func (p *Pack) Tier() model.Tier  { return model.TierEmulate }
func (p *Pack) Operations() []string {
	return []string{
		"CreateApp", "GetApp", "ListApps", "UpdateApp", "DeleteApp",
		"CreateBranch", "GetBranch", "ListBranches", "DeleteBranch",
		"StartJob", "GetJob", "ListJobs",
	}
}

func (p *Pack) col(req *spi.Request, n string) spi.Collection {
	return p.deps.Store.Scope(req.Identity.Account, req.Identity.Region).Collection(n)
}

func (p *Pack) Invoke(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	id := first(req.Input, "appId", "AppId")
	switch req.Operation {
	case "CreateApp", "UpdateApp":
		if id == "" {
			id = p.deps.Rand.Hex(8)
		}
		name := first(req.Input, "name", "Name")
		rec := map[string]any{"appId": id, "name": name, "defaultDomain": id + ".amplifyapp.localhost"}
		for k, v := range req.Input {
			rec[k] = v
		}
		rec["appId"] = id
		b, _ := json.Marshal(rec)
		_ = p.col(req, "ampapp").Put(ctx, id, b)
		return &spi.Response{Output: map[string]any{"app": rec}}, nil
	case "GetApp":
		b, ok, _ := p.col(req, "ampapp").Get(ctx, id)
		if !ok {
			return nil, &spi.Fault{Code: "NotFoundException", HTTPStatus: 400, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		return &spi.Response{Output: map[string]any{"app": rec}}, nil
	case "ListApps":
		kvs, _, _ := p.col(req, "ampapp").List(ctx, "", "", 0)
		var items []any
		for _, kv := range kvs {
			var rec map[string]any
			_ = json.Unmarshal(kv.Value, &rec)
			items = append(items, rec)
		}
		return &spi.Response{Output: map[string]any{"apps": items}}, nil
	case "DeleteApp":
		_ = p.col(req, "ampapp").Delete(ctx, id)
		return &spi.Response{Output: map[string]any{}}, nil
	case "CreateBranch":
		br := first(req.Input, "branchName", "BranchName")
		rec := map[string]any{"branchName": br, "appId": id, "activeJobId": ""}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "ampbr:"+id).Put(ctx, br, b)
		return &spi.Response{Output: map[string]any{"branch": rec}}, nil
	case "GetBranch":
		br := first(req.Input, "branchName", "BranchName")
		b, ok, _ := p.col(req, "ampbr:"+id).Get(ctx, br)
		if !ok {
			return nil, &spi.Fault{Code: "NotFoundException", HTTPStatus: 400, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		return &spi.Response{Output: map[string]any{"branch": rec}}, nil
	case "ListBranches":
		kvs, _, _ := p.col(req, "ampbr:"+id).List(ctx, "", "", 0)
		var items []any
		for _, kv := range kvs {
			var rec map[string]any
			_ = json.Unmarshal(kv.Value, &rec)
			items = append(items, rec)
		}
		return &spi.Response{Output: map[string]any{"branches": items}}, nil
	case "DeleteBranch":
		_ = p.col(req, "ampbr:"+id).Delete(ctx, first(req.Input, "branchName", "BranchName"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "StartJob":
		jid := p.deps.Rand.Hex(8)
		rec := map[string]any{"jobId": jid, "appId": id, "branchName": first(req.Input, "branchName"), "status": "SUCCEED"}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "ampjob:"+id).Put(ctx, jid, b)
		return &spi.Response{Output: map[string]any{"jobSummary": rec}}, nil
	case "GetJob":
		jid := first(req.Input, "jobId", "JobId")
		b, ok, _ := p.col(req, "ampjob:"+id).Get(ctx, jid)
		if !ok {
			return nil, &spi.Fault{Code: "NotFoundException", HTTPStatus: 400, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		return &spi.Response{Output: map[string]any{"job": rec}}, nil
	case "ListJobs":
		kvs, _, _ := p.col(req, "ampjob:"+id).List(ctx, "", "", 0)
		var items []any
		for _, kv := range kvs {
			var rec map[string]any
			_ = json.Unmarshal(kv.Value, &rec)
			items = append(items, rec)
		}
		return &spi.Response{Output: map[string]any{"jobSummaries": items}}, nil
	default:
		return nil, spi.NotImplemented("aws.amplify", req.Operation, "emulate")
	}
}

func first(in map[string]any, keys ...string) string {
	if in == nil {
		return ""
	}
	for _, k := range keys {
		if s, ok := in[k].(string); ok && s != "" {
			return s
		}
	}
	return ""
}
