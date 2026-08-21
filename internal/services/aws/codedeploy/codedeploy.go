// Package codedeploy stores applications, groups, and deployments (no fleet agent).
package codedeploy

import (
	"context"
	"encoding/json"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func init() {
	registry.Register(registry.Factory{ServiceID: "aws.codedeploy", Tier: model.TierEmulate, New: func(d spi.Deps) (spi.BehaviorPack, error) {
		return New(d), nil
	}})
}

// Pack implements CodeDeploy-lite.
type Pack struct{ deps spi.Deps }

// New constructs the pack.
func New(d spi.Deps) *Pack { return &Pack{deps: d} }

func (p *Pack) ServiceID() string { return "aws.codedeploy" }
func (p *Pack) Tier() model.Tier  { return model.TierEmulate }
func (p *Pack) Operations() []string {
	return []string{
		"CreateApplication", "GetApplication", "ListApplications", "DeleteApplication",
		"CreateDeploymentGroup", "GetDeploymentGroup", "ListDeploymentGroups", "DeleteDeploymentGroup",
		"CreateDeployment", "GetDeployment", "ListDeployments", "StopDeployment",
	}
}

func (p *Pack) col(req *spi.Request, n string) spi.Collection {
	return p.deps.Store.Scope(req.Identity.Account, req.Identity.Region).Collection(n)
}

func (p *Pack) Invoke(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	app := first(req.Input, "applicationName", "ApplicationName")
	switch req.Operation {
	case "CreateApplication":
		if app == "" {
			return nil, &spi.Fault{Code: "InvalidApplicationNameException", HTTPStatus: 400, Fault: "client"}
		}
		rec := map[string]any{"applicationName": app, "applicationId": p.deps.Rand.Hex(8), "computePlatform": first(req.Input, "computePlatform")}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "cdapp").Put(ctx, app, b)
		return &spi.Response{Output: map[string]any{"applicationId": rec["applicationId"]}}, nil
	case "GetApplication":
		b, ok, _ := p.col(req, "cdapp").Get(ctx, app)
		if !ok {
			return nil, &spi.Fault{Code: "ApplicationDoesNotExistException", HTTPStatus: 400, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		return &spi.Response{Output: map[string]any{"application": rec}}, nil
	case "ListApplications":
		kvs, _, _ := p.col(req, "cdapp").List(ctx, "", "", 0)
		var names []any
		for _, kv := range kvs {
			names = append(names, kv.Key)
		}
		return &spi.Response{Output: map[string]any{"applications": names}}, nil
	case "DeleteApplication":
		_ = p.col(req, "cdapp").Delete(ctx, app)
		return &spi.Response{Output: map[string]any{}}, nil
	case "CreateDeploymentGroup":
		g := first(req.Input, "deploymentGroupName", "DeploymentGroupName")
		rec := map[string]any{"applicationName": app, "deploymentGroupName": g, "deploymentGroupId": p.deps.Rand.Hex(8)}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "cdg:"+app).Put(ctx, g, b)
		return &spi.Response{Output: map[string]any{"deploymentGroupId": rec["deploymentGroupId"]}}, nil
	case "GetDeploymentGroup":
		g := first(req.Input, "deploymentGroupName", "DeploymentGroupName")
		b, ok, _ := p.col(req, "cdg:"+app).Get(ctx, g)
		if !ok {
			return nil, &spi.Fault{Code: "DeploymentGroupDoesNotExistException", HTTPStatus: 400, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		return &spi.Response{Output: map[string]any{"deploymentGroupInfo": rec}}, nil
	case "ListDeploymentGroups":
		kvs, _, _ := p.col(req, "cdg:"+app).List(ctx, "", "", 0)
		var names []any
		for _, kv := range kvs {
			names = append(names, kv.Key)
		}
		return &spi.Response{Output: map[string]any{"deploymentGroups": names}}, nil
	case "DeleteDeploymentGroup":
		_ = p.col(req, "cdg:"+app).Delete(ctx, first(req.Input, "deploymentGroupName", "DeploymentGroupName"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "CreateDeployment":
		id := p.deps.Rand.Hex(8)
		rec := map[string]any{"deploymentId": id, "applicationName": app, "status": "Succeeded"}
		for k, v := range req.Input {
			rec[k] = v
		}
		rec["deploymentId"] = id
		rec["status"] = "Succeeded"
		b, _ := json.Marshal(rec)
		_ = p.col(req, "cddep").Put(ctx, id, b)
		return &spi.Response{Output: map[string]any{"deploymentId": id}}, nil
	case "GetDeployment":
		id := first(req.Input, "deploymentId", "DeploymentId")
		b, ok, _ := p.col(req, "cddep").Get(ctx, id)
		if !ok {
			return nil, &spi.Fault{Code: "DeploymentDoesNotExistException", HTTPStatus: 400, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		return &spi.Response{Output: map[string]any{"deploymentInfo": rec}}, nil
	case "ListDeployments":
		kvs, _, _ := p.col(req, "cddep").List(ctx, "", "", 0)
		var ids []any
		for _, kv := range kvs {
			ids = append(ids, kv.Key)
		}
		return &spi.Response{Output: map[string]any{"deployments": ids}}, nil
	case "StopDeployment":
		id := first(req.Input, "deploymentId", "DeploymentId")
		b, ok, _ := p.col(req, "cddep").Get(ctx, id)
		if ok {
			var rec map[string]any
			_ = json.Unmarshal(b, &rec)
			rec["status"] = "Stopped"
			nb, _ := json.Marshal(rec)
			_ = p.col(req, "cddep").Put(ctx, id, nb)
		}
		return &spi.Response{Output: map[string]any{"status": "Stopped"}}, nil
	default:
		return nil, spi.NotImplemented("aws.codedeploy", req.Operation, "emulate")
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
