// Package elasticbeanstalk stores applications, versions, and environments (no EC2/ASG deploy).
package elasticbeanstalk

import (
	"context"
	"encoding/json"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func init() {
	registry.Register(registry.Factory{ServiceID: "aws.elasticbeanstalk", Tier: model.TierEmulate, New: func(d spi.Deps) (spi.BehaviorPack, error) {
		return New(d), nil
	}})
}

// Pack implements Elastic Beanstalk-lite.
type Pack struct{ deps spi.Deps }

// New constructs the pack.
func New(d spi.Deps) *Pack { return &Pack{deps: d} }

func (p *Pack) ServiceID() string { return "aws.elasticbeanstalk" }
func (p *Pack) Tier() model.Tier  { return model.TierEmulate }
func (p *Pack) Operations() []string {
	return []string{
		"CreateApplication", "DescribeApplications", "UpdateApplication", "DeleteApplication",
		"CreateApplicationVersion", "DescribeApplicationVersions", "DeleteApplicationVersion",
		"CreateEnvironment", "DescribeEnvironments", "UpdateEnvironment", "TerminateEnvironment",
	}
}

func (p *Pack) col(req *spi.Request, n string) spi.Collection {
	return p.deps.Store.Scope(req.Identity.Account, req.Identity.Region).Collection(n)
}

func (p *Pack) Invoke(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	app := first(req.Input, "ApplicationName")
	switch req.Operation {
	case "CreateApplication", "UpdateApplication":
		if app == "" {
			return nil, &spi.Fault{Code: "InvalidParameterValue", HTTPStatus: 400, Fault: "client"}
		}
		rec := map[string]any{"ApplicationName": app, "Description": first(req.Input, "Description")}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "ebapp").Put(ctx, app, b)
		return &spi.Response{Output: map[string]any{"Application": rec}}, nil
	case "DescribeApplications":
		return listOrGet(ctx, p.col(req, "ebapp"), app, "Applications")
	case "DeleteApplication":
		_ = p.col(req, "ebapp").Delete(ctx, app)
		return &spi.Response{Output: map[string]any{}}, nil
	case "CreateApplicationVersion":
		ver := first(req.Input, "VersionLabel")
		rec := map[string]any{"ApplicationName": app, "VersionLabel": ver, "Status": "UNPROCESSED"}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "ebver:"+app).Put(ctx, ver, b)
		return &spi.Response{Output: map[string]any{"ApplicationVersion": rec}}, nil
	case "DescribeApplicationVersions":
		return listOrGet(ctx, p.col(req, "ebver:"+app), first(req.Input, "VersionLabel"), "ApplicationVersions")
	case "DeleteApplicationVersion":
		_ = p.col(req, "ebver:"+app).Delete(ctx, first(req.Input, "VersionLabel"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "CreateEnvironment", "UpdateEnvironment":
		env := first(req.Input, "EnvironmentName")
		if env == "" {
			env = p.deps.Rand.Hex(8)
		}
		rec := map[string]any{
			"EnvironmentName": env, "ApplicationName": app, "Status": "Ready", "Health": "Green",
			"EnvironmentId": "e-" + p.deps.Rand.Hex(8), "VersionLabel": first(req.Input, "VersionLabel"),
		}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "ebenv").Put(ctx, env, b)
		return &spi.Response{Output: rec}, nil
	case "DescribeEnvironments":
		return listOrGet(ctx, p.col(req, "ebenv"), first(req.Input, "EnvironmentName"), "Environments")
	case "TerminateEnvironment":
		env := first(req.Input, "EnvironmentName")
		b, ok, _ := p.col(req, "ebenv").Get(ctx, env)
		rec := map[string]any{"EnvironmentName": env, "Status": "Terminated"}
		if ok {
			_ = json.Unmarshal(b, &rec)
			rec["Status"] = "Terminated"
		}
		_ = p.col(req, "ebenv").Delete(ctx, env)
		return &spi.Response{Output: rec}, nil
	default:
		return nil, spi.NotImplemented("aws.elasticbeanstalk", req.Operation, "emulate")
	}
}

func listOrGet(ctx context.Context, c spi.Collection, want, key string) (*spi.Response, error) {
	if want != "" {
		b, ok, _ := c.Get(ctx, want)
		if !ok {
			return &spi.Response{Output: map[string]any{key: []any{}}}, nil
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		return &spi.Response{Output: map[string]any{key: []any{rec}}}, nil
	}
	kvs, _, _ := c.List(ctx, "", "", 0)
	var items []any
	for _, kv := range kvs {
		var rec map[string]any
		_ = json.Unmarshal(kv.Value, &rec)
		items = append(items, rec)
	}
	return &spi.Response{Output: map[string]any{key: items}}, nil
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
