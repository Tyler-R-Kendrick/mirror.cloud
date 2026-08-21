// Package appconfig stores applications, environments, and hosted configuration (no Agent polling).
package appconfig

import (
	"context"
	"encoding/json"
	"strconv"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func init() {
	registry.Register(registry.Factory{ServiceID: "aws.appconfig", Tier: model.TierEmulate, New: func(d spi.Deps) (spi.BehaviorPack, error) {
		return New(d), nil
	}})
}

// Pack implements AppConfig-lite.
type Pack struct{ deps spi.Deps }

// New constructs the pack.
func New(d spi.Deps) *Pack { return &Pack{deps: d} }

func (p *Pack) ServiceID() string { return "aws.appconfig" }
func (p *Pack) Tier() model.Tier  { return model.TierEmulate }
func (p *Pack) Operations() []string {
	return []string{
		"CreateApplication", "GetApplication", "ListApplications", "DeleteApplication",
		"CreateEnvironment", "GetEnvironment", "ListEnvironments", "DeleteEnvironment",
		"CreateConfigurationProfile", "GetConfigurationProfile", "ListConfigurationProfiles", "DeleteConfigurationProfile",
		"CreateHostedConfigurationVersion", "GetHostedConfigurationVersion", "ListHostedConfigurationVersions",
		"StartDeployment", "GetDeployment", "GetLatestConfiguration",
	}
}

func (p *Pack) col(req *spi.Request, n string) spi.Collection {
	return p.deps.Store.Scope(req.Identity.Account, req.Identity.Region).Collection(n)
}

func (p *Pack) Invoke(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	switch req.Operation {
	case "CreateApplication":
		id := p.deps.Rand.Hex(8)
		rec := map[string]any{"Id": id, "Name": first(req.Input, "Name")}
		return put(ctx, p.col(req, "acapp"), id, rec)
	case "GetApplication":
		return get(ctx, p.col(req, "acapp"), first(req.Input, "ApplicationId"), "aws.appconfig")
	case "ListApplications":
		return listCol(ctx, p.col(req, "acapp"), "Items")
	case "DeleteApplication":
		_ = p.col(req, "acapp").Delete(ctx, first(req.Input, "ApplicationId"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "CreateEnvironment":
		id := p.deps.Rand.Hex(8)
		app := first(req.Input, "ApplicationId")
		rec := map[string]any{"Id": id, "ApplicationId": app, "Name": first(req.Input, "Name"), "State": "ReadyForDeployment"}
		return put(ctx, p.col(req, "acenv:"+app), id, rec)
	case "GetEnvironment":
		return get(ctx, p.col(req, "acenv:"+first(req.Input, "ApplicationId")), first(req.Input, "EnvironmentId"), "aws.appconfig")
	case "ListEnvironments":
		return listCol(ctx, p.col(req, "acenv:"+first(req.Input, "ApplicationId")), "Items")
	case "DeleteEnvironment":
		_ = p.col(req, "acenv:"+first(req.Input, "ApplicationId")).Delete(ctx, first(req.Input, "EnvironmentId"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "CreateConfigurationProfile":
		id := p.deps.Rand.Hex(8)
		app := first(req.Input, "ApplicationId")
		rec := map[string]any{"Id": id, "ApplicationId": app, "Name": first(req.Input, "Name"), "LocationUri": first(req.Input, "LocationUri")}
		return put(ctx, p.col(req, "acprof:"+app), id, rec)
	case "GetConfigurationProfile":
		return get(ctx, p.col(req, "acprof:"+first(req.Input, "ApplicationId")), first(req.Input, "ConfigurationProfileId"), "aws.appconfig")
	case "ListConfigurationProfiles":
		return listCol(ctx, p.col(req, "acprof:"+first(req.Input, "ApplicationId")), "Items")
	case "DeleteConfigurationProfile":
		_ = p.col(req, "acprof:"+first(req.Input, "ApplicationId")).Delete(ctx, first(req.Input, "ConfigurationProfileId"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "CreateHostedConfigurationVersion":
		app, prof := first(req.Input, "ApplicationId"), first(req.Input, "ConfigurationProfileId")
		kvs, _, _ := p.col(req, "acver:"+app+":"+prof).List(ctx, "", "", 0)
		ver := len(kvs) + 1
		content := first(req.Input, "Content")
		rec := map[string]any{"VersionNumber": ver, "Content": content, "ContentType": first(req.Input, "ContentType")}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "acver:"+app+":"+prof).Put(ctx, strconv.Itoa(ver), b)
		_ = p.col(req, "aclatest").Put(ctx, app+":"+prof, b)
		return &spi.Response{Output: rec}, nil
	case "GetHostedConfigurationVersion":
		app, prof := first(req.Input, "ApplicationId"), first(req.Input, "ConfigurationProfileId")
		return get(ctx, p.col(req, "acver:"+app+":"+prof), first(req.Input, "VersionNumber"), "aws.appconfig")
	case "ListHostedConfigurationVersions":
		return listCol(ctx, p.col(req, "acver:"+first(req.Input, "ApplicationId")+":"+first(req.Input, "ConfigurationProfileId")), "Items")
	case "StartDeployment":
		id := 1
		rec := map[string]any{"DeploymentNumber": id, "State": "COMPLETE", "ApplicationId": first(req.Input, "ApplicationId"), "EnvironmentId": first(req.Input, "EnvironmentId")}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "acdep").Put(ctx, "1", b)
		return &spi.Response{Output: rec}, nil
	case "GetDeployment":
		return get(ctx, p.col(req, "acdep"), first(req.Input, "DeploymentNumber"), "aws.appconfig")
	case "GetLatestConfiguration":
		b, ok, _ := p.col(req, "aclatest").Get(ctx, first(req.Input, "ApplicationId")+":"+first(req.Input, "ConfigurationProfileId"))
		if !ok {
			return &spi.Response{Output: map[string]any{"Content": ""}}, nil
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		return &spi.Response{Output: rec}, nil
	default:
		return nil, spi.NotImplemented("aws.appconfig", req.Operation, "emulate")
	}
}

func put(ctx context.Context, c spi.Collection, id string, rec map[string]any) (*spi.Response, error) {
	b, _ := json.Marshal(rec)
	_ = c.Put(ctx, id, b)
	return &spi.Response{Output: rec}, nil
}

func get(ctx context.Context, c spi.Collection, id, svc string) (*spi.Response, error) {
	b, ok, _ := c.Get(ctx, id)
	if !ok {
		return nil, &spi.Fault{Code: "ResourceNotFoundException", HTTPStatus: 400, Fault: "client"}
	}
	var rec map[string]any
	_ = json.Unmarshal(b, &rec)
	return &spi.Response{Output: rec}, nil
}

func listCol(ctx context.Context, c spi.Collection, key string) (*spi.Response, error) {
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
	for _, k := range keys {
		switch v := in[k].(type) {
		case string:
			if v != "" {
				return v
			}
		case float64:
			return strconv.Itoa(int(v))
		}
	}
	return ""
}
