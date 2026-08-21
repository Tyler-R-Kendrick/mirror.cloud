// Package serverlessrepo stores application records (no SAR deploy).
package serverlessrepo

import (
	"context"
	"encoding/json"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func init() {
	registry.Register(registry.Factory{ServiceID: "aws.serverlessrepo", Tier: model.TierEmulate, New: func(d spi.Deps) (spi.BehaviorPack, error) {
		return New(d), nil
	}})
}

// Pack implements Serverless Application Repository-lite.
type Pack struct{ deps spi.Deps }

// New constructs the pack.
func New(d spi.Deps) *Pack { return &Pack{deps: d} }

func (p *Pack) ServiceID() string { return "aws.serverlessrepo" }
func (p *Pack) Tier() model.Tier  { return model.TierEmulate }
func (p *Pack) Operations() []string {
	return []string{"CreateApplication", "GetApplication", "ListApplications", "DeleteApplication", "CreateApplicationVersion", "ListApplicationVersions"}
}

func (p *Pack) col(req *spi.Request, n string) spi.Collection {
	return p.deps.Store.Scope(req.Identity.Account, req.Identity.Region).Collection(n)
}

func (p *Pack) Invoke(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	id := first(req.Input, "ApplicationId", "Name")
	switch req.Operation {
	case "CreateApplication":
		name := first(req.Input, "Name")
		if name == "" {
			return nil, &spi.Fault{Code: "BadRequestException", HTTPStatus: 400, Fault: "client"}
		}
		arn := "arn:aws:serverlessrepo:" + req.Identity.Region + ":" + req.Identity.Account + ":applications/" + name
		rec := map[string]any{"ApplicationId": arn, "Name": name, "Author": first(req.Input, "Author")}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "sarapp").Put(ctx, name, b)
		return &spi.Response{Output: rec}, nil
	case "GetApplication":
		name := lastSlash(id)
		b, ok, _ := p.col(req, "sarapp").Get(ctx, name)
		if !ok {
			return nil, &spi.Fault{Code: "NotFoundException", HTTPStatus: 404, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		return &spi.Response{Output: rec}, nil
	case "ListApplications":
		return listWrap(ctx, p.col(req, "sarapp"), "Applications")
	case "DeleteApplication":
		_ = p.col(req, "sarapp").Delete(ctx, lastSlash(id))
		return &spi.Response{Output: map[string]any{}}, nil
	case "CreateApplicationVersion":
		ver := first(req.Input, "SemanticVersion")
		rec := map[string]any{"ApplicationId": id, "SemanticVersion": ver}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "sarver").Put(ctx, lastSlash(id)+"/"+ver, b)
		return &spi.Response{Output: rec}, nil
	case "ListApplicationVersions":
		return listWrap(ctx, p.col(req, "sarver"), "Versions")
	default:
		return nil, spi.NotImplemented("aws.serverlessrepo", req.Operation, "emulate")
	}
}

func lastSlash(s string) string {
	for i := len(s) - 1; i >= 0; i-- {
		if s[i] == '/' {
			return s[i+1:]
		}
	}
	return s
}

func listWrap(ctx context.Context, c spi.Collection, key string) (*spi.Response, error) {
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
