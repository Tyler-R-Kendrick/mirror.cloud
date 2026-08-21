// Package proton stores environment records (no IaC provision).
package proton

import (
	"context"
	"encoding/json"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func init() {
	registry.Register(registry.Factory{ServiceID: "aws.proton", Tier: model.TierEmulate, New: func(d spi.Deps) (spi.BehaviorPack, error) {
		return New(d), nil
	}})
}

// Pack implements Proton-lite.
type Pack struct{ deps spi.Deps }

// New constructs the pack.
func New(d spi.Deps) *Pack { return &Pack{deps: d} }

func (p *Pack) ServiceID() string { return "aws.proton" }
func (p *Pack) Tier() model.Tier  { return model.TierEmulate }
func (p *Pack) Operations() []string {
	return []string{"CreateEnvironment", "GetEnvironment", "ListEnvironments", "DeleteEnvironment", "CreateService", "GetService"}
}

func (p *Pack) col(req *spi.Request, n string) spi.Collection {
	return p.deps.Store.Scope(req.Identity.Account, req.Identity.Region).Collection(n)
}

func (p *Pack) Invoke(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	name := first(req.Input, "name")
	switch req.Operation {
	case "CreateEnvironment":
		if name == "" {
			return nil, &spi.Fault{Code: "ValidationException", HTTPStatus: 400, Fault: "client"}
		}
		arn := "arn:aws:proton:" + req.Identity.Region + ":" + req.Identity.Account + ":environment/" + name
		rec := map[string]any{"name": name, "arn": arn, "deploymentStatus": "SUCCEEDED"}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "prenv").Put(ctx, name, b)
		return &spi.Response{Output: map[string]any{"environment": rec}}, nil
	case "GetEnvironment":
		b, ok, _ := p.col(req, "prenv").Get(ctx, name)
		if !ok {
			return nil, &spi.Fault{Code: "ResourceNotFoundException", HTTPStatus: 400, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		return &spi.Response{Output: map[string]any{"environment": rec}}, nil
	case "ListEnvironments":
		return listWrap(ctx, p.col(req, "prenv"), "environments")
	case "DeleteEnvironment":
		_ = p.col(req, "prenv").Delete(ctx, name)
		return &spi.Response{Output: map[string]any{}}, nil
	case "CreateService":
		rec := map[string]any{"name": name, "arn": "arn:aws:proton:" + req.Identity.Region + ":" + req.Identity.Account + ":service/" + name, "status": "ACTIVE"}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "prsvc").Put(ctx, name, b)
		return &spi.Response{Output: map[string]any{"service": rec}}, nil
	case "GetService":
		b, ok, _ := p.col(req, "prsvc").Get(ctx, name)
		if !ok {
			return nil, &spi.Fault{Code: "ResourceNotFoundException", HTTPStatus: 400, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		return &spi.Response{Output: map[string]any{"service": rec}}, nil
	default:
		return nil, spi.NotImplemented("aws.proton", req.Operation, "emulate")
	}
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
