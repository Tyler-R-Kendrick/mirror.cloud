// Package directconnect stores connection records (no physical circuits).
package directconnect

import (
	"context"
	"encoding/json"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func init() {
	registry.Register(registry.Factory{ServiceID: "aws.directconnect", Tier: model.TierEmulate, New: func(d spi.Deps) (spi.BehaviorPack, error) {
		return New(d), nil
	}})
}

// Pack implements Direct Connect-lite.
type Pack struct{ deps spi.Deps }

// New constructs the pack.
func New(d spi.Deps) *Pack { return &Pack{deps: d} }

func (p *Pack) ServiceID() string { return "aws.directconnect" }
func (p *Pack) Tier() model.Tier  { return model.TierEmulate }
func (p *Pack) Operations() []string {
	return []string{
		"CreateConnection", "DescribeConnections", "DeleteConnection",
		"CreateLag", "DescribeLags", "DeleteLag",
	}
}

func (p *Pack) col(req *spi.Request, n string) spi.Collection {
	return p.deps.Store.Scope(req.Identity.Account, req.Identity.Region).Collection(n)
}

func (p *Pack) Invoke(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	switch req.Operation {
	case "CreateConnection":
		id := "dxcon-" + p.deps.Rand.Hex(8)
		rec := map[string]any{
			"connectionId": id, "connectionName": first(req.Input, "connectionName"),
			"bandwidth": first(req.Input, "bandwidth"), "location": first(req.Input, "location"),
			"connectionState": "available",
		}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "dxcon").Put(ctx, id, b)
		return &spi.Response{Output: rec}, nil
	case "DescribeConnections":
		return listOrGet(ctx, p.col(req, "dxcon"), first(req.Input, "connectionId"), "connections")
	case "DeleteConnection":
		_ = p.col(req, "dxcon").Delete(ctx, first(req.Input, "connectionId"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "CreateLag":
		id := "dxlag-" + p.deps.Rand.Hex(8)
		rec := map[string]any{"lagId": id, "lagName": first(req.Input, "lagName"), "lagState": "available", "connectionsBandwidth": first(req.Input, "connectionsBandwidth")}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "dxlag").Put(ctx, id, b)
		return &spi.Response{Output: rec}, nil
	case "DescribeLags":
		return listOrGet(ctx, p.col(req, "dxlag"), first(req.Input, "lagId"), "lags")
	case "DeleteLag":
		_ = p.col(req, "dxlag").Delete(ctx, first(req.Input, "lagId"))
		return &spi.Response{Output: map[string]any{}}, nil
	default:
		return nil, spi.NotImplemented("aws.directconnect", req.Operation, "emulate")
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
