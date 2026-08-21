// Package resourceexplorer2 stores index records (no resource crawl).
package resourceexplorer2

import (
	"context"
	"encoding/json"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func init() {
	registry.Register(registry.Factory{ServiceID: "aws.resource-explorer-2", Tier: model.TierEmulate, New: func(d spi.Deps) (spi.BehaviorPack, error) {
		return New(d), nil
	}})
}

// Pack implements Resource Explorer-lite.
type Pack struct{ deps spi.Deps }

// New constructs the pack.
func New(d spi.Deps) *Pack { return &Pack{deps: d} }

func (p *Pack) ServiceID() string { return "aws.resource-explorer-2" }
func (p *Pack) Tier() model.Tier  { return model.TierEmulate }
func (p *Pack) Operations() []string {
	return []string{"CreateIndex", "GetIndex", "ListIndexes", "DeleteIndex", "CreateView", "GetView"}
}

func (p *Pack) col(req *spi.Request, n string) spi.Collection {
	return p.deps.Store.Scope(req.Identity.Account, req.Identity.Region).Collection(n)
}

func (p *Pack) Invoke(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	switch req.Operation {
	case "CreateIndex":
		arn := "arn:aws:resource-explorer-2:" + req.Identity.Region + ":" + req.Identity.Account + ":index/" + p.deps.Rand.Hex(8)
		rec := map[string]any{"Arn": arn, "State": "ACTIVE", "Type": "LOCAL"}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "reidx").Put(ctx, req.Identity.Region, b)
		return &spi.Response{Output: rec}, nil
	case "GetIndex":
		b, ok, _ := p.col(req, "reidx").Get(ctx, req.Identity.Region)
		if !ok {
			return nil, &spi.Fault{Code: "ResourceNotFoundException", HTTPStatus: 400, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		return &spi.Response{Output: rec}, nil
	case "ListIndexes":
		return listWrap(ctx, p.col(req, "reidx"), "Indexes")
	case "DeleteIndex":
		_ = p.col(req, "reidx").Delete(ctx, req.Identity.Region)
		return &spi.Response{Output: map[string]any{}}, nil
	case "CreateView":
		name := first(req.Input, "ViewName")
		arn := "arn:aws:resource-explorer-2:" + req.Identity.Region + ":" + req.Identity.Account + ":view/" + name
		rec := map[string]any{"ViewName": name, "ViewArn": arn}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "review").Put(ctx, name, b)
		return &spi.Response{Output: map[string]any{"View": rec}}, nil
	case "GetView":
		name := lastSlash(first(req.Input, "ViewArn", "ViewName"))
		b, ok, _ := p.col(req, "review").Get(ctx, name)
		if !ok {
			return nil, &spi.Fault{Code: "ResourceNotFoundException", HTTPStatus: 400, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		return &spi.Response{Output: map[string]any{"View": rec}}, nil
	default:
		return nil, spi.NotImplemented("aws.resource-explorer-2", req.Operation, "emulate")
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
