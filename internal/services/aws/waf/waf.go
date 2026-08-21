// Package waf stores classic WebACL and IP set records (no packet inspection).
package waf

import (
	"context"
	"encoding/json"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func init() {
	registry.Register(registry.Factory{ServiceID: "aws.waf", Tier: model.TierEmulate, New: func(d spi.Deps) (spi.BehaviorPack, error) {
		return New(d), nil
	}})
}

// Pack implements WAF classic-lite.
type Pack struct{ deps spi.Deps }

// New constructs the pack.
func New(d spi.Deps) *Pack { return &Pack{deps: d} }

func (p *Pack) ServiceID() string { return "aws.waf" }
func (p *Pack) Tier() model.Tier  { return model.TierEmulate }
func (p *Pack) Operations() []string {
	return []string{
		"CreateWebACL", "GetWebACL", "ListWebACLs", "DeleteWebACL",
		"CreateIPSet", "GetIPSet", "ListIPSets", "DeleteIPSet",
	}
}

func (p *Pack) col(req *spi.Request, n string) spi.Collection {
	return p.deps.Store.Scope(req.Identity.Account, req.Identity.Region).Collection(n)
}

func (p *Pack) Invoke(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	switch req.Operation {
	case "CreateWebACL":
		id := p.deps.Rand.Hex(8)
		rec := map[string]any{"WebACLId": id, "Name": first(req.Input, "Name"), "DefaultAction": req.Input["DefaultAction"]}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "wafacl").Put(ctx, id, b)
		return &spi.Response{Output: map[string]any{"WebACL": rec}}, nil
	case "GetWebACL":
		return getWrap(ctx, p.col(req, "wafacl"), first(req.Input, "WebACLId"), "WebACL")
	case "ListWebACLs":
		return listWrap(ctx, p.col(req, "wafacl"), "WebACLs")
	case "DeleteWebACL":
		_ = p.col(req, "wafacl").Delete(ctx, first(req.Input, "WebACLId"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "CreateIPSet":
		id := p.deps.Rand.Hex(8)
		rec := map[string]any{"IPSetId": id, "Name": first(req.Input, "Name")}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "wafip").Put(ctx, id, b)
		return &spi.Response{Output: map[string]any{"IPSet": rec}}, nil
	case "GetIPSet":
		return getWrap(ctx, p.col(req, "wafip"), first(req.Input, "IPSetId"), "IPSet")
	case "ListIPSets":
		return listWrap(ctx, p.col(req, "wafip"), "IPSets")
	case "DeleteIPSet":
		_ = p.col(req, "wafip").Delete(ctx, first(req.Input, "IPSetId"))
		return &spi.Response{Output: map[string]any{}}, nil
	default:
		return nil, spi.NotImplemented("aws.waf", req.Operation, "emulate")
	}
}

func getWrap(ctx context.Context, c spi.Collection, id, key string) (*spi.Response, error) {
	b, ok, _ := c.Get(ctx, id)
	if !ok {
		return nil, &spi.Fault{Code: "WAFNonexistentItemException", HTTPStatus: 400, Fault: "client"}
	}
	var rec map[string]any
	_ = json.Unmarshal(b, &rec)
	return &spi.Response{Output: map[string]any{key: rec}}, nil
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
