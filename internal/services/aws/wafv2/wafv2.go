// Package wafv2 stores WebACLs, IP sets, and rule groups (no packet inspection).
package wafv2

import (
	"context"
	"encoding/json"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func init() {
	registry.Register(registry.Factory{ServiceID: "aws.wafv2", Tier: model.TierEmulate, New: func(d spi.Deps) (spi.BehaviorPack, error) {
		return New(d), nil
	}})
}

// Pack implements WAFv2-lite.
type Pack struct{ deps spi.Deps }

// New constructs the pack.
func New(d spi.Deps) *Pack { return &Pack{deps: d} }

func (p *Pack) ServiceID() string { return "aws.wafv2" }
func (p *Pack) Tier() model.Tier  { return model.TierEmulate }
func (p *Pack) Operations() []string {
	return []string{
		"CreateWebACL", "GetWebACL", "ListWebACLs", "DeleteWebACL", "UpdateWebACL",
		"CreateIPSet", "GetIPSet", "ListIPSets", "DeleteIPSet",
		"CreateRuleGroup", "GetRuleGroup", "ListRuleGroups", "DeleteRuleGroup",
	}
}

func (p *Pack) col(req *spi.Request, n string) spi.Collection {
	return p.deps.Store.Scope(req.Identity.Account, req.Identity.Region).Collection(n)
}

func (p *Pack) Invoke(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	scope := first(req.Input, "Scope")
	if scope == "" {
		scope = "REGIONAL"
	}
	name := first(req.Input, "Name")
	id := first(req.Input, "Id")
	switch req.Operation {
	case "CreateWebACL":
		id = "acl-" + p.deps.Rand.Hex(8)
		rec := map[string]any{"Name": name, "Id": id, "ARN": arn(req, "webacl", id), "Scope": scope, "DefaultAction": req.Input["DefaultAction"]}
		return p.putNamed(ctx, req, "wafacl:"+scope, id, rec, "Summary")
	case "GetWebACL":
		return p.getNamed(ctx, req, "wafacl:"+scope, id, "WebACL")
	case "ListWebACLs":
		return listCol(ctx, p.col(req, "wafacl:"+scope), "WebACLs")
	case "UpdateWebACL":
		return p.putNamed(ctx, req, "wafacl:"+scope, id, map[string]any{"Name": name, "Id": id, "Scope": scope, "DefaultAction": req.Input["DefaultAction"]}, "Summary")
	case "DeleteWebACL":
		_ = p.col(req, "wafacl:"+scope).Delete(ctx, id)
		return &spi.Response{Output: map[string]any{}}, nil
	case "CreateIPSet":
		id = "ipset-" + p.deps.Rand.Hex(8)
		rec := map[string]any{"Name": name, "Id": id, "ARN": arn(req, "ipset", id), "Scope": scope, "Addresses": req.Input["Addresses"]}
		return p.putNamed(ctx, req, "wafip:"+scope, id, rec, "Summary")
	case "GetIPSet":
		return p.getNamed(ctx, req, "wafip:"+scope, id, "IPSet")
	case "ListIPSets":
		return listCol(ctx, p.col(req, "wafip:"+scope), "IPSets")
	case "DeleteIPSet":
		_ = p.col(req, "wafip:"+scope).Delete(ctx, id)
		return &spi.Response{Output: map[string]any{}}, nil
	case "CreateRuleGroup":
		id = "rg-" + p.deps.Rand.Hex(8)
		rec := map[string]any{"Name": name, "Id": id, "ARN": arn(req, "rulegroup", id), "Scope": scope}
		return p.putNamed(ctx, req, "wafrg:"+scope, id, rec, "Summary")
	case "GetRuleGroup":
		return p.getNamed(ctx, req, "wafrg:"+scope, id, "RuleGroup")
	case "ListRuleGroups":
		return listCol(ctx, p.col(req, "wafrg:"+scope), "RuleGroups")
	case "DeleteRuleGroup":
		_ = p.col(req, "wafrg:"+scope).Delete(ctx, id)
		return &spi.Response{Output: map[string]any{}}, nil
	default:
		return nil, spi.NotImplemented("aws.wafv2", req.Operation, "emulate")
	}
}

func (p *Pack) putNamed(ctx context.Context, req *spi.Request, col, id string, rec map[string]any, wrap string) (*spi.Response, error) {
	b, _ := json.Marshal(rec)
	_ = p.col(req, col).Put(ctx, id, b)
	return &spi.Response{Output: map[string]any{wrap: rec, "Id": id}}, nil
}

func (p *Pack) getNamed(ctx context.Context, req *spi.Request, col, id, wrap string) (*spi.Response, error) {
	b, ok, _ := p.col(req, col).Get(ctx, id)
	if !ok {
		return nil, &spi.Fault{Code: "WAFNonexistentItemException", HTTPStatus: 400, Fault: "client"}
	}
	var rec map[string]any
	_ = json.Unmarshal(b, &rec)
	return &spi.Response{Output: map[string]any{wrap: rec}}, nil
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

func arn(req *spi.Request, kind, id string) string {
	return "arn:aws:wafv2:" + req.Identity.Region + ":" + req.Identity.Account + ":regional/" + kind + "/" + id
}

func first(in map[string]any, keys ...string) string {
	for _, k := range keys {
		if s, ok := in[k].(string); ok && s != "" {
			return s
		}
	}
	return ""
}
