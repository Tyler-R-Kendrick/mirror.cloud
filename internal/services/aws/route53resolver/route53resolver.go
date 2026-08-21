// Package route53resolver stores resolver endpoints and rules (no DNS forwarding).
package route53resolver

import (
	"context"
	"encoding/json"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func init() {
	registry.Register(registry.Factory{ServiceID: "aws.route53resolver", Tier: model.TierEmulate, New: func(d spi.Deps) (spi.BehaviorPack, error) {
		return New(d), nil
	}})
}

// Pack implements Route 53 Resolver-lite.
type Pack struct{ deps spi.Deps }

// New constructs the pack.
func New(d spi.Deps) *Pack { return &Pack{deps: d} }

func (p *Pack) ServiceID() string { return "aws.route53resolver" }
func (p *Pack) Tier() model.Tier  { return model.TierEmulate }
func (p *Pack) Operations() []string {
	return []string{
		"CreateResolverEndpoint", "GetResolverEndpoint", "ListResolverEndpoints", "DeleteResolverEndpoint",
		"CreateResolverRule", "GetResolverRule", "ListResolverRules", "DeleteResolverRule",
	}
}

func (p *Pack) col(req *spi.Request, n string) spi.Collection {
	return p.deps.Store.Scope(req.Identity.Account, req.Identity.Region).Collection(n)
}

func (p *Pack) Invoke(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	switch req.Operation {
	case "CreateResolverEndpoint":
		id := "rslvr-in-" + p.deps.Rand.Hex(8)
		rec := map[string]any{
			"Id": id, "Name": first(req.Input, "Name"), "Direction": first(req.Input, "Direction"),
			"Status": "OPERATIONAL", "Arn": "arn:aws:route53resolver:" + req.Identity.Region + ":" + req.Identity.Account + ":resolver-endpoint/" + id,
		}
		if rec["Direction"] == "" {
			rec["Direction"] = "INBOUND"
		}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "r53ep").Put(ctx, id, b)
		return &spi.Response{Output: map[string]any{"ResolverEndpoint": rec}}, nil
	case "GetResolverEndpoint":
		return getWrap(ctx, p.col(req, "r53ep"), first(req.Input, "ResolverEndpointId", "Id"), "ResolverEndpoint")
	case "ListResolverEndpoints":
		return listWrap(ctx, p.col(req, "r53ep"), "ResolverEndpoints")
	case "DeleteResolverEndpoint":
		_ = p.col(req, "r53ep").Delete(ctx, first(req.Input, "ResolverEndpointId", "Id"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "CreateResolverRule":
		id := "rslvr-rr-" + p.deps.Rand.Hex(8)
		rec := map[string]any{
			"Id": id, "Name": first(req.Input, "Name"), "DomainName": first(req.Input, "DomainName"),
			"RuleType": first(req.Input, "RuleType"), "Status": "COMPLETE",
			"ResolverEndpointId": first(req.Input, "ResolverEndpointId"),
		}
		if rec["RuleType"] == "" {
			rec["RuleType"] = "FORWARD"
		}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "r53rr").Put(ctx, id, b)
		return &spi.Response{Output: map[string]any{"ResolverRule": rec}}, nil
	case "GetResolverRule":
		return getWrap(ctx, p.col(req, "r53rr"), first(req.Input, "ResolverRuleId", "Id"), "ResolverRule")
	case "ListResolverRules":
		return listWrap(ctx, p.col(req, "r53rr"), "ResolverRules")
	case "DeleteResolverRule":
		_ = p.col(req, "r53rr").Delete(ctx, first(req.Input, "ResolverRuleId", "Id"))
		return &spi.Response{Output: map[string]any{}}, nil
	default:
		return nil, spi.NotImplemented("aws.route53resolver", req.Operation, "emulate")
	}
}

func getWrap(ctx context.Context, c spi.Collection, id, key string) (*spi.Response, error) {
	b, ok, _ := c.Get(ctx, id)
	if !ok {
		return nil, &spi.Fault{Code: "ResourceNotFoundException", HTTPStatus: 400, Fault: "client"}
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
