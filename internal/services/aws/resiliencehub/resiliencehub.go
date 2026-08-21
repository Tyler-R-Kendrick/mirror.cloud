// Package resiliencehub stores app records (no resiliency scoring).
package resiliencehub

import (
	"context"
	"encoding/json"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func init() {
	registry.Register(registry.Factory{ServiceID: "aws.resiliencehub", Tier: model.TierEmulate, New: func(d spi.Deps) (spi.BehaviorPack, error) {
		return New(d), nil
	}})
}

// Pack implements Resilience Hub-lite.
type Pack struct{ deps spi.Deps }

// New constructs the pack.
func New(d spi.Deps) *Pack { return &Pack{deps: d} }

func (p *Pack) ServiceID() string { return "aws.resiliencehub" }
func (p *Pack) Tier() model.Tier  { return model.TierEmulate }
func (p *Pack) Operations() []string {
	return []string{"CreateApp", "DescribeApp", "ListApps", "DeleteApp", "CreateResiliencyPolicy", "ListResiliencyPolicies"}
}

func (p *Pack) col(req *spi.Request, n string) spi.Collection {
	return p.deps.Store.Scope(req.Identity.Account, req.Identity.Region).Collection(n)
}

func (p *Pack) Invoke(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	name := first(req.Input, "name")
	switch req.Operation {
	case "CreateApp":
		arn := "arn:aws:resiliencehub:" + req.Identity.Region + ":" + req.Identity.Account + ":app/" + name
		rec := map[string]any{"name": name, "appArn": arn, "status": "Active"}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "rhapp").Put(ctx, name, b)
		return &spi.Response{Output: map[string]any{"app": rec}}, nil
	case "DescribeApp":
		key := first(req.Input, "appArn", "name")
		if i := lastSlash(key); i != "" {
			key = i
		}
		b, ok, _ := p.col(req, "rhapp").Get(ctx, key)
		if !ok {
			return nil, &spi.Fault{Code: "ResourceNotFoundException", HTTPStatus: 400, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		return &spi.Response{Output: map[string]any{"app": rec}}, nil
	case "ListApps":
		return listWrap(ctx, p.col(req, "rhapp"), "appSummaries")
	case "DeleteApp":
		key := first(req.Input, "appArn", "name")
		if i := lastSlash(key); i != "" {
			key = i
		}
		_ = p.col(req, "rhapp").Delete(ctx, key)
		return &spi.Response{Output: map[string]any{}}, nil
	case "CreateResiliencyPolicy":
		rec := map[string]any{"policyName": first(req.Input, "policyName"), "policyArn": "arn:aws:resiliencehub:" + req.Identity.Region + ":" + req.Identity.Account + ":resiliency-policy/" + first(req.Input, "policyName")}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "rhpol").Put(ctx, first(req.Input, "policyName"), b)
		return &spi.Response{Output: map[string]any{"policy": rec}}, nil
	case "ListResiliencyPolicies":
		return listWrap(ctx, p.col(req, "rhpol"), "resiliencyPolicies")
	default:
		return nil, spi.NotImplemented("aws.resiliencehub", req.Operation, "emulate")
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
