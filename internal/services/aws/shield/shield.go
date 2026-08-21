// Package shield stores protection records (no DDoS mitigation).
package shield

import (
	"context"
	"encoding/json"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func init() {
	registry.Register(registry.Factory{ServiceID: "aws.shield", Tier: model.TierEmulate, New: func(d spi.Deps) (spi.BehaviorPack, error) {
		return New(d), nil
	}})
}

// Pack implements Shield-lite.
type Pack struct{ deps spi.Deps }

// New constructs the pack.
func New(d spi.Deps) *Pack { return &Pack{deps: d} }

func (p *Pack) ServiceID() string { return "aws.shield" }
func (p *Pack) Tier() model.Tier  { return model.TierEmulate }
func (p *Pack) Operations() []string {
	return []string{
		"CreateProtection", "DescribeProtection", "ListProtections", "DeleteProtection",
		"CreateSubscription", "GetSubscriptionState",
	}
}

func (p *Pack) col(req *spi.Request, n string) spi.Collection {
	return p.deps.Store.Scope(req.Identity.Account, req.Identity.Region).Collection(n)
}

func (p *Pack) Invoke(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	switch req.Operation {
	case "CreateProtection":
		id := p.deps.Rand.Hex(8)
		rec := map[string]any{
			"Id": id, "Name": first(req.Input, "Name"), "ResourceArn": first(req.Input, "ResourceArn"),
		}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "shprot").Put(ctx, id, b)
		return &spi.Response{Output: map[string]any{"ProtectionId": id}}, nil
	case "DescribeProtection":
		id := first(req.Input, "ProtectionId", "Id")
		b, ok, _ := p.col(req, "shprot").Get(ctx, id)
		if !ok {
			return nil, &spi.Fault{Code: "ResourceNotFoundException", HTTPStatus: 400, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		return &spi.Response{Output: map[string]any{"Protection": rec}}, nil
	case "ListProtections":
		return listWrap(ctx, p.col(req, "shprot"), "Protections")
	case "DeleteProtection":
		_ = p.col(req, "shprot").Delete(ctx, first(req.Input, "ProtectionId", "Id"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "CreateSubscription":
		_ = p.col(req, "shsub").Put(ctx, "state", []byte(`{"SubscriptionState":"ACTIVE"}`))
		return &spi.Response{Output: map[string]any{}}, nil
	case "GetSubscriptionState":
		b, ok, _ := p.col(req, "shsub").Get(ctx, "state")
		state := "INACTIVE"
		if ok {
			var rec map[string]any
			_ = json.Unmarshal(b, &rec)
			if s, _ := rec["SubscriptionState"].(string); s != "" {
				state = s
			}
		}
		return &spi.Response{Output: map[string]any{"SubscriptionState": state}}, nil
	default:
		return nil, spi.NotImplemented("aws.shield", req.Operation, "emulate")
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
