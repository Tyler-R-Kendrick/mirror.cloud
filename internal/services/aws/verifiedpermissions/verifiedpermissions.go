// Package verifiedpermissions stores policy-store records (no Cedar evaluation).
package verifiedpermissions

import (
	"context"
	"encoding/json"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func init() {
	registry.Register(registry.Factory{ServiceID: "aws.verifiedpermissions", Tier: model.TierEmulate, New: func(d spi.Deps) (spi.BehaviorPack, error) {
		return New(d), nil
	}})
}

// Pack implements Verified Permissions-lite.
type Pack struct{ deps spi.Deps }

// New constructs the pack.
func New(d spi.Deps) *Pack { return &Pack{deps: d} }

func (p *Pack) ServiceID() string { return "aws.verifiedpermissions" }
func (p *Pack) Tier() model.Tier  { return model.TierEmulate }
func (p *Pack) Operations() []string {
	return []string{
		"CreatePolicyStore", "GetPolicyStore", "ListPolicyStores", "DeletePolicyStore",
		"CreatePolicy", "GetPolicy", "DeletePolicy",
	}
}

func (p *Pack) col(req *spi.Request, n string) spi.Collection {
	return p.deps.Store.Scope(req.Identity.Account, req.Identity.Region).Collection(n)
}

func (p *Pack) Invoke(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	switch req.Operation {
	case "CreatePolicyStore":
		id := p.deps.Rand.Hex(8)
		rec := map[string]any{
			"policyStoreId": id, "arn": "arn:aws:verifiedpermissions:" + req.Identity.Region + ":" + req.Identity.Account + ":policy-store/" + id,
			"description": first(req.Input, "description"),
		}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "vpps").Put(ctx, id, b)
		return &spi.Response{Output: map[string]any{"policyStoreId": id, "arn": rec["arn"]}}, nil
	case "GetPolicyStore":
		id := first(req.Input, "policyStoreId")
		b, ok, _ := p.col(req, "vpps").Get(ctx, id)
		if !ok {
			return nil, &spi.Fault{Code: "ResourceNotFoundException", HTTPStatus: 400, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		return &spi.Response{Output: rec}, nil
	case "ListPolicyStores":
		return listWrap(ctx, p.col(req, "vpps"), "policyStores")
	case "DeletePolicyStore":
		_ = p.col(req, "vpps").Delete(ctx, first(req.Input, "policyStoreId"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "CreatePolicy":
		id := p.deps.Rand.Hex(8)
		rec := map[string]any{"policyId": id, "policyStoreId": first(req.Input, "policyStoreId")}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "vppol").Put(ctx, first(req.Input, "policyStoreId")+"/"+id, b)
		return &spi.Response{Output: map[string]any{"policyId": id, "policyStoreId": rec["policyStoreId"]}}, nil
	case "GetPolicy":
		key := first(req.Input, "policyStoreId") + "/" + first(req.Input, "policyId")
		b, ok, _ := p.col(req, "vppol").Get(ctx, key)
		if !ok {
			return nil, &spi.Fault{Code: "ResourceNotFoundException", HTTPStatus: 400, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		return &spi.Response{Output: rec}, nil
	case "DeletePolicy":
		_ = p.col(req, "vppol").Delete(ctx, first(req.Input, "policyStoreId")+"/"+first(req.Input, "policyId"))
		return &spi.Response{Output: map[string]any{}}, nil
	default:
		return nil, spi.NotImplemented("aws.verifiedpermissions", req.Operation, "emulate")
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
