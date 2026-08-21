// Package cognitoidentity is Cognito Identity (federated identities) control-plane, not a real IdP.
package cognitoidentity

import (
	"context"
	"encoding/json"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func init() {
	registry.Register(registry.Factory{ServiceID: "aws.cognito-identity", Tier: model.TierEmulate, New: func(d spi.Deps) (spi.BehaviorPack, error) {
		return New(d), nil
	}})
}

// Pack implements Cognito Identity-lite.
type Pack struct{ deps spi.Deps }

// New constructs the pack.
func New(d spi.Deps) *Pack { return &Pack{deps: d} }

func (p *Pack) ServiceID() string { return "aws.cognito-identity" }
func (p *Pack) Tier() model.Tier  { return model.TierEmulate }
func (p *Pack) Operations() []string {
	return []string{
		"CreateIdentityPool", "DescribeIdentityPool", "ListIdentityPools", "DeleteIdentityPool", "UpdateIdentityPool",
		"GetId", "GetCredentialsForIdentity", "GetOpenIdToken",
		"SetIdentityPoolRoles", "GetIdentityPoolRoles",
	}
}

func (p *Pack) col(req *spi.Request, n string) spi.Collection {
	return p.deps.Store.Scope(req.Identity.Account, req.Identity.Region).Collection(n)
}

func (p *Pack) Invoke(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	switch req.Operation {
	case "CreateIdentityPool":
		id := req.Identity.Region + ":" + p.deps.Rand.Hex(8) + "-" + p.deps.Rand.Hex(4)
		rec := map[string]any{"IdentityPoolId": id, "IdentityPoolName": first(req.Input, "IdentityPoolName"), "AllowUnauthenticatedIdentities": req.Input["AllowUnauthenticatedIdentities"]}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "cip").Put(ctx, id, b)
		return &spi.Response{Output: rec}, nil
	case "DescribeIdentityPool", "UpdateIdentityPool":
		id := first(req.Input, "IdentityPoolId")
		b, ok, _ := p.col(req, "cip").Get(ctx, id)
		if !ok {
			return nil, &spi.Fault{Code: "ResourceNotFoundException", HTTPStatus: 400, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		if req.Operation == "UpdateIdentityPool" {
			if n := first(req.Input, "IdentityPoolName"); n != "" {
				rec["IdentityPoolName"] = n
			}
			nb, _ := json.Marshal(rec)
			_ = p.col(req, "cip").Put(ctx, id, nb)
		}
		return &spi.Response{Output: rec}, nil
	case "ListIdentityPools":
		kvs, _, _ := p.col(req, "cip").List(ctx, "", "", 0)
		var items []any
		for _, kv := range kvs {
			var rec map[string]any
			_ = json.Unmarshal(kv.Value, &rec)
			items = append(items, rec)
		}
		return &spi.Response{Output: map[string]any{"IdentityPools": items}}, nil
	case "DeleteIdentityPool":
		_ = p.col(req, "cip").Delete(ctx, first(req.Input, "IdentityPoolId"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "GetId":
		pool := first(req.Input, "IdentityPoolId")
		id := pool + ":" + p.deps.Rand.Hex(8)
		rec := map[string]any{"IdentityId": id}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "cid").Put(ctx, id, b)
		return &spi.Response{Output: rec}, nil
	case "GetCredentialsForIdentity":
		ak := "ASIATEST" + p.deps.Rand.Hex(8)
		return &spi.Response{Output: map[string]any{
			"IdentityId": first(req.Input, "IdentityId"),
			"Credentials": map[string]any{
				"AccessKeyId": ak, "SecretKey": p.deps.Rand.Hex(16), "SessionToken": p.deps.Rand.Hex(16),
			},
		}}, nil
	case "GetOpenIdToken":
		return &spi.Response{Output: map[string]any{"IdentityId": first(req.Input, "IdentityId"), "Token": "mirror." + p.deps.Rand.Hex(16)}}, nil
	case "SetIdentityPoolRoles":
		b, _ := json.Marshal(req.Input["Roles"])
		_ = p.col(req, "ciprole").Put(ctx, first(req.Input, "IdentityPoolId"), b)
		return &spi.Response{Output: map[string]any{}}, nil
	case "GetIdentityPoolRoles":
		b, ok, _ := p.col(req, "ciprole").Get(ctx, first(req.Input, "IdentityPoolId"))
		var roles any = map[string]any{}
		if ok {
			_ = json.Unmarshal(b, &roles)
		}
		return &spi.Response{Output: map[string]any{"IdentityPoolId": first(req.Input, "IdentityPoolId"), "Roles": roles}}, nil
	default:
		return nil, spi.NotImplemented("aws.cognito-identity", req.Operation, "emulate")
	}
}

func first(in map[string]any, keys ...string) string {
	for _, k := range keys {
		if s, ok := in[k].(string); ok && s != "" {
			return s
		}
	}
	return ""
}
