// Package ssoadmin stores permission sets and assignments (no IdP login).
package ssoadmin

import (
	"context"
	"encoding/json"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func init() {
	registry.Register(registry.Factory{ServiceID: "aws.sso-admin", Tier: model.TierEmulate, New: func(d spi.Deps) (spi.BehaviorPack, error) {
		return New(d), nil
	}})
}

// Pack implements SSO Admin-lite.
type Pack struct{ deps spi.Deps }

// New constructs the pack.
func New(d spi.Deps) *Pack { return &Pack{deps: d} }

func (p *Pack) ServiceID() string { return "aws.sso-admin" }
func (p *Pack) Tier() model.Tier  { return model.TierEmulate }
func (p *Pack) Operations() []string {
	return []string{
		"CreatePermissionSet", "DescribePermissionSet", "ListPermissionSets", "DeletePermissionSet",
		"CreateAccountAssignment", "ListAccountAssignments", "DeleteAccountAssignment",
	}
}

func (p *Pack) col(req *spi.Request, n string) spi.Collection {
	return p.deps.Store.Scope(req.Identity.Account, req.Identity.Region).Collection(n)
}

func (p *Pack) Invoke(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	switch req.Operation {
	case "CreatePermissionSet":
		arn := "arn:aws:sso:::permissionSet/ssoins-local/ps-" + p.deps.Rand.Hex(8)
		rec := map[string]any{"PermissionSetArn": arn, "Name": first(req.Input, "Name"), "InstanceArn": first(req.Input, "InstanceArn")}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "ssops").Put(ctx, arn, b)
		return &spi.Response{Output: map[string]any{"PermissionSet": rec}}, nil
	case "DescribePermissionSet":
		arn := first(req.Input, "PermissionSetArn")
		b, ok, _ := p.col(req, "ssops").Get(ctx, arn)
		if !ok {
			return nil, &spi.Fault{Code: "ResourceNotFoundException", HTTPStatus: 400, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		return &spi.Response{Output: map[string]any{"PermissionSet": rec}}, nil
	case "ListPermissionSets":
		kvs, _, _ := p.col(req, "ssops").List(ctx, "", "", 0)
		var arns []any
		for _, kv := range kvs {
			arns = append(arns, kv.Key)
		}
		return &spi.Response{Output: map[string]any{"PermissionSets": arns}}, nil
	case "DeletePermissionSet":
		_ = p.col(req, "ssops").Delete(ctx, first(req.Input, "PermissionSetArn"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "CreateAccountAssignment":
		id := p.deps.Rand.Hex(8)
		rec := map[string]any{
			"AccountId": first(req.Input, "TargetId"), "PermissionSetArn": first(req.Input, "PermissionSetArn"),
			"PrincipalId": first(req.Input, "PrincipalId"), "PrincipalType": first(req.Input, "PrincipalType"),
		}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "ssoasg").Put(ctx, id, b)
		return &spi.Response{Output: map[string]any{"AccountAssignment": rec}}, nil
	case "ListAccountAssignments":
		return listWrap(ctx, p.col(req, "ssoasg"), "AccountAssignments")
	case "DeleteAccountAssignment":
		kvs, _, _ := p.col(req, "ssoasg").List(ctx, "", "", 0)
		want := first(req.Input, "PrincipalId")
		for _, kv := range kvs {
			var rec map[string]any
			_ = json.Unmarshal(kv.Value, &rec)
			if rec["PrincipalId"] == want {
				_ = p.col(req, "ssoasg").Delete(ctx, kv.Key)
			}
		}
		return &spi.Response{Output: map[string]any{}}, nil
	default:
		return nil, spi.NotImplemented("aws.sso-admin", req.Operation, "emulate")
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
