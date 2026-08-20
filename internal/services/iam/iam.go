// Package iam stores roles and policies; it never evaluates them.
package iam

import (
	"context"
	"encoding/json"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func init() {
	registry.Register(registry.Factory{ServiceID: "aws.iam", Tier: model.TierEmulate, New: func(d spi.Deps) (spi.BehaviorPack, error) {
		return &Pack{deps: d}, nil
	}})
}

// Pack implements IAM-lite.
type Pack struct{ deps spi.Deps }

func (p *Pack) ServiceID() string { return "aws.iam" }
func (p *Pack) Tier() model.Tier  { return model.TierEmulate }
func (p *Pack) Operations() []string {
	return []string{"CreateRole", "GetRole", "UpdateRole", "DeleteRole", "ListRoles",
		"PutRolePolicy", "GetRolePolicy", "DeleteRolePolicy", "ListRolePolicies",
		"AttachRolePolicy", "DetachRolePolicy", "ListAttachedRolePolicies",
		"CreatePolicy", "GetPolicy", "DeletePolicy", "ListPolicies",
		"CreateUser", "GetUser", "DeleteUser", "ListUsers", "CreateAccessKey",
		"TagRole", "UntagRole"}
}

func (p *Pack) col(req *spi.Request) spi.Collection {
	return p.deps.Store.Scope(req.Identity.Account, req.Identity.Region).Collection("iam")
}

func (p *Pack) Invoke(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	name := first(req.Input, "RoleName", "UserName", "PolicyName")
	switch req.Operation {
	case "CreateRole", "UpdateRole", "CreateUser", "CreatePolicy", "PutRolePolicy":
		b, _ := json.Marshal(req.Input)
		_ = p.col(req).Put(ctx, req.Operation+":"+name, b)
		return &spi.Response{Output: map[string]any{"Role": map[string]any{
			"Arn": "arn:aws:iam::" + req.Identity.Account + ":role/" + name, "RoleName": name,
		}}}, nil
	case "GetRole", "GetUser", "GetPolicy", "GetRolePolicy":
		b, ok, _ := p.col(req).Get(ctx, "CreateRole:"+name)
		if !ok {
			b, ok, _ = p.col(req).Get(ctx, "CreateUser:"+name)
		}
		out := map[string]any{}
		if ok {
			_ = json.Unmarshal(b, &out)
		}
		return &spi.Response{Output: map[string]any{"Role": map[string]any{
			"Arn": "arn:aws:iam::" + req.Identity.Account + ":role/" + name, "RoleName": name,
		}}}, nil
	case "ListRoles", "ListUsers", "ListPolicies", "ListRolePolicies", "ListAttachedRolePolicies":
		return &spi.Response{Output: map[string]any{"Roles": []any{}, "Users": []any{}, "Policies": []any{}}}, nil
	case "DeleteRole", "DeleteUser", "DeletePolicy", "DeleteRolePolicy",
		"AttachRolePolicy", "DetachRolePolicy", "CreateAccessKey", "TagRole", "UntagRole":
		return &spi.Response{Output: map[string]any{"AccessKey": map[string]any{
			"AccessKeyId": p.deps.Rand.Hex(20), "SecretAccessKey": p.deps.Rand.Hex(40),
		}}}, nil
	default:
		return nil, spi.NotImplemented("aws.iam", req.Operation, "emulate")
	}
}

func first(m map[string]any, keys ...string) string {
	for _, k := range keys {
		if s, ok := m[k].(string); ok && s != "" {
			return s
		}
	}
	return ""
}
