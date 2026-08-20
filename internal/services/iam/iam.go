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
	acct := req.Identity.Account
	switch req.Operation {
	case "CreateRole", "UpdateRole":
		name := first(req.Input, "RoleName")
		rec := map[string]any{"RoleName": name, "Arn": "arn:aws:iam::" + acct + ":role/" + name, "AssumeRolePolicyDocument": req.Input["AssumeRolePolicyDocument"]}
		b, _ := json.Marshal(rec)
		_ = p.col(req).Put(ctx, "role:"+name, b)
		return &spi.Response{Output: map[string]any{"Role": rec}}, nil
	case "GetRole":
		name := first(req.Input, "RoleName")
		b, ok, _ := p.col(req).Get(ctx, "role:"+name)
		if !ok {
			return nil, &spi.Fault{Code: "NoSuchEntity", HTTPStatus: 404, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		return &spi.Response{Output: map[string]any{"Role": rec}}, nil
	case "DeleteRole":
		_ = p.col(req).Delete(ctx, "role:"+first(req.Input, "RoleName"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "ListRoles":
		return p.listKind(ctx, req, "role:", "Roles")
	case "CreateUser":
		name := first(req.Input, "UserName")
		rec := map[string]any{"UserName": name, "Arn": "arn:aws:iam::" + acct + ":user/" + name}
		b, _ := json.Marshal(rec)
		_ = p.col(req).Put(ctx, "user:"+name, b)
		return &spi.Response{Output: map[string]any{"User": rec}}, nil
	case "GetUser":
		name := first(req.Input, "UserName")
		b, ok, _ := p.col(req).Get(ctx, "user:"+name)
		if !ok {
			return nil, &spi.Fault{Code: "NoSuchEntity", HTTPStatus: 404, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		return &spi.Response{Output: map[string]any{"User": rec}}, nil
	case "DeleteUser":
		_ = p.col(req).Delete(ctx, "user:"+first(req.Input, "UserName"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "ListUsers":
		return p.listKind(ctx, req, "user:", "Users")
	case "CreatePolicy":
		name := first(req.Input, "PolicyName")
		rec := map[string]any{"PolicyName": name, "Arn": "arn:aws:iam::" + acct + ":policy/" + name, "PolicyDocument": req.Input["PolicyDocument"]}
		b, _ := json.Marshal(rec)
		_ = p.col(req).Put(ctx, "policy:"+name, b)
		return &spi.Response{Output: map[string]any{"Policy": rec}}, nil
	case "GetPolicy":
		name := first(req.Input, "PolicyName")
		if name == "" {
			name = first(req.Input, "PolicyArn")
		}
		b, ok, _ := p.col(req).Get(ctx, "policy:"+name)
		if !ok {
			return nil, &spi.Fault{Code: "NoSuchEntity", HTTPStatus: 404, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		return &spi.Response{Output: map[string]any{"Policy": rec}}, nil
	case "DeletePolicy":
		_ = p.col(req).Delete(ctx, "policy:"+first(req.Input, "PolicyName"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "ListPolicies":
		return p.listKind(ctx, req, "policy:", "Policies")
	case "PutRolePolicy":
		role, pol := first(req.Input, "RoleName"), first(req.Input, "PolicyName")
		b, _ := json.Marshal(req.Input)
		_ = p.col(req).Put(ctx, "rolepolicy:"+role+":"+pol, b)
		return &spi.Response{Output: map[string]any{}}, nil
	case "GetRolePolicy":
		role, pol := first(req.Input, "RoleName"), first(req.Input, "PolicyName")
		b, ok, _ := p.col(req).Get(ctx, "rolepolicy:"+role+":"+pol)
		if !ok {
			return nil, &spi.Fault{Code: "NoSuchEntity", HTTPStatus: 404, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		return &spi.Response{Output: rec}, nil
	case "DeleteRolePolicy":
		role, pol := first(req.Input, "RoleName"), first(req.Input, "PolicyName")
		_ = p.col(req).Delete(ctx, "rolepolicy:"+role+":"+pol)
		return &spi.Response{Output: map[string]any{}}, nil
	case "ListRolePolicies":
		role := first(req.Input, "RoleName")
		kvs, _, _ := p.col(req).List(ctx, "rolepolicy:"+role+":", "", 0)
		var names []any
		for _, kv := range kvs {
			names = append(names, kv.Key[len("rolepolicy:"+role+":"):])
		}
		return &spi.Response{Output: map[string]any{"PolicyNames": names}}, nil
	case "AttachRolePolicy", "DetachRolePolicy", "ListAttachedRolePolicies", "TagRole", "UntagRole":
		return &spi.Response{Output: map[string]any{"AttachedPolicies": []any{}}}, nil
	case "CreateAccessKey":
		return &spi.Response{Output: map[string]any{"AccessKey": map[string]any{
			"AccessKeyId": p.deps.Rand.Hex(20), "SecretAccessKey": p.deps.Rand.Hex(40),
			"UserName": first(req.Input, "UserName"), "Status": "Active",
		}}}, nil
	default:
		return nil, spi.NotImplemented("aws.iam", req.Operation, "emulate")
	}
}

func (p *Pack) listKind(ctx context.Context, req *spi.Request, prefix, outKey string) (*spi.Response, error) {
	kvs, _, _ := p.col(req).List(ctx, prefix, "", 0)
	items := make([]any, 0, len(kvs))
	for _, kv := range kvs {
		var rec map[string]any
		_ = json.Unmarshal(kv.Value, &rec)
		items = append(items, rec)
	}
	return &spi.Response{Output: map[string]any{outKey: items}}, nil
}

func first(m map[string]any, keys ...string) string {
	for _, k := range keys {
		if s, ok := m[k].(string); ok && s != "" {
			return s
		}
	}
	return ""
}
