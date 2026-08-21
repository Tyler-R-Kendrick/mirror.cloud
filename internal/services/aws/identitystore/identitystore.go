// Package identitystore stores users and groups (no IdP directory).
package identitystore

import (
	"context"
	"encoding/json"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func init() {
	registry.Register(registry.Factory{ServiceID: "aws.identitystore", Tier: model.TierEmulate, New: func(d spi.Deps) (spi.BehaviorPack, error) {
		return New(d), nil
	}})
}

// Pack implements Identity Store-lite.
type Pack struct{ deps spi.Deps }

// New constructs the pack.
func New(d spi.Deps) *Pack { return &Pack{deps: d} }

func (p *Pack) ServiceID() string { return "aws.identitystore" }
func (p *Pack) Tier() model.Tier  { return model.TierEmulate }
func (p *Pack) Operations() []string {
	return []string{
		"CreateUser", "DescribeUser", "ListUsers", "DeleteUser",
		"CreateGroup", "DescribeGroup", "ListGroups", "DeleteGroup",
	}
}

func (p *Pack) col(req *spi.Request, n string) spi.Collection {
	return p.deps.Store.Scope(req.Identity.Account, req.Identity.Region).Collection(n)
}

func (p *Pack) Invoke(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	store := first(req.Input, "IdentityStoreId")
	switch req.Operation {
	case "CreateUser":
		id := p.deps.Rand.Hex(8)
		rec := map[string]any{"UserId": id, "IdentityStoreId": store, "UserName": first(req.Input, "UserName")}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "isuser").Put(ctx, id, b)
		return &spi.Response{Output: map[string]any{"UserId": id, "IdentityStoreId": store}}, nil
	case "DescribeUser":
		id := first(req.Input, "UserId")
		b, ok, _ := p.col(req, "isuser").Get(ctx, id)
		if !ok {
			return nil, &spi.Fault{Code: "ResourceNotFoundException", HTTPStatus: 400, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		return &spi.Response{Output: rec}, nil
	case "ListUsers":
		return listWrap(ctx, p.col(req, "isuser"), "Users")
	case "DeleteUser":
		_ = p.col(req, "isuser").Delete(ctx, first(req.Input, "UserId"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "CreateGroup":
		id := p.deps.Rand.Hex(8)
		rec := map[string]any{"GroupId": id, "IdentityStoreId": store, "DisplayName": first(req.Input, "DisplayName")}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "isgrp").Put(ctx, id, b)
		return &spi.Response{Output: map[string]any{"GroupId": id, "IdentityStoreId": store}}, nil
	case "DescribeGroup":
		id := first(req.Input, "GroupId")
		b, ok, _ := p.col(req, "isgrp").Get(ctx, id)
		if !ok {
			return nil, &spi.Fault{Code: "ResourceNotFoundException", HTTPStatus: 400, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		return &spi.Response{Output: rec}, nil
	case "ListGroups":
		return listWrap(ctx, p.col(req, "isgrp"), "Groups")
	case "DeleteGroup":
		_ = p.col(req, "isgrp").Delete(ctx, first(req.Input, "GroupId"))
		return &spi.Response{Output: map[string]any{}}, nil
	default:
		return nil, spi.NotImplemented("aws.identitystore", req.Operation, "emulate")
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
