// Package workmail stores organization and user records (no mail delivery).
package workmail

import (
	"context"
	"encoding/json"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func init() {
	registry.Register(registry.Factory{ServiceID: "aws.workmail", Tier: model.TierEmulate, New: func(d spi.Deps) (spi.BehaviorPack, error) {
		return New(d), nil
	}})
}

// Pack implements WorkMail-lite.
type Pack struct{ deps spi.Deps }

// New constructs the pack.
func New(d spi.Deps) *Pack { return &Pack{deps: d} }

func (p *Pack) ServiceID() string { return "aws.workmail" }
func (p *Pack) Tier() model.Tier  { return model.TierEmulate }
func (p *Pack) Operations() []string {
	return []string{
		"CreateOrganization", "DescribeOrganization", "ListOrganizations", "DeleteOrganization",
		"CreateUser", "DescribeUser", "ListUsers", "DeleteUser",
	}
}

func (p *Pack) col(req *spi.Request, n string) spi.Collection {
	return p.deps.Store.Scope(req.Identity.Account, req.Identity.Region).Collection(n)
}

func (p *Pack) Invoke(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	switch req.Operation {
	case "CreateOrganization":
		id := "m-" + p.deps.Rand.Hex(8)
		rec := map[string]any{"OrganizationId": id, "Alias": first(req.Input, "Alias"), "State": "Active"}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "wmorg").Put(ctx, id, b)
		return &spi.Response{Output: map[string]any{"OrganizationId": id}}, nil
	case "DescribeOrganization":
		id := first(req.Input, "OrganizationId")
		b, ok, _ := p.col(req, "wmorg").Get(ctx, id)
		if !ok {
			return nil, &spi.Fault{Code: "OrganizationNotFoundException", HTTPStatus: 400, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		return &spi.Response{Output: rec}, nil
	case "ListOrganizations":
		return listWrap(ctx, p.col(req, "wmorg"), "OrganizationSummaries")
	case "DeleteOrganization":
		_ = p.col(req, "wmorg").Delete(ctx, first(req.Input, "OrganizationId"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "CreateUser":
		id := p.deps.Rand.Hex(8)
		org := first(req.Input, "OrganizationId")
		rec := map[string]any{"UserId": id, "Name": first(req.Input, "Name"), "Email": first(req.Input, "Email"), "OrganizationId": org, "State": "ENABLED"}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "wmuser").Put(ctx, org+"/"+id, b)
		return &spi.Response{Output: map[string]any{"UserId": id}}, nil
	case "DescribeUser":
		key := first(req.Input, "OrganizationId") + "/" + first(req.Input, "UserId")
		b, ok, _ := p.col(req, "wmuser").Get(ctx, key)
		if !ok {
			return nil, &spi.Fault{Code: "EntityNotFoundException", HTTPStatus: 400, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		return &spi.Response{Output: rec}, nil
	case "ListUsers":
		return listWrap(ctx, p.col(req, "wmuser"), "Users")
	case "DeleteUser":
		_ = p.col(req, "wmuser").Delete(ctx, first(req.Input, "OrganizationId")+"/"+first(req.Input, "UserId"))
		return &spi.Response{Output: map[string]any{}}, nil
	default:
		return nil, spi.NotImplemented("aws.workmail", req.Operation, "emulate")
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
