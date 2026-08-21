// Package resourcegroups stores group records (no resource query).
package resourcegroups

import (
	"context"
	"encoding/json"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func init() {
	registry.Register(registry.Factory{ServiceID: "aws.resource-groups", Tier: model.TierEmulate, New: func(d spi.Deps) (spi.BehaviorPack, error) {
		return New(d), nil
	}})
}

// Pack implements Resource Groups-lite.
type Pack struct{ deps spi.Deps }

// New constructs the pack.
func New(d spi.Deps) *Pack { return &Pack{deps: d} }

func (p *Pack) ServiceID() string { return "aws.resource-groups" }
func (p *Pack) Tier() model.Tier  { return model.TierEmulate }
func (p *Pack) Operations() []string {
	return []string{
		"CreateGroup", "GetGroup", "ListGroups", "DeleteGroup",
		"GroupResources", "ListGroupResources", "UngroupResources",
	}
}

func (p *Pack) col(req *spi.Request, n string) spi.Collection {
	return p.deps.Store.Scope(req.Identity.Account, req.Identity.Region).Collection(n)
}

func (p *Pack) Invoke(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	name := first(req.Input, "Name", "GroupName")
	switch req.Operation {
	case "CreateGroup":
		if name == "" {
			return nil, &spi.Fault{Code: "BadRequestException", HTTPStatus: 400, Fault: "client"}
		}
		rec := map[string]any{
			"Name": name, "Description": first(req.Input, "Description"),
			"GroupArn": "arn:aws:resource-groups:" + req.Identity.Region + ":" + req.Identity.Account + ":group/" + name,
		}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "rg").Put(ctx, name, b)
		return &spi.Response{Output: map[string]any{"Group": rec}}, nil
	case "GetGroup":
		b, ok, _ := p.col(req, "rg").Get(ctx, name)
		if !ok {
			return nil, &spi.Fault{Code: "NotFoundException", HTTPStatus: 404, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		return &spi.Response{Output: map[string]any{"Group": rec}}, nil
	case "ListGroups":
		return listWrap(ctx, p.col(req, "rg"), "GroupIdentifiers")
	case "DeleteGroup":
		_ = p.col(req, "rg").Delete(ctx, name)
		return &spi.Response{Output: map[string]any{}}, nil
	case "GroupResources":
		return &spi.Response{Output: map[string]any{"Succeeded": req.Input["ResourceArns"], "Failed": []any{}}}, nil
	case "ListGroupResources":
		return &spi.Response{Output: map[string]any{"Resources": []any{}}}, nil
	case "UngroupResources":
		return &spi.Response{Output: map[string]any{"Succeeded": req.Input["ResourceArns"], "Failed": []any{}}}, nil
	default:
		return nil, spi.NotImplemented("aws.resource-groups", req.Operation, "emulate")
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
