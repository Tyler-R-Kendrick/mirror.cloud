// Package lakeformation stores lake settings and grants (no Glue enforcement).
package lakeformation

import (
	"context"
	"encoding/json"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func init() {
	registry.Register(registry.Factory{ServiceID: "aws.lakeformation", Tier: model.TierEmulate, New: func(d spi.Deps) (spi.BehaviorPack, error) {
		return New(d), nil
	}})
}

// Pack implements Lake Formation-lite.
type Pack struct{ deps spi.Deps }

// New constructs the pack.
func New(d spi.Deps) *Pack { return &Pack{deps: d} }

func (p *Pack) ServiceID() string { return "aws.lakeformation" }
func (p *Pack) Tier() model.Tier  { return model.TierEmulate }
func (p *Pack) Operations() []string {
	return []string{
		"PutDataLakeSettings", "GetDataLakeSettings",
		"GrantPermissions", "ListPermissions", "RevokePermissions",
		"RegisterResource", "ListResources", "DeregisterResource",
	}
}

func (p *Pack) col(req *spi.Request, n string) spi.Collection {
	return p.deps.Store.Scope(req.Identity.Account, req.Identity.Region).Collection(n)
}

func (p *Pack) Invoke(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	switch req.Operation {
	case "PutDataLakeSettings":
		b, _ := json.Marshal(req.Input["DataLakeSettings"])
		_ = p.col(req, "lfset").Put(ctx, "settings", b)
		return &spi.Response{Output: map[string]any{}}, nil
	case "GetDataLakeSettings":
		b, ok, _ := p.col(req, "lfset").Get(ctx, "settings")
		if !ok {
			return &spi.Response{Output: map[string]any{"DataLakeSettings": map[string]any{"DataLakeAdmins": []any{}}}}, nil
		}
		var rec any
		_ = json.Unmarshal(b, &rec)
		return &spi.Response{Output: map[string]any{"DataLakeSettings": rec}}, nil
	case "GrantPermissions":
		id := p.deps.Rand.Hex(8)
		rec := map[string]any{"Principal": req.Input["Principal"], "Resource": req.Input["Resource"], "Permissions": req.Input["Permissions"]}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "lfperm").Put(ctx, id, b)
		return &spi.Response{Output: map[string]any{}}, nil
	case "ListPermissions":
		return listWrap(ctx, p.col(req, "lfperm"), "PrincipalResourcePermissions")
	case "RevokePermissions":
		kvs, _, _ := p.col(req, "lfperm").List(ctx, "", "", 0)
		want, _ := json.Marshal(req.Input["Principal"])
		for _, kv := range kvs {
			var rec map[string]any
			_ = json.Unmarshal(kv.Value, &rec)
			got, _ := json.Marshal(rec["Principal"])
			if string(got) == string(want) {
				_ = p.col(req, "lfperm").Delete(ctx, kv.Key)
			}
		}
		return &spi.Response{Output: map[string]any{}}, nil
	case "RegisterResource":
		arn := first(req.Input, "ResourceArn")
		rec := map[string]any{"ResourceArn": arn, "RoleArn": first(req.Input, "RoleArn")}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "lfres").Put(ctx, arn, b)
		return &spi.Response{Output: map[string]any{}}, nil
	case "ListResources":
		return listWrap(ctx, p.col(req, "lfres"), "ResourceInfoList")
	case "DeregisterResource":
		_ = p.col(req, "lfres").Delete(ctx, first(req.Input, "ResourceArn"))
		return &spi.Response{Output: map[string]any{}}, nil
	default:
		return nil, spi.NotImplemented("aws.lakeformation", req.Operation, "emulate")
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
