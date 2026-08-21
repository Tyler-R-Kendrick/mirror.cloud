// Package directoryservice stores directory records (no AD or LDAP).
package directoryservice

import (
	"context"
	"encoding/json"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func init() {
	registry.Register(registry.Factory{ServiceID: "aws.ds", Tier: model.TierEmulate, New: func(d spi.Deps) (spi.BehaviorPack, error) {
		return New(d), nil
	}})
}

// Pack implements Directory Service-lite.
type Pack struct{ deps spi.Deps }

// New constructs the pack.
func New(d spi.Deps) *Pack { return &Pack{deps: d} }

func (p *Pack) ServiceID() string { return "aws.ds" }
func (p *Pack) Tier() model.Tier  { return model.TierEmulate }
func (p *Pack) Operations() []string {
	return []string{
		"CreateDirectory", "DescribeDirectories", "DeleteDirectory",
		"CreateMicrosoftAD", "CreateAlias", "DescribeTrusts",
	}
}

func (p *Pack) col(req *spi.Request, n string) spi.Collection {
	return p.deps.Store.Scope(req.Identity.Account, req.Identity.Region).Collection(n)
}

func (p *Pack) Invoke(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	switch req.Operation {
	case "CreateDirectory", "CreateMicrosoftAD":
		id := "d-" + p.deps.Rand.Hex(8)
		kind := "SimpleAD"
		if req.Operation == "CreateMicrosoftAD" {
			kind = "MicrosoftAD"
		}
		rec := map[string]any{
			"DirectoryId": id, "Name": first(req.Input, "Name"), "Type": kind, "Stage": "Active",
			"ShortName": first(req.Input, "ShortName"),
		}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "dsdir").Put(ctx, id, b)
		return &spi.Response{Output: map[string]any{"DirectoryId": id}}, nil
	case "DescribeDirectories":
		return listOrGet(ctx, p.col(req, "dsdir"), firstID(req.Input), "DirectoryDescriptions")
	case "DeleteDirectory":
		_ = p.col(req, "dsdir").Delete(ctx, first(req.Input, "DirectoryId"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "CreateAlias":
		id := first(req.Input, "DirectoryId")
		b, ok, _ := p.col(req, "dsdir").Get(ctx, id)
		rec := map[string]any{"DirectoryId": id}
		if ok {
			_ = json.Unmarshal(b, &rec)
		}
		rec["Alias"] = first(req.Input, "Alias")
		nb, _ := json.Marshal(rec)
		_ = p.col(req, "dsdir").Put(ctx, id, nb)
		return &spi.Response{Output: map[string]any{"DirectoryId": id, "Alias": rec["Alias"]}}, nil
	case "DescribeTrusts":
		return &spi.Response{Output: map[string]any{"Trusts": []any{}}}, nil
	default:
		return nil, spi.NotImplemented("aws.ds", req.Operation, "emulate")
	}
}

func firstID(in map[string]any) string {
	if s := first(in, "DirectoryId"); s != "" {
		return s
	}
	if arr, ok := in["DirectoryIds"].([]any); ok && len(arr) > 0 {
		if s, ok := arr[0].(string); ok {
			return s
		}
	}
	return ""
}

func listOrGet(ctx context.Context, c spi.Collection, want, key string) (*spi.Response, error) {
	if want != "" {
		b, ok, _ := c.Get(ctx, want)
		if !ok {
			return &spi.Response{Output: map[string]any{key: []any{}}}, nil
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		return &spi.Response{Output: map[string]any{key: []any{rec}}}, nil
	}
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
