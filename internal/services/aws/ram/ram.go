// Package ram stores resource shares (no actual cross-account grant).
package ram

import (
	"context"
	"encoding/json"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func init() {
	registry.Register(registry.Factory{ServiceID: "aws.ram", Tier: model.TierEmulate, New: func(d spi.Deps) (spi.BehaviorPack, error) {
		return New(d), nil
	}})
}

// Pack implements RAM-lite.
type Pack struct{ deps spi.Deps }

// New constructs the pack.
func New(d spi.Deps) *Pack { return &Pack{deps: d} }

func (p *Pack) ServiceID() string { return "aws.ram" }
func (p *Pack) Tier() model.Tier  { return model.TierEmulate }
func (p *Pack) Operations() []string {
	return []string{
		"CreateResourceShare", "GetResourceShares", "UpdateResourceShare", "DeleteResourceShare",
		"AssociateResourceShare", "DisassociateResourceShare", "GetResourceShareAssociations",
		"ListResources",
	}
}

func (p *Pack) col(req *spi.Request, n string) spi.Collection {
	return p.deps.Store.Scope(req.Identity.Account, req.Identity.Region).Collection(n)
}

func (p *Pack) Invoke(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	switch req.Operation {
	case "CreateResourceShare", "UpdateResourceShare":
		arn := first(req.Input, "resourceShareArn")
		if arn == "" {
			arn = "arn:aws:ram:" + req.Identity.Region + ":" + req.Identity.Account + ":resource-share/" + p.deps.Rand.Hex(8)
		}
		rec := map[string]any{
			"resourceShareArn": arn, "name": first(req.Input, "name", "Name"), "status": "ACTIVE",
			"resourceArns": req.Input["resourceArns"], "principals": req.Input["principals"],
		}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "ram").Put(ctx, arn, b)
		return &spi.Response{Output: map[string]any{"resourceShare": rec}}, nil
	case "GetResourceShares":
		arn := first(req.Input, "resourceShareArns", "resourceShareArn")
		if arn != "" {
			b, ok, _ := p.col(req, "ram").Get(ctx, arn)
			if !ok {
				return &spi.Response{Output: map[string]any{"resourceShares": []any{}}}, nil
			}
			var rec map[string]any
			_ = json.Unmarshal(b, &rec)
			return &spi.Response{Output: map[string]any{"resourceShares": []any{rec}}}, nil
		}
		return listWrap(ctx, p.col(req, "ram"), "resourceShares")
	case "DeleteResourceShare":
		_ = p.col(req, "ram").Delete(ctx, first(req.Input, "resourceShareArn"))
		return &spi.Response{Output: map[string]any{"returnValue": true}}, nil
	case "AssociateResourceShare", "DisassociateResourceShare":
		arn := first(req.Input, "resourceShareArn")
		b, ok, _ := p.col(req, "ram").Get(ctx, arn)
		rec := map[string]any{"resourceShareArn": arn}
		if ok {
			_ = json.Unmarshal(b, &rec)
		}
		if req.Operation == "AssociateResourceShare" {
			rec["resourceArns"] = req.Input["resourceArns"]
			rec["principals"] = req.Input["principals"]
		}
		nb, _ := json.Marshal(rec)
		_ = p.col(req, "ram").Put(ctx, arn, nb)
		return &spi.Response{Output: map[string]any{"resourceShareAssociations": []any{rec}}}, nil
	case "GetResourceShareAssociations":
		return listWrap(ctx, p.col(req, "ram"), "resourceShareAssociations")
	case "ListResources":
		return listWrap(ctx, p.col(req, "ram"), "resources")
	default:
		return nil, spi.NotImplemented("aws.ram", req.Operation, "emulate")
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
		switch t := in[k].(type) {
		case string:
			if t != "" {
				return t
			}
		case []any:
			if len(t) > 0 {
				if s, ok := t[0].(string); ok {
					return s
				}
			}
		}
	}
	return ""
}
