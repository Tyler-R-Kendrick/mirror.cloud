// Package account stores alternate-contact records (no org account mutation).
package account

import (
	"context"
	"encoding/json"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func init() {
	registry.Register(registry.Factory{ServiceID: "aws.account", Tier: model.TierEmulate, New: func(d spi.Deps) (spi.BehaviorPack, error) {
		return New(d), nil
	}})
}

// Pack implements Account-lite.
type Pack struct{ deps spi.Deps }

// New constructs the pack.
func New(d spi.Deps) *Pack { return &Pack{deps: d} }

func (p *Pack) ServiceID() string { return "aws.account" }
func (p *Pack) Tier() model.Tier  { return model.TierEmulate }
func (p *Pack) Operations() []string {
	return []string{
		"PutAlternateContact", "GetAlternateContact", "DeleteAlternateContact",
		"PutContactInformation", "GetContactInformation", "ListRegions",
	}
}

func (p *Pack) col(req *spi.Request, n string) spi.Collection {
	return p.deps.Store.Scope(req.Identity.Account, req.Identity.Region).Collection(n)
}

func (p *Pack) Invoke(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	kind := first(req.Input, "AlternateContactType")
	switch req.Operation {
	case "PutAlternateContact":
		if kind == "" {
			return nil, &spi.Fault{Code: "ValidationException", HTTPStatus: 400, Fault: "client"}
		}
		rec := map[string]any{
			"AlternateContactType": kind, "EmailAddress": first(req.Input, "EmailAddress"),
			"Name": first(req.Input, "Name"), "PhoneNumber": first(req.Input, "PhoneNumber"),
			"Title": first(req.Input, "Title"),
		}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "acctc").Put(ctx, kind, b)
		return &spi.Response{Output: map[string]any{}}, nil
	case "GetAlternateContact":
		b, ok, _ := p.col(req, "acctc").Get(ctx, kind)
		if !ok {
			return nil, &spi.Fault{Code: "ResourceNotFoundException", HTTPStatus: 400, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		return &spi.Response{Output: map[string]any{"AlternateContact": rec}}, nil
	case "DeleteAlternateContact":
		_ = p.col(req, "acctc").Delete(ctx, kind)
		return &spi.Response{Output: map[string]any{}}, nil
	case "PutContactInformation":
		raw, _ := json.Marshal(req.Input["ContactInformation"])
		_ = p.col(req, "acctinfo").Put(ctx, "primary", raw)
		return &spi.Response{Output: map[string]any{}}, nil
	case "GetContactInformation":
		b, ok, _ := p.col(req, "acctinfo").Get(ctx, "primary")
		if !ok {
			return &spi.Response{Output: map[string]any{"ContactInformation": map[string]any{}}}, nil
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		return &spi.Response{Output: map[string]any{"ContactInformation": rec}}, nil
	case "ListRegions":
		return &spi.Response{Output: map[string]any{"Regions": []any{map[string]any{"RegionName": req.Identity.Region, "RegionOptStatus": "ENABLED"}}}}, nil
	default:
		return nil, spi.NotImplemented("aws.account", req.Operation, "emulate")
	}
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
