// Package servicecatalog stores products and portfolios (no provisioning).
package servicecatalog

import (
	"context"
	"encoding/json"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func init() {
	registry.Register(registry.Factory{ServiceID: "aws.servicecatalog", Tier: model.TierEmulate, New: func(d spi.Deps) (spi.BehaviorPack, error) {
		return New(d), nil
	}})
}

// Pack implements Service Catalog-lite.
type Pack struct{ deps spi.Deps }

// New constructs the pack.
func New(d spi.Deps) *Pack { return &Pack{deps: d} }

func (p *Pack) ServiceID() string { return "aws.servicecatalog" }
func (p *Pack) Tier() model.Tier  { return model.TierEmulate }
func (p *Pack) Operations() []string {
	return []string{
		"CreateProduct", "DescribeProduct", "DeleteProduct",
		"CreatePortfolio", "DescribePortfolio", "ListPortfolios", "DeletePortfolio",
		"AssociateProductWithPortfolio", "DisassociateProductFromPortfolio",
	}
}

func (p *Pack) col(req *spi.Request, n string) spi.Collection {
	return p.deps.Store.Scope(req.Identity.Account, req.Identity.Region).Collection(n)
}

func (p *Pack) Invoke(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	switch req.Operation {
	case "CreateProduct":
		id := "prod-" + p.deps.Rand.Hex(8)
		rec := map[string]any{"Id": id, "Name": first(req.Input, "Name"), "Owner": first(req.Input, "Owner"), "ProductType": first(req.Input, "ProductType")}
		if rec["ProductType"] == "" {
			rec["ProductType"] = "CLOUD_FORMATION_TEMPLATE"
		}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "scprod").Put(ctx, id, b)
		return &spi.Response{Output: map[string]any{"ProductViewDetail": map[string]any{"ProductViewSummary": rec}}}, nil
	case "DescribeProduct":
		id := first(req.Input, "Id", "ProductId")
		b, ok, _ := p.col(req, "scprod").Get(ctx, id)
		if !ok {
			return nil, &spi.Fault{Code: "ResourceNotFoundException", HTTPStatus: 400, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		return &spi.Response{Output: map[string]any{"ProductViewSummary": rec}}, nil
	case "DeleteProduct":
		_ = p.col(req, "scprod").Delete(ctx, first(req.Input, "Id", "ProductId"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "CreatePortfolio":
		id := "port-" + p.deps.Rand.Hex(8)
		rec := map[string]any{"Id": id, "DisplayName": first(req.Input, "DisplayName"), "ProviderName": first(req.Input, "ProviderName")}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "scport").Put(ctx, id, b)
		return &spi.Response{Output: map[string]any{"PortfolioDetail": rec}}, nil
	case "DescribePortfolio":
		id := first(req.Input, "Id")
		b, ok, _ := p.col(req, "scport").Get(ctx, id)
		if !ok {
			return nil, &spi.Fault{Code: "ResourceNotFoundException", HTTPStatus: 400, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		return &spi.Response{Output: map[string]any{"PortfolioDetail": rec}}, nil
	case "ListPortfolios":
		return listWrap(ctx, p.col(req, "scport"), "PortfolioDetails")
	case "DeletePortfolio":
		_ = p.col(req, "scport").Delete(ctx, first(req.Input, "Id"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "AssociateProductWithPortfolio", "DisassociateProductFromPortfolio":
		key := first(req.Input, "ProductId") + "/" + first(req.Input, "PortfolioId")
		if req.Operation == "AssociateProductWithPortfolio" {
			b, _ := json.Marshal(map[string]any{"ProductId": first(req.Input, "ProductId"), "PortfolioId": first(req.Input, "PortfolioId")})
			_ = p.col(req, "scassoc").Put(ctx, key, b)
		} else {
			_ = p.col(req, "scassoc").Delete(ctx, key)
		}
		return &spi.Response{Output: map[string]any{}}, nil
	default:
		return nil, spi.NotImplemented("aws.servicecatalog", req.Operation, "emulate")
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
