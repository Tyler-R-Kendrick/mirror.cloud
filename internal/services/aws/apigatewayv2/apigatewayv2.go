// Package apigatewayv2 stores HTTP/WebSocket API records (no real WebSocket fanout).
package apigatewayv2

import (
	"context"
	"encoding/json"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func init() {
	registry.Register(registry.Factory{ServiceID: "aws.apigatewayv2", Tier: model.TierEmulate, New: func(d spi.Deps) (spi.BehaviorPack, error) {
		return New(d), nil
	}})
}

// Pack implements API Gateway v2-lite.
type Pack struct{ deps spi.Deps }

// New constructs the pack.
func New(d spi.Deps) *Pack { return &Pack{deps: d} }

func (p *Pack) ServiceID() string { return "aws.apigatewayv2" }
func (p *Pack) Tier() model.Tier  { return model.TierEmulate }
func (p *Pack) Operations() []string {
	return []string{
		"CreateApi", "GetApi", "GetApis", "UpdateApi", "DeleteApi",
		"CreateRoute", "GetRoute", "GetRoutes", "DeleteRoute",
		"CreateIntegration", "GetIntegration", "GetIntegrations", "DeleteIntegration",
		"CreateStage", "GetStage", "GetStages", "DeleteStage",
	}
}

func (p *Pack) col(req *spi.Request, n string) spi.Collection {
	return p.deps.Store.Scope(req.Identity.Account, req.Identity.Region).Collection(n)
}

func (p *Pack) Invoke(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	aid := first(req.Input, "ApiId")
	switch req.Operation {
	case "CreateApi":
		id := p.deps.Rand.Hex(8)
		rec := map[string]any{
			"ApiId": id, "Name": first(req.Input, "Name"), "ProtocolType": first(req.Input, "ProtocolType"),
			"ApiEndpoint": "https://" + id + ".execute-api.localhost",
		}
		if rec["ProtocolType"] == "" {
			rec["ProtocolType"] = "HTTP"
		}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "ag2").Put(ctx, id, b)
		return &spi.Response{Output: rec}, nil
	case "GetApi":
		return get(ctx, p.col(req, "ag2"), aid)
	case "GetApis":
		return listCol(ctx, p.col(req, "ag2"), "Items")
	case "UpdateApi":
		b, ok, _ := p.col(req, "ag2").Get(ctx, aid)
		if !ok {
			return nil, &spi.Fault{Code: "NotFoundException", HTTPStatus: 400, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		if n := first(req.Input, "Name"); n != "" {
			rec["Name"] = n
		}
		nb, _ := json.Marshal(rec)
		_ = p.col(req, "ag2").Put(ctx, aid, nb)
		return &spi.Response{Output: rec}, nil
	case "DeleteApi":
		_ = p.col(req, "ag2").Delete(ctx, aid)
		return &spi.Response{Output: map[string]any{}}, nil
	case "CreateRoute":
		id := p.deps.Rand.Hex(8)
		rec := map[string]any{"RouteId": id, "ApiId": aid, "RouteKey": first(req.Input, "RouteKey"), "Target": first(req.Input, "Target")}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "ag2rt:"+aid).Put(ctx, id, b)
		return &spi.Response{Output: rec}, nil
	case "GetRoute":
		return get(ctx, p.col(req, "ag2rt:"+aid), first(req.Input, "RouteId"))
	case "GetRoutes":
		return listCol(ctx, p.col(req, "ag2rt:"+aid), "Items")
	case "DeleteRoute":
		_ = p.col(req, "ag2rt:"+aid).Delete(ctx, first(req.Input, "RouteId"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "CreateIntegration":
		id := p.deps.Rand.Hex(8)
		rec := map[string]any{"IntegrationId": id, "ApiId": aid, "IntegrationType": first(req.Input, "IntegrationType"), "IntegrationUri": first(req.Input, "IntegrationUri")}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "ag2in:"+aid).Put(ctx, id, b)
		return &spi.Response{Output: rec}, nil
	case "GetIntegration":
		return get(ctx, p.col(req, "ag2in:"+aid), first(req.Input, "IntegrationId"))
	case "GetIntegrations":
		return listCol(ctx, p.col(req, "ag2in:"+aid), "Items")
	case "DeleteIntegration":
		_ = p.col(req, "ag2in:"+aid).Delete(ctx, first(req.Input, "IntegrationId"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "CreateStage":
		name := first(req.Input, "StageName")
		rec := map[string]any{"StageName": name, "ApiId": aid}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "ag2st:"+aid).Put(ctx, name, b)
		return &spi.Response{Output: rec}, nil
	case "GetStage":
		return get(ctx, p.col(req, "ag2st:"+aid), first(req.Input, "StageName"))
	case "GetStages":
		return listCol(ctx, p.col(req, "ag2st:"+aid), "Items")
	case "DeleteStage":
		_ = p.col(req, "ag2st:"+aid).Delete(ctx, first(req.Input, "StageName"))
		return &spi.Response{Output: map[string]any{}}, nil
	default:
		return nil, spi.NotImplemented("aws.apigatewayv2", req.Operation, "emulate")
	}
}

func get(ctx context.Context, c spi.Collection, id string) (*spi.Response, error) {
	b, ok, _ := c.Get(ctx, id)
	if !ok {
		return nil, &spi.Fault{Code: "NotFoundException", HTTPStatus: 400, Fault: "client"}
	}
	var rec map[string]any
	_ = json.Unmarshal(b, &rec)
	return &spi.Response{Output: rec}, nil
}

func listCol(ctx context.Context, c spi.Collection, key string) (*spi.Response, error) {
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
	for _, k := range keys {
		if s, ok := in[k].(string); ok && s != "" {
			return s
		}
	}
	return ""
}
