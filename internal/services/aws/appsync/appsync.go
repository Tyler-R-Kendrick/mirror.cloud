// Package appsync stores GraphQL APIs, keys, data sources, and schemas (no VTL/AppSync runtime).
package appsync

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func init() {
	registry.Register(registry.Factory{ServiceID: "aws.appsync", Tier: model.TierEmulate, New: func(d spi.Deps) (spi.BehaviorPack, error) {
		return New(d), nil
	}})
}

// Pack implements AppSync-lite.
type Pack struct{ deps spi.Deps }

// New constructs the pack.
func New(d spi.Deps) *Pack { return &Pack{deps: d} }

func (p *Pack) ServiceID() string { return "aws.appsync" }
func (p *Pack) Tier() model.Tier  { return model.TierEmulate }
func (p *Pack) Operations() []string {
	return []string{
		"CreateGraphqlApi", "GetGraphqlApi", "ListGraphqlApis", "UpdateGraphqlApi", "DeleteGraphqlApi",
		"CreateApiKey", "ListApiKeys", "DeleteApiKey",
		"StartSchemaCreation", "GetSchemaCreationStatus",
		"CreateDataSource", "GetDataSource", "ListDataSources", "DeleteDataSource",
		"GraphQL",
	}
}

func (p *Pack) col(req *spi.Request, n string) spi.Collection {
	return p.deps.Store.Scope(req.Identity.Account, req.Identity.Region).Collection(n)
}

func (p *Pack) Invoke(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	id := first(req.Input, "apiId", "ApiId")
	switch req.Operation {
	case "CreateGraphqlApi":
		id = p.deps.Rand.Hex(8)
		name := first(req.Input, "name", "Name")
		rec := map[string]any{
			"apiId": id, "name": name, "authenticationType": first(req.Input, "authenticationType", "AuthenticationType"),
			"uris": map[string]any{"GRAPHQL": "http://appsync.localhost/" + id + "/graphql"},
			"arn":  "arn:aws:appsync:" + req.Identity.Region + ":" + req.Identity.Account + ":apis/" + id,
		}
		if rec["authenticationType"] == "" {
			rec["authenticationType"] = "API_KEY"
		}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "asapi").Put(ctx, id, b)
		return &spi.Response{Output: map[string]any{"graphqlApi": rec}}, nil
	case "GetGraphqlApi":
		b, ok, _ := p.col(req, "asapi").Get(ctx, id)
		if !ok {
			return nil, &spi.Fault{Code: "NotFoundException", HTTPStatus: 400, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		return &spi.Response{Output: map[string]any{"graphqlApi": rec}}, nil
	case "ListGraphqlApis":
		return listCol(ctx, p.col(req, "asapi"), "graphqlApis")
	case "UpdateGraphqlApi":
		b, ok, _ := p.col(req, "asapi").Get(ctx, id)
		if !ok {
			return nil, &spi.Fault{Code: "NotFoundException", HTTPStatus: 400, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		if n := first(req.Input, "name", "Name"); n != "" {
			rec["name"] = n
		}
		nb, _ := json.Marshal(rec)
		_ = p.col(req, "asapi").Put(ctx, id, nb)
		return &spi.Response{Output: map[string]any{"graphqlApi": rec}}, nil
	case "DeleteGraphqlApi":
		_ = p.col(req, "asapi").Delete(ctx, id)
		return &spi.Response{Output: map[string]any{}}, nil
	case "CreateApiKey":
		kid := p.deps.Rand.Hex(16)
		rec := map[string]any{"id": kid, "description": first(req.Input, "description"), "expires": 0}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "askey:"+id).Put(ctx, kid, b)
		return &spi.Response{Output: map[string]any{"apiKey": rec}}, nil
	case "ListApiKeys":
		return listCol(ctx, p.col(req, "askey:"+id), "apiKeys")
	case "DeleteApiKey":
		_ = p.col(req, "askey:"+id).Delete(ctx, first(req.Input, "id"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "StartSchemaCreation":
		def := first(req.Input, "definition", "Definition")
		rec := map[string]any{"status": "SUCCESS", "definition": def}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "asschema").Put(ctx, id, b)
		return &spi.Response{Output: map[string]any{"status": "SUCCESS"}}, nil
	case "GetSchemaCreationStatus":
		b, ok, _ := p.col(req, "asschema").Get(ctx, id)
		if !ok {
			return &spi.Response{Output: map[string]any{"status": "PROCESSING"}}, nil
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		return &spi.Response{Output: map[string]any{"status": rec["status"]}}, nil
	case "CreateDataSource":
		name := first(req.Input, "name", "Name")
		rec := map[string]any{"name": name, "type": first(req.Input, "type", "Type"), "apiId": id}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "asds:"+id).Put(ctx, name, b)
		return &spi.Response{Output: map[string]any{"dataSource": rec}}, nil
	case "GetDataSource":
		return getWrap(ctx, p.col(req, "asds:"+id), first(req.Input, "name", "Name"), "dataSource")
	case "ListDataSources":
		return listCol(ctx, p.col(req, "asds:"+id), "dataSources")
	case "DeleteDataSource":
		_ = p.col(req, "asds:"+id).Delete(ctx, first(req.Input, "name", "Name"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "GraphQL":
		q := first(req.Input, "query", "Query")
		data := map[string]any{}
		if strings.Contains(q, "__typename") {
			data["__typename"] = "Query"
		}
		if strings.Contains(q, "hello") {
			data["hello"] = "world"
		}
		if len(data) == 0 {
			data["_echo"] = q
		}
		return &spi.Response{Output: map[string]any{"data": data}}, nil
	default:
		return nil, spi.NotImplemented("aws.appsync", req.Operation, "emulate")
	}
}

func getWrap(ctx context.Context, c spi.Collection, id, wrap string) (*spi.Response, error) {
	b, ok, _ := c.Get(ctx, id)
	if !ok {
		return nil, &spi.Fault{Code: "NotFoundException", HTTPStatus: 400, Fault: "client"}
	}
	var rec map[string]any
	_ = json.Unmarshal(b, &rec)
	return &spi.Response{Output: map[string]any{wrap: rec}}, nil
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
