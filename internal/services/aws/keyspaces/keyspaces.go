// Package keyspaces stores keyspace and table records (no Cassandra CQL).
package keyspaces

import (
	"context"
	"encoding/json"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func init() {
	registry.Register(registry.Factory{ServiceID: "aws.keyspaces", Tier: model.TierEmulate, New: func(d spi.Deps) (spi.BehaviorPack, error) {
		return New(d), nil
	}})
}

// Pack implements Keyspaces-lite.
type Pack struct{ deps spi.Deps }

// New constructs the pack.
func New(d spi.Deps) *Pack { return &Pack{deps: d} }

func (p *Pack) ServiceID() string { return "aws.keyspaces" }
func (p *Pack) Tier() model.Tier  { return model.TierEmulate }
func (p *Pack) Operations() []string {
	return []string{
		"CreateKeyspace", "GetKeyspace", "ListKeyspaces", "DeleteKeyspace",
		"CreateTable", "GetTable", "ListTables", "DeleteTable",
	}
}

func (p *Pack) col(req *spi.Request, n string) spi.Collection {
	return p.deps.Store.Scope(req.Identity.Account, req.Identity.Region).Collection(n)
}

func (p *Pack) Invoke(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	ks := first(req.Input, "keyspaceName", "KeyspaceName")
	tbl := first(req.Input, "tableName", "TableName")
	switch req.Operation {
	case "CreateKeyspace":
		if ks == "" {
			return nil, &spi.Fault{Code: "ValidationException", HTTPStatus: 400, Fault: "client"}
		}
		arn := "arn:aws:cassandra:" + req.Identity.Region + ":" + req.Identity.Account + ":/keyspace/" + ks + "/"
		rec := map[string]any{"keyspaceName": ks, "resourceArn": arn}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "ksks").Put(ctx, ks, b)
		return &spi.Response{Output: map[string]any{"resourceArn": arn}}, nil
	case "GetKeyspace":
		b, ok, _ := p.col(req, "ksks").Get(ctx, ks)
		if !ok {
			return nil, &spi.Fault{Code: "ResourceNotFoundException", HTTPStatus: 400, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		return &spi.Response{Output: rec}, nil
	case "ListKeyspaces":
		return listWrap(ctx, p.col(req, "ksks"), "keyspaces")
	case "DeleteKeyspace":
		_ = p.col(req, "ksks").Delete(ctx, ks)
		return &spi.Response{Output: map[string]any{}}, nil
	case "CreateTable":
		if ks == "" || tbl == "" {
			return nil, &spi.Fault{Code: "ValidationException", HTTPStatus: 400, Fault: "client"}
		}
		arn := "arn:aws:cassandra:" + req.Identity.Region + ":" + req.Identity.Account + ":/keyspace/" + ks + "/table/" + tbl
		rec := map[string]any{"keyspaceName": ks, "tableName": tbl, "resourceArn": arn, "status": "ACTIVE"}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "kstbl:"+ks).Put(ctx, tbl, b)
		return &spi.Response{Output: map[string]any{"resourceArn": arn}}, nil
	case "GetTable":
		b, ok, _ := p.col(req, "kstbl:"+ks).Get(ctx, tbl)
		if !ok {
			return nil, &spi.Fault{Code: "ResourceNotFoundException", HTTPStatus: 400, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		return &spi.Response{Output: rec}, nil
	case "ListTables":
		return listWrap(ctx, p.col(req, "kstbl:"+ks), "tables")
	case "DeleteTable":
		_ = p.col(req, "kstbl:"+ks).Delete(ctx, tbl)
		return &spi.Response{Output: map[string]any{}}, nil
	default:
		return nil, spi.NotImplemented("aws.keyspaces", req.Operation, "emulate")
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
