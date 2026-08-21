// Package timestream stores databases, tables, and records (not a time-series engine).
package timestream

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func init() {
	registry.Register(registry.Factory{ServiceID: "aws.timestream", Tier: model.TierEmulate, New: func(d spi.Deps) (spi.BehaviorPack, error) {
		return New(d), nil
	}})
}

// Pack implements Timestream-lite.
type Pack struct{ deps spi.Deps }

// New constructs the pack.
func New(d spi.Deps) *Pack { return &Pack{deps: d} }

func (p *Pack) ServiceID() string { return "aws.timestream" }
func (p *Pack) Tier() model.Tier  { return model.TierEmulate }
func (p *Pack) Operations() []string {
	return []string{
		"CreateDatabase", "DescribeDatabase", "ListDatabases", "UpdateDatabase", "DeleteDatabase",
		"CreateTable", "DescribeTable", "ListTables", "DeleteTable",
		"WriteRecords", "Query", "DescribeEndpoints",
	}
}

func (p *Pack) col(req *spi.Request, n string) spi.Collection {
	return p.deps.Store.Scope(req.Identity.Account, req.Identity.Region).Collection(n)
}

func (p *Pack) Invoke(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	db := first(req.Input, "DatabaseName")
	tbl := first(req.Input, "TableName")
	switch req.Operation {
	case "CreateDatabase", "UpdateDatabase":
		if db == "" {
			return nil, &spi.Fault{Code: "ValidationException", HTTPStatus: 400, Fault: "client"}
		}
		rec := map[string]any{"DatabaseName": db, "Arn": "arn:aws:timestream:" + req.Identity.Region + ":" + req.Identity.Account + ":database/" + db}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "tsdb").Put(ctx, db, b)
		return &spi.Response{Output: map[string]any{"Database": rec}}, nil
	case "DescribeDatabase":
		b, ok, _ := p.col(req, "tsdb").Get(ctx, db)
		if !ok {
			return nil, &spi.Fault{Code: "ResourceNotFoundException", HTTPStatus: 400, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		return &spi.Response{Output: map[string]any{"Database": rec}}, nil
	case "ListDatabases":
		return listWrap(ctx, p.col(req, "tsdb"), "Databases")
	case "DeleteDatabase":
		_ = p.col(req, "tsdb").Delete(ctx, db)
		return &spi.Response{Output: map[string]any{}}, nil
	case "CreateTable":
		if db == "" || tbl == "" {
			return nil, &spi.Fault{Code: "ValidationException", HTTPStatus: 400, Fault: "client"}
		}
		rec := map[string]any{"DatabaseName": db, "TableName": tbl, "Arn": "arn:aws:timestream:" + req.Identity.Region + ":" + req.Identity.Account + ":database/" + db + "/table/" + tbl}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "tstab:"+db).Put(ctx, tbl, b)
		return &spi.Response{Output: map[string]any{"Table": rec}}, nil
	case "DescribeTable":
		b, ok, _ := p.col(req, "tstab:"+db).Get(ctx, tbl)
		if !ok {
			return nil, &spi.Fault{Code: "ResourceNotFoundException", HTTPStatus: 400, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		return &spi.Response{Output: map[string]any{"Table": rec}}, nil
	case "ListTables":
		return listWrap(ctx, p.col(req, "tstab:"+db), "Tables")
	case "DeleteTable":
		_ = p.col(req, "tstab:"+db).Delete(ctx, tbl)
		return &spi.Response{Output: map[string]any{}}, nil
	case "WriteRecords":
		recs, _ := req.Input["Records"].([]any)
		key := db + "/" + tbl
		b, _ := json.Marshal(recs)
		_ = p.col(req, "tsrec").Put(ctx, key, b)
		return &spi.Response{Output: map[string]any{"RecordsIngested": map[string]any{"Total": len(recs)}}}, nil
	case "Query":
		// ponytail: not SQL; returns WriteRecords for a table name that appears in QueryString.
		q := first(req.Input, "QueryString")
		kvs, _, _ := p.col(req, "tsrec").List(ctx, "", "", 0)
		var rows []any
		cols := []any{map[string]any{"Name": "measure", "Type": "VARCHAR"}}
		for _, kv := range kvs {
			parts := strings.Split(kv.Key, "/")
			hit := q == ""
			for _, part := range parts {
				if part != "" && strings.Contains(q, part) {
					hit = true
					break
				}
			}
			if !hit {
				continue
			}
			var recs []any
			_ = json.Unmarshal(kv.Value, &recs)
			for _, r := range recs {
				rows = append(rows, map[string]any{"Data": []any{map[string]any{"ScalarValue": str(r)}}})
			}
		}
		return &spi.Response{Output: map[string]any{"ColumnInfo": cols, "Rows": rows}}, nil
	case "DescribeEndpoints":
		return &spi.Response{Output: map[string]any{"Endpoints": []any{map[string]any{"Address": "127.0.0.1", "CachePeriodInMinutes": 60}}}}, nil
	default:
		return nil, spi.NotImplemented("aws.timestream", req.Operation, "emulate")
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

func str(v any) string {
	switch t := v.(type) {
	case string:
		return t
	default:
		b, _ := json.Marshal(v)
		return string(b)
	}
}
