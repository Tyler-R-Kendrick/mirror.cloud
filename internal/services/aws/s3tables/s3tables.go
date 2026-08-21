// Package s3tables stores table-bucket records (no Iceberg engine).
package s3tables

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func init() {
	registry.Register(registry.Factory{ServiceID: "aws.s3tables", Tier: model.TierEmulate, New: func(d spi.Deps) (spi.BehaviorPack, error) {
		return New(d), nil
	}})
}

// Pack implements S3 Tables-lite.
type Pack struct{ deps spi.Deps }

// New constructs the pack.
func New(d spi.Deps) *Pack { return &Pack{deps: d} }

func (p *Pack) ServiceID() string { return "aws.s3tables" }
func (p *Pack) Tier() model.Tier  { return model.TierEmulate }
func (p *Pack) Operations() []string {
	return []string{
		"CreateTableBucket", "GetTableBucket", "ListTableBuckets", "DeleteTableBucket",
		"CreateNamespace", "ListNamespaces",
		"CreateTable", "GetTable", "ListTables", "DeleteTable", "RenameTable", "GetTableMetadataLocation", "UpdateTableMetadataLocation",
	}
}

func (p *Pack) col(req *spi.Request, n string) spi.Collection {
	return p.deps.Store.Scope(req.Identity.Account, req.Identity.Region).Collection(n)
}

func (p *Pack) Invoke(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	name := first(req.Input, "name", "Name")
	switch req.Operation {
	case "CreateTableBucket":
		if name == "" {
			return nil, &spi.Fault{Code: "ValidationException", HTTPStatus: 400, Fault: "client"}
		}
		arn := "arn:aws:s3tables:" + req.Identity.Region + ":" + req.Identity.Account + ":bucket/" + name
		rec := map[string]any{"name": name, "arn": arn}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "s3tb").Put(ctx, name, b)
		return &spi.Response{Output: map[string]any{"arn": arn}}, nil
	case "GetTableBucket":
		b, ok, _ := p.col(req, "s3tb").Get(ctx, name)
		if !ok {
			return nil, &spi.Fault{Code: "NotFoundException", HTTPStatus: 404, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		return &spi.Response{Output: rec}, nil
	case "ListTableBuckets":
		return listWrap(ctx, p.col(req, "s3tb"), "tableBuckets")
	case "DeleteTableBucket":
		_ = p.col(req, "s3tb").Delete(ctx, name)
		return &spi.Response{Output: map[string]any{}}, nil
	case "CreateNamespace":
		ns := first(req.Input, "namespace", "Namespace")
		rec := map[string]any{"namespace": ns, "tableBucketArn": first(req.Input, "tableBucketARN", "tableBucketArn")}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "s3tns").Put(ctx, ns, b)
		return &spi.Response{Output: rec}, nil
	case "ListNamespaces":
		return listWrap(ctx, p.col(req, "s3tns"), "namespaces")
	case "CreateTable":
		key := tableKey(req.Input)
		rec := map[string]any{
			"name": name, "namespace": req.Input["namespace"], "tableBucketARN": first(req.Input, "tableBucketARN", "tableBucketArn"),
			"format": first(req.Input, "format", "Format"), "metadataLocation": first(req.Input, "metadataLocation", "MetadataLocation"),
		}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "s3tt").Put(ctx, key, b)
		return &spi.Response{Output: rec}, nil
	case "GetTable":
		return getTable(ctx, p.col(req, "s3tt"), tableKey(req.Input))
	case "ListTables":
		prefix := tableBucketName(req.Input) + "/" + namespaceName(req.Input) + "/"
		return listPrefix(ctx, p.col(req, "s3tt"), prefix, "tables")
	case "DeleteTable":
		key := tableKey(req.Input)
		_ = p.col(req, "s3tt").Delete(ctx, key)
		_ = p.col(req, "s3trows").Delete(ctx, key)
		return &spi.Response{Output: map[string]any{}}, nil
	case "RenameTable":
		key := tableKey(req.Input)
		b, ok, _ := p.col(req, "s3tt").Get(ctx, key)
		if !ok {
			return nil, &spi.Fault{Code: "NotFoundException", HTTPStatus: 404, Fault: "client"}
		}
		newName := first(req.Input, "newName", "NewName")
		newKey := strings.TrimSuffix(key, name) + newName
		_ = p.col(req, "s3tt").Put(ctx, newKey, b)
		_ = p.col(req, "s3tt").Delete(ctx, key)
		if rows, found, _ := p.col(req, "s3trows").Get(ctx, key); found {
			_ = p.col(req, "s3trows").Put(ctx, newKey, rows)
			_ = p.col(req, "s3trows").Delete(ctx, key)
		}
		return &spi.Response{Output: map[string]any{}}, nil
	case "GetTableMetadataLocation":
		resp, err := getTable(ctx, p.col(req, "s3tt"), tableKey(req.Input))
		if err != nil {
			return nil, err
		}
		return &spi.Response{Output: map[string]any{"metadataLocation": resp.Output["metadataLocation"]}}, nil
	case "UpdateTableMetadataLocation":
		key := tableKey(req.Input)
		b, ok, _ := p.col(req, "s3tt").Get(ctx, key)
		if !ok {
			return nil, &spi.Fault{Code: "NotFoundException", HTTPStatus: 404, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		rec["metadataLocation"] = first(req.Input, "metadataLocation", "MetadataLocation")
		b, _ = json.Marshal(rec)
		_ = p.col(req, "s3tt").Put(ctx, key, b)
		return &spi.Response{Output: map[string]any{}}, nil
	default:
		return nil, spi.NotImplemented("aws.s3tables", req.Operation, "emulate")
	}
}

func tableKey(in map[string]any) string {
	return tableBucketName(in) + "/" + namespaceName(in) + "/" + first(in, "name", "Name")
}

func tableBucketName(in map[string]any) string {
	value := first(in, "tableBucketARN", "tableBucketArn", "tableBucketName")
	if i := strings.LastIndex(value, "/"); i >= 0 {
		return value[i+1:]
	}
	return value
}

func namespaceName(in map[string]any) string {
	if value := first(in, "namespace", "Namespace"); value != "" {
		return value
	}
	if values, ok := in["namespace"].([]any); ok && len(values) > 0 {
		return first(map[string]any{"value": values[0]}, "value")
	}
	return ""
}

func getTable(ctx context.Context, col spi.Collection, key string) (*spi.Response, error) {
	b, ok, _ := col.Get(ctx, key)
	if !ok {
		return nil, &spi.Fault{Code: "NotFoundException", HTTPStatus: 404, Fault: "client"}
	}
	var rec map[string]any
	_ = json.Unmarshal(b, &rec)
	return &spi.Response{Output: rec}, nil
}

func listPrefix(ctx context.Context, col spi.Collection, prefix, output string) (*spi.Response, error) {
	kvs, _, _ := col.List(ctx, prefix, "", 0)
	items := make([]any, 0, len(kvs))
	for _, kv := range kvs {
		var rec map[string]any
		_ = json.Unmarshal(kv.Value, &rec)
		items = append(items, rec)
	}
	return &spi.Response{Output: map[string]any{output: items}}, nil
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
