// Package s3tables stores table-bucket records (no Iceberg engine).
package s3tables

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
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

// RowMutation applies one Iceberg-style row operation.
type RowMutation struct {
	Operation  string
	Values     map[string]any
	UniqueKeys []string
}

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

// CreateTable creates a local Iceberg table with an explicit row schema.
func (p *Pack) CreateTable(ctx context.Context, identity spi.Identity, bucket, namespace, table string, columns []string) error {
	req := &spi.Request{Identity: identity}
	if _, ok, _ := p.col(req, "s3tb").Get(ctx, bucket); !ok || bucket == "" || namespace == "" || table == "" || len(columns) == 0 {
		return errors.New("S3 table configuration is invalid")
	}
	record := map[string]any{
		"name": table, "namespace": namespace, "tableBucketARN": "arn:aws:s3tables:" + identity.Region + ":" + identity.Account + ":bucket/" + bucket,
		"format": "ICEBERG", "columns": columns,
	}
	encoded, _ := json.Marshal(record)
	return p.col(req, "s3tt").Put(ctx, bucket+"/"+namespace+"/"+table, encoded)
}

// ApplyRows commits insert, update, and delete mutations to a local table.
func (p *Pack) ApplyRows(ctx context.Context, identity spi.Identity, bucket, namespace, table string, mutations []RowMutation) error {
	req := &spi.Request{Identity: identity}
	key := bucket + "/" + namespace + "/" + table
	encoded, ok, _ := p.col(req, "s3tt").Get(ctx, key)
	if !ok {
		return errors.New("S3 table not found")
	}
	var description map[string]any
	_ = json.Unmarshal(encoded, &description)
	columns := stringsFrom(description["columns"])
	if len(columns) == 0 {
		return errors.New("S3 table schema is unavailable")
	}
	columnIndex := make(map[string]int, len(columns))
	for index, column := range columns {
		columnIndex[column] = index
	}
	return p.col(req, "s3trows").Txn(ctx, func(tx spi.Tx) error {
		stored, _, err := tx.Get(key)
		if err != nil {
			return err
		}
		var rows [][]any
		_ = json.Unmarshal(stored, &rows)
		for _, mutation := range mutations {
			for column := range mutation.Values {
				if _, exists := columnIndex[column]; !exists {
					return errors.New("S3 table column does not exist")
				}
			}
			row := make([]any, len(columns))
			for column, value := range mutation.Values {
				row[columnIndex[column]] = value
			}
			switch strings.ToLower(mutation.Operation) {
			case "", "insert":
				rows = append(rows, row)
			case "update", "delete":
				indexes, err := uniqueIndexes(columnIndex, mutation.UniqueKeys)
				if err != nil {
					return err
				}
				matched := -1
				for index, existing := range rows {
					if sameKeys(existing, row, indexes) {
						matched = index
						break
					}
				}
				if strings.EqualFold(mutation.Operation, "delete") {
					if matched >= 0 {
						rows = append(rows[:matched], rows[matched+1:]...)
					}
				} else if matched >= 0 {
					rows[matched] = row
				} else {
					rows = append(rows, row)
				}
			default:
				return errors.New("S3 table operation is invalid")
			}
		}
		// ponytail: whole-table JSON rewrite; replace with Iceberg manifests when a file engine exists.
		stored, _ = json.Marshal(rows)
		return tx.Put(key, stored)
	})
}

// TableRows returns rows using table column names.
func (p *Pack) TableRows(ctx context.Context, identity spi.Identity, bucket, namespace, table string) ([]map[string]any, error) {
	req := &spi.Request{Identity: identity}
	key := bucket + "/" + namespace + "/" + table
	descriptionBody, ok, _ := p.col(req, "s3tt").Get(ctx, key)
	if !ok {
		return nil, errors.New("S3 table not found")
	}
	var description map[string]any
	_ = json.Unmarshal(descriptionBody, &description)
	columns := stringsFrom(description["columns"])
	rowsBody, _, _ := p.col(req, "s3trows").Get(ctx, key)
	var stored [][]any
	_ = json.Unmarshal(rowsBody, &stored)
	rows := make([]map[string]any, 0, len(stored))
	for _, storedRow := range stored {
		row := map[string]any{}
		for index, column := range columns {
			if index < len(storedRow) {
				row[column] = storedRow[index]
			}
		}
		rows = append(rows, row)
	}
	return rows, nil
}

func stringsFrom(value any) []string {
	values, _ := value.([]any)
	if strings, ok := value.([]string); ok {
		return strings
	}
	result := make([]string, 0, len(values))
	for _, value := range values {
		if stringValue, ok := value.(string); ok {
			result = append(result, stringValue)
		}
	}
	return result
}

func uniqueIndexes(columns map[string]int, keys []string) ([]int, error) {
	if len(keys) == 0 {
		return nil, errors.New("S3 table unique keys are required")
	}
	indexes := make([]int, 0, len(keys))
	for _, key := range keys {
		index, ok := columns[key]
		if !ok {
			return nil, errors.New("S3 table unique key does not exist")
		}
		indexes = append(indexes, index)
	}
	return indexes, nil
}

func sameKeys(left, right []any, indexes []int) bool {
	for _, index := range indexes {
		if index >= len(left) || index >= len(right) || !reflect.DeepEqual(left[index], right[index]) {
			return false
		}
	}
	return true
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
