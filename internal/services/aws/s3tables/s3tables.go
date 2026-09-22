// Package s3tables is the S3 Tables row plane: the local Iceberg-style tables
// aws.firehose delivers into and the athena tests read. The table-bucket API is
// served by behavior/aws/s3tables; this package holds only what no SPI
// operation reaches -- creating a table with a row schema, applying row
// mutations and reading rows back -- over the collections that bundle owns.
package s3tables

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

// Tables applies and reads table rows.
type Tables struct{ deps spi.Deps }

// RowMutation applies one Iceberg-style row operation.
type RowMutation struct {
	Operation  string
	Values     map[string]any
	UniqueKeys []string
}

// rowsDoc is a table's stored rows. It is an object rather than a bare array
// so the bundle's DeleteTable and RenameTable can delete and move it as a
// record.
type rowsDoc struct {
	Rows [][]any `json:"rows"`
}

// New constructs the row plane.
func New(d spi.Deps) *Tables { return &Tables{deps: d} }

func (t *Tables) col(identity spi.Identity, n string) spi.Collection {
	return t.deps.Store.Scope(identity.Account, identity.Region).Collection(n)
}

// CreateTable creates a local Iceberg table with an explicit row schema.
func (t *Tables) CreateTable(ctx context.Context, identity spi.Identity, bucket, namespace, table string, columns []string) error {
	if _, ok, _ := t.col(identity, "s3tb").Get(ctx, bucket); !ok || bucket == "" || namespace == "" || table == "" || len(columns) == 0 {
		return errors.New("S3 table configuration is invalid")
	}
	record := map[string]any{
		"name": table, "namespace": namespace, "tableBucketARN": "arn:aws:s3tables:" + identity.Region + ":" + identity.Account + ":bucket/" + bucket,
		"format": "ICEBERG", "columns": columns,
		// Written as the bundle's CreateTable writes it, so GetTable and
		// RenameTable read a row-plane table the same as an API one.
		"metadataLocation": "",
	}
	encoded, _ := json.Marshal(record)
	return t.col(identity, "s3tt").Put(ctx, bucket+"/"+namespace+"/"+table, encoded)
}

// ApplyRows commits insert, update, and delete mutations to a local table.
func (t *Tables) ApplyRows(ctx context.Context, identity spi.Identity, bucket, namespace, table string, mutations []RowMutation) error {
	key := bucket + "/" + namespace + "/" + table
	encoded, ok, _ := t.col(identity, "s3tt").Get(ctx, key)
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
	return t.col(identity, "s3trows").Txn(ctx, func(tx spi.Tx) error {
		stored, _, err := tx.Get(key)
		if err != nil {
			return err
		}
		var doc rowsDoc
		_ = json.Unmarshal(stored, &doc)
		rows := doc.Rows
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
		stored, _ = json.Marshal(rowsDoc{Rows: rows})
		return tx.Put(key, stored)
	})
}

// TableRows returns rows using table column names.
func (t *Tables) TableRows(ctx context.Context, identity spi.Identity, bucket, namespace, table string) ([]map[string]any, error) {
	key := bucket + "/" + namespace + "/" + table
	descriptionBody, ok, _ := t.col(identity, "s3tt").Get(ctx, key)
	if !ok {
		return nil, errors.New("S3 table not found")
	}
	var description map[string]any
	_ = json.Unmarshal(descriptionBody, &description)
	columns := stringsFrom(description["columns"])
	rowsBody, _, _ := t.col(identity, "s3trows").Get(ctx, key)
	var doc rowsDoc
	_ = json.Unmarshal(rowsBody, &doc)
	rows := make([]map[string]any, 0, len(doc.Rows))
	for _, storedRow := range doc.Rows {
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
