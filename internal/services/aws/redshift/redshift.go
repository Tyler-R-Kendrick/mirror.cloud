// Package redshift is the Redshift COPY data plane: the local tables
// aws.firehose loads through COPY. The cluster API is served by
// behavior/aws/redshift; this package holds only what no SPI operation reaches
// -- declaring a table, COPY row parsing with its credential check, and
// reading rows back -- over the collections that bundle owns.
package redshift

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

// Warehouse loads and reads COPY tables.
type Warehouse struct{ deps spi.Deps }

// CopyInput is one COPY command.
type CopyInput struct {
	Cluster, Database, Table, Username, Password, Columns, Options string
	Data                                                           [][]byte
}

// tableData is a declared table and its rows. Cluster is stored so the
// bundle's DeleteCluster can remove a cluster's tables by predicate.
type tableData struct {
	Cluster string `json:",omitempty"`
	Columns []string
	Rows    []map[string]any
}

// New constructs the data plane.
func New(d spi.Deps) *Warehouse { return &Warehouse{deps: d} }

func (w *Warehouse) col(identity spi.Identity, n string) spi.Collection {
	return w.deps.Store.Scope(identity.Account, identity.Region).Collection(n)
}

// CreateTable declares the existing table required by a Firehose COPY command.
func (w *Warehouse) CreateTable(ctx context.Context, identity spi.Identity, cluster, database, table string, columns []string) error {
	if cluster == "" || database == "" || table == "" || len(columns) == 0 {
		return errors.New("cluster, database, table, and columns are required")
	}
	if _, ok, _ := w.col(identity, "rscluster").Get(ctx, cluster); !ok {
		return errors.New("Redshift cluster not found")
	}
	for _, column := range columns {
		if strings.TrimSpace(column) == "" {
			return errors.New("Redshift table column is empty")
		}
	}
	body, _ := json.Marshal(tableData{Cluster: cluster, Columns: columns})
	return w.col(identity, "rstable").Put(ctx, redshiftTableKey(cluster, database, table), body)
}

// Copy loads records into a declared table using the common Firehose COPY formats.
func (w *Warehouse) Copy(ctx context.Context, identity spi.Identity, input CopyInput) error {
	clusterBody, ok, _ := w.col(identity, "rscluster").Get(ctx, input.Cluster)
	if !ok {
		return errors.New("Redshift cluster not found")
	}
	var cluster map[string]any
	_ = json.Unmarshal(clusterBody, &cluster)
	if first(cluster, "ClusterStatus") != "available" || first(cluster, "DBName") != input.Database {
		return errors.New("Redshift cluster is unavailable or database does not exist")
	}
	credentialBody, ok, _ := w.col(identity, "rscredential").Get(ctx, input.Cluster)
	if !ok {
		return errors.New("Redshift credentials are unavailable")
	}
	var credential map[string]any
	_ = json.Unmarshal(credentialBody, &credential)
	if first(credential, "Username") != input.Username || first(credential, "PasswordHash") != fmt.Sprintf("%x", sha256.Sum256([]byte(input.Password))) {
		return errors.New("Redshift credentials are invalid")
	}
	key := redshiftTableKey(input.Cluster, input.Database, input.Table)
	return w.col(identity, "rstable").Txn(ctx, func(tx spi.Tx) error {
		body, ok, err := tx.Get(key)
		if err != nil {
			return err
		}
		if !ok {
			return errors.New("Redshift table not found")
		}
		var table tableData
		if json.Unmarshal(body, &table) != nil || len(table.Columns) == 0 {
			return errors.New("Redshift table is invalid")
		}
		columns := table.Columns
		if input.Columns != "" {
			columns = splitColumns(input.Columns)
			if !columnsExist(table.Columns, columns) {
				return errors.New("Redshift COPY column does not exist")
			}
		}
		rows, err := parseCopyRows(input.Data, columns, input.Options)
		if err != nil {
			return err
		}
		// ponytail: whole-table JSON rewrite; move to a SQL engine when the Redshift Data API lands.
		table.Rows = append(table.Rows, rows...)
		stored, _ := json.Marshal(table)
		return tx.Put(key, stored)
	})
}

// TableRows returns a copy of rows loaded by COPY for local data-plane consumers.
func (w *Warehouse) TableRows(ctx context.Context, identity spi.Identity, cluster, database, table string) ([]map[string]any, error) {
	body, ok, err := w.col(identity, "rstable").Get(ctx, redshiftTableKey(cluster, database, table))
	if err != nil || !ok {
		return nil, errors.New("Redshift table not found")
	}
	var data tableData
	if json.Unmarshal(body, &data) != nil {
		return nil, errors.New("Redshift table is invalid")
	}
	return data.Rows, nil
}

func redshiftTableKey(cluster, database, table string) string {
	return cluster + "|" + database + "|" + table
}

func splitColumns(value string) []string {
	columns := strings.Split(value, ",")
	for index := range columns {
		columns[index] = strings.TrimSpace(columns[index])
	}
	return columns
}

func columnsExist(table, requested []string) bool {
	available := map[string]bool{}
	for _, column := range table {
		available[column] = true
	}
	for _, column := range requested {
		if column == "" || !available[column] {
			return false
		}
	}
	return true
}

func parseCopyRows(data [][]byte, columns []string, options string) ([]map[string]any, error) {
	jsonFormat := strings.Contains(strings.ToUpper(options), "JSON")
	delimiter := "|"
	if index := strings.Index(strings.ToLower(options), "delimiter"); index >= 0 {
		remainder := strings.TrimSpace(options[index+len("delimiter"):])
		if len(remainder) < 3 || (remainder[0] != '\'' && remainder[0] != '"') {
			return nil, errors.New("Redshift COPY delimiter is invalid")
		}
		end := strings.IndexByte(remainder[1:], remainder[0])
		if end < 0 {
			return nil, errors.New("Redshift COPY delimiter is invalid")
		}
		delimiter = remainder[1 : end+1]
		if delimiter == `\t` {
			delimiter = "\t"
		}
		if len(delimiter) != 1 {
			return nil, errors.New("Redshift COPY delimiter must be one byte")
		}
	}
	var rows []map[string]any
	for _, line := range bytes.Split(bytes.Join(data, nil), []byte{'\n'}) {
		line = bytes.TrimSuffix(line, []byte{'\r'})
		if len(line) == 0 {
			continue
		}
		if jsonFormat {
			row := map[string]any{}
			if json.Unmarshal(line, &row) != nil {
				return nil, errors.New("Redshift COPY JSON row is invalid")
			}
			rows = append(rows, row)
			continue
		}
		fields := strings.Split(string(line), delimiter)
		if len(fields) != len(columns) {
			return nil, errors.New("Redshift COPY row has the wrong column count")
		}
		row := make(map[string]any, len(columns))
		for index, column := range columns {
			row[column] = fields[index]
		}
		rows = append(rows, row)
	}
	if len(rows) == 0 {
		return nil, errors.New("Redshift COPY contains no rows")
	}
	return rows, nil
}

func listOrGet(ctx context.Context, c spi.Collection, id, listKey string) (*spi.Response, error) {
	if id != "" {
		b, ok, _ := c.Get(ctx, id)
		if !ok {
			return nil, &spi.Fault{Code: "ClusterNotFound", HTTPStatus: 404, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		return &spi.Response{Output: map[string]any{listKey: []any{rec}}}, nil
	}
	kvs, _, _ := c.List(ctx, "", "", 0)
	var items []any
	for _, kv := range kvs {
		var rec map[string]any
		_ = json.Unmarshal(kv.Value, &rec)
		items = append(items, rec)
	}
	return &spi.Response{Output: map[string]any{listKey: items}}, nil
}

func first(in map[string]any, keys ...string) string {
	for _, k := range keys {
		if s, ok := in[k].(string); ok && s != "" {
			return s
		}
	}
	return ""
}
