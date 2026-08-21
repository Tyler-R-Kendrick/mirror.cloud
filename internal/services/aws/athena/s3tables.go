package athena

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func (p *Pack) runS3TablesQuery(ctx context.Context, req *spi.Request, sql, database, bucket string) ([]any, []any, error) {
	query := strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(sql), ";"))
	upper := strings.ToUpper(query)
	switch {
	case strings.HasPrefix(upper, "CREATE TABLE "):
		if at := strings.Index(upper, " AS SELECT "); at >= 0 {
			db, table := tableName(strings.TrimSpace(query[len("CREATE TABLE "):at]), database)
			selection, ok := parseSelect(query[at+len(" AS "):], database)
			if !ok {
				return nil, nil, fmt.Errorf("invalid CTAS query")
			}
			cols, rows, err := p.scanS3Table(ctx, req, bucket, selection)
			if err == nil {
				err = p.putS3Table(ctx, req, bucket, db, table, cols, rows)
			}
			return []any{}, []any{}, err
		}
		open, close := strings.Index(query, "("), strings.LastIndex(query, ")")
		if open < 0 || close <= open {
			return nil, nil, fmt.Errorf("invalid CREATE TABLE")
		}
		db, table := tableName(strings.TrimSpace(query[len("CREATE TABLE "):open]), database)
		var cols []any
		for _, definition := range splitQuoted(query[open+1 : close]) {
			fields := strings.Fields(strings.TrimSpace(definition))
			if len(fields) > 0 {
				cols = append(cols, unquoteIdent(fields[0]))
			}
		}
		return []any{}, []any{}, p.putS3Table(ctx, req, bucket, db, table, cols, []any{})
	case strings.HasPrefix(upper, "INSERT INTO "):
		at := strings.Index(upper, " VALUES ")
		if at < 0 {
			return nil, nil, fmt.Errorf("only INSERT VALUES is supported")
		}
		name := strings.TrimSpace(query[len("INSERT INTO "):at])
		if i := strings.Index(name, "("); i >= 0 {
			name = strings.TrimSpace(name[:i])
		}
		db, table := tableName(name, database)
		key := s3TableKey(bucket, db, table)
		if _, ok, _ := p.col(req, "s3tt").Get(ctx, key); !ok {
			return nil, nil, fmt.Errorf("table %s.%s not found", db, table)
		}
		raw, _, _ := p.col(req, "s3trows").Get(ctx, key)
		var rows []any
		_ = json.Unmarshal(raw, &rows)
		rows = append(rows, parseValues(query[at+len(" VALUES "):])...)
		raw, _ = json.Marshal(rows)
		_ = p.col(req, "s3trows").Put(ctx, key, raw)
		return []any{}, []any{}, nil
	default:
		selection, ok := parseSelect(query, database)
		if !ok {
			return nil, nil, fmt.Errorf("unsupported S3 Tables query")
		}
		return p.scanS3Table(ctx, req, bucket, selection)
	}
}

func (p *Pack) putS3Table(ctx context.Context, req *spi.Request, bucket, database, table string, cols, rows []any) error {
	if _, ok, _ := p.col(req, "s3tb").Get(ctx, bucket); !ok {
		return fmt.Errorf("table bucket %s not found", bucket)
	}
	key := s3TableKey(bucket, database, table)
	rec, _ := json.Marshal(map[string]any{
		"name": table, "namespace": database, "tableBucketARN": "arn:aws:s3tables:" + req.Identity.Region + ":" + req.Identity.Account + ":bucket/" + bucket,
		"format": "ICEBERG", "columns": cols,
	})
	_ = p.col(req, "s3tt").Put(ctx, key, rec)
	raw, _ := json.Marshal(rows)
	return p.col(req, "s3trows").Put(ctx, key, raw)
}

func (p *Pack) scanS3Table(ctx context.Context, req *spi.Request, bucket string, selection sel) ([]any, []any, error) {
	key := s3TableKey(bucket, selection.db, selection.table)
	raw, ok, _ := p.col(req, "s3tt").Get(ctx, key)
	if !ok {
		return nil, nil, fmt.Errorf("table %s.%s not found", selection.db, selection.table)
	}
	var table map[string]any
	_ = json.Unmarshal(raw, &table)
	columns := stringValues(table["columns"])
	raw, _, _ = p.col(req, "s3trows").Get(ctx, key)
	var stored []any
	_ = json.Unmarshal(raw, &stored)
	projection := columns
	if len(selection.cols) > 0 {
		projection = selection.cols
	}
	cols := make([]any, len(projection))
	for i, column := range projection {
		cols[i] = column
	}
	indexes := map[string]int{}
	for i, column := range columns {
		indexes[strings.ToLower(column)] = i
	}
	var rows []any
	for _, rawRow := range stored {
		row, _ := rawRow.([]any)
		if selection.whereCol != "" {
			i, found := indexes[strings.ToLower(selection.whereCol)]
			if !found || i >= len(row) || fmt.Sprint(row[i]) != selection.whereVal {
				continue
			}
		}
		out := make([]any, 0, len(projection))
		for _, column := range projection {
			i, found := indexes[strings.ToLower(column)]
			if found && i < len(row) {
				out = append(out, row[i])
			} else {
				out = append(out, "")
			}
		}
		rows = append(rows, out)
	}
	if rows == nil {
		rows = []any{}
	}
	return cols, rows, nil
}

func tableName(name, defaultDatabase string) (string, string) {
	parts := strings.Split(strings.TrimSpace(name), ".")
	for i := range parts {
		parts[i] = unquoteIdent(parts[i])
	}
	if len(parts) >= 2 {
		return parts[len(parts)-2], parts[len(parts)-1]
	}
	return defaultDatabase, parts[0]
}

func s3TableKey(bucket, database, table string) string { return bucket + "/" + database + "/" + table }

func parseValues(input string) []any {
	var rows []any
	depth, quoted, start := 0, false, -1
	for i, r := range input {
		if r == '\'' {
			quoted = !quoted
		}
		if quoted {
			continue
		}
		switch r {
		case '(':
			if depth == 0 {
				start = i + 1
			}
			depth++
		case ')':
			depth--
			if depth == 0 && start >= 0 {
				values := splitQuoted(input[start:i])
				row := make([]any, len(values))
				for j, value := range values {
					row[j] = unquoteLit(strings.TrimSpace(value))
				}
				rows = append(rows, row)
			}
		}
	}
	return rows
}

func splitQuoted(input string) []string {
	var parts []string
	quoted, start := false, 0
	for i, r := range input {
		if r == '\'' {
			quoted = !quoted
		} else if r == ',' && !quoted {
			parts = append(parts, input[start:i])
			start = i + 1
		}
	}
	return append(parts, input[start:])
}

func stringValues(value any) []string {
	items, _ := value.([]any)
	out := make([]string, 0, len(items))
	for _, item := range items {
		if text, ok := item.(string); ok {
			out = append(out, text)
		}
	}
	return out
}
