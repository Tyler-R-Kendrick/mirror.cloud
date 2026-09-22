// Package athena is StartQueryExecution: the query engine behavior/aws/athena
// lists as native. It runs SELECT 1 / SELECT 'lit', SELECT over Glue table
// locations on S3, and DDL/DML against S3 Tables, synchronously; workgroups
// and the execution reads are the bundle's.
package athena

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/bundled"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/s3"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func init() {
	bundled.RegisterNative("aws.athena", "StartQueryExecution", func(ctx context.Context, deps spi.Deps, req *spi.Request) (*spi.Response, error) {
		return (&runner{deps}).start(ctx, req)
	})
}

// runner is the query engine behind StartQueryExecution.
type runner struct{ deps spi.Deps }

func (p *runner) col(req *spi.Request, n string) spi.Collection {
	return p.deps.Store.Scope(req.Identity.Account, req.Identity.Region).Collection(n)
}

// start runs the query synchronously and stores the execution record the
// bundle's GetQueryExecution and GetQueryResults read.
func (p *runner) start(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	id := p.deps.Rand.Hex(16)
	sql := first(req.Input, "QueryString")
	defDB := ""
	catalog := ""
	if qec, ok := req.Input["QueryExecutionContext"].(map[string]any); ok {
		defDB = first(qec, "Database")
		catalog = first(qec, "Catalog")
	}
	cols, rows, state, reason := p.runQuery(ctx, req, sql, defDB, catalog)
	status := map[string]any{"State": state}
	if reason != "" {
		status["StateChangeReason"] = reason
	}
	rec := map[string]any{
		"QueryExecutionId": id, "Query": sql, "Status": status,
		"WorkGroup": first(req.Input, "WorkGroup"), "columns": cols, "rows": rows,
	}
	b, _ := json.Marshal(rec)
	_ = p.col(req, "atq").Put(ctx, id, b)
	return &spi.Response{Output: map[string]any{"QueryExecutionId": id}}, nil
}

type sel struct {
	cols               []string
	db, table          string
	whereCol, whereVal string
}

func (p *runner) runQuery(ctx context.Context, req *spi.Request, sql, defDB, catalog string) (cols []any, rows []any, state, reason string) {
	if strings.HasPrefix(strings.ToLower(catalog), "s3tablescatalog/") {
		cols, rows, err := p.runS3TablesQuery(ctx, req, sql, defDB, strings.TrimPrefix(catalog, "s3tablescatalog/"))
		if err != nil {
			return []any{}, []any{}, "FAILED", err.Error()
		}
		return cols, rows, "SUCCEEDED", ""
	}
	cols, rows = runSQL(sql)
	if len(cols) > 0 || len(rows) > 0 {
		return cols, rows, "SUCCEEDED", ""
	}
	q, ok := parseSelect(sql, defDB)
	if !ok {
		return []any{}, []any{}, "SUCCEEDED", ""
	}
	cols, rows, err := p.scanTable(ctx, req, q)
	if err != nil {
		return []any{}, []any{}, "FAILED", err.Error()
	}
	return cols, rows, "SUCCEEDED", ""
}

func (p *runner) scanTable(ctx context.Context, req *spi.Request, q sel) ([]any, []any, error) {
	// Glue is a bundle now, so it is reached the way any service reaches
	// another: through the registry-backed handler, which carries a build
	// failure into the call rather than to this caller.
	gp := bundled.Handler("aws.glue", p.deps)
	tresp, err := gp.Invoke(ctx, &spi.Request{Identity: req.Identity, Operation: "GetTable", Input: map[string]any{"DatabaseName": q.db, "Name": q.table}})
	if err != nil {
		return nil, nil, err
	}
	tbl, _ := tresp.Output["Table"].(map[string]any)
	sd, _ := tbl["StorageDescriptor"].(map[string]any)
	names := colNames(sd)
	loc := first(sd, "Location")
	bucket, prefix := splitS3(loc)
	var records []map[string]string
	if bucket != "" {
		sp := s3.New(p.deps)
		listed, err := sp.Invoke(ctx, &spi.Request{Identity: req.Identity, Operation: "ListObjectsV2", Input: map[string]any{"Bucket": bucket, "Prefix": prefix}})
		if err != nil {
			return nil, nil, err
		}
		contents, _ := listed.Output["Contents"].([]any)
		for _, c := range contents {
			m, _ := c.(map[string]any)
			key := first(m, "Key")
			if key == "" || strings.HasSuffix(key, "/") {
				continue
			}
			got, err := sp.Invoke(ctx, &spi.Request{Identity: req.Identity, Operation: "GetObject", Input: map[string]any{"Bucket": bucket, "Key": key}})
			if err != nil || got == nil || got.Stream == nil {
				continue
			}
			raw, _ := io.ReadAll(got.Stream)
			_ = got.Stream.Close()
			recs, hdr := parseObject(raw, names)
			if len(names) == 0 {
				names = hdr
			}
			records = append(records, recs...)
		}
	}
	if len(names) == 0 {
		names = q.cols
	}
	proj := names
	if len(q.cols) > 0 {
		proj = q.cols
	}
	var cols []any
	for _, n := range proj {
		cols = append(cols, n)
	}
	var rows []any
	for _, rec := range records {
		if q.whereCol != "" && !whereMatch(rec, q.whereCol, q.whereVal) {
			continue
		}
		var row []any
		for _, n := range proj {
			row = append(row, recGet(rec, n))
		}
		rows = append(rows, row)
	}
	if rows == nil {
		rows = []any{}
	}
	return cols, rows, nil
}

func colNames(sd map[string]any) []string {
	var names []string
	if sd == nil {
		return names
	}
	cols, _ := sd["Columns"].([]any)
	for _, c := range cols {
		m, _ := c.(map[string]any)
		if n := first(m, "Name"); n != "" {
			names = append(names, n)
		}
	}
	return names
}

func splitS3(loc string) (bucket, prefix string) {
	loc = strings.TrimSpace(loc)
	for _, pfx := range []string{"s3://", "S3://"} {
		loc = strings.TrimPrefix(loc, pfx)
	}
	i := strings.IndexByte(loc, '/')
	if i < 0 {
		return loc, ""
	}
	return loc[:i], loc[i+1:]
}

func parseSelect(sql, defaultDB string) (sel, bool) {
	s := strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(sql), ";"))
	u := strings.ToUpper(s)
	if !strings.HasPrefix(u, "SELECT ") {
		return sel{}, false
	}
	fromIdx := strings.Index(u, " FROM ")
	if fromIdx < 0 {
		return sel{}, false
	}
	colPart := strings.TrimSpace(s[len("SELECT "):fromIdx])
	after := strings.TrimSpace(s[fromIdx+len(" FROM "):])
	whereCol, whereVal := "", ""
	au := strings.ToUpper(after)
	if i := strings.Index(au, " WHERE "); i >= 0 {
		wexpr := strings.TrimSpace(after[i+len(" WHERE "):])
		after = strings.TrimSpace(after[:i])
		if eq := strings.Index(wexpr, "="); eq >= 0 {
			whereCol = unquoteIdent(strings.TrimSpace(wexpr[:eq]))
			whereVal = unquoteLit(strings.TrimSpace(wexpr[eq+1:]))
		}
	}
	db, table := "", unquoteIdent(after)
	if i := strings.LastIndex(after, "."); i >= 0 {
		db = unquoteIdent(after[:i])
		table = unquoteIdent(after[i+1:])
	}
	if db == "" {
		db = defaultDB
	}
	var cols []string
	if colPart != "*" {
		for _, c := range strings.Split(colPart, ",") {
			c = unquoteIdent(strings.TrimSpace(c))
			if c != "" {
				cols = append(cols, c)
			}
		}
	}
	if db == "" || table == "" {
		return sel{}, false
	}
	return sel{cols: cols, db: db, table: table, whereCol: whereCol, whereVal: whereVal}, true
}

func unquoteIdent(s string) string {
	return strings.Trim(strings.TrimSpace(s), "`\"")
}

func unquoteLit(s string) string {
	s = strings.TrimSpace(s)
	if len(s) >= 2 {
		if (s[0] == '\'' && s[len(s)-1] == '\'') || (s[0] == '"' && s[len(s)-1] == '"') {
			return s[1 : len(s)-1]
		}
	}
	return s
}

func parseObject(raw []byte, names []string) ([]map[string]string, []string) {
	s := strings.TrimSpace(string(raw))
	if s == "" {
		return nil, names
	}
	if s[0] == '{' || s[0] == '[' {
		return parseJSONLines(raw, names)
	}
	return parseCSV(raw, names)
}

func parseCSV(raw []byte, names []string) ([]map[string]string, []string) {
	r := csv.NewReader(bytes.NewReader(raw))
	r.FieldsPerRecord = -1
	recs, err := r.ReadAll()
	if err != nil || len(recs) == 0 {
		return nil, names
	}
	hdr := names
	start := 0
	if looksHeader(recs[0], names) {
		hdr = recs[0]
		start = 1
	} else if len(hdr) == 0 {
		hdr = make([]string, len(recs[0]))
		for i := range recs[0] {
			hdr[i] = fmt.Sprintf("_col%d", i)
		}
	}
	var out []map[string]string
	for _, rec := range recs[start:] {
		m := map[string]string{}
		for i, h := range hdr {
			if i < len(rec) {
				m[h] = rec[i]
			}
		}
		out = append(out, m)
	}
	return out, hdr
}

func looksHeader(row, names []string) bool {
	if len(names) == 0 || len(row) == 0 {
		return false
	}
	for i, n := range names {
		if i >= len(row) || !strings.EqualFold(strings.TrimSpace(row[i]), n) {
			return false
		}
	}
	return true
}

func parseJSONLines(raw []byte, names []string) ([]map[string]string, []string) {
	s := strings.TrimSpace(string(raw))
	var objs []map[string]any
	if strings.HasPrefix(s, "[") {
		_ = json.Unmarshal([]byte(s), &objs)
	} else {
		for _, line := range strings.Split(s, "\n") {
			line = strings.TrimSpace(line)
			if line == "" {
				continue
			}
			var m map[string]any
			if json.Unmarshal([]byte(line), &m) == nil {
				objs = append(objs, m)
			}
		}
	}
	hdr := names
	if len(hdr) == 0 && len(objs) > 0 {
		for k := range objs[0] {
			hdr = append(hdr, k)
		}
		sort.Strings(hdr)
	}
	var out []map[string]string
	for _, o := range objs {
		m := map[string]string{}
		for _, h := range hdr {
			if v, ok := o[h]; ok && v != nil {
				m[h] = fmt.Sprint(v)
			} else {
				m[h] = ""
			}
		}
		out = append(out, m)
	}
	return out, hdr
}

func whereMatch(rec map[string]string, col, val string) bool {
	return recGet(rec, col) == val
}

func recGet(rec map[string]string, col string) string {
	if v, ok := rec[col]; ok {
		return v
	}
	for k, v := range rec {
		if strings.EqualFold(k, col) {
			return v
		}
	}
	return ""
}

// ponytail: no joins/aggregates/partitions; upgrade is Presto/Trino over Glue/S3.
func runSQL(q string) (cols []any, rows []any) {
	s := strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(q), ";"))
	u := strings.ToUpper(s)
	if u == "SELECT 1" || strings.HasPrefix(u, "SELECT 1 ") || u == "SELECT 1 AS N" {
		return []any{"_col0"}, []any{[]any{"1"}}
	}
	if strings.HasPrefix(u, "SELECT '") || strings.HasPrefix(u, "SELECT \"") {
		quote := s[7]
		rest := s[8:]
		i := strings.IndexByte(rest, quote)
		if i >= 0 {
			return []any{"_col0"}, []any{[]any{rest[:i]}}
		}
	}
	return []any{}, []any{}
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
