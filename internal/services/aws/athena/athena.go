// Package athena runs SELECT 1 / SELECT 'lit' and SELECT over Glue table locations on S3.
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

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/glue"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/s3"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func init() {
	registry.Register(registry.Factory{ServiceID: "aws.athena", Tier: model.TierEmulate, New: func(d spi.Deps) (spi.BehaviorPack, error) {
		return New(d), nil
	}})
}

// Pack implements Athena-lite.
type Pack struct{ deps spi.Deps }

// New constructs the pack.
func New(d spi.Deps) *Pack { return &Pack{deps: d} }

func (p *Pack) ServiceID() string { return "aws.athena" }
func (p *Pack) Tier() model.Tier  { return model.TierEmulate }
func (p *Pack) Operations() []string {
	return []string{
		"StartQueryExecution", "GetQueryExecution", "GetQueryResults", "StopQueryExecution", "ListQueryExecutions",
		"CreateWorkGroup", "GetWorkGroup", "ListWorkGroups", "DeleteWorkGroup",
	}
}

func (p *Pack) col(req *spi.Request, n string) spi.Collection {
	return p.deps.Store.Scope(req.Identity.Account, req.Identity.Region).Collection(n)
}

func (p *Pack) Invoke(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	switch req.Operation {
	case "CreateWorkGroup":
		name := first(req.Input, "Name")
		rec := map[string]any{"Name": name, "State": "ENABLED", "Description": first(req.Input, "Description")}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "atwg").Put(ctx, name, b)
		return &spi.Response{Output: map[string]any{}}, nil
	case "GetWorkGroup":
		name := first(req.Input, "WorkGroup")
		b, ok, _ := p.col(req, "atwg").Get(ctx, name)
		if !ok {
			return nil, &spi.Fault{Code: "InvalidRequestException", HTTPStatus: 400, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		return &spi.Response{Output: map[string]any{"WorkGroup": rec}}, nil
	case "ListWorkGroups":
		kvs, _, _ := p.col(req, "atwg").List(ctx, "", "", 0)
		var items []any
		for _, kv := range kvs {
			var rec map[string]any
			_ = json.Unmarshal(kv.Value, &rec)
			items = append(items, rec)
		}
		return &spi.Response{Output: map[string]any{"WorkGroups": items}}, nil
	case "DeleteWorkGroup":
		_ = p.col(req, "atwg").Delete(ctx, first(req.Input, "WorkGroup"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "StartQueryExecution":
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
	case "GetQueryExecution":
		id := first(req.Input, "QueryExecutionId")
		b, ok, _ := p.col(req, "atq").Get(ctx, id)
		if !ok {
			return nil, &spi.Fault{Code: "InvalidRequestException", HTTPStatus: 400, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		out := map[string]any{
			"QueryExecutionId": rec["QueryExecutionId"], "Query": rec["Query"],
			"Status": rec["Status"], "WorkGroup": rec["WorkGroup"],
		}
		return &spi.Response{Output: map[string]any{"QueryExecution": out}}, nil
	case "GetQueryResults":
		id := first(req.Input, "QueryExecutionId")
		b, ok, _ := p.col(req, "atq").Get(ctx, id)
		if !ok {
			return nil, &spi.Fault{Code: "InvalidRequestException", HTTPStatus: 400, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		cols, _ := rec["columns"].([]any)
		rows, _ := rec["rows"].([]any)
		var headers []any
		for _, c := range cols {
			headers = append(headers, map[string]any{"VarCharValue": c})
		}
		rsRows := []any{map[string]any{"Data": headers}}
		for _, r := range rows {
			var cells []any
			if arr, ok := r.([]any); ok {
				for _, cell := range arr {
					cells = append(cells, map[string]any{"VarCharValue": cell})
				}
			}
			rsRows = append(rsRows, map[string]any{"Data": cells})
		}
		return &spi.Response{Output: map[string]any{
			"ResultSet": map[string]any{
				"Rows":              rsRows,
				"ResultSetMetadata": map[string]any{"ColumnInfo": colInfo(cols)},
			},
		}}, nil
	case "StopQueryExecution":
		return &spi.Response{Output: map[string]any{}}, nil
	case "ListQueryExecutions":
		kvs, _, _ := p.col(req, "atq").List(ctx, "", "", 0)
		var ids []any
		for _, kv := range kvs {
			ids = append(ids, kv.Key)
		}
		return &spi.Response{Output: map[string]any{"QueryExecutionIds": ids}}, nil
	default:
		return nil, spi.NotImplemented("aws.athena", req.Operation, "emulate")
	}
}

type sel struct {
	cols               []string
	db, table          string
	whereCol, whereVal string
}

func (p *Pack) runQuery(ctx context.Context, req *spi.Request, sql, defDB, catalog string) (cols []any, rows []any, state, reason string) {
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

func (p *Pack) scanTable(ctx context.Context, req *spi.Request, q sel) ([]any, []any, error) {
	gp := glue.New(p.deps)
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

func colInfo(cols []any) []any {
	var out []any
	for _, c := range cols {
		out = append(out, map[string]any{"Name": c, "Type": "varchar"})
	}
	return out
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
