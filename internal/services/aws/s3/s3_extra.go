package s3

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func (p *Pack) createSession(req *spi.Request) (*spi.Response, error) {
	ak := p.deps.Rand.Derive("s3sess/" + str(req.Input["Bucket"])).Hex(20)
	return &spi.Response{Output: map[string]any{
		"Credentials": map[string]any{
			"AccessKeyId":     ak,
			"SecretAccessKey": p.deps.Rand.Derive(ak).Hex(40),
			"SessionToken":    p.deps.Rand.Derive(ak + "tok").Hex(32),
			"Expiration":      p.deps.Clock.Now().Add(time.Hour).UTC().Format("2006-01-02T15:04:05Z"),
		},
	}}, nil
}

func (p *Pack) renameObject(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	b := str(req.Input["Bucket"])
	dest := str(req.Input["Key"])
	src := str(req.Input["RenameSourceKey"])
	if src == "" && req.HTTP != nil {
		src = req.HTTP.Header.Get("x-amz-rename-source")
	}
	src = strings.TrimPrefix(src, "/")
	if i := strings.IndexByte(src, '/'); i >= 0 && strings.HasPrefix(src, b+"/") {
		src = src[i+1:]
	}
	if src == "" || dest == "" {
		return nil, &spi.Fault{Code: "InvalidArgument", Message: "rename source and destination required", HTTPStatus: 400, Fault: "client"}
	}
	copyReq := &spi.Request{Identity: req.Identity, HTTP: req.HTTP, Input: map[string]any{
		"Bucket": b, "Key": dest, "CopySource": b + "/" + src,
	}}
	if _, err := p.copyObject(ctx, copyReq); err != nil {
		return nil, err
	}
	delReq := &spi.Request{Identity: req.Identity, HTTP: req.HTTP, Input: map[string]any{"Bucket": b, "Key": src}}
	if _, err := p.deleteObject(ctx, delReq); err != nil {
		return nil, err
	}
	return &spi.Response{Output: map[string]any{"Bucket": b, "Key": dest}}, nil
}

func (p *Pack) selectObject(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	b, key := str(req.Input["Bucket"]), str(req.Input["Key"])
	if err := p.requireBucket(ctx, req, b); err != nil {
		return nil, err
	}
	rc, _, err := p.deps.Blobs.Get(ctx, blobKey(req, b, key))
	if err != nil {
		return nil, &spi.Fault{Code: "NoSuchKey", HTTPStatus: 404, Fault: "client"}
	}
	defer rc.Close()
	raw, _ := io.ReadAll(rc)
	expr := str(req.Input["Expression"])
	if expr == "" && req.HTTP != nil {
		_ = req.HTTP.ParseForm()
		expr = req.HTTP.Form.Get("Expression")
	}
	header := false
	if ser, ok := req.Input["InputSerialization"].(map[string]any); ok {
		if csvm, ok := ser["CSV"].(map[string]any); ok {
			header = strings.EqualFold(str(csvm["FileHeaderInfo"]), "USE")
		}
	}
	out := runSelect(raw, expr, header)
	return &spi.Response{Output: map[string]any{"Payload": string(out), "Records": map[string]any{"Payload": string(out)}}}, nil
}

// ponytail: CSV/JSON SELECT + WHERE col=lit only; no Parquet, aggregates, nested paths.
func runSelect(raw []byte, expr string, header bool) []byte {
	s := strings.TrimSpace(string(raw))
	if s == "" {
		return raw
	}
	var rows []map[string]string
	var hdr []string
	if s[0] == '{' || s[0] == '[' {
		rows, hdr = parseSelectJSON(raw)
	} else {
		rows, hdr = parseSelectCSV(raw, header)
	}
	q, ok := parseS3Select(expr)
	if !ok {
		return raw
	}
	var kept []string
	for _, rec := range rows {
		if q.whereCol != "" && recGet(rec, q.whereCol) != q.whereVal {
			continue
		}
		cols := hdr
		if len(q.cols) > 0 {
			cols = q.cols
		}
		var cells []string
		for _, c := range cols {
			cells = append(cells, recGet(rec, c))
		}
		kept = append(kept, strings.Join(cells, ","))
	}
	return []byte(strings.Join(kept, "\n"))
}

type s3sel struct {
	cols               []string
	whereCol, whereVal string
}

func parseS3Select(expr string) (s3sel, bool) {
	s := strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(expr), ";"))
	u := strings.ToUpper(s)
	if !strings.HasPrefix(u, "SELECT ") {
		return s3sel{}, false
	}
	rest := strings.TrimSpace(s[len("SELECT "):])
	ru := strings.ToUpper(rest)
	whereCol, whereVal := "", ""
	if i := strings.Index(ru, " WHERE "); i >= 0 {
		wexpr := strings.TrimSpace(rest[i+len(" WHERE "):])
		rest = strings.TrimSpace(rest[:i])
		if eq := strings.Index(wexpr, "="); eq >= 0 {
			whereCol = strings.Trim(strings.TrimSpace(wexpr[:eq]), "`\"")
			whereVal = strings.TrimSpace(wexpr[eq+1:])
			if len(whereVal) >= 2 && ((whereVal[0] == '\'' && whereVal[len(whereVal)-1] == '\'') || (whereVal[0] == '"' && whereVal[len(whereVal)-1] == '"')) {
				whereVal = whereVal[1 : len(whereVal)-1]
			}
		}
	}
	if i := strings.Index(strings.ToUpper(rest), " FROM "); i >= 0 {
		rest = strings.TrimSpace(rest[:i])
	}
	var cols []string
	if rest != "*" {
		for _, c := range strings.Split(rest, ",") {
			c = strings.Trim(strings.TrimSpace(c), "`\"")
			if c != "" {
				cols = append(cols, c)
			}
		}
	}
	return s3sel{cols: cols, whereCol: whereCol, whereVal: whereVal}, true
}

func parseSelectCSV(raw []byte, header bool) ([]map[string]string, []string) {
	r := csv.NewReader(bytes.NewReader(raw))
	r.FieldsPerRecord = -1
	recs, err := r.ReadAll()
	if err != nil || len(recs) == 0 {
		return nil, nil
	}
	start := 0
	var hdr []string
	if header {
		hdr = recs[0]
		start = 1
	} else {
		n := len(recs[0])
		hdr = make([]string, n)
		for i := 0; i < n; i++ {
			hdr[i] = fmt.Sprintf("_%d", i+1)
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

func parseSelectJSON(raw []byte) ([]map[string]string, []string) {
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
	var hdr []string
	if len(objs) > 0 {
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
			}
		}
		out = append(out, m)
	}
	return out, hdr
}

func recGet(rec map[string]string, col string) string {
	col = strings.TrimPrefix(col, "s.")
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

func (p *Pack) writeGetObjectResponse(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	route := str(req.Input["RequestRoute"])
	tok := str(req.Input["RequestToken"])
	if req.HTTP != nil {
		if route == "" {
			route = req.HTTP.Header.Get("x-amz-request-route")
		}
		if tok == "" {
			tok = req.HTTP.Header.Get("x-amz-request-token")
		}
	}
	var body []byte
	if req.Body != nil {
		body, _ = io.ReadAll(req.Body)
	}
	_ = p.col(req, "wgor").Put(ctx, route+"/"+tok, body)
	return &spi.Response{Status: 200, Output: map[string]any{"RequestRoute": route, "RequestToken": tok}}, nil
}

func (p *Pack) updateObjectEncryption(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	b, key := str(req.Input["Bucket"]), str(req.Input["Key"])
	if err := p.requireBucket(ctx, req, b); err != nil {
		return nil, err
	}
	algo := str(req.Input["SSEAlgorithm"])
	if algo == "" {
		algo = "AES256"
	}
	doc, _ := json.Marshal(map[string]any{"SSEAlgorithm": algo})
	_ = p.col(req, "bktcfg").Put(ctx, b+"/"+key+"/objenc", doc)
	return &spi.Response{Output: map[string]any{"SSEAlgorithm": algo, "Bucket": b, "Key": key}}, nil
}

func (p *Pack) objectAnnotation(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	b, key := str(req.Input["Bucket"]), str(req.Input["Key"])
	if err := p.requireBucket(ctx, req, b); err != nil {
		return nil, err
	}
	id := str(req.Input["AnnotationId"])
	if id == "" {
		id = "default"
	}
	col := p.col(req, "annots")
	ck := b + "/" + key + "/" + id
	op := req.Operation
	if strings.HasPrefix(op, "Put") {
		doc := map[string]any{"AnnotationId": id, "Bucket": b, "Key": key}
		for k, v := range req.Input {
			doc[k] = v
		}
		raw, _ := json.Marshal(doc)
		_ = col.Put(ctx, ck, raw)
		return &spi.Response{Output: doc}, nil
	}
	if strings.HasPrefix(op, "Delete") {
		_ = col.Delete(ctx, ck)
		return &spi.Response{Status: 204, Output: map[string]any{}}, nil
	}
	if strings.HasPrefix(op, "List") {
		kvs, _, _ := col.List(ctx, b+"/"+key+"/", "", 0)
		var items []any
		for _, kv := range kvs {
			var rec map[string]any
			_ = json.Unmarshal(kv.Value, &rec)
			items = append(items, rec)
		}
		return &spi.Response{Output: map[string]any{"Annotations": items}}, nil
	}
	raw, ok, _ := col.Get(ctx, ck)
	if !ok {
		return nil, &spi.Fault{Code: "NoSuchAnnotation", HTTPStatus: 404, Fault: "client"}
	}
	var rec map[string]any
	_ = json.Unmarshal(raw, &rec)
	return &spi.Response{Output: rec}, nil
}

func (p *Pack) metadataCfg(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	b := str(req.Input["Bucket"])
	if err := p.requireBucket(ctx, req, b); err != nil {
		return nil, err
	}
	kind := "metadata"
	switch {
	case strings.Contains(req.Operation, "Table") && !strings.Contains(req.Operation, "Update"):
		kind = "metadatatable"
	case strings.Contains(req.Operation, "Journal"):
		kind = "metadatajournal"
	case strings.Contains(req.Operation, "Inventory"):
		kind = "metadatainventory"
	case strings.Contains(req.Operation, "Annotation"):
		kind = "metadataannotation"
	}
	col := p.col(req, "bktcfg")
	ck := b + "/" + kind
	if strings.HasPrefix(req.Operation, "Delete") {
		_ = col.Delete(ctx, ck)
		return &spi.Response{Status: 204, Output: map[string]any{}}, nil
	}
	if strings.HasPrefix(req.Operation, "Get") {
		raw, ok, _ := col.Get(ctx, ck)
		if !ok {
			return nil, &spi.Fault{Code: "NoSuchMetadataConfiguration", HTTPStatus: 404, Fault: "client"}
		}
		var doc map[string]any
		_ = json.Unmarshal(raw, &doc)
		return &spi.Response{Output: doc}, nil
	}
	doc := map[string]any{"Bucket": b}
	if raw, ok, _ := col.Get(ctx, ck); ok {
		_ = json.Unmarshal(raw, &doc)
	}
	for k, v := range req.Input {
		if k == "Bucket" {
			continue
		}
		doc[k] = v
	}
	if req.HTTP != nil && req.HTTP.Body != nil && req.Body == nil {
		req.Body = req.HTTP.Body
	}
	if req.Body != nil {
		if body, _ := io.ReadAll(req.Body); len(body) > 0 {
			doc["_body"] = string(body)
		}
	}
	raw, _ := json.Marshal(doc)
	_ = col.Put(ctx, ck, raw)
	return &spi.Response{Output: doc}, nil
}
