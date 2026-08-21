package dynamodb

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func (p *Pack) partiql(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	if req.Operation == "BatchExecuteStatement" {
		stmts := asSlice(req.Input["Statements"])
		var resps []any
		for _, s := range stmts {
			sm := asMap(s)
			sub := &spi.Request{Identity: req.Identity, HTTP: req.HTTP, Operation: "ExecuteStatement", Input: map[string]any{"Statement": sm["Statement"], "Parameters": sm["Parameters"]}}
			out, err := p.partiql(ctx, sub)
			if err != nil {
				resps = append(resps, map[string]any{"Error": map[string]any{"Code": "ValidationException", "Message": err.Error()}})
				continue
			}
			resps = append(resps, out.Output)
		}
		return &spi.Response{Output: map[string]any{"Responses": resps}}, nil
	}
	if req.Operation == "ExecuteTransaction" {
		stmts := asSlice(req.Input["TransactStatements"])
		if len(stmts) == 0 {
			stmts = asSlice(req.Input["Statements"])
		}
		req.Input["Statements"] = stmts
		req.Operation = "BatchExecuteStatement"
		return p.partiql(ctx, req)
	}
	st := strings.TrimSpace(str(req.Input["Statement"]))
	up := strings.ToUpper(st)
	switch {
	case strings.HasPrefix(up, "INSERT"):
		table, item := parseInsert(st)
		if table == "" || item == nil {
			return nil, &spi.Fault{Code: "ValidationException", Message: "INSERT", HTTPStatus: 400, Fault: "client"}
		}
		return p.Invoke(ctx, &spi.Request{Identity: req.Identity, HTTP: req.HTTP, Operation: "PutItem", Input: map[string]any{"TableName": table, "Item": item}})
	case strings.HasPrefix(up, "DELETE"):
		table, key := parseWhereKey(st)
		if table == "" {
			return nil, &spi.Fault{Code: "ValidationException", Message: "DELETE", HTTPStatus: 400, Fault: "client"}
		}
		return p.Invoke(ctx, &spi.Request{Identity: req.Identity, HTTP: req.HTTP, Operation: "DeleteItem", Input: map[string]any{"TableName": table, "Key": key}})
	default:
		table, key := parseWhereKey(st)
		if table == "" {
			table = str(req.Input["TableName"])
		}
		if len(key) > 0 {
			return p.Invoke(ctx, &spi.Request{Identity: req.Identity, HTTP: req.HTTP, Operation: "GetItem", Input: map[string]any{"TableName": table, "Key": key}})
		}
		return p.Invoke(ctx, &spi.Request{Identity: req.Identity, HTTP: req.HTTP, Operation: "Scan", Input: map[string]any{"TableName": table}})
	}
}

func parseInsert(st string) (string, map[string]any) {
	up := strings.ToUpper(st)
	i := strings.Index(up, "INTO ")
	if i < 0 {
		return "", nil
	}
	rest := strings.TrimSpace(st[i+5:])
	name := strings.Fields(rest)[0]
	vi := strings.Index(up, "VALUE")
	if vi < 0 {
		return name, nil
	}
	raw := strings.TrimSpace(st[vi+5:])
	raw = strings.TrimPrefix(raw, "E")
	raw = strings.TrimSpace(raw)
	item := parsePartiqlMap(raw)
	return name, item
}

func parseWhereKey(st string) (string, map[string]any) {
	up := strings.ToUpper(st)
	from := strings.Index(up, "FROM ")
	if from < 0 {
		from = strings.Index(up, "INTO ")
	}
	if from < 0 {
		return "", nil
	}
	rest := strings.TrimSpace(st[from+5:])
	name := strings.Fields(rest)[0]
	wi := strings.Index(up, "WHERE")
	if wi < 0 {
		return name, nil
	}
	cond := strings.TrimSpace(st[wi+5:])
	eq := strings.Index(cond, "=")
	if eq < 0 {
		return name, nil
	}
	attr := strings.TrimSpace(cond[:eq])
	val := strings.Trim(strings.TrimSpace(cond[eq+1:]), "; ")
	val = strings.Trim(val, `"'`)
	return name, map[string]any{attr: map[string]any{"S": val}}
}

func parsePartiqlMap(raw string) map[string]any {
	raw = strings.TrimSpace(raw)
	raw = strings.TrimSuffix(raw, ";")
	js := strings.ReplaceAll(raw, "'", `"`)
	var item map[string]any
	if json.Unmarshal([]byte(js), &item) != nil {
		return nil
	}
	if _, ok := item["id"].(map[string]any); ok {
		return item
	}
	out := map[string]any{}
	for k, v := range item {
		switch t := v.(type) {
		case map[string]any:
			out[k] = t
		case string:
			out[k] = map[string]any{"S": t}
		default:
			b, _ := json.Marshal(t)
			out[k] = map[string]any{"N": string(b)}
		}
	}
	return out
}

func (p *Pack) globalTable(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	name := str(req.Input["GlobalTableName"])
	col := p.col(req, "gtables")
	switch req.Operation {
	case "CreateGlobalTable":
		rec := map[string]any{"GlobalTableName": name, "ReplicationGroup": req.Input["ReplicationGroup"], "GlobalTableStatus": "ACTIVE"}
		b, _ := json.Marshal(rec)
		_ = col.Put(ctx, name, b)
		return &spi.Response{Output: map[string]any{"GlobalTableDescription": rec}}, nil
	case "DescribeGlobalTable":
		b, ok, _ := col.Get(ctx, name)
		if !ok {
			return nil, &spi.Fault{Code: "GlobalTableNotFoundException", HTTPStatus: 400, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		return &spi.Response{Output: map[string]any{"GlobalTableDescription": rec}}, nil
	case "ListGlobalTables":
		kvs, _, _ := col.List(ctx, "", "", 0)
		var out []any
		for _, kv := range kvs {
			var rec map[string]any
			_ = json.Unmarshal(kv.Value, &rec)
			out = append(out, rec)
		}
		return &spi.Response{Output: map[string]any{"GlobalTables": out}}, nil
	case "UpdateGlobalTable":
		b, ok, _ := col.Get(ctx, name)
		rec := map[string]any{"GlobalTableName": name, "GlobalTableStatus": "ACTIVE"}
		if ok {
			_ = json.Unmarshal(b, &rec)
		}
		reps := asSlice(rec["ReplicationGroup"])
		for _, a := range asSlice(req.Input["ReplicaUpdates"]) {
			um := asMap(a)
			if cr := asMap(um["Create"]); str(cr["RegionName"]) != "" {
				reps = append(reps, map[string]any{"RegionName": cr["RegionName"]})
			}
		}
		rec["ReplicationGroup"] = reps
		nb, _ := json.Marshal(rec)
		_ = col.Put(ctx, name, nb)
		return &spi.Response{Output: map[string]any{"GlobalTableDescription": rec}}, nil
	case "UpdateGlobalTableSettings":
		b, _ := json.Marshal(req.Input)
		_ = p.col(req, "gtset").Put(ctx, name, b)
		return &spi.Response{Output: map[string]any{"GlobalTableName": name, "ReplicaSettings": req.Input["ReplicaSettings"]}}, nil
	case "DescribeGlobalTableSettings":
		b, ok, _ := p.col(req, "gtset").Get(ctx, name)
		if !ok {
			return &spi.Response{Output: map[string]any{"GlobalTableName": name, "ReplicaSettings": []any{}}}, nil
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		return &spi.Response{Output: rec}, nil
	}
	return nil, spi.NotImplemented("aws.dynamodb", req.Operation, "emulate")
}

func (p *Pack) insights(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	table := str(req.Input["TableName"])
	col := p.col(req, "insights")
	if req.Operation == "UpdateContributorInsights" {
		st := str(req.Input["ContributorInsightsAction"])
		if st == "ENABLE" {
			st = "ENABLED"
		}
		if st == "DISABLE" {
			st = "DISABLED"
		}
		rec := map[string]any{"TableName": table, "ContributorInsightsStatus": st}
		b, _ := json.Marshal(rec)
		_ = col.Put(ctx, table, b)
		return &spi.Response{Output: rec}, nil
	}
	if req.Operation == "ListContributorInsights" {
		kvs, _, _ := col.List(ctx, "", "", 0)
		var out []any
		for _, kv := range kvs {
			var rec map[string]any
			_ = json.Unmarshal(kv.Value, &rec)
			out = append(out, rec)
		}
		return &spi.Response{Output: map[string]any{"ContributorInsightsSummaries": out}}, nil
	}
	b, ok, _ := col.Get(ctx, table)
	if !ok {
		return &spi.Response{Output: map[string]any{"TableName": table, "ContributorInsightsStatus": "DISABLED"}}, nil
	}
	var rec map[string]any
	_ = json.Unmarshal(b, &rec)
	return &spi.Response{Output: rec}, nil
}

func (p *Pack) exports(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	col := p.col(req, "exports")
	if req.Operation == "ExportTableToPointInTime" {
		table := str(req.Input["TableName"])
		id := p.deps.Rand.Hex(12)
		arn := "arn:aws:dynamodb:" + req.Identity.Region + ":" + req.Identity.Account + ":table/" + table + "/export/" + id
		kvs, _, _ := p.col(req, "items:"+table).List(ctx, "", "", 0)
		rec := map[string]any{"ExportArn": arn, "TableArn": table, "ExportStatus": "COMPLETED", "ExportedItemCount": len(kvs), "S3Bucket": req.Input["S3Bucket"]}
		b, _ := json.Marshal(rec)
		_ = col.Put(ctx, arn, b)
		return &spi.Response{Output: map[string]any{"ExportDescription": rec}}, nil
	}
	if req.Operation == "ListExports" {
		kvs, _, _ := col.List(ctx, "", "", 0)
		var out []any
		for _, kv := range kvs {
			var rec map[string]any
			_ = json.Unmarshal(kv.Value, &rec)
			out = append(out, rec)
		}
		return &spi.Response{Output: map[string]any{"ExportSummaries": out}}, nil
	}
	arn := str(req.Input["ExportArn"])
	b, ok, _ := col.Get(ctx, arn)
	if !ok {
		return nil, &spi.Fault{Code: "ExportNotFoundException", HTTPStatus: 400, Fault: "client"}
	}
	var rec map[string]any
	_ = json.Unmarshal(b, &rec)
	return &spi.Response{Output: map[string]any{"ExportDescription": rec}}, nil
}

func (p *Pack) imports(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	col := p.col(req, "imports")
	if req.Operation == "ImportTable" {
		tcp := asMap(req.Input["TableCreationParameters"])
		name := str(tcp["TableName"])
		if name == "" {
			name = str(req.Input["TableName"])
		}
		if name != "" {
			_, _ = p.Invoke(ctx, &spi.Request{Identity: req.Identity, HTTP: req.HTTP, Operation: "CreateTable", Input: tcp})
		}
		id := p.deps.Rand.Hex(12)
		arn := "arn:aws:dynamodb:" + req.Identity.Region + ":" + req.Identity.Account + ":table/" + name + "/import/" + id
		rec := map[string]any{"ImportArn": arn, "TableName": name, "ImportStatus": "COMPLETED"}
		b, _ := json.Marshal(rec)
		_ = col.Put(ctx, arn, b)
		return &spi.Response{Output: map[string]any{"ImportTableDescription": rec}}, nil
	}
	if req.Operation == "ListImports" {
		kvs, _, _ := col.List(ctx, "", "", 0)
		var out []any
		for _, kv := range kvs {
			var rec map[string]any
			_ = json.Unmarshal(kv.Value, &rec)
			out = append(out, rec)
		}
		return &spi.Response{Output: map[string]any{"ImportSummaryList": out}}, nil
	}
	arn := str(req.Input["ImportArn"])
	b, ok, _ := col.Get(ctx, arn)
	if !ok {
		return nil, &spi.Fault{Code: "ImportNotFoundException", HTTPStatus: 400, Fault: "client"}
	}
	var rec map[string]any
	_ = json.Unmarshal(b, &rec)
	return &spi.Response{Output: map[string]any{"ImportTableDescription": rec}}, nil
}

func (p *Pack) restorePITR(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	src := str(req.Input["SourceTableName"])
	dst := str(req.Input["TargetTableName"])
	tb, ok, _ := p.col(req, "tables").Get(ctx, src)
	if !ok {
		return nil, &spi.Fault{Code: "ResourceNotFoundException", HTTPStatus: 400, Fault: "client"}
	}
	_ = p.col(req, "tables").Put(ctx, dst, tb)
	kvs, _, _ := p.col(req, "items:"+src).List(ctx, "", "", 0)
	for _, kv := range kvs {
		_ = p.col(req, "items:"+dst).Put(ctx, kv.Key, kv.Value)
	}
	return &spi.Response{Output: map[string]any{"TableDescription": map[string]any{"TableName": dst, "TableStatus": "ACTIVE"}}}, nil
}

func (p *Pack) replicaScaling(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	table := str(req.Input["TableName"])
	col := p.col(req, "rscale")
	if req.Operation == "UpdateTableReplicaAutoScaling" {
		b, _ := json.Marshal(req.Input)
		_ = col.Put(ctx, table, b)
		return &spi.Response{Output: map[string]any{"TableName": table, "TableAutoScalingDescription": req.Input}}, nil
	}
	b, ok, _ := col.Get(ctx, table)
	if !ok {
		return &spi.Response{Output: map[string]any{"TableAutoScalingDescription": map[string]any{"TableName": table}}}, nil
	}
	var rec map[string]any
	_ = json.Unmarshal(b, &rec)
	return &spi.Response{Output: map[string]any{"TableAutoScalingDescription": rec}}, nil
}

func (p *Pack) searchVectors(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	table := str(req.Input["TableName"])
	q := str(req.Input["Query"])
	if q == "" {
		q = str(req.Input["VectorSearch"])
	}
	kvs, _, _ := p.col(req, "items:"+table).List(ctx, "", "", 0)
	var hits []any
	for _, kv := range kvs {
		if q != "" && !strings.Contains(string(kv.Value), q) {
			continue
		}
		var item map[string]any
		_ = json.Unmarshal(kv.Value, &item)
		hits = append(hits, item)
	}
	return &spi.Response{Output: map[string]any{"Items": hits}}, nil
}

func (p *Pack) updateKinesisDest(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	table := str(req.Input["TableName"])
	stream := str(req.Input["StreamArn"])
	col := p.col(req, "kinesis:"+table)
	rec := map[string]any{"StreamArn": stream, "DestinationStatus": "ACTIVE"}
	if v := req.Input["ApproximateCreationDateTimePrecision"]; v != nil {
		rec["ApproximateCreationDateTimePrecision"] = v
	}
	b, _ := json.Marshal(rec)
	_ = col.Put(ctx, stream, b)
	return &spi.Response{Output: rec}, nil
}
