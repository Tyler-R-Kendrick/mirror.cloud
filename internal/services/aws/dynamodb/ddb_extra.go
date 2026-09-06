package dynamodb

import (
	"context"
	"encoding/json"
	"strconv"
	"strings"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func (p *Pack) partiql(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	if req.Operation == "BatchExecuteStatement" {
		stmts := asSlice(req.Input["Statements"])
		var resps []any
		for _, s := range stmts {
			sm := asMap(s)
			input := map[string]any{"Statement": sm["Statement"]}
			if parameters, ok := sm["Parameters"]; ok {
				input["Parameters"] = parameters
			}
			sub := &spi.Request{Identity: req.Identity, HTTP: req.HTTP, Operation: "ExecuteStatement", Input: input}
			out, err := p.partiql(ctx, sub)
			response := map[string]any{"TableName": partiqlTable(str(sm["Statement"]))}
			if err != nil {
				response["Error"] = map[string]any{"Code": "ValidationException", "Message": err.Error()}
				resps = append(resps, response)
				continue
			}
			for key, value := range out.Output {
				response[key] = value
			}
			resps = append(resps, response)
		}
		return &spi.Response{Output: map[string]any{"Responses": resps}}, nil
	}
	if req.Operation == "ExecuteTransaction" {
		stmts := asSlice(req.Input["TransactStatements"])
		if len(stmts) == 0 {
			stmts = asSlice(req.Input["Statements"])
		}
		responses := []any{}
		for _, statement := range stmts {
			sm := asMap(statement)
			input := map[string]any{"Statement": sm["Statement"]}
			if parameters, ok := sm["Parameters"]; ok {
				input["Parameters"] = parameters
			}
			out, err := p.partiql(ctx, &spi.Request{Identity: req.Identity, HTTP: req.HTTP, Operation: "ExecuteStatement", Input: input})
			if err != nil {
				return nil, err
			}
			if strings.HasPrefix(strings.ToUpper(strings.TrimSpace(str(sm["Statement"]))), "SELECT") {
				responses = append(responses, out.Output)
			}
		}
		return &spi.Response{Output: map[string]any{"Responses": responses}}, nil
	}
	if parameters, ok := req.Input["Parameters"]; ok && len(asSlice(parameters)) == 0 {
		return nil, &spi.Fault{Code: "ValidationException", Message: "1 validation error detected: Value '[]' at 'parameters' failed to satisfy constraint: Member must have length greater than or equal to 1", HTTPStatus: 400, Fault: "client"}
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
	case strings.HasPrefix(up, "UPDATE"):
		table, key, update, values := parsePartiqlUpdate(st)
		if table == "" || len(key) == 0 || update == "" {
			return nil, &spi.Fault{Code: "ValidationException", Message: "UPDATE", HTTPStatus: 400, Fault: "client"}
		}
		return p.Invoke(ctx, &spi.Request{Identity: req.Identity, HTTP: req.HTTP, Operation: "UpdateItem", Input: map[string]any{"TableName": table, "Key": key, "UpdateExpression": update, "ExpressionAttributeValues": values}})
	default:
		table, key := parseWhereKey(st)
		if table == "" {
			table = str(req.Input["TableName"])
		}
		if len(key) > 0 {
			return p.Invoke(ctx, &spi.Request{Identity: req.Identity, HTTP: req.HTTP, Operation: "GetItem", Input: map[string]any{"TableName": table, "Key": key}})
		}
		return p.Invoke(ctx, &spi.Request{Identity: req.Identity, HTTP: req.HTTP, Operation: "Scan", Input: map[string]any{"TableName": table, "FilterExpression": partiqlMissingFilter(st)}})
	}
}

func partiqlTable(statement string) string {
	if table, _ := parseWhereKey(statement); table != "" {
		return table
	}
	table, _ := parseInsert(statement)
	return table
}

func parsePartiqlUpdate(statement string) (string, map[string]any, string, map[string]any) {
	table, key := parseWhereKey(statement)
	upper := strings.ToUpper(statement)
	set, where := strings.Index(upper, " SET "), strings.Index(upper, " WHERE ")
	if set < 0 || where < set {
		return table, key, "", nil
	}
	var clauses []string
	values := map[string]any{}
	// ponytail: scalar assignments only; replace with a PartiQL parser when nested expressions are supported.
	for index, assignment := range strings.Split(statement[set+5:where], ",") {
		name, raw, ok := strings.Cut(assignment, "=")
		value := parsePartiqlMap(`{"value":` + strings.TrimSpace(raw) + `}`)
		if !ok || strings.TrimSpace(name) == "" || value == nil {
			return table, key, "", nil
		}
		token := ":v" + strconv.Itoa(index)
		clauses = append(clauses, strings.TrimSpace(name)+" = "+token)
		values[token] = value["value"]
	}
	return table, key, "SET " + strings.Join(clauses, ", "), values
}

func partiqlMissingFilter(statement string) string {
	upper := strings.ToUpper(statement)
	where := strings.Index(upper, " WHERE ")
	if where < 0 {
		return ""
	}
	condition := strings.TrimSpace(statement[where+7:])
	upperCondition := strings.ToUpper(condition)
	for suffix, function := range map[string]string{" IS NOT MISSING": "attribute_exists", " IS MISSING": "attribute_not_exists"} {
		if strings.HasSuffix(upperCondition, suffix) {
			return function + "(" + strings.TrimSpace(condition[:len(condition)-len(suffix)]) + ")"
		}
	}
	return ""
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
	start := -1
	for _, keyword := range []string{"FROM ", "INTO ", "UPDATE "} {
		if start = strings.Index(up, keyword); start >= 0 {
			start += len(keyword)
			break
		}
	}
	if start < 0 {
		return "", nil
	}
	rest := strings.TrimSpace(st[start:])
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
		if err := col.Txn(ctx, func(tx spi.Tx) error {
			if _, ok, err := tx.Get(name); err != nil {
				return err
			} else if ok {
				return &spi.Fault{Code: "GlobalTableAlreadyExistsException", Message: "Global Table already exists: " + name, HTTPStatus: 400, Fault: "client"}
			}
			return tx.Put(name, b)
		}); err != nil {
			return nil, err
		}
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
		if !ok {
			return nil, &spi.Fault{Code: "GlobalTableNotFoundException", Message: "Global Table not found: " + name, HTTPStatus: 400, Fault: "client"}
		}
		rec := map[string]any{}
		_ = json.Unmarshal(b, &rec)
		reps := asSlice(rec["ReplicationGroup"])
		for _, a := range asSlice(req.Input["ReplicaUpdates"]) {
			um := asMap(a)
			if cr := asMap(um["Create"]); str(cr["RegionName"]) != "" {
				if !hasReplica(reps, str(cr["RegionName"])) {
					reps = append(reps, map[string]any{"RegionName": cr["RegionName"]})
				}
			}
			if del := asMap(um["Delete"]); str(del["RegionName"]) != "" {
				region := str(del["RegionName"])
				kept := make([]any, 0, len(reps))
				for _, replica := range reps {
					if str(asMap(replica)["RegionName"]) != region {
						kept = append(kept, replica)
					}
				}
				reps = kept
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

func (p *Pack) updateTableReplicas(ctx context.Context, req *spi.Request, table string, definition map[string]any) (bool, error) {
	state := map[string]any{"SourceRegion": req.Identity.Region, "ReplicationGroup": []any{}}
	if encoded, ok, _ := p.col(req, "ddbglobal").Get(ctx, table); ok {
		_ = json.Unmarshal(encoded, &state)
	}
	replicas := asSlice(state["ReplicationGroup"])
	deletedCurrent := false
	for _, raw := range asSlice(req.Input["ReplicaUpdates"]) {
		update := asMap(raw)
		if create := asMap(update["Create"]); str(create["RegionName"]) != "" {
			region := str(create["RegionName"])
			if region == str(state["SourceRegion"]) || hasReplica(replicas, region) {
				return false, &spi.Fault{Code: "ValidationException", Message: "Update global table operation failed because one or more replicas already existed", HTTPStatus: 400, Fault: "client"}
			}
			target := regionalRequest(req, region)
			if _, exists, _ := p.col(target, "tables").Get(ctx, table); exists {
				return false, &spi.Fault{Code: "ResourceInUseException", Message: "Table already exists: " + table, HTTPStatus: 400, Fault: "client"}
			}
			clone := cloneMap(definition)
			clone["TableArn"] = "arn:aws:dynamodb:" + region + ":" + req.Identity.Account + ":table/" + table
			clone["TableId"] = p.deps.Rand.Derive("dynamodb:table:" + req.Identity.Account + ":" + region + ":" + table).UUID()
			delete(clone, "LatestStreamArn")
			delete(clone, "LatestStreamLabel")
			p.ensureStream(target, clone, table)
			encoded, _ := json.Marshal(clone)
			if err := p.col(target, "tables").Put(ctx, table, encoded); err != nil {
				return false, err
			}
			items, _, err := p.col(req, "items:"+table).List(ctx, "", "", 0)
			if err != nil {
				return false, err
			}
			for _, item := range items {
				if err := p.col(target, "items:"+table).Put(ctx, item.Key, item.Value); err != nil {
					return false, err
				}
			}
			replica := cloneMap(create)
			replica["ReplicaStatus"] = "ACTIVE"
			replicas = append(replicas, replica)
		}
		if remove := asMap(update["Delete"]); str(remove["RegionName"]) != "" {
			region := str(remove["RegionName"])
			if !hasReplica(replicas, region) {
				return false, &spi.Fault{Code: "ValidationException", Message: "Update global table operation failed because one or more replicas were not part of the global table", HTTPStatus: 400, Fault: "client"}
			}
			replicas = withoutReplica(replicas, region)
			target := regionalRequest(req, region)
			_ = p.col(target, "tables").Delete(ctx, table)
			_ = p.col(target, "ddbglobal").Delete(ctx, table)
			clearCollection(ctx, p.col(target, "items:"+table))
			clearCollection(ctx, p.col(target, "ddbstream:"+table))
			deletedCurrent = region == req.Identity.Region
		}
	}
	state["ReplicationGroup"] = replicas
	regions := []string{str(state["SourceRegion"])}
	for _, replica := range replicas {
		regions = append(regions, str(asMap(replica)["RegionName"]))
	}
	for _, region := range regions {
		target := regionalRequest(req, region)
		stored := p.tableDef(ctx, target, table)
		if len(stored) == 0 {
			continue
		}
		if len(replicas) == 0 {
			delete(stored, "Replicas")
			_ = p.col(target, "ddbglobal").Delete(ctx, table)
		} else {
			stored["Replicas"] = replicas
			encoded, _ := json.Marshal(state)
			_ = p.col(target, "ddbglobal").Put(ctx, table, encoded)
		}
		encoded, _ := json.Marshal(stored)
		_ = p.col(target, "tables").Put(ctx, table, encoded)
		if region == req.Identity.Region {
			for key := range definition {
				delete(definition, key)
			}
			for key, value := range stored {
				definition[key] = value
			}
		}
	}
	return deletedCurrent, nil
}

func (p *Pack) globalTableIdentities(ctx context.Context, req *spi.Request, table string) []spi.Identity {
	encoded, ok, _ := p.col(req, "ddbglobal").Get(ctx, table)
	if !ok {
		return []spi.Identity{req.Identity}
	}
	var state map[string]any
	_ = json.Unmarshal(encoded, &state)
	regions := []string{str(state["SourceRegion"])}
	for _, replica := range asSlice(state["ReplicationGroup"]) {
		regions = append(regions, str(asMap(replica)["RegionName"]))
	}
	identities := make([]spi.Identity, 0, len(regions))
	seen := map[string]bool{}
	for _, region := range regions {
		if region != "" && !seen[region] {
			identity := req.Identity
			identity.Region = region
			identities = append(identities, identity)
			seen[region] = true
		}
	}
	return identities
}

func regionalRequest(req *spi.Request, region string) *spi.Request {
	clone := *req
	clone.Identity.Region = region
	return &clone
}

func hasReplica(replicas []any, region string) bool {
	for _, replica := range replicas {
		if str(asMap(replica)["RegionName"]) == region {
			return true
		}
	}
	return false
}

func withoutReplica(replicas []any, region string) []any {
	kept := make([]any, 0, len(replicas))
	for _, replica := range replicas {
		if str(asMap(replica)["RegionName"]) != region {
			kept = append(kept, replica)
		}
	}
	return kept
}

func clearCollection(ctx context.Context, collection spi.Collection) {
	entries, _, _ := collection.List(ctx, "", "", 0)
	for _, entry := range entries {
		_ = collection.Delete(ctx, entry.Key)
	}
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
