// Package dynamodb is the emulate-tier DynamoDB pack. Expression evaluation
// lives in package expr.
package dynamodb

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"sort"
	"strconv"
	"strings"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/bundled"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/dynamodb/expr"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func init() {
	for _, op := range []string{"CreateTable", "DeleteTable", "DescribeTable", "UpdateTable",
		"PutItem", "GetItem", "DeleteItem", "UpdateItem", "BatchGetItem", "BatchWriteItem",
		"Query", "Scan", "TransactGetItems", "TransactWriteItems",
		"ExecuteStatement", "BatchExecuteStatement", "ExecuteTransaction",
		"CreateBackup", "RestoreTableFromBackup", "RestoreTableToPointInTime",
		"ExportTableToPointInTime", "ImportTable", "SearchVectors",
		"CreateGlobalTable", "DescribeGlobalTable", "ListGlobalTables", "UpdateGlobalTable",
		"DescribeGlobalTableSettings", "UpdateGlobalTableSettings",
		"EnableKinesisStreamingDestination", "DisableKinesisStreamingDestination",
		"DescribeKinesisStreamingDestination", "UpdateKinesisStreamingDestination",
		"ListStreams", "DescribeStream", "GetShardIterator", "GetRecords"} {
		bundled.RegisterNative("aws.dynamodb", op, func(ctx context.Context, deps spi.Deps, req *spi.Request) (*spi.Response, error) {
			return (&Pack{deps: deps}).Invoke(ctx, req)
		})
	}
}

// Pack is the receiver the natives run on; it is built per call.
type Pack struct{ deps spi.Deps }

// New answers with the DynamoDB service, bundle and natives together.
func New(d spi.Deps) spi.BehaviorPack { return bundled.Handler("aws.dynamodb", d) }

func (p *Pack) col(req *spi.Request, n string) spi.Collection {
	return p.deps.Store.Scope(req.Identity.Account, req.Identity.Region).Collection(n)
}

func (p *Pack) Invoke(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	table := spi.Str(req.Input["TableName"])
	requireTable := func(name string) error {
		_, ok, err := p.col(req, "tables").Get(ctx, name)
		if err != nil {
			return err
		}
		if !ok {
			return &spi.Fault{Code: "ResourceNotFoundException", Message: "Requested resource not found", HTTPStatus: 400, Fault: "client"}
		}
		return nil
	}
	switch req.Operation {
	case "PutItem", "GetItem", "DeleteItem", "UpdateItem", "Query", "Scan":
		if err := requireTable(table); err != nil {
			return nil, err
		}
	}
	switch req.Operation {
	case "CreateTable":
		b, _ := json.Marshal(req.Input)
		rec := spi.JSONObject(b)
		if spi.Str(rec["BillingMode"]) == "PAY_PER_REQUEST" && len(spi.Object(rec["ProvisionedThroughput"])) > 0 {
			return nil, &spi.Fault{Code: "ValidationException", Message: "One or more parameter values were invalid: Neither ReadCapacityUnits nor WriteCapacityUnits can be specified when BillingMode is PAY_PER_REQUEST", HTTPStatus: 400, Fault: "client"}
		}
		arn := "arn:aws:dynamodb:" + req.Identity.Region + ":" + req.Identity.Account + ":table/" + table
		tags := rec["Tags"]
		delete(rec, "Tags")
		rec["TableArn"] = arn
		rec["TableId"] = p.deps.Rand.Derive("dynamodb:table:" + req.Identity.Account + ":" + req.Identity.Region + ":" + table).UUID()
		rec["CreationDateTime"] = p.deps.Clock.Now().Unix()
		rec["DeletionProtectionEnabled"] = false
		rec["ItemCount"] = 0
		rec["TableSizeBytes"] = 0
		if err := p.prepareTableMetadata(ctx, req, rec); err != nil {
			return nil, err
		}
		if class := spi.Str(rec["TableClass"]); class != "" {
			rec["TableClassSummary"] = map[string]any{"TableClass": class}
			delete(rec, "TableClass")
		}
		p.ensureStream(req, rec, table)
		b, _ = json.Marshal(rec)
		if err := p.col(req, "tables").Txn(ctx, func(tx spi.Tx) error {
			if _, ok, err := tx.Get(table); err != nil {
				return err
			} else if ok {
				return &spi.Fault{Code: "ResourceInUseException", Message: "Table already exists: " + table, HTTPStatus: 400, Fault: "client"}
			}
			return tx.Put(table, b)
		}); err != nil {
			return nil, err
		}
		if len(spi.Slice(tags)) > 0 {
			_, _ = New(p.deps).Invoke(ctx, &spi.Request{Identity: req.Identity, Operation: "TagResource", Input: map[string]any{"ResourceArn": arn, "Tags": tags}})
		}
		description := tableDescription(rec, "CREATING")
		return &spi.Response{Output: map[string]any{"TableDescription": description}}, nil
	case "DeleteTable":
		var existing map[string]any
		if err := p.col(req, "tables").Txn(ctx, func(tx spi.Tx) error {
			b, ok, err := tx.Get(table)
			if err != nil {
				return err
			} else if !ok {
				return &spi.Fault{Code: "ResourceNotFoundException", Message: "Requested resource not found: Table: " + table + " not found", HTTPStatus: 400, Fault: "client"}
			}
			_ = json.Unmarshal(b, &existing)
			return tx.Delete(table)
		}); err != nil {
			return nil, err
		}
		_ = p.col(req, "ttl").Delete(ctx, table)
		_ = p.col(req, "tags").Delete(ctx, "arn:aws:dynamodb:"+req.Identity.Region+":"+req.Identity.Account+":table/"+table)
		return &spi.Response{Output: map[string]any{"TableDescription": map[string]any{
			"DeletionProtectionEnabled": existing["DeletionProtectionEnabled"],
			"ItemCount":                 existing["ItemCount"],
			"ProvisionedThroughput":     existing["ProvisionedThroughput"],
			"TableArn":                  existing["TableArn"],
			"TableId":                   existing["TableId"],
			"TableName":                 table,
			"TableStatus":               "DELETING",
		}}}, nil
	case "DescribeTable":
		b, ok, _ := p.col(req, "tables").Get(ctx, table)
		if !ok {
			return nil, &spi.Fault{Code: "ResourceNotFoundException", HTTPStatus: 400, Fault: "client"}
		}
		m := spi.JSONObject(b)
		return &spi.Response{Output: map[string]any{"Table": tableDescription(m, "ACTIVE")}}, nil
	case "PutItem":
		item, _ := req.Input["Item"].(map[string]any)
		if err := p.validateItemKey(ctx, req, table, item); err != nil {
			return nil, err
		}
		key := p.itemKeyFrom(ctx, req, table, item)
		b, _ := json.Marshal(item)
		var old map[string]any
		if err := p.col(req, "items:"+table).Txn(ctx, func(tx spi.Tx) error {
			if raw, ok, err := tx.Get(key); err != nil {
				return err
			} else if ok {
				_ = json.Unmarshal(raw, &old)
			}
			if err := p.checkCond(req, old); err != nil {
				return err
			}
			return tx.Put(key, b)
		}); err != nil {
			return nil, err
		}
		ev := "INSERT"
		if old != nil {
			ev = "MODIFY"
		}
		p.emitStream(ctx, req, table, ev, item, old)
		return p.returnValues(req, old, item, nil), nil
	case "GetItem":
		key := p.itemKeyFrom(ctx, req, table, spi.Object(req.Input["Key"]))
		item := p.loadItem(ctx, req, table, key)
		if item == nil {
			return &spi.Response{Output: map[string]any{}}, nil
		}
		item = expr.Project(spi.Str(req.Input["ProjectionExpression"]), item, spi.Object(req.Input["ExpressionAttributeNames"]))
		return &spi.Response{Output: map[string]any{"Item": item}}, nil
	case "DeleteItem":
		key := p.itemKeyFrom(ctx, req, table, spi.Object(req.Input["Key"]))
		var old map[string]any
		if err := p.col(req, "items:"+table).Txn(ctx, func(tx spi.Tx) error {
			if raw, ok, err := tx.Get(key); err != nil {
				return err
			} else if ok {
				_ = json.Unmarshal(raw, &old)
			}
			if err := p.checkCond(req, old); err != nil {
				return err
			}
			return tx.Delete(key)
		}); err != nil {
			return nil, err
		}
		p.emitStream(ctx, req, table, "REMOVE", spi.Object(req.Input["Key"]), old)
		return p.returnValues(req, old, nil, nil), nil
	case "Scan":
		return p.listItems(ctx, req, table, "", spi.Str(req.Input["FilterExpression"]))
	case "Query":
		if indexName := spi.Str(req.Input["IndexName"]); indexName != "" {
			index := indexSpec(p.tableDef(ctx, req, table), indexName)
			if len(index) == 0 {
				return nil, &spi.Fault{Code: "ValidationException", Message: "The table does not have the specified index: " + indexName, HTTPStatus: 400, Fault: "client"}
			}
			if spi.Str(req.Input["Select"]) == "ALL_ATTRIBUTES" && spi.Str(spi.Object(index["Projection"])["ProjectionType"]) != "ALL" {
				return nil, &spi.Fault{Code: "ValidationException", Message: "Select type ALL_ATTRIBUTES is not supported for global secondary index " + indexName + " because its projection type is not ALL", HTTPStatus: 400, Fault: "client"}
			}
		}
		return p.listItems(ctx, req, table, spi.Str(req.Input["KeyConditionExpression"]), spi.Str(req.Input["FilterExpression"]))
	case "UpdateItem":
		key := p.itemKeyFrom(ctx, req, table, spi.Object(req.Input["Key"]))
		var old, item map[string]any
		var touched []string
		if err := p.col(req, "items:"+table).Txn(ctx, func(tx spi.Tx) error {
			if raw, ok, err := tx.Get(key); err != nil {
				return err
			} else if ok {
				_ = json.Unmarshal(raw, &old)
			}
			if err := p.checkCond(req, old); err != nil {
				return err
			}
			item = cloneMap(old)
			if item == nil {
				item = cloneMap(spi.Object(req.Input["Key"]))
			}
			if ue := spi.Str(req.Input["UpdateExpression"]); ue != "" {
				var err error
				touched, err = expr.ApplyUpdate(ue, item, spi.Object(req.Input["ExpressionAttributeNames"]), spi.Object(req.Input["ExpressionAttributeValues"]))
				if err != nil {
					return &spi.Fault{Code: "ValidationException", Message: err.Error(), HTTPStatus: 400, Fault: "client"}
				}
			}
			return tx.Put(key, []byte(spi.JSONText(item)))
		}); err != nil {
			return nil, err
		}
		ev := "INSERT"
		if old != nil {
			ev = "MODIFY"
		}
		p.emitStream(ctx, req, table, ev, item, old)
		return p.returnValues(req, old, item, touched), nil
	case "CreateBackup":
		name := spi.First(req.Input, "BackupName")
		id := p.deps.Rand.Hex(8)
		td, _, _ := p.col(req, "tables").Get(ctx, table)
		items, _, _ := p.col(req, "items:"+table).List(ctx, "", "", 0)
		rec := map[string]any{"BackupArn": "arn:aws:dynamodb:" + req.Identity.Region + ":" + req.Identity.Account + ":table/" + table + "/backup/" + id, "BackupName": name, "TableName": table, "BackupStatus": "AVAILABLE", "Table": string(td), "Items": itemsKV(items)}
		raw, _ := json.Marshal(rec)
		_ = p.col(req, "backups").Put(ctx, id, raw)
		return &spi.Response{Output: map[string]any{"BackupDetails": map[string]any{"BackupArn": rec["BackupArn"], "BackupName": name, "BackupStatus": "AVAILABLE"}}}, nil
	case "RestoreTableFromBackup":
		id := spi.LastSlash(spi.First(req.Input, "BackupArn"))
		b, ok, _ := p.col(req, "backups").Get(ctx, id)
		if !ok {
			return nil, &spi.Fault{Code: "BackupNotFoundException", HTTPStatus: 400, Fault: "client"}
		}
		m := spi.JSONObject(b)
		dst := spi.First(req.Input, "TargetTableName")
		if s := spi.Str(m["Table"]); s != "" {
			_ = p.col(req, "tables").Put(ctx, dst, []byte(s))
		}
		if items, ok := m["Items"].([]any); ok {
			for _, it := range items {
				im := spi.Object(it)
				if key := spi.Str(im["Key"]); key != "" {
					_ = p.col(req, "items:"+dst).Put(ctx, key, []byte(spi.Str(im["Value"])))
				}
			}
		}
		return &spi.Response{Output: map[string]any{"TableDescription": map[string]any{"TableName": dst, "TableStatus": "ACTIVE"}}}, nil
	case "EnableKinesisStreamingDestination", "DisableKinesisStreamingDestination", "DescribeKinesisStreamingDestination", "UpdateKinesisStreamingDestination":
		return p.kinesisDestination(ctx, req)
	case "BatchGetItem":
		out := map[string]any{}
		if ri, ok := req.Input["RequestItems"].(map[string]any); ok {
			for tbl, spec := range ri {
				if err := requireTable(tbl); err != nil {
					return nil, err
				}
				var items []any
				keys, _ := spi.Object(spec)["Keys"].([]any)
				for _, k := range keys {
					key := p.itemKeyFrom(ctx, req, tbl, spi.Object(k))
					b, ok, _ := p.col(req, "items:"+tbl).Get(ctx, key)
					if !ok {
						continue
					}
					item := spi.JSONObject(b)
					items = append(items, item)
				}
				out[tbl] = items
			}
		}
		return &spi.Response{Output: map[string]any{"Responses": out, "UnprocessedKeys": map[string]any{}}}, nil
	case "BatchWriteItem":
		if ri, ok := req.Input["RequestItems"].(map[string]any); ok {
			for tbl, spec := range ri {
				if err := requireTable(tbl); err != nil {
					return nil, err
				}
				reqs, _ := spec.([]any)
				for _, r := range reqs {
					m := spi.Object(r)
					if put := spi.Object(m["PutRequest"]); len(put) > 0 {
						item := spi.Object(put["Item"])
						if err := p.validateItemKey(ctx, req, tbl, item); err != nil {
							return nil, err
						}
						key := p.itemKeyFrom(ctx, req, tbl, item)
						old := p.loadItem(ctx, req, tbl, key)
						b, _ := json.Marshal(item)
						_ = p.col(req, "items:"+tbl).Put(ctx, key, b)
						ev := "INSERT"
						if old != nil {
							ev = "MODIFY"
						}
						p.emitStream(ctx, req, tbl, ev, item, old)
					}
					if del := spi.Object(m["DeleteRequest"]); len(del) > 0 {
						dk := spi.Object(del["Key"])
						key := p.itemKeyFrom(ctx, req, tbl, dk)
						old := p.loadItem(ctx, req, tbl, key)
						_ = p.col(req, "items:"+tbl).Delete(ctx, key)
						p.emitStream(ctx, req, tbl, "REMOVE", dk, old)
					}
				}
			}
		}
		return &spi.Response{Output: map[string]any{"UnprocessedItems": map[string]any{}}}, nil
	case "TransactGetItems":
		return p.transactGetItems(ctx, req)
	case "TransactWriteItems":
		return p.transactWriteItems(ctx, req)
	case "UpdateTable":
		b, ok, _ := p.col(req, "tables").Get(ctx, table)
		if !ok {
			return nil, &spi.Fault{Code: "ResourceNotFoundException", Message: "Requested resource not found", HTTPStatus: 400, Fault: "client"}
		}
		m := map[string]any{}
		_ = json.Unmarshal(b, &m)
		if req.Input["ReplicaUpdates"] != nil {
			deletedCurrent, err := p.updateTableReplicas(ctx, req, table, m)
			if err != nil {
				return nil, err
			}
			if deletedCurrent {
				return &spi.Response{Output: map[string]any{"TableDescription": tableDescription(m, "UPDATING")}}, nil
			}
		}
		if gsi := req.Input["GlobalSecondaryIndexUpdates"]; gsi != nil {
			indexes, _ := m["GlobalSecondaryIndexes"].([]any)
			if ups, ok := gsi.([]any); ok {
				for _, u := range ups {
					um := spi.Object(u)
					if cr := spi.Object(um["Create"]); len(cr) > 0 {
						indexes = append(indexes, cr)
					}
					if del := spi.Object(um["Delete"]); len(del) > 0 {
						name := spi.Str(del["IndexName"])
						var keep []any
						for _, ix := range indexes {
							if spi.Str(spi.Object(ix)["IndexName"]) != name {
								keep = append(keep, ix)
							}
						}
						indexes = keep
					}
				}
			}
			m["GlobalSecondaryIndexes"] = indexes
		}
		if spec := req.Input["StreamSpecification"]; spec != nil {
			m["StreamSpecification"] = spec
			p.ensureStream(req, m, table)
		}
		if class := spi.Str(req.Input["TableClass"]); class != "" {
			m["TableClassSummary"] = map[string]any{"TableClass": class}
		}
		for _, key := range []string{"AttributeDefinitions", "BillingMode", "ProvisionedThroughput", "SSESpecification", "WarmThroughput"} {
			if value := req.Input[key]; value != nil {
				m[key] = value
			}
		}
		if err := p.prepareTableMetadata(ctx, req, m); err != nil {
			return nil, err
		}
		nb, _ := json.Marshal(m)
		_ = p.col(req, "tables").Put(ctx, table, nb)
		description := tableDescription(m, "UPDATING")
		if spec := spi.Object(req.Input["SSESpecification"]); spec["Enabled"] == false {
			if sse := spi.Object(description["SSEDescription"]); len(sse) > 0 {
				sse["Status"] = "UPDATING"
			}
		}
		return &spi.Response{Output: map[string]any{"TableDescription": description}}, nil
	case "ExecuteStatement", "BatchExecuteStatement", "ExecuteTransaction":
		return p.partiql(ctx, req)
	case "CreateGlobalTable", "DescribeGlobalTable", "ListGlobalTables", "UpdateGlobalTable",
		"DescribeGlobalTableSettings", "UpdateGlobalTableSettings":
		return p.globalTable(ctx, req)
	case "ExportTableToPointInTime":
		return p.export(ctx, req)
	case "ImportTable":
		return p.importTable(ctx, req)
	case "RestoreTableToPointInTime":
		return p.restorePITR(ctx, req)
	case "SearchVectors":
		return p.searchVectors(ctx, req)
	case "ListStreams":
		return p.listStreams(ctx, req)
	case "DescribeStream":
		return p.describeStream(ctx, req)
	case "GetShardIterator":
		return p.getShardIterator(ctx, req)
	case "GetRecords":
		return p.getStreamRecords(ctx, req)
	default:
		return nil, spi.NotImplemented("aws.dynamodb", req.Operation, "emulate")
	}
}

func (p *Pack) validateItemKey(ctx context.Context, req *spi.Request, table string, item map[string]any) error {
	return validateItemKeyDefinition(p.tableDef(ctx, req, table), item)
}

func validateItemKeyDefinition(definition, item map[string]any) error {
	for _, key := range spi.Slice(definition["KeySchema"]) {
		name := spi.Str(spi.Object(key)["AttributeName"])
		if name != "" && len(spi.Object(item[name])) == 0 {
			return &spi.Fault{Code: "ValidationException", Message: "One or more parameter values were invalid: Missing the key " + name + " in the item", HTTPStatus: 400, Fault: "client"}
		}
	}
	return nil
}

func (p *Pack) listItems(ctx context.Context, req *spi.Request, table, keyCond, filter string) (*spi.Response, error) {
	td := p.tableDef(ctx, req, table)
	kvs, _, _ := p.col(req, "items:"+table).List(ctx, "", "", 0)
	names := spi.Object(req.Input["ExpressionAttributeNames"])
	values := spi.Object(req.Input["ExpressionAttributeValues"])
	var matched []map[string]any
	for _, kv := range kvs {
		item := spi.JSONObject(kv.Value)
		if keyCond != "" {
			ok, err := expr.EvalBool(keyCond, item, names, values)
			if err != nil || !ok {
				continue
			}
		}
		if filter != "" {
			ok, err := expr.EvalBool(filter, item, names, values)
			if err != nil || !ok {
				continue
			}
		}
		matched = append(matched, p.projectIndex(td, spi.Str(req.Input["IndexName"]), item))
	}
	sort.Slice(matched, func(i, j int) bool {
		return itemKey(matched[i]) < itemKey(matched[j])
	})
	start := spi.Object(req.Input["ExclusiveStartKey"])
	if len(start) > 0 {
		sk := itemKey(start)
		var rest []map[string]any
		seen := false
		for _, it := range matched {
			if seen {
				rest = append(rest, it)
				continue
			}
			if itemKey(p.tableKey(td, it)) == sk || itemKey(it) == sk {
				seen = true
			}
		}
		matched = rest
	}
	limit := asInt(req.Input["Limit"])
	var last map[string]any
	if limit > 0 && len(matched) > limit {
		last = p.tableKey(td, matched[limit-1])
		matched = matched[:limit]
	}
	proj := spi.Str(req.Input["ProjectionExpression"])
	var items []any
	for _, it := range matched {
		if proj != "" {
			it = expr.Project(proj, it, names)
		}
		items = append(items, it)
	}
	out := map[string]any{"Items": items, "Count": len(items), "ScannedCount": len(kvs)}
	if last != nil {
		out["LastEvaluatedKey"] = last
	}
	return &spi.Response{Output: map[string]any(out)}, nil
}

func truthy(v any) bool {
	switch t := v.(type) {
	case bool:
		return t
	case string:
		return t == "true" || t == "TRUE" || t == "1"
	case float64:
		return t != 0
	}
	return false
}

func itemsKV(kvs []spi.KV) []any {
	out := make([]any, 0, len(kvs))
	for _, kv := range kvs {
		out = append(out, map[string]any{"Key": kv.Key, "Value": string(kv.Value)})
	}
	return out
}

func asInt(v any) int {
	switch n := v.(type) {
	case float64:
		return int(n)
	case int:
		return n
	case int64:
		return int(n)
	case string:
		i, _ := strconv.Atoi(n)
		return i
	}
	return 0
}

func cloneMap(m map[string]any) map[string]any {
	if m == nil {
		return nil
	}
	b, _ := json.Marshal(m)
	o := spi.JSONObject(b)
	return o
}

func (p *Pack) prepareTableMetadata(ctx context.Context, req *spi.Request, table map[string]any) error {
	billing := spi.Str(table["BillingMode"])
	if billing == "" {
		billing = spi.Str(spi.Object(table["BillingModeSummary"])["BillingMode"])
	}
	if billing == "PAY_PER_REQUEST" {
		table["BillingModeSummary"] = map[string]any{"BillingMode": billing}
	}
	delete(table, "BillingMode")
	throughput := cloneMap(spi.Object(table["ProvisionedThroughput"]))
	if billing == "PAY_PER_REQUEST" {
		throughput = map[string]any{"ReadCapacityUnits": 0, "WriteCapacityUnits": 0}
	}
	if len(throughput) > 0 {
		throughput["NumberOfDecreasesToday"] = 0
		table["ProvisionedThroughput"] = throughput
	}
	arn := spi.Str(table["TableArn"])
	for _, raw := range spi.Slice(table["GlobalSecondaryIndexes"]) {
		index := spi.Object(raw)
		index["IndexArn"] = arn + "/index/" + spi.Str(index["IndexName"])
		index["IndexSizeBytes"] = 0
		index["ItemCount"] = 0
		indexThroughput := cloneMap(spi.Object(index["ProvisionedThroughput"]))
		if billing == "PAY_PER_REQUEST" {
			indexThroughput = map[string]any{"ReadCapacityUnits": 0, "WriteCapacityUnits": 0}
		}
		if len(indexThroughput) > 0 {
			indexThroughput["NumberOfDecreasesToday"] = 0
			index["ProvisionedThroughput"] = indexThroughput
		}
	}
	if spec := spi.Object(table["SSESpecification"]); len(spec) > 0 {
		if spec["Enabled"] == true {
			key := spi.Str(spec["KMSMasterKeyId"])
			if key == "" {
				var err error
				key, err = p.defaultDynamoDBKey(ctx, req)
				if err != nil {
					return err
				}
			} else if !strings.HasPrefix(key, "arn:") {
				key = "arn:aws:kms:" + req.Identity.Region + ":" + req.Identity.Account + ":key/" + key
			}
			table["SSEDescription"] = map[string]any{"Status": "ENABLED", "SSEType": "KMS", "KMSMasterKeyArn": key}
		}
		delete(table, "SSESpecification")
	}
	return nil
}

func (p *Pack) defaultDynamoDBKey(ctx context.Context, req *spi.Request) (string, error) {
	const alias = "alias/aws/dynamodb"
	var arn string
	err := p.deps.Store.Scope(req.Identity.Account, req.Identity.Region).Txn(ctx, func(tx spi.ScopeTx) error {
		aliases := tx.Collection("kmsalias")
		keys := tx.Collection("kms")
		if raw, ok, err := aliases.Get(alias); err != nil {
			return err
		} else if ok {
			record := spi.JSONObject(raw)
			if raw, ok, err = keys.Get(spi.Str(record["TargetKeyId"])); err != nil {
				return err
			} else if ok {
				_ = json.Unmarshal(raw, &record)
				arn = spi.Str(record["Arn"])
				return nil
			}
		}
		random := p.deps.Rand.Derive("aws-managed-kms:" + req.Identity.Account + ":" + req.Identity.Region + ":" + alias)
		id := random.Hex(8)
		arn = "arn:aws:kms:" + req.Identity.Region + ":" + req.Identity.Account + ":key/" + id
		key := map[string]any{
			"AWSAccountId": req.Identity.Account, "Arn": arn, "CreationDate": p.deps.Clock.Now().Unix(),
			"CurrentKeyMaterialId": random.UUID(), "CustomerMasterKeySpec": "SYMMETRIC_DEFAULT",
			"Description": "Default key that protects my DynamoDB data when no other key is defined",
			"Enabled":     true, "EncryptionAlgorithms": []any{"SYMMETRIC_DEFAULT"}, "KeyId": id, "KeyManager": "AWS",
			"KeyMaterial": base64.StdEncoding.EncodeToString(random.Bytes(32)), "KeySpec": "SYMMETRIC_DEFAULT",
			"KeyState": "Enabled", "KeyUsage": "ENCRYPT_DECRYPT", "MultiRegion": false, "Origin": "AWS_KMS",
		}
		raw, _ := json.Marshal(key)
		if err := keys.Put(id, raw); err != nil {
			return err
		}
		raw, _ = json.Marshal(map[string]any{"AliasName": alias, "TargetKeyId": id})
		return aliases.Put(alias, raw)
	})
	return arn, err
}

func tableDescription(table map[string]any, status string) map[string]any {
	description := cloneMap(table)
	description["TableStatus"] = status
	warm := spi.Object(description["WarmThroughput"])
	if status == "ACTIVE" && len(warm) == 0 {
		if throughput := spi.Object(description["ProvisionedThroughput"]); asInt(throughput["ReadCapacityUnits"]) > 0 || asInt(throughput["WriteCapacityUnits"]) > 0 {
			warm = map[string]any{"ReadUnitsPerSecond": throughput["ReadCapacityUnits"], "WriteUnitsPerSecond": throughput["WriteCapacityUnits"]}
			description["WarmThroughput"] = warm
		}
	}
	if len(warm) > 0 {
		warm["Status"] = "ACTIVE"
		if status != "ACTIVE" {
			warm["Status"] = "UPDATING"
		}
	}
	for _, raw := range spi.Slice(description["GlobalSecondaryIndexes"]) {
		spi.Object(raw)["IndexStatus"] = status
	}
	return description
}

func (p *Pack) loadItem(ctx context.Context, req *spi.Request, table, key string) map[string]any {
	b, ok, _ := p.col(req, "items:"+table).Get(ctx, key)
	if !ok {
		return nil
	}
	item := spi.JSONObject(b)
	return item
}

func (p *Pack) checkCond(req *spi.Request, item map[string]any) error {
	ok, err := conditionOK(req.Input, item)
	if err != nil {
		return &spi.Fault{Code: "ValidationException", Message: err.Error(), HTTPStatus: 400, Fault: "client"}
	}
	if !ok {
		fields := map[string]any{}
		if spi.Str(req.Input["ReturnValuesOnConditionCheckFailure"]) == "ALL_OLD" && item != nil {
			fields["Item"] = item
		}
		return &spi.Fault{Code: "ConditionalCheckFailedException", Message: "The conditional request failed", HTTPStatus: 400, Fault: "client", Fields: fields}
	}
	return nil
}

func conditionOK(input, item map[string]any) (bool, error) {
	cond := spi.Str(input["ConditionExpression"])
	if cond == "" {
		return true, nil
	}
	if item == nil {
		item = map[string]any{}
	}
	return expr.EvalBool(cond, item, spi.Object(input["ExpressionAttributeNames"]), spi.Object(input["ExpressionAttributeValues"]))
}

func (p *Pack) returnValues(req *spi.Request, old, neu map[string]any, touched []string) *spi.Response {
	mode := spi.Str(req.Input["ReturnValues"])
	if mode == "" || mode == "NONE" {
		return &spi.Response{Output: map[string]any{}}
	}
	pick := func(src map[string]any) map[string]any {
		if src == nil {
			return map[string]any{}
		}
		if mode != "UPDATED_OLD" && mode != "UPDATED_NEW" {
			return src
		}
		out := map[string]any{}
		for _, k := range touched {
			if v, ok := src[k]; ok {
				out[k] = v
			}
		}
		return out
	}
	var attrs map[string]any
	switch mode {
	case "ALL_OLD", "UPDATED_OLD":
		attrs = pick(old)
	case "ALL_NEW", "UPDATED_NEW":
		attrs = pick(neu)
	default:
		attrs = map[string]any{}
	}
	if len(attrs) == 0 {
		return &spi.Response{Output: map[string]any{}}
	}
	return &spi.Response{Output: map[string]any{"Attributes": attrs}}
}

func (p *Pack) tableDef(ctx context.Context, req *spi.Request, table string) map[string]any {
	b, ok, _ := p.col(req, "tables").Get(ctx, table)
	if !ok {
		return map[string]any{}
	}
	m := spi.JSONObject(b)
	return m
}

func (p *Pack) tableKey(td, item map[string]any) map[string]any {
	out := map[string]any{}
	ks, _ := td["KeySchema"].([]any)
	for _, e := range ks {
		name := spi.Str(spi.Object(e)["AttributeName"])
		if value, ok := item[name]; name != "" && ok {
			out[name] = value
		}
	}
	if len(out) == 0 {
		return cloneMap(item)
	}
	return out
}

func (p *Pack) projectIndex(td map[string]any, indexName string, item map[string]any) map[string]any {
	if indexName == "" {
		return item
	}
	spec := indexSpec(td, indexName)
	if len(spec) == 0 {
		return item
	}
	proj := spi.Object(spec["Projection"])
	pt := spi.Str(proj["ProjectionType"])
	if pt == "" || pt == "ALL" {
		return item
	}
	keep := map[string]bool{}
	for _, e := range append(spi.Slice(td["KeySchema"]), spi.Slice(spec["KeySchema"])...) {
		if n := spi.Str(spi.Object(e)["AttributeName"]); n != "" {
			keep[n] = true
		}
	}
	if pt == "INCLUDE" {
		for _, a := range spi.Slice(proj["NonKeyAttributes"]) {
			keep[spi.Str(a)] = true
		}
	}
	out := map[string]any{}
	for k, v := range item {
		if keep[k] {
			out[k] = v
		}
	}
	return out
}

func indexSpec(td map[string]any, indexName string) map[string]any {
	for _, key := range []string{"GlobalSecondaryIndexes", "LocalSecondaryIndexes"} {
		for _, ix := range spi.Slice(td[key]) {
			m := spi.Object(ix)
			if spi.Str(m["IndexName"]) == indexName {
				return m
			}
		}
	}
	return nil
}

func itemKey(m map[string]any) string {
	b, _ := json.Marshal(m)
	return string(b)
}

func (p *Pack) itemKeyFrom(ctx context.Context, req *spi.Request, table string, attrs map[string]any) string {
	b, ok, _ := p.col(req, "tables").Get(ctx, table)
	if ok {
		td := spi.JSONObject(b)
		return itemKeyFromDefinition(td, attrs)
	}
	return itemKey(attrs)
}

func itemKeyFromDefinition(definition, attrs map[string]any) string {
	if keys := spi.Slice(definition["KeySchema"]); len(keys) > 0 {
		item := map[string]any{}
		for _, key := range keys {
			if name := spi.Str(spi.Object(key)["AttributeName"]); name != "" {
				item[name] = attrs[name]
			}
		}
		return itemKey(item)
	}
	return itemKey(attrs)
}

// ExpireItems deletes every item whose TTL attribute has passed, in every
// table of id's scope with TTL enabled, and reports how many it deleted. It
// serves the edge's /_aws/dynamodb/expired, which is not an operation.
func ExpireItems(ctx context.Context, deps spi.Deps, id spi.Identity) (int, error) {
	p, req := &Pack{deps: deps}, &spi.Request{Identity: id}
	tables, _, err := p.col(req, "tables").List(ctx, "", "", 0)
	if err != nil {
		return 0, err
	}
	expired := 0
	for _, tableRecord := range tables {
		table := tableRecord.Key
		rawTTL, ok, err := p.col(req, "ttl").Get(ctx, table)
		if err != nil {
			return 0, err
		}
		var ttl map[string]any
		if !ok || json.Unmarshal(rawTTL, &ttl) != nil || !truthy(ttl["Enabled"]) || spi.Str(ttl["AttributeName"]) == "" {
			continue
		}
		items, _, err := p.col(req, "items:"+table).List(ctx, "", "", 0)
		if err != nil {
			return 0, err
		}
		definition := p.tableDef(ctx, req, table)
		for _, itemRecord := range items {
			item := spi.JSONObject(itemRecord.Value)
			expires, err := strconv.ParseInt(spi.Str(spi.Object(item[spi.Str(ttl["AttributeName"])])["N"]), 10, 64)
			if err != nil || expires > p.deps.Clock.Now().Unix() {
				continue
			}
			deleted := false
			if err := p.col(req, "items:"+table).Txn(ctx, func(tx spi.Tx) error {
				if _, ok, err := tx.Get(itemRecord.Key); err != nil || !ok {
					return err
				}
				deleted = true
				return tx.Delete(itemRecord.Key)
			}); err != nil {
				return 0, err
			}
			if !deleted {
				continue
			}
			expired++
			p.emitStream(ctx, req, table, "REMOVE", p.tableKey(definition, item), item)
		}
	}
	return expired, nil
}
