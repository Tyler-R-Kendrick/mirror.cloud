// Package dynamodb is the emulate-tier DynamoDB pack. Expression evaluation
// lives in package expr.
package dynamodb

import (
	"context"
	"encoding/json"
	"sort"
	"strconv"
	"strings"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/dynamodb/expr"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func init() {
	registry.Register(registry.Factory{ServiceID: "aws.dynamodb", Tier: model.TierEmulate, New: func(d spi.Deps) (spi.BehaviorPack, error) {
		return &Pack{deps: d}, nil
	}})
}

// Pack implements DynamoDB.
type Pack struct{ deps spi.Deps }

// New constructs the pack.
func New(d spi.Deps) *Pack { return &Pack{deps: d} }

func (p *Pack) ServiceID() string { return "aws.dynamodb" }
func (p *Pack) Tier() model.Tier  { return model.TierEmulate }
func (p *Pack) Operations() []string {
	core := []string{"CreateTable", "DeleteTable", "DescribeTable", "ListTables", "UpdateTable",
		"PutItem", "GetItem", "DeleteItem", "UpdateItem", "BatchGetItem", "BatchWriteItem",
		"Query", "Scan", "TransactGetItems", "TransactWriteItems",
		"TagResource", "UntagResource", "ListTagsOfResource",
		"DescribeTimeToLive", "UpdateTimeToLive",
		"DescribeContinuousBackups", "UpdateContinuousBackups",
		"DescribeEndpoints", "DescribeLimits",
		"PutResourcePolicy", "GetResourcePolicy", "DeleteResourcePolicy",
		"CreateBackup", "ListBackups", "DescribeBackup", "DeleteBackup", "RestoreTableFromBackup",
		"EnableKinesisStreamingDestination", "DisableKinesisStreamingDestination", "DescribeKinesisStreamingDestination",
		"BatchExecuteStatement", "CreateGlobalTable", "DescribeContributorInsights", "DescribeExport",
		"DescribeGlobalTable", "DescribeGlobalTableSettings", "DescribeImport", "DescribeTableReplicaAutoScaling",
		"ExecuteStatement", "ExecuteTransaction", "ExportTableToPointInTime", "ImportTable",
		"ListContributorInsights", "ListExports", "ListGlobalTables", "ListImports",
		"RestoreTableToPointInTime", "SearchVectors", "UpdateContributorInsights", "UpdateGlobalTable",
		"UpdateGlobalTableSettings", "UpdateKinesisStreamingDestination", "UpdateTableReplicaAutoScaling",
		"ListStreams", "DescribeStream", "GetShardIterator", "GetRecords"}
	return core
}

func (p *Pack) col(req *spi.Request, n string) spi.Collection {
	return p.deps.Store.Scope(req.Identity.Account, req.Identity.Region).Collection(n)
}

func (p *Pack) Invoke(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	table := str(req.Input["TableName"])
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
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		arn := "arn:aws:dynamodb:" + req.Identity.Region + ":" + req.Identity.Account + ":table/" + table
		tags := rec["Tags"]
		delete(rec, "Tags")
		rec["TableArn"] = arn
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
		if len(asSlice(tags)) > 0 {
			_, _ = p.Invoke(ctx, &spi.Request{Identity: req.Identity, Operation: "TagResource", Input: map[string]any{"ResourceArn": arn, "Tags": tags}})
		}
		return &spi.Response{Output: map[string]any{"TableDescription": map[string]any{"TableName": table, "TableArn": arn, "TableStatus": "ACTIVE", "LatestStreamArn": rec["LatestStreamArn"]}}}, nil
	case "DeleteTable":
		if err := p.col(req, "tables").Txn(ctx, func(tx spi.Tx) error {
			if _, ok, err := tx.Get(table); err != nil {
				return err
			} else if !ok {
				return &spi.Fault{Code: "ResourceNotFoundException", Message: "Requested resource not found: Table: " + table + " not found", HTTPStatus: 400, Fault: "client"}
			}
			return tx.Delete(table)
		}); err != nil {
			return nil, err
		}
		_ = p.col(req, "ttl").Delete(ctx, table)
		_ = p.col(req, "tags").Delete(ctx, "arn:aws:dynamodb:"+req.Identity.Region+":"+req.Identity.Account+":table/"+table)
		return &spi.Response{Output: map[string]any{"TableDescription": map[string]any{"TableName": table, "TableStatus": "DELETING"}}}, nil
	case "DescribeTable":
		b, ok, _ := p.col(req, "tables").Get(ctx, table)
		if !ok {
			return nil, &spi.Fault{Code: "ResourceNotFoundException", HTTPStatus: 400, Fault: "client"}
		}
		var m map[string]any
		_ = json.Unmarshal(b, &m)
		m["TableStatus"] = "ACTIVE"
		m["TableName"] = table
		return &spi.Response{Output: map[string]any{"Table": m}}, nil
	case "ListTables":
		kvs, _, _ := p.col(req, "tables").List(ctx, "", "", 0)
		var names []any
		for _, kv := range kvs {
			names = append(names, kv.Key)
		}
		return &spi.Response{Output: map[string]any{"TableNames": names}}, nil
	case "PutItem":
		item, _ := req.Input["Item"].(map[string]any)
		if err := p.validateItemKey(ctx, req, table, item); err != nil {
			return nil, err
		}
		if err := p.checkCond(req, item); err != nil {
			return nil, err
		}
		key := p.itemKeyFrom(ctx, req, table, item)
		old := p.loadItem(ctx, req, table, key)
		b, _ := json.Marshal(item)
		_ = p.col(req, "items:"+table).Put(ctx, key, b)
		ev := "INSERT"
		if old != nil {
			ev = "MODIFY"
		}
		p.emitStream(ctx, req, table, ev, item, old)
		return p.returnValues(req, old, item, nil), nil
	case "GetItem":
		key := p.itemKeyFrom(ctx, req, table, asMap(req.Input["Key"]))
		item := p.loadItem(ctx, req, table, key)
		if item == nil {
			return &spi.Response{Output: map[string]any{}}, nil
		}
		item = expr.Project(str(req.Input["ProjectionExpression"]), item, asMap(req.Input["ExpressionAttributeNames"]))
		return &spi.Response{Output: map[string]any{"Item": item}}, nil
	case "DeleteItem":
		key := p.itemKeyFrom(ctx, req, table, asMap(req.Input["Key"]))
		old := p.loadItem(ctx, req, table, key)
		if err := p.checkCond(req, old); err != nil {
			return nil, err
		}
		_ = p.col(req, "items:"+table).Delete(ctx, key)
		p.emitStream(ctx, req, table, "REMOVE", asMap(req.Input["Key"]), old)
		return p.returnValues(req, old, nil, nil), nil
	case "Scan":
		return p.listItems(ctx, req, table, "", str(req.Input["FilterExpression"]))
	case "Query":
		if indexName := str(req.Input["IndexName"]); indexName != "" {
			index := indexSpec(p.tableDef(ctx, req, table), indexName)
			if len(index) == 0 {
				return nil, &spi.Fault{Code: "ValidationException", Message: "The table does not have the specified index: " + indexName, HTTPStatus: 400, Fault: "client"}
			}
			if str(req.Input["Select"]) == "ALL_ATTRIBUTES" && str(asMap(index["Projection"])["ProjectionType"]) != "ALL" {
				return nil, &spi.Fault{Code: "ValidationException", Message: "Select type ALL_ATTRIBUTES is not supported for global secondary index " + indexName + " because its projection type is not ALL", HTTPStatus: 400, Fault: "client"}
			}
		}
		return p.listItems(ctx, req, table, str(req.Input["KeyConditionExpression"]), str(req.Input["FilterExpression"]))
	case "UpdateItem":
		key := p.itemKeyFrom(ctx, req, table, asMap(req.Input["Key"]))
		old := p.loadItem(ctx, req, table, key)
		item := cloneMap(old)
		if item == nil {
			item = cloneMap(asMap(req.Input["Key"]))
		}
		if err := p.checkCond(req, item); err != nil {
			return nil, err
		}
		var touched []string
		if ue := str(req.Input["UpdateExpression"]); ue != "" {
			var err error
			touched, err = expr.ApplyUpdate(ue, item, asMap(req.Input["ExpressionAttributeNames"]), asMap(req.Input["ExpressionAttributeValues"]))
			if err != nil {
				return nil, &spi.Fault{Code: "ValidationException", Message: err.Error(), HTTPStatus: 400, Fault: "client"}
			}
		}
		raw, _ := json.Marshal(item)
		_ = p.col(req, "items:"+table).Put(ctx, key, raw)
		ev := "INSERT"
		if old != nil {
			ev = "MODIFY"
		}
		p.emitStream(ctx, req, table, ev, item, old)
		return p.returnValues(req, old, item, touched), nil
	case "UpdateTimeToLive":
		if _, ok, _ := p.col(req, "tables").Get(ctx, table); !ok {
			return nil, &spi.Fault{Code: "ResourceNotFoundException", Message: "Cannot do operations on a non-existent table", HTTPStatus: 400, Fault: "client"}
		}
		spec := req.Input["TimeToLiveSpecification"]
		b, _ := json.Marshal(spec)
		_ = p.col(req, "ttl").Put(ctx, table, b)
		return &spi.Response{Output: map[string]any{"TimeToLiveSpecification": spec}}, nil
	case "DescribeTimeToLive":
		if _, ok, _ := p.col(req, "tables").Get(ctx, table); !ok {
			return nil, &spi.Fault{Code: "ResourceNotFoundException", Message: "Cannot do operations on a non-existent table", HTTPStatus: 400, Fault: "client"}
		}
		b, ok, _ := p.col(req, "ttl").Get(ctx, table)
		if !ok {
			return &spi.Response{Output: map[string]any{"TimeToLiveDescription": map[string]any{"TimeToLiveStatus": "DISABLED"}}}, nil
		}
		var spec map[string]any
		_ = json.Unmarshal(b, &spec)
		status := "DISABLED"
		if truthy(spec["Enabled"]) {
			status = "ENABLED"
		}
		out := map[string]any{"TimeToLiveStatus": status, "AttributeName": spec["AttributeName"]}
		return &spi.Response{Output: map[string]any{"TimeToLiveDescription": out}}, nil
	case "ExpireItems":
		tables, _, err := p.col(req, "tables").List(ctx, "", "", 0)
		if err != nil {
			return nil, err
		}
		expired := 0
		for _, tableRecord := range tables {
			table := tableRecord.Key
			rawTTL, ok, err := p.col(req, "ttl").Get(ctx, table)
			if err != nil {
				return nil, err
			}
			var ttl map[string]any
			if !ok || json.Unmarshal(rawTTL, &ttl) != nil || !truthy(ttl["Enabled"]) || str(ttl["AttributeName"]) == "" {
				continue
			}
			items, _, err := p.col(req, "items:"+table).List(ctx, "", "", 0)
			if err != nil {
				return nil, err
			}
			definition := p.tableDef(ctx, req, table)
			for _, itemRecord := range items {
				var item map[string]any
				_ = json.Unmarshal(itemRecord.Value, &item)
				expires, err := strconv.ParseInt(str(asMap(item[str(ttl["AttributeName"])])["N"]), 10, 64)
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
					return nil, err
				}
				if !deleted {
					continue
				}
				expired++
				p.emitStream(ctx, req, table, "REMOVE", p.tableKey(definition, item), item)
			}
		}
		return &spi.Response{Output: map[string]any{"ExpiredItems": expired}}, nil
	case "UpdateContinuousBackups":
		spec := asMap(req.Input["PointInTimeRecoverySpecification"])
		b, _ := json.Marshal(spec)
		_ = p.col(req, "pitr").Put(ctx, table, b)
		st := "DISABLED"
		if truthy(spec["PointInTimeRecoveryEnabled"]) {
			st = "ENABLED"
		}
		return &spi.Response{Output: map[string]any{"ContinuousBackupsDescription": map[string]any{
			"ContinuousBackupsStatus":        "ENABLED",
			"PointInTimeRecoveryDescription": map[string]any{"PointInTimeRecoveryStatus": st},
		}}}, nil
	case "DescribeContinuousBackups":
		st := "DISABLED"
		if b, ok, _ := p.col(req, "pitr").Get(ctx, table); ok {
			var spec map[string]any
			_ = json.Unmarshal(b, &spec)
			if truthy(spec["PointInTimeRecoveryEnabled"]) {
				st = "ENABLED"
			}
		}
		return &spi.Response{Output: map[string]any{"ContinuousBackupsDescription": map[string]any{
			"ContinuousBackupsStatus":        "ENABLED",
			"PointInTimeRecoveryDescription": map[string]any{"PointInTimeRecoveryStatus": st},
		}}}, nil
	case "DescribeEndpoints":
		addr := "http://127.0.0.1:4566"
		if req.HTTP != nil && req.HTTP.Host != "" {
			addr = "http://" + req.HTTP.Host
		}
		return &spi.Response{Output: map[string]any{"Endpoints": []any{map[string]any{"Address": addr, "CachePeriodInMinutes": 1440}}}}, nil
	case "DescribeLimits":
		return &spi.Response{Output: map[string]any{
			"AccountMaxReadCapacityUnits": 80000, "AccountMaxWriteCapacityUnits": 80000,
			"TableMaxReadCapacityUnits": 40000, "TableMaxWriteCapacityUnits": 40000,
		}}, nil
	case "PutResourcePolicy":
		_ = p.col(req, "ddbpolicy").Put(ctx, first(req.Input, "ResourceArn"), []byte(str(req.Input["Policy"])))
		return &spi.Response{Output: map[string]any{"RevisionId": "1"}}, nil
	case "GetResourcePolicy":
		b, ok, _ := p.col(req, "ddbpolicy").Get(ctx, first(req.Input, "ResourceArn"))
		if !ok {
			return nil, &spi.Fault{Code: "PolicyNotFoundException", HTTPStatus: 400, Fault: "client"}
		}
		return &spi.Response{Output: map[string]any{"Policy": string(b), "RevisionId": "1"}}, nil
	case "DeleteResourcePolicy":
		_ = p.col(req, "ddbpolicy").Delete(ctx, first(req.Input, "ResourceArn"))
		return &spi.Response{Output: map[string]any{"RevisionId": "1"}}, nil
	case "CreateBackup":
		name := first(req.Input, "BackupName")
		id := p.deps.Rand.Hex(8)
		td, _, _ := p.col(req, "tables").Get(ctx, table)
		items, _, _ := p.col(req, "items:"+table).List(ctx, "", "", 0)
		rec := map[string]any{"BackupArn": "arn:aws:dynamodb:" + req.Identity.Region + ":" + req.Identity.Account + ":table/" + table + "/backup/" + id, "BackupName": name, "TableName": table, "BackupStatus": "AVAILABLE", "Table": string(td), "Items": itemsKV(items)}
		raw, _ := json.Marshal(rec)
		_ = p.col(req, "backups").Put(ctx, id, raw)
		return &spi.Response{Output: map[string]any{"BackupDetails": map[string]any{"BackupArn": rec["BackupArn"], "BackupName": name, "BackupStatus": "AVAILABLE"}}}, nil
	case "ListBackups":
		kvs, _, _ := p.col(req, "backups").List(ctx, "", "", 0)
		var sums []any
		for _, kv := range kvs {
			var m map[string]any
			_ = json.Unmarshal(kv.Value, &m)
			sums = append(sums, map[string]any{"BackupArn": m["BackupArn"], "BackupName": m["BackupName"], "BackupStatus": m["BackupStatus"], "TableName": m["TableName"]})
		}
		return &spi.Response{Output: map[string]any{"BackupSummaries": sums}}, nil
	case "DescribeBackup":
		id := backupID(first(req.Input, "BackupArn"))
		b, ok, _ := p.col(req, "backups").Get(ctx, id)
		if !ok {
			return nil, &spi.Fault{Code: "BackupNotFoundException", HTTPStatus: 400, Fault: "client"}
		}
		var m map[string]any
		_ = json.Unmarshal(b, &m)
		return &spi.Response{Output: map[string]any{"BackupDescription": map[string]any{"BackupDetails": m, "SourceTableDetails": map[string]any{"TableName": m["TableName"]}}}}, nil
	case "DeleteBackup":
		_ = p.col(req, "backups").Delete(ctx, backupID(first(req.Input, "BackupArn")))
		return &spi.Response{Output: map[string]any{"BackupDescription": map[string]any{"BackupDetails": map[string]any{"BackupStatus": "DELETED"}}}}, nil
	case "RestoreTableFromBackup":
		id := backupID(first(req.Input, "BackupArn"))
		b, ok, _ := p.col(req, "backups").Get(ctx, id)
		if !ok {
			return nil, &spi.Fault{Code: "BackupNotFoundException", HTTPStatus: 400, Fault: "client"}
		}
		var m map[string]any
		_ = json.Unmarshal(b, &m)
		dst := first(req.Input, "TargetTableName")
		if s := str(m["Table"]); s != "" {
			_ = p.col(req, "tables").Put(ctx, dst, []byte(s))
		}
		if items, ok := m["Items"].([]any); ok {
			for _, it := range items {
				im := asMap(it)
				if key := str(im["Key"]); key != "" {
					_ = p.col(req, "items:"+dst).Put(ctx, key, []byte(str(im["Value"])))
				}
			}
		}
		return &spi.Response{Output: map[string]any{"TableDescription": map[string]any{"TableName": dst, "TableStatus": "ACTIVE"}}}, nil
	case "EnableKinesisStreamingDestination":
		stream := first(req.Input, "StreamArn")
		_ = p.col(req, "kinesisdest").Put(ctx, table+"/"+stream, []byte(stream))
		return &spi.Response{Output: map[string]any{"TableName": table, "StreamArn": stream, "DestinationStatus": "ACTIVE"}}, nil
	case "DisableKinesisStreamingDestination":
		stream := first(req.Input, "StreamArn")
		_ = p.col(req, "kinesisdest").Delete(ctx, table+"/"+stream)
		return &spi.Response{Output: map[string]any{"TableName": table, "StreamArn": stream, "DestinationStatus": "DISABLED"}}, nil
	case "DescribeKinesisStreamingDestination":
		kvs, _, _ := p.col(req, "kinesisdest").List(ctx, table+"/", "", 0)
		var dest []any
		for _, kv := range kvs {
			dest = append(dest, map[string]any{"StreamArn": string(kv.Value), "DestinationStatus": "ACTIVE"})
		}
		return &spi.Response{Output: map[string]any{"TableName": table, "KinesisDataStreamDestinations": dest}}, nil
	case "BatchGetItem":
		out := map[string]any{}
		if ri, ok := req.Input["RequestItems"].(map[string]any); ok {
			for tbl, spec := range ri {
				if err := requireTable(tbl); err != nil {
					return nil, err
				}
				var items []any
				keys, _ := asMap(spec)["Keys"].([]any)
				for _, k := range keys {
					key := p.itemKeyFrom(ctx, req, tbl, asMap(k))
					b, ok, _ := p.col(req, "items:"+tbl).Get(ctx, key)
					if !ok {
						continue
					}
					var item map[string]any
					_ = json.Unmarshal(b, &item)
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
					m := asMap(r)
					if put := asMap(m["PutRequest"]); len(put) > 0 {
						item := asMap(put["Item"])
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
					if del := asMap(m["DeleteRequest"]); len(del) > 0 {
						dk := asMap(del["Key"])
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
		ri := map[string]any{}
		items, _ := req.Input["TransactItems"].([]any)
		for _, it := range items {
			g := asMap(asMap(it)["Get"])
			tbl := str(g["TableName"])
			if tbl == "" {
				continue
			}
			slot := asMap(ri[tbl])
			keys, _ := slot["Keys"].([]any)
			keys = append(keys, g["Key"])
			slot["Keys"] = keys
			ri[tbl] = slot
		}
		return p.Invoke(ctx, &spi.Request{Identity: req.Identity, HTTP: req.HTTP, Operation: "BatchGetItem", Input: map[string]any{"RequestItems": ri}})
	case "TransactWriteItems":
		ri := map[string]any{}
		items, _ := req.Input["TransactItems"].([]any)
		for _, it := range items {
			m := asMap(it)
			if put := asMap(m["Put"]); len(put) > 0 {
				tbl := str(put["TableName"])
				reqs, _ := ri[tbl].([]any)
				ri[tbl] = append(reqs, map[string]any{"PutRequest": map[string]any{"Item": put["Item"]}})
			}
			if del := asMap(m["Delete"]); len(del) > 0 {
				tbl := str(del["TableName"])
				reqs, _ := ri[tbl].([]any)
				ri[tbl] = append(reqs, map[string]any{"DeleteRequest": map[string]any{"Key": del["Key"]}})
			}
		}
		return p.Invoke(ctx, &spi.Request{Identity: req.Identity, HTTP: req.HTTP, Operation: "BatchWriteItem", Input: map[string]any{"RequestItems": ri}})
	case "UpdateTable":
		b, ok, _ := p.col(req, "tables").Get(ctx, table)
		m := map[string]any{"TableName": table}
		if ok {
			_ = json.Unmarshal(b, &m)
		}
		if gsi := req.Input["GlobalSecondaryIndexUpdates"]; gsi != nil {
			m["GlobalSecondaryIndexUpdates"] = gsi
			indexes, _ := m["GlobalSecondaryIndexes"].([]any)
			if ups, ok := gsi.([]any); ok {
				for _, u := range ups {
					um := asMap(u)
					if cr := asMap(um["Create"]); len(cr) > 0 {
						indexes = append(indexes, cr)
					}
					if del := asMap(um["Delete"]); len(del) > 0 {
						name := str(del["IndexName"])
						var keep []any
						for _, ix := range indexes {
							if str(asMap(ix)["IndexName"]) != name {
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
		nb, _ := json.Marshal(m)
		_ = p.col(req, "tables").Put(ctx, table, nb)
		return &spi.Response{Output: map[string]any{"TableDescription": m}}, nil
	case "TagResource":
		arn := str(req.Input["ResourceArn"])
		var tags []any
		if b, ok, _ := p.col(req, "tags").Get(ctx, arn); ok {
			_ = json.Unmarshal(b, &tags)
		}
		indexes := map[string]int{}
		for i, tag := range tags {
			indexes[str(asMap(tag)["Key"])] = i
		}
		for _, tag := range asSlice(req.Input["Tags"]) {
			key := str(asMap(tag)["Key"])
			if i, ok := indexes[key]; ok {
				tags[i] = tag
			} else {
				indexes[key] = len(tags)
				tags = append(tags, tag)
			}
		}
		b, _ := json.Marshal(tags)
		_ = p.col(req, "tags").Put(ctx, arn, b)
		return &spi.Response{Output: map[string]any{}}, nil
	case "UntagResource":
		arn := str(req.Input["ResourceArn"])
		var tags []any
		if b, ok, _ := p.col(req, "tags").Get(ctx, arn); ok {
			_ = json.Unmarshal(b, &tags)
		}
		drop := map[string]bool{}
		for _, key := range asSlice(req.Input["TagKeys"]) {
			drop[str(key)] = true
		}
		kept := tags[:0]
		for _, tag := range tags {
			if !drop[str(asMap(tag)["Key"])] {
				kept = append(kept, tag)
			}
		}
		b, _ := json.Marshal(kept)
		_ = p.col(req, "tags").Put(ctx, arn, b)
		return &spi.Response{Output: map[string]any{}}, nil
	case "ListTagsOfResource":
		b, ok, _ := p.col(req, "tags").Get(ctx, str(req.Input["ResourceArn"]))
		var tags any = []any{}
		if ok {
			_ = json.Unmarshal(b, &tags)
		}
		return &spi.Response{Output: map[string]any{"Tags": tags}}, nil
	case "ExecuteStatement", "BatchExecuteStatement", "ExecuteTransaction":
		return p.partiql(ctx, req)
	case "CreateGlobalTable", "DescribeGlobalTable", "ListGlobalTables", "UpdateGlobalTable",
		"DescribeGlobalTableSettings", "UpdateGlobalTableSettings":
		return p.globalTable(ctx, req)
	case "UpdateContributorInsights", "DescribeContributorInsights", "ListContributorInsights":
		return p.insights(ctx, req)
	case "ExportTableToPointInTime", "DescribeExport", "ListExports":
		return p.exports(ctx, req)
	case "ImportTable", "DescribeImport", "ListImports":
		return p.imports(ctx, req)
	case "RestoreTableToPointInTime":
		return p.restorePITR(ctx, req)
	case "DescribeTableReplicaAutoScaling", "UpdateTableReplicaAutoScaling":
		return p.replicaScaling(ctx, req)
	case "SearchVectors":
		return p.searchVectors(ctx, req)
	case "UpdateKinesisStreamingDestination":
		return p.updateKinesisDest(ctx, req)
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
	for _, key := range asSlice(p.tableDef(ctx, req, table)["KeySchema"]) {
		name := str(asMap(key)["AttributeName"])
		if name != "" && len(asMap(item[name])) == 0 {
			return &spi.Fault{Code: "ValidationException", Message: "One or more parameter values were invalid: Missing the key " + name + " in the item", HTTPStatus: 400, Fault: "client"}
		}
	}
	return nil
}

func (p *Pack) listItems(ctx context.Context, req *spi.Request, table, keyCond, filter string) (*spi.Response, error) {
	td := p.tableDef(ctx, req, table)
	kvs, _, _ := p.col(req, "items:"+table).List(ctx, "", "", 0)
	names := asMap(req.Input["ExpressionAttributeNames"])
	values := asMap(req.Input["ExpressionAttributeValues"])
	var matched []map[string]any
	for _, kv := range kvs {
		var item map[string]any
		_ = json.Unmarshal(kv.Value, &item)
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
		matched = append(matched, p.projectIndex(td, str(req.Input["IndexName"]), item))
	}
	sort.Slice(matched, func(i, j int) bool {
		return itemKey(matched[i]) < itemKey(matched[j])
	})
	start := asMap(req.Input["ExclusiveStartKey"])
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
	proj := str(req.Input["ProjectionExpression"])
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

func asMap(v any) map[string]any {
	m, _ := v.(map[string]any)
	if m == nil {
		return map[string]any{}
	}
	return m
}

func str(v any) string { s, _ := v.(string); return s }

func first(in map[string]any, keys ...string) string {
	for _, k := range keys {
		if s := str(in[k]); s != "" {
			return s
		}
	}
	return ""
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

func backupID(arn string) string {
	if i := strings.LastIndex(arn, "/"); i >= 0 {
		return arn[i+1:]
	}
	return arn
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
	var o map[string]any
	_ = json.Unmarshal(b, &o)
	return o
}

func (p *Pack) loadItem(ctx context.Context, req *spi.Request, table, key string) map[string]any {
	b, ok, _ := p.col(req, "items:"+table).Get(ctx, key)
	if !ok {
		return nil
	}
	var item map[string]any
	_ = json.Unmarshal(b, &item)
	return item
}

func (p *Pack) checkCond(req *spi.Request, item map[string]any) error {
	cond := str(req.Input["ConditionExpression"])
	if cond == "" {
		return nil
	}
	if item == nil {
		item = map[string]any{}
	}
	ok, err := expr.EvalBool(cond, item, asMap(req.Input["ExpressionAttributeNames"]), asMap(req.Input["ExpressionAttributeValues"]))
	if err != nil {
		return &spi.Fault{Code: "ValidationException", Message: err.Error(), HTTPStatus: 400, Fault: "client"}
	}
	if !ok {
		return &spi.Fault{Code: "ConditionalCheckFailedException", Message: "The conditional request failed", HTTPStatus: 400, Fault: "client", Fields: map[string]any{"Item": item}}
	}
	return nil
}

func (p *Pack) returnValues(req *spi.Request, old, neu map[string]any, touched []string) *spi.Response {
	mode := str(req.Input["ReturnValues"])
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
	var m map[string]any
	_ = json.Unmarshal(b, &m)
	return m
}

func (p *Pack) tableKey(td, item map[string]any) map[string]any {
	out := map[string]any{}
	ks, _ := td["KeySchema"].([]any)
	for _, e := range ks {
		name := str(asMap(e)["AttributeName"])
		if name != "" {
			out[name] = item[name]
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
	proj := asMap(spec["Projection"])
	pt := str(proj["ProjectionType"])
	if pt == "" || pt == "ALL" {
		return item
	}
	keep := map[string]bool{}
	for _, e := range append(asSlice(td["KeySchema"]), asSlice(spec["KeySchema"])...) {
		if n := str(asMap(e)["AttributeName"]); n != "" {
			keep[n] = true
		}
	}
	if pt == "INCLUDE" {
		for _, a := range asSlice(proj["NonKeyAttributes"]) {
			keep[str(a)] = true
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
		for _, ix := range asSlice(td[key]) {
			m := asMap(ix)
			if str(m["IndexName"]) == indexName {
				return m
			}
		}
	}
	return nil
}

func asSlice(v any) []any {
	s, _ := v.([]any)
	return s
}

func itemKey(m map[string]any) string {
	b, _ := json.Marshal(m)
	return string(b)
}

func (p *Pack) itemKeyFrom(ctx context.Context, req *spi.Request, table string, attrs map[string]any) string {
	b, ok, _ := p.col(req, "tables").Get(ctx, table)
	if ok {
		var td map[string]any
		_ = json.Unmarshal(b, &td)
		if ks, ok := td["KeySchema"].([]any); ok && len(ks) > 0 {
			km := map[string]any{}
			for _, e := range ks {
				name := str(asMap(e)["AttributeName"])
				if name != "" {
					km[name] = attrs[name]
				}
			}
			return itemKey(km)
		}
	}
	return itemKey(attrs)
}
