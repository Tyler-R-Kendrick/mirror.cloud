package dynamodb

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/kms"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spitest"
)

func FuzzTableLifecycle(f *testing.F) {
	f.Add([]byte("table"))
	f.Fuzz(func(t *testing.T, raw []byte) {
		p := New(spitest.Deps(t))
		ctx := context.Background()
		id := spi.Identity{Account: "000000000000", Region: "us-east-1"}
		name := hex.EncodeToString(raw)
		call := func(operation string, input map[string]any) (*spi.Response, error) {
			return p.Invoke(ctx, &spi.Request{Identity: id, Operation: operation, Input: input})
		}
		table := map[string]any{"TableName": name, "KeySchema": []any{map[string]any{"AttributeName": "id", "KeyType": "HASH"}}, "GlobalSecondaryIndexes": []any{map[string]any{"IndexName": "keys", "KeySchema": []any{map[string]any{"AttributeName": "id", "KeyType": "HASH"}}, "Projection": map[string]any{"ProjectionType": "KEYS_ONLY"}}}, "Tags": []any{map[string]any{"Key": "fuzz", "Value": name}}}
		_, created := call("CreateTable", table)
		_, duplicate := call("CreateTable", table)
		arn := "arn:aws:dynamodb:us-east-1:000000000000:table/" + name
		listed, tagsErr := call("ListTagsOfResource", map[string]any{"ResourceArn": arn})
		value := strings.ToValidUTF8(string(raw), "�")
		item := map[string]any{"id": map[string]any{"S": "item"}, "data": map[string]any{"S": value}}
		firstReturn, firstPut := call("PutItem", map[string]any{"TableName": name, "Item": item, "ReturnValues": "ALL_OLD"})
		secondReturn, putErr := call("PutItem", map[string]any{"TableName": name, "Item": item, "ReturnValues": "ALL_OLD"})
		got, getErr := call("GetItem", map[string]any{"TableName": name, "Key": map[string]any{"id": map[string]any{"S": "item"}}})
		_, invalidProjection := call("Query", map[string]any{"TableName": name, "IndexName": "keys", "Select": "ALL_ATTRIBUTES"})
		_, ttlEnable := call("UpdateTimeToLive", map[string]any{"TableName": name, "TimeToLiveSpecification": map[string]any{"Enabled": true, "AttributeName": "ttl"}})
		_, expiredPut := call("PutItem", map[string]any{"TableName": name, "Item": map[string]any{"id": map[string]any{"S": "expired"}, "ttl": map[string]any{"N": "-1"}}})
		expiration, expirationErr := call("ExpireItems", nil)
		expiredItem, expiredGet := call("GetItem", map[string]any{"TableName": name, "Key": map[string]any{"id": map[string]any{"S": "expired"}}})
		_, deleted := call("DeleteTable", table)
		_, missing := call("DeleteTable", table)
		_, missingQuery := call("Query", map[string]any{"TableName": name})
		_, missingTransaction := call("TransactWriteItems", map[string]any{"TransactItems": []any{map[string]any{"Put": map[string]any{"TableName": name, "Item": map[string]any{}}}}})
		_, ttlDescribe := call("DescribeTimeToLive", table)
		_, ttlUpdate := call("UpdateTimeToLive", table)
		tags := asSlice(listed.Output["Tags"])
		if created != nil || duplicate == nil || tagsErr != nil || len(tags) != 1 || str(asMap(tags[0])["Value"]) != name || firstPut != nil || firstReturn.Output["Attributes"] != nil || putErr != nil || secondReturn.Output["Attributes"] == nil || getErr != nil || str(asMap(asMap(got.Output["Item"])["data"])["S"]) != value || invalidProjection == nil || ttlEnable != nil || expiredPut != nil || expirationErr != nil || expiration.Output["ExpiredItems"] != 1 || expiredGet != nil || expiredItem.Output["Item"] != nil || deleted != nil || missing == nil || missingQuery == nil || missingTransaction == nil || ttlDescribe == nil || ttlUpdate == nil {
			t.Fatal("table lifecycle was not create/tag/conflict/delete/missing")
		}
	})
}

func FuzzDynamoDBBinaryValues(f *testing.F) {
	f.Add([]byte{0x90})
	f.Add([]byte("test \xc0 \xed"))
	f.Fuzz(func(t *testing.T, raw []byte) {
		if len(raw) > 1024 {
			t.Skip()
		}
		p := New(spitest.Deps(t))
		ctx := context.Background()
		id := spi.Identity{Account: "000000000000", Region: "us-east-1"}
		call := func(operation string, input map[string]any) (*spi.Response, error) {
			return p.Invoke(ctx, &spi.Request{Identity: id, Operation: operation, Input: input})
		}
		_, _ = call("CreateTable", map[string]any{"TableName": "T", "KeySchema": []any{map[string]any{"AttributeName": "id", "KeyType": "HASH"}}})
		encoded := base64.StdEncoding.EncodeToString(raw)
		_, putErr := call("PutItem", map[string]any{"TableName": "T", "Item": map[string]any{"id": map[string]any{"S": "one"}, "data": map[string]any{"B": encoded}}})
		batch, batchErr := call("BatchWriteItem", map[string]any{"RequestItems": map[string]any{"T": []any{map[string]any{"PutRequest": map[string]any{"Item": map[string]any{"id": map[string]any{"S": "two"}, "data": map[string]any{"B": encoded}}}}}}})
		one, oneErr := call("GetItem", map[string]any{"TableName": "T", "Key": map[string]any{"id": map[string]any{"S": "one"}}})
		two, twoErr := call("GetItem", map[string]any{"TableName": "T", "Key": map[string]any{"id": map[string]any{"S": "two"}}})
		gotBatch, getBatchErr := call("BatchGetItem", map[string]any{"RequestItems": map[string]any{"T": map[string]any{"Keys": []any{
			map[string]any{"id": map[string]any{"S": "one"}}, map[string]any{"id": map[string]any{"S": "two"}}, map[string]any{"id": map[string]any{"S": "missing"}},
		}}}})
		changed, changeErr := call("BatchWriteItem", map[string]any{"RequestItems": map[string]any{"T": []any{
			map[string]any{"DeleteRequest": map[string]any{"Key": map[string]any{"id": map[string]any{"S": "one"}}}},
			map[string]any{"PutRequest": map[string]any{"Item": map[string]any{"id": map[string]any{"S": "three"}, "data": map[string]any{"B": encoded}}}},
		}}})
		deleted, deletedErr := call("GetItem", map[string]any{"TableName": "T", "Key": map[string]any{"id": map[string]any{"S": "one"}}})
		three, threeErr := call("GetItem", map[string]any{"TableName": "T", "Key": map[string]any{"id": map[string]any{"S": "three"}}})
		if putErr != nil || batchErr != nil || oneErr != nil || twoErr != nil || getBatchErr != nil || changeErr != nil || deletedErr != nil || threeErr != nil || len(asMap(batch.Output["UnprocessedItems"])) != 0 || len(asMap(changed.Output["UnprocessedItems"])) != 0 || len(asSlice(asMap(gotBatch.Output["Responses"])["T"])) != 2 || len(asMap(gotBatch.Output["UnprocessedKeys"])) != 0 || deleted.Output["Item"] != nil || str(asMap(asMap(one.Output["Item"])["data"])["B"]) != encoded || str(asMap(asMap(two.Output["Item"])["data"])["B"]) != encoded || str(asMap(asMap(three.Output["Item"])["data"])["B"]) != encoded {
			t.Fatal("binary values did not round trip")
		}
	})
}

func FuzzDynamoDBTableClass(f *testing.F) {
	f.Add(false)
	f.Fuzz(func(t *testing.T, infrequent bool) {
		p := New(spitest.Deps(t))
		ctx := context.Background()
		id := spi.Identity{Account: "000000000000", Region: "us-east-1"}
		call := func(operation string, input map[string]any) (*spi.Response, error) {
			return p.Invoke(ctx, &spi.Request{Identity: id, Operation: operation, Input: input})
		}
		_, createErr := call("CreateTable", map[string]any{"TableName": "T", "KeySchema": []any{map[string]any{"AttributeName": "id", "KeyType": "HASH"}}, "TableClass": "STANDARD"})
		class := "STANDARD"
		if infrequent {
			class = "STANDARD_INFREQUENT_ACCESS"
		}
		updated, updateErr := call("UpdateTable", map[string]any{"TableName": "T", "TableClass": class})
		described, describeErr := call("DescribeTable", map[string]any{"TableName": "T"})
		if createErr != nil || updateErr != nil || describeErr != nil || str(asMap(asMap(updated.Output["TableDescription"])["TableClassSummary"])["TableClass"]) != class || str(asMap(asMap(described.Output["Table"])["TableClassSummary"])["TableClass"]) != class {
			t.Fatal("table class did not persist")
		}
	})
}

func FuzzDynamoDBTableMetadata(f *testing.F) {
	f.Add(uint64(1000), uint64(1200), true, []byte("key"))
	f.Fuzz(func(t *testing.T, reads, writes uint64, onDemand bool, rawKey []byte) {
		p := New(spitest.Deps(t))
		ctx := context.Background()
		id := spi.Identity{Account: "000000000000", Region: "us-east-1"}
		call := func(operation string, input map[string]any) (*spi.Response, error) {
			return p.Invoke(ctx, &spi.Request{Identity: id, Operation: operation, Input: input})
		}
		reads = reads%100000 + 1
		writes = writes%100000 + 1
		key := hex.EncodeToString(rawKey)
		if key == "" {
			key = "0"
		}
		input := map[string]any{
			"TableName": "T", "KeySchema": []any{map[string]any{"AttributeName": "id", "KeyType": "HASH"}},
			"GlobalSecondaryIndexes": []any{map[string]any{"IndexName": "by-value", "ProvisionedThroughput": map[string]any{"ReadCapacityUnits": 1, "WriteCapacityUnits": 1}}},
			"SSESpecification":       map[string]any{"Enabled": true, "KMSMasterKeyId": key},
			"WarmThroughput":         map[string]any{"ReadUnitsPerSecond": reads, "WriteUnitsPerSecond": writes},
		}
		if onDemand {
			input["BillingMode"] = "PAY_PER_REQUEST"
			delete(asMap(asSlice(input["GlobalSecondaryIndexes"])[0]), "ProvisionedThroughput")
		} else {
			input["ProvisionedThroughput"] = map[string]any{"ReadCapacityUnits": 5, "WriteCapacityUnits": 5}
		}
		created, createErr := call("CreateTable", input)
		described, describeErr := call("DescribeTable", map[string]any{"TableName": "T"})
		_, invalidErr := call("CreateTable", map[string]any{"TableName": "Invalid", "BillingMode": "PAY_PER_REQUEST", "ProvisionedThroughput": map[string]any{"ReadCapacityUnits": 1, "WriteCapacityUnits": 1}})
		if createErr != nil || describeErr != nil || invalidErr == nil {
			t.Fatalf("metadata calls: create=%v describe=%v invalid=%v", createErr, describeErr, invalidErr)
		}
		createdTable := asMap(created.Output["TableDescription"])
		describedTable := asMap(described.Output["Table"])
		if createdTable["TableStatus"] != "CREATING" || describedTable["TableStatus"] != "ACTIVE" || asInt(asMap(describedTable["WarmThroughput"])["ReadUnitsPerSecond"]) != int(reads) || asInt(asMap(describedTable["WarmThroughput"])["WriteUnitsPerSecond"]) != int(writes) || asMap(describedTable["WarmThroughput"])["Status"] != "ACTIVE" || str(asMap(describedTable["SSEDescription"])["KMSMasterKeyArn"]) != "arn:aws:kms:us-east-1:000000000000:key/"+key || len(asSlice(describedTable["GlobalSecondaryIndexes"])) != 1 {
			t.Fatalf("table metadata did not round trip: %#v %#v", createdTable, describedTable)
		}
		throughput := asMap(describedTable["ProvisionedThroughput"])
		if onDemand && (str(asMap(describedTable["BillingModeSummary"])["BillingMode"]) != "PAY_PER_REQUEST" || asInt(throughput["ReadCapacityUnits"]) != 0) || !onDemand && asInt(throughput["ReadCapacityUnits"]) != 5 {
			t.Fatalf("billing metadata did not round trip: %#v", describedTable)
		}
	})
}

func FuzzDynamoDBDefaultSSE(f *testing.F) {
	f.Add([]byte("table"), true)
	f.Fuzz(func(t *testing.T, raw []byte, disable bool) {
		if len(raw) > 128 {
			t.Skip()
		}
		deps := spitest.Deps(t)
		p := New(deps)
		ctx := context.Background()
		id := spi.Identity{Account: "000000000000", Region: "us-east-1"}
		call := func(operation string, input map[string]any) (*spi.Response, error) {
			return p.Invoke(ctx, &spi.Request{Identity: id, Operation: operation, Input: input})
		}
		name := hex.EncodeToString(raw)
		first, firstErr := call("CreateTable", map[string]any{"TableName": "A" + name, "SSESpecification": map[string]any{"Enabled": true}})
		second, secondErr := call("CreateTable", map[string]any{"TableName": "B" + name, "SSESpecification": map[string]any{"Enabled": true}})
		if firstErr != nil || secondErr != nil {
			t.Fatalf("default SSE create: %v %v", firstErr, secondErr)
		}
		firstARN := str(asMap(asMap(first.Output["TableDescription"])["SSEDescription"])["KMSMasterKeyArn"])
		secondARN := str(asMap(asMap(second.Output["TableDescription"])["SSEDescription"])["KMSMasterKeyArn"])
		if disable {
			disabled, err := call("UpdateTable", map[string]any{"TableName": "A" + name, "SSESpecification": map[string]any{"Enabled": false}})
			if err != nil {
				t.Fatal(err)
			}
			if asMap(asMap(disabled.Output["TableDescription"])["SSEDescription"])["Status"] != "UPDATING" {
				t.Fatalf("default SSE disable: %#v", disabled)
			}
		}
		updated, updateErr := call("UpdateTable", map[string]any{"TableName": "A" + name, "BillingMode": "PAY_PER_REQUEST"})
		key, keyErr := kms.New(deps).Invoke(ctx, &spi.Request{Identity: id, Operation: "DescribeKey", Input: map[string]any{"KeyId": firstARN}})
		if updateErr != nil || keyErr != nil {
			t.Fatalf("default SSE did not persist: first=%q second=%q update=%#v key=%#v errors=%v/%v", firstARN, secondARN, updated, key, updateErr, keyErr)
		}
		if firstARN == "" || firstARN != secondARN || str(asMap(asMap(updated.Output["TableDescription"])["SSEDescription"])["KMSMasterKeyArn"]) != firstARN || asMap(key.Output["KeyMetadata"])["KeyManager"] != "AWS" {
			t.Fatalf("default SSE did not persist: first=%q second=%q update=%#v key=%#v", firstARN, secondARN, updated, key)
		}
	})
}

func FuzzDynamoDBBackupInsights(f *testing.F) {
	f.Add(uint16(60), true)
	f.Fuzz(func(t *testing.T, seconds uint16, enabled bool) {
		deps := spitest.Deps(t)
		p := New(deps)
		ctx := context.Background()
		id := spi.Identity{Account: "000000000000", Region: "us-east-1"}
		call := func(operation string, input map[string]any) (*spi.Response, error) {
			return p.Invoke(ctx, &spi.Request{Identity: id, Operation: operation, Input: input})
		}
		if _, err := call("CreateTable", map[string]any{"TableName": "T"}); err != nil {
			t.Fatal(err)
		}
		updated, updateErr := call("UpdateContinuousBackups", map[string]any{"TableName": "T", "PointInTimeRecoverySpecification": map[string]any{"PointInTimeRecoveryEnabled": enabled}})
		if err := deps.Clock.Advance(time.Duration(seconds) * time.Second); err != nil {
			t.Fatal(err)
		}
		described, describeErr := call("DescribeContinuousBackups", map[string]any{"TableName": "T"})
		insights, insightsErr := call("DescribeContributorInsights", map[string]any{"TableName": "T"})
		if updateErr != nil || describeErr != nil || insightsErr != nil {
			t.Fatalf("backup calls: update=%v describe=%v insights=%v", updateErr, describeErr, insightsErr)
		}
		updatedRecovery := asMap(asMap(updated.Output["ContinuousBackupsDescription"])["PointInTimeRecoveryDescription"])
		describedRecovery := asMap(asMap(described.Output["ContinuousBackupsDescription"])["PointInTimeRecoveryDescription"])
		wantStatus := "DISABLED"
		if enabled {
			wantStatus = "ENABLED"
			if asInt(updatedRecovery["RecoveryPeriodInDays"]) != 35 || asInt(describedRecovery["EarliestRestorableDateTime"]) != 0 || asInt(describedRecovery["LatestRestorableDateTime"]) != int(seconds) {
				t.Fatalf("recovery window %#v %#v", updatedRecovery, describedRecovery)
			}
		} else if describedRecovery["EarliestRestorableDateTime"] != nil || describedRecovery["LatestRestorableDateTime"] != nil {
			t.Fatalf("disabled recovery exposed dates %#v", describedRecovery)
		}
		if updatedRecovery["PointInTimeRecoveryStatus"] != wantStatus || describedRecovery["PointInTimeRecoveryStatus"] != wantStatus || insights.Output["ContributorInsightsStatus"] != "DISABLED" {
			t.Fatalf("backup or insights status: update=%#v describe=%#v insights=%#v", updated.Output, described.Output, insights.Output)
		}
	})
}

func FuzzDynamoDBPartiQL(f *testing.F) {
	f.Add(int64(20), true)
	f.Fuzz(func(t *testing.T, age int64, hasName bool) {
		p := New(spitest.Deps(t))
		ctx := context.Background()
		id := spi.Identity{Account: "000000000000", Region: "us-east-1"}
		call := func(operation string, input map[string]any) (*spi.Response, error) {
			return p.Invoke(ctx, &spi.Request{Identity: id, Operation: operation, Input: input})
		}
		_, _ = call("CreateTable", map[string]any{"TableName": "T", "KeySchema": []any{map[string]any{"AttributeName": "Username", "KeyType": "HASH"}}})
		item := map[string]any{"Username": map[string]any{"S": "user"}}
		if hasName {
			item["FirstName"] = map[string]any{"S": "Alice"}
		}
		_, _ = call("PutItem", map[string]any{"TableName": "T", "Item": item})
		value := strconv.FormatInt(age, 10)
		batch, batchErr := call("BatchExecuteStatement", map[string]any{"Statements": []any{map[string]any{"Statement": "UPDATE T SET Age=" + value + " WHERE Username='user'"}}})
		got, getErr := call("GetItem", map[string]any{"TableName": "T", "Key": map[string]any{"Username": map[string]any{"S": "user"}}})
		present, presentErr := call("ExecuteStatement", map[string]any{"Statement": "SELECT * FROM T WHERE FirstName IS NOT MISSING"})
		missing, missingErr := call("ExecuteStatement", map[string]any{"Statement": "SELECT * FROM T WHERE FirstName IS MISSING"})
		_, emptyErr := call("ExecuteStatement", map[string]any{"Statement": "SELECT * FROM T", "Parameters": []any{}})
		var responses, presentItems, missingItems []any
		ageValue := ""
		if batch != nil {
			responses = asSlice(batch.Output["Responses"])
		}
		if got != nil {
			ageValue = str(asMap(asMap(got.Output["Item"])["Age"])["N"])
		}
		if present != nil {
			presentItems = asSlice(present.Output["Items"])
		}
		if missing != nil {
			missingItems = asSlice(missing.Output["Items"])
		}
		if batchErr != nil || getErr != nil || presentErr != nil || missingErr != nil || emptyErr == nil || len(responses) != 1 || str(asMap(responses[0])["TableName"]) != "T" || ageValue != value || (len(presentItems) == 1) != hasName || (len(missingItems) == 1) == hasName {
			t.Fatal("PartiQL update, missing predicate, or validation changed")
		}
	})
}

func FuzzDynamoDBStreamRecords(f *testing.F) {
	f.Add([]byte{0x90}, false)
	f.Fuzz(func(t *testing.T, raw []byte, keysOnly bool) {
		if len(raw) > 1024 {
			t.Skip()
		}
		p := New(spitest.Deps(t))
		ctx := context.Background()
		id := spi.Identity{Account: "000000000000", Region: "us-east-1"}
		call := func(operation string, input map[string]any) (*spi.Response, error) {
			return p.Invoke(ctx, &spi.Request{Identity: id, Operation: operation, Input: input})
		}
		view := "NEW_IMAGE"
		if keysOnly {
			view = "KEYS_ONLY"
		}
		created, createErr := call("CreateTable", map[string]any{"TableName": "T", "KeySchema": []any{map[string]any{"AttributeName": "id", "KeyType": "HASH"}}, "StreamSpecification": map[string]any{"StreamEnabled": true, "StreamViewType": view}})
		if createErr != nil {
			t.Fatal(createErr)
		}
		item := map[string]any{"id": map[string]any{"S": "one"}, "data": map[string]any{"B": base64.StdEncoding.EncodeToString(raw)}}
		_, firstErr := call("PutItem", map[string]any{"TableName": "T", "Item": item})
		_, duplicateErr := call("PutItem", map[string]any{"TableName": "T", "Item": item})
		arn := str(asMap(created.Output["TableDescription"])["LatestStreamArn"])
		iterator, iteratorErr := call("GetShardIterator", map[string]any{"StreamArn": arn, "ShardId": "shardId-000000000000", "ShardIteratorType": "TRIM_HORIZON"})
		if iteratorErr != nil {
			t.Fatal(iteratorErr)
		}
		stream, recordsErr := call("GetRecords", map[string]any{"ShardIterator": iterator.Output["ShardIterator"]})
		if recordsErr != nil {
			t.Fatal(recordsErr)
		}
		records := asSlice(stream.Output["Records"])
		if firstErr != nil || duplicateErr != nil || len(records) != 1 {
			t.Fatalf("stream writes: %#v %v %v", stream, firstErr, duplicateErr)
		}
		dynamodb := asMap(asMap(records[0])["dynamodb"])
		wantSize := 5
		if !keysOnly {
			wantSize += 9 + len(raw)
			if str(asMap(asMap(dynamodb["NewImage"])["data"])["B"]) != base64.StdEncoding.EncodeToString(raw) {
				t.Fatal("binary stream value changed")
			}
		}
		if asInt(dynamodb["SizeBytes"]) != wantSize || dynamodb["StreamViewType"] != view || !strings.HasPrefix(str(stream.Output["NextShardIterator"]), arn+"|") {
			t.Fatalf("stream record metadata: %#v", dynamodb)
		}
	})
}

func FuzzDynamoDBTransactions(f *testing.F) {
	f.Add([]byte{0x90}, false)
	f.Add([]byte("transaction"), true)
	f.Fuzz(func(t *testing.T, raw []byte, cancel bool) {
		if len(raw) > 1024 {
			t.Skip()
		}
		p := New(spitest.Deps(t))
		ctx := context.Background()
		id := spi.Identity{Account: "000000000000", Region: "us-east-1"}
		call := func(operation string, input map[string]any) (*spi.Response, error) {
			return p.Invoke(ctx, &spi.Request{Identity: id, Operation: operation, Input: input})
		}
		_, _ = call("CreateTable", map[string]any{"TableName": "T", "KeySchema": []any{map[string]any{"AttributeName": "id", "KeyType": "HASH"}}})
		_, _ = call("PutItem", map[string]any{"TableName": "T", "Item": map[string]any{"id": map[string]any{"S": "lock"}}})
		guard := "missing"
		if cancel {
			guard = "lock"
		}
		encoded := base64.StdEncoding.EncodeToString(raw)
		input := map[string]any{"ClientRequestToken": "fuzz-token", "TransactItems": []any{
			map[string]any{"ConditionCheck": map[string]any{"TableName": "T", "Key": map[string]any{"id": map[string]any{"S": guard}}, "ConditionExpression": "attribute_not_exists(id)"}},
			map[string]any{"Put": map[string]any{"TableName": "T", "Item": map[string]any{"id": map[string]any{"S": "item"}, "data": map[string]any{"B": encoded}}}},
		}}
		_, firstErr := call("TransactWriteItems", input)
		_, replayErr := call("TransactWriteItems", input)
		got, getErr := call("GetItem", map[string]any{"TableName": "T", "Key": map[string]any{"id": map[string]any{"S": "item"}}})
		item := asMap(got.Output["Item"])
		if cancel {
			if firstErr == nil || replayErr == nil || getErr != nil || got.Output["Item"] != nil {
				t.Fatal("canceled transaction committed")
			}
		} else if firstErr != nil || replayErr != nil || getErr != nil || str(asMap(item["data"])["B"]) != encoded {
			t.Fatal("transaction or idempotent replay changed binary data")
		}
	})
}
