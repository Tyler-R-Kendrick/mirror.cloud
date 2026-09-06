package dynamodb

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/config"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/golden"
	rtpkg "github.com/tyler-r-kendrick/mirror.cloud/internal/runtime"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/kinesis"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spitest"
)

func TestDynamoDBStreamOpsCount(t *testing.T) {
	p := New(spitest.Deps(t))
	if n := len(p.Operations()); n != 62 {
		t.Fatalf("dynamodb Operations() %d want 62", n)
	}
}

func TestDynamoDBStreamPublishesRecords(t *testing.T) {
	id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
	deps := spitest.Deps(t)
	p := New(deps)
	invoke := func(operation string, input map[string]any) {
		t.Helper()
		if _, err := p.Invoke(context.Background(), &spi.Request{Identity: id, Operation: operation, Input: input}); err != nil {
			t.Fatal(err)
		}
	}
	invoke("CreateTable", map[string]any{
		"TableName": "T", "KeySchema": []any{map[string]any{"AttributeName": "id", "KeyType": "HASH"}}, "StreamSpecification": map[string]any{"StreamEnabled": true, "StreamViewType": "NEW_IMAGE"},
	})
	published := 0
	cancel := deps.Bus.Subscribe("dynamodb-stream", func(context.Context, []byte) { published++ })
	defer cancel()
	invoke("PutItem", map[string]any{"TableName": "T", "Item": map[string]any{"id": map[string]any{"S": "1"}}})
	if published != 1 {
		t.Fatalf("published %d stream records", published)
	}
}

func TestDynamoDBStreamAttributeSizes(t *testing.T) {
	tests := []struct {
		name string
		attr map[string]any
		want int
	}{
		{"string", map[string]any{"S": "abc"}, 3},
		{"number", map[string]any{"N": "-12.5"}, 5},
		{"binary", map[string]any{"B": "kA=="}, 1},
		{"bool", map[string]any{"BOOL": true}, 1},
		{"null", map[string]any{"NULL": true}, 1},
		{"string set", map[string]any{"SS": []any{"a", "bc"}}, 3},
		{"number set", map[string]any{"NS": []any{"1", "22"}}, 3},
		{"binary set", map[string]any{"BS": []any{"kA==", "dGVzdA=="}}, 5},
		{"list", map[string]any{"L": []any{map[string]any{"S": "ab"}, map[string]any{"N": "1"}}}, 3},
		{"map", map[string]any{"M": map[string]any{"foo": map[string]any{"S": "x"}}}, 4},
		{"unknown", map[string]any{"X": "ignored"}, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := streamAttributeSize(tt.attr); got != tt.want {
				t.Fatalf("streamAttributeSize(%v) = %d, want %d", tt.attr, got, tt.want)
			}
		})
	}
}

func TestDynamoDBStreamCharacterization(t *testing.T) {
	p := New(spitest.Deps(t))
	ctx := context.Background()
	id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
	must := func(operation string, input map[string]any) map[string]any {
		response, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: operation, Input: input})
		if err != nil {
			t.Fatalf("%s: %v", operation, err)
		}
		return response.Output
	}
	created := must("CreateTable", map[string]any{
		"TableName": "T", "KeySchema": []any{map[string]any{"AttributeName": "Username", "KeyType": "HASH"}}, "StreamSpecification": map[string]any{"StreamEnabled": true, "StreamViewType": "KEYS_ONLY"},
	})
	arn := str(asMap(created["TableDescription"])["LatestStreamArn"])
	must("PutItem", map[string]any{"TableName": "T", "Item": map[string]any{"Username": map[string]any{"S": "Fred"}}})
	must("PutItem", map[string]any{"TableName": "T", "Item": map[string]any{"Username": map[string]any{"S": "Fred"}}})
	update := map[string]any{"TableName": "T", "Key": map[string]any{"Username": map[string]any{"S": "Fred"}}, "UpdateExpression": "SET S = :r", "ExpressionAttributeValues": map[string]any{":r": map[string]any{"S": "Fred_Modified"}}}
	must("UpdateItem", update)
	must("UpdateItem", update)
	must("DeleteItem", map[string]any{"TableName": "T", "Key": map[string]any{"Username": map[string]any{"S": "Fred"}}})
	must("ExecuteStatement", map[string]any{"Statement": "INSERT INTO T VALUE {'Username': 'Alice'}"})
	must("ExecuteStatement", map[string]any{"Statement": "UPDATE T SET partiql=1 WHERE Username='Alice'"})
	must("ExecuteStatement", map[string]any{"Statement": "DELETE FROM T WHERE Username='Alice'"})
	described := must("DescribeStream", map[string]any{"StreamArn": arn})
	shard := str(asMap(asSlice(asMap(described["StreamDescription"])["Shards"])[0])["ShardId"])
	excluded := must("DescribeStream", map[string]any{"StreamArn": arn, "ExclusiveStartShardId": shard})
	latest := str(must("GetShardIterator", map[string]any{"StreamArn": arn, "ShardId": shard, "ShardIteratorType": "LATEST"})["ShardIterator"])
	at := str(must("GetShardIterator", map[string]any{"StreamArn": arn, "ShardId": shard, "ShardIteratorType": "AT_SEQUENCE_NUMBER", "SequenceNumber": "1"})["ShardIterator"])
	trim := str(must("GetShardIterator", map[string]any{"StreamArn": arn, "ShardId": shard, "ShardIteratorType": "TRIM_HORIZON"})["ShardIterator"])
	records := asSlice(must("GetRecords", map[string]any{"ShardIterator": trim})["Records"])
	summaries := make([]any, 0, len(records))
	for _, raw := range records {
		record := asMap(raw)
		dynamodb := asMap(record["dynamodb"])
		summaries = append(summaries, map[string]any{"eventName": record["eventName"], "keys": dynamodb["Keys"], "size": dynamodb["SizeBytes"], "view": dynamodb["StreamViewType"]})
	}
	updated := must("CreateTable", map[string]any{
		"TableName": "U", "KeySchema": []any{map[string]any{"AttributeName": "pk", "KeyType": "HASH"}}, "StreamSpecification": map[string]any{"StreamEnabled": true, "StreamViewType": "NEW_AND_OLD_IMAGES"},
	})
	updateARN := str(asMap(updated["TableDescription"])["LatestStreamArn"])
	values := map[string]any{":v1": map[string]any{"S": "value1"}, ":v2": map[string]any{"S": "value2"}}
	updateInput := map[string]any{"TableName": "U", "Key": map[string]any{"pk": map[string]any{"S": "my-item-id"}}, "UpdateExpression": "SET attr1 = :v1, attr2 = :v2", "ExpressionAttributeValues": values}
	must("UpdateItem", updateInput)
	must("UpdateItem", updateInput)
	updateInput["ExpressionAttributeValues"] = map[string]any{":v1": map[string]any{"S": "value2"}, ":v2": map[string]any{"S": "value3"}}
	must("UpdateItem", updateInput)
	updateIterator := str(must("GetShardIterator", map[string]any{"StreamArn": updateARN, "ShardId": "shardId-000000000000", "ShardIteratorType": "TRIM_HORIZON"})["ShardIterator"])
	updateRecords := asSlice(must("GetRecords", map[string]any{"ShardIterator": updateIterator})["Records"])
	updateSummaries := make([]any, 0, len(updateRecords))
	for _, raw := range updateRecords {
		record := asMap(raw)
		dynamodb := asMap(record["dynamodb"])
		updateSummaries = append(updateSummaries, map[string]any{"eventName": record["eventName"], "keys": dynamodb["Keys"], "newImage": dynamodb["NewImage"], "oldImage": dynamodb["OldImage"], "sequenceNumber": dynamodb["SequenceNumber"], "size": dynamodb["SizeBytes"]})
	}
	golden.AssertJSON(t, map[string]any{
		"description":     map[string]any{"keySchema": asMap(described["StreamDescription"])["KeySchema"], "streamLabel": asMap(described["StreamDescription"])["StreamLabel"], "streamViewType": asMap(described["StreamDescription"])["StreamViewType"]},
		"exclusiveShards": asMap(excluded["StreamDescription"])["Shards"],
		"iteratorFormat":  strings.HasPrefix(latest, arn+"|") && strings.Count(latest, "|") == 2 && strings.HasPrefix(at, arn+"|1|") && strings.Count(at, "|") == 2,
		"records":         summaries,
		"updateRecords":   updateSummaries,
	})
}

func TestDynamoDBDataEncodingCharacterization(t *testing.T) {
	p := New(spitest.Deps(t))
	ctx := context.Background()
	id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
	must := func(operation string, input map[string]any) map[string]any {
		response, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: operation, Input: input})
		if err != nil {
			t.Fatalf("%s: %v", operation, err)
		}
		return response.Output
	}
	created := must("CreateTable", map[string]any{
		"TableName": "T", "KeySchema": []any{map[string]any{"AttributeName": "id", "KeyType": "HASH"}}, "StreamSpecification": map[string]any{"StreamEnabled": true, "StreamViewType": "NEW_AND_OLD_IMAGES"},
	})
	arn := str(asMap(created["TableDescription"])["LatestStreamArn"])
	must("PutItem", map[string]any{"TableName": "T", "Item": map[string]any{"id": map[string]any{"S": "id1"}, "version": map[string]any{"N": "1"}, "data": map[string]any{"B": "kA=="}}})
	firstItem := must("GetItem", map[string]any{"TableName": "T", "Key": map[string]any{"id": map[string]any{"S": "id1"}}})["Item"]
	iterator := must("GetShardIterator", map[string]any{"StreamArn": arn, "ShardId": "shardId-000000000000", "ShardIteratorType": "AT_SEQUENCE_NUMBER", "SequenceNumber": "1"})["ShardIterator"]
	firstRecords := asSlice(must("GetRecords", map[string]any{"ShardIterator": iterator})["Records"])
	must("UpdateItem", map[string]any{"TableName": "T", "Key": map[string]any{"id": map[string]any{"S": "id1"}}, "UpdateExpression": "SET version=:v", "ExpressionAttributeValues": map[string]any{":v": map[string]any{"N": "2"}}})
	updatedItem := must("GetItem", map[string]any{"TableName": "T", "Key": map[string]any{"id": map[string]any{"S": "id1"}}})["Item"]
	updatedRecords := asSlice(must("GetRecords", map[string]any{"ShardIterator": iterator})["Records"])
	golden.AssertJSON(t, map[string]any{
		"firstItem":       firstItem,
		"firstStream":     asMap(asMap(firstRecords[0])["dynamodb"])["NewImage"],
		"updatedItem":     updatedItem,
		"updatedStream":   asMap(asMap(updatedRecords[1])["dynamodb"])["NewImage"],
		"recordEventName": asMap(updatedRecords[1])["eventName"],
	})
}

func TestDynamoDBKinesisDestination(t *testing.T) {
	deps := spitest.Deps(t)
	p := New(deps)
	kinesisPack := kinesis.New(deps)
	ctx := context.Background()
	id := spi.Identity{Account: "000000000000", Region: "us-east-1"}
	call := func(operation string, input map[string]any) (*spi.Response, error) {
		return p.Invoke(ctx, &spi.Request{Identity: id, Operation: operation, Input: input})
	}
	must := func(operation string, input map[string]any) map[string]any {
		response, err := call(operation, input)
		if err != nil {
			t.Fatal(err)
		}
		return response.Output
	}
	if _, err := kinesisPack.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateStream", Input: map[string]any{"StreamName": "s"}}); err != nil {
		t.Fatal(err)
	}
	streamARN := "arn:aws:kinesis:us-east-1:000000000000:stream/s"
	must("CreateTable", map[string]any{"TableName": "T", "KeySchema": []any{map[string]any{"AttributeName": "id", "KeyType": "HASH"}}})
	if _, err := call("EnableKinesisStreamingDestination", map[string]any{"TableName": "missing", "StreamArn": streamARN}); err == nil {
		t.Fatal("enabled destination for missing table")
	}
	enabled := must("EnableKinesisStreamingDestination", map[string]any{"TableName": "T", "StreamArn": streamARN})
	if enabled["DestinationStatus"] != "ENABLING" || len(asMap(enabled["EnableKinesisStreamingConfiguration"])) != 0 {
		t.Fatalf("enable destination %#v", enabled)
	}
	described := must("DescribeKinesisStreamingDestination", map[string]any{"TableName": "T"})
	destination := asMap(asSlice(described["KinesisDataStreamDestinations"])[0])
	if destination["StreamArn"] != streamARN || destination["DestinationStatus"] != "ACTIVE" {
		t.Fatalf("describe destination %#v", described)
	}
	must("PutItem", map[string]any{"TableName": "T", "Item": map[string]any{"id": map[string]any{"S": "one"}, "data": map[string]any{"B": "kA=="}}})
	must("UpdateItem", map[string]any{"TableName": "T", "Key": map[string]any{"id": map[string]any{"S": "one"}}, "UpdateExpression": "SET value=:v", "ExpressionAttributeValues": map[string]any{":v": map[string]any{"S": "changed"}}})
	must("DeleteItem", map[string]any{"TableName": "T", "Key": map[string]any{"id": map[string]any{"S": "one"}}})
	iterator, err := kinesisPack.Invoke(ctx, &spi.Request{Identity: id, Operation: "GetShardIterator", Input: map[string]any{"StreamName": "s", "ShardIteratorType": "TRIM_HORIZON"}})
	if err != nil {
		t.Fatal(err)
	}
	recordResponse, err := kinesisPack.Invoke(ctx, &spi.Request{Identity: id, Operation: "GetRecords", Input: map[string]any{"ShardIterator": iterator.Output["ShardIterator"]}})
	if err != nil {
		t.Fatal(err)
	}
	records := asSlice(recordResponse.Output["Records"])
	if len(records) != 3 {
		t.Fatalf("Kinesis destination records %#v", records)
	}
	for index, event := range []string{"INSERT", "MODIFY", "REMOVE"} {
		encoded := str(asMap(records[index])["Data"])
		payload, err := base64.StdEncoding.DecodeString(encoded)
		var record map[string]any
		if err != nil || json.Unmarshal(payload, &record) != nil || record["tableName"] != "T" || record["eventName"] != event || record["eventSourceARN"] != nil {
			t.Fatalf("%s Kinesis record %s", event, payload)
		}
		dynamodb := asMap(record["dynamodb"])
		if event == "INSERT" && (dynamodb["NewImage"] == nil || dynamodb["OldImage"] != nil) || event == "MODIFY" && (dynamodb["NewImage"] == nil || dynamodb["OldImage"] == nil) || event == "REMOVE" && (dynamodb["NewImage"] != nil || dynamodb["OldImage"] == nil) {
			t.Fatalf("%s Kinesis images %#v", event, dynamodb)
		}
	}
	for _, input := range []map[string]any{
		{"TableName": "T", "StreamArn": streamARN},
		{"TableName": "T", "StreamArn": streamARN, "UpdateKinesisStreamingConfiguration": map[string]any{"ApproximateCreationDateTimePrecision": "SECOND"}},
		{"TableName": "T", "StreamArn": "arn:aws:kinesis:us-east-1:000000000000:stream/missing", "UpdateKinesisStreamingConfiguration": map[string]any{"ApproximateCreationDateTimePrecision": "MICROSECOND"}},
	} {
		if _, err := call("UpdateKinesisStreamingDestination", input); err == nil {
			t.Fatalf("invalid destination update succeeded %#v", input)
		}
	}
	configuration := map[string]any{"ApproximateCreationDateTimePrecision": "MICROSECOND"}
	updated := must("UpdateKinesisStreamingDestination", map[string]any{"TableName": "T", "StreamArn": streamARN, "UpdateKinesisStreamingConfiguration": configuration})
	if updated["DestinationStatus"] != "UPDATING" || asMap(updated["UpdateKinesisStreamingConfiguration"])["ApproximateCreationDateTimePrecision"] != "MICROSECOND" {
		t.Fatalf("update destination %#v", updated)
	}
	if _, err := call("UpdateKinesisStreamingDestination", map[string]any{"TableName": "T", "StreamArn": streamARN, "UpdateKinesisStreamingConfiguration": configuration}); err == nil {
		t.Fatal("idempotent destination update succeeded")
	}
	disabled := must("DisableKinesisStreamingDestination", map[string]any{"TableName": "T", "StreamArn": streamARN})
	if disabled["DestinationStatus"] != "DISABLING" {
		t.Fatalf("disable destination %#v", disabled)
	}
	described = must("DescribeKinesisStreamingDestination", map[string]any{"TableName": "T"})
	if asMap(asSlice(described["KinesisDataStreamDestinations"])[0])["DestinationStatus"] != "DISABLED" {
		t.Fatalf("disabled destination %#v", described)
	}
	must("PutItem", map[string]any{"TableName": "T", "Item": map[string]any{"id": map[string]any{"S": "after-disable"}}})
	recordResponse, err = kinesisPack.Invoke(ctx, &spi.Request{Identity: id, Operation: "GetRecords", Input: map[string]any{"ShardIterator": iterator.Output["ShardIterator"]}})
	if err != nil || len(asSlice(recordResponse.Output["Records"])) != 3 {
		t.Fatalf("disabled destination emitted records %#v %v", recordResponse, err)
	}
}

func TestBootedServerDynamoDBStreams(t *testing.T) {
	cfg := config.Default()
	cfg.Services = []string{"aws.dynamodb"}
	cfg.Seed = "ddb-stream-1"
	rt, err := rtpkg.Boot(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(rt.Handler())
	defer ts.Close()
	ddbAuth := "AWS4-HMAC-SHA256 Credential=test/20200101/us-east-1/dynamodb/aws4_request, SignedHeaders=host, Signature=00"
	stAuth := "AWS4-HMAC-SHA256 Credential=test/20200101/us-east-1/streams/aws4_request, SignedHeaders=host, Signature=00"
	call := func(target, op, body string) map[string]any {
		t.Helper()
		req, _ := http.NewRequest(http.MethodPost, ts.URL+"/", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/x-amz-json-1.0")
		req.Header.Set("X-Amz-Target", target+"."+op)
		auth := ddbAuth
		if target == "DynamoDBStreams_20120810" {
			auth = stAuth
		}
		req.Header.Set("Authorization", auth)
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		raw, _ := io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode >= 300 {
			t.Fatalf("%s %d %s", op, res.StatusCode, raw)
		}
		if res.Header.Get("x-mirror-fidelity") != "emulate" {
			t.Fatalf("fidelity %q", res.Header.Get("x-mirror-fidelity"))
		}
		out := map[string]any{}
		_ = json.Unmarshal(raw, &out)
		return out
	}
	created := call("DynamoDB_20120810", "CreateTable", `{"TableName":"T","KeySchema":[{"AttributeName":"id","KeyType":"HASH"}],"StreamSpecification":{"StreamEnabled":true,"StreamViewType":"NEW_AND_OLD_IMAGES"}}`)
	td, _ := created["TableDescription"].(map[string]any)
	if str(td["LatestStreamArn"]) == "" {
		t.Fatalf("create stream arn %v", created)
	}
	call("DynamoDB_20120810", "PutItem", `{"TableName":"T","Item":{"id":{"S":"1"},"n":{"N":"2"}}}`)
	listed := call("DynamoDBStreams_20120810", "ListStreams", `{"TableName":"T"}`)
	streams, _ := listed["Streams"].([]any)
	if len(streams) != 1 {
		t.Fatalf("list %v", listed)
	}
	arn := str(asMap(streams[0])["StreamArn"])
	desc := call("DynamoDBStreams_20120810", "DescribeStream", `{"StreamArn":"`+arn+`"}`)
	sd, _ := desc["StreamDescription"].(map[string]any)
	if sd["StreamStatus"] != "ENABLED" {
		t.Fatalf("describe %v", desc)
	}
	it := call("DynamoDBStreams_20120810", "GetShardIterator", `{"StreamArn":"`+arn+`","ShardId":"shardId-000000000000","ShardIteratorType":"TRIM_HORIZON"}`)
	iter := str(it["ShardIterator"])
	if iter == "" {
		t.Fatalf("iterator %v", it)
	}
	got := call("DynamoDBStreams_20120810", "GetRecords", `{"ShardIterator":"`+iter+`"}`)
	recs, _ := got["Records"].([]any)
	if len(recs) != 1 {
		t.Fatalf("records %v", got)
	}
	raw, _ := json.Marshal(recs[0])
	if !strings.Contains(string(raw), `"INSERT"`) || !strings.Contains(string(raw), `"1"`) {
		t.Fatalf("insert %s", raw)
	}
	call("DynamoDB_20120810", "DeleteItem", `{"TableName":"T","Key":{"id":{"S":"1"}}}`)
	got = call("DynamoDBStreams_20120810", "GetRecords", `{"ShardIterator":"`+iter+`"}`)
	recs, _ = got["Records"].([]any)
	if len(recs) != 2 {
		t.Fatalf("after delete %v", got)
	}
	draw, _ := json.Marshal(recs[1])
	if !strings.Contains(string(draw), `"REMOVE"`) {
		t.Fatalf("remove %s", draw)
	}
}
