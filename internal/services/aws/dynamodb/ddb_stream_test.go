package dynamodb

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/config"
	rtpkg "github.com/tyler-r-kendrick/mirror.cloud/internal/runtime"
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
