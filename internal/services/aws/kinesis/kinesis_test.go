package kinesis

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/config"
	rtpkg "github.com/tyler-r-kendrick/mirror.cloud/internal/runtime"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spitest"
)

func TestBootedServerKinesisPutGet(t *testing.T) {
	cfg := config.Default()
	cfg.Services = []string{"aws.kinesis"}
	cfg.Seed = "kin-1"
	rt, err := rtpkg.Boot(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(rt.Handler())
	defer ts.Close()
	auth := "AWS4-HMAC-SHA256 Credential=test/20200101/us-east-1/kinesis/aws4_request, SignedHeaders=host, Signature=00"
	call := func(op, body string) (int, map[string]any) {
		t.Helper()
		req, _ := http.NewRequest(http.MethodPost, ts.URL+"/", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/x-amz-json-1.1")
		req.Header.Set("X-Amz-Target", "Kinesis_20131202."+op)
		req.Header.Set("Authorization", auth)
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		raw, _ := io.ReadAll(res.Body)
		res.Body.Close()
		out := map[string]any{}
		_ = json.Unmarshal(raw, &out)
		if res.StatusCode >= 300 {
			t.Fatalf("%s %d %s", op, res.StatusCode, raw)
		}
		if res.Header.Get("x-mirror-fidelity") != "emulate" {
			t.Fatalf("fidelity %q", res.Header.Get("x-mirror-fidelity"))
		}
		return res.StatusCode, out
	}
	call("CreateStream", `{"StreamName":"s"}`)
	data := base64.StdEncoding.EncodeToString([]byte("hello-kinesis"))
	_, put := call("PutRecord", `{"StreamName":"s","PartitionKey":"p","Data":"`+data+`"}`)
	if put["SequenceNumber"] == nil {
		t.Fatalf("put %v", put)
	}
	_, it := call("GetShardIterator", `{"StreamName":"s","ShardId":"shardId-000000000000","ShardIteratorType":"TRIM_HORIZON"}`)
	iter, _ := it["ShardIterator"].(string)
	if iter == "" {
		t.Fatalf("iter %v", it)
	}
	_, got := call("GetRecords", `{"ShardIterator":"`+iter+`"}`)
	recs, _ := got["Records"].([]any)
	if len(recs) < 1 {
		t.Fatalf("records %v", got)
	}
	_, listed := call("ListStreams", `{}`)
	if listed["StreamNames"] == nil {
		t.Fatalf("list %v", listed)
	}
	call("AddTagsToStream", `{"StreamName":"s","Tags":{"k":"v"}}`)
	_, tags := call("ListTagsForStream", `{"StreamName":"s"}`)
	tb, _ := json.Marshal(tags["Tags"])
	if !strings.Contains(string(tb), `"k"`) {
		t.Fatalf("tags %v", tags)
	}
	call("RemoveTagsFromStream", `{"StreamName":"s","TagKeys":["k"]}`)
	call("IncreaseStreamRetentionPeriod", `{"StreamName":"s","RetentionPeriodHours":48}`)
	call("PutResourcePolicy", `{"ResourceARN":"arn:aws:kinesis:us-east-1:000000000000:stream/s","Policy":"{\"Version\":\"2012-10-17\"}"}`)
	_, pol := call("GetResourcePolicy", `{"ResourceARN":"arn:aws:kinesis:us-east-1:000000000000:stream/s"}`)
	ps, _ := pol["Policy"].(string)
	if ps == "" {
		t.Fatalf("policy %v", pol)
	}
	_, cons := call("RegisterStreamConsumer", `{"StreamName":"s","ConsumerName":"c"}`)
	cm, _ := cons["Consumer"].(map[string]any)
	carn, _ := cm["ConsumerARN"].(string)
	if carn == "" {
		t.Fatalf("consumer %v", cons)
	}
	call("ListStreamConsumers", `{"StreamName":"s"}`)
	call("DescribeStreamConsumer", `{"ConsumerARN":"`+carn+`"}`)
	call("SubscribeToShard", `{"ConsumerARN":"`+carn+`","ShardId":"shardId-000000000000"}`)
	call("EnableEnhancedMonitoring", `{"StreamName":"s","ShardLevelMetrics":["IncomingBytes"]}`)
	call("UpdateShardCount", `{"StreamName":"s","TargetShardCount":2,"ScalingType":"UNIFORM_SCALING"}`)
	call("StartStreamEncryption", `{"StreamName":"s","EncryptionType":"KMS","KeyId":"alias/aws/kinesis"}`)
	call("DescribeLimits", `{}`)
	call("UpdateStreamMode", `{"StreamARN":"arn:aws:kinesis:us-east-1:000000000000:stream/s","StreamModeDetails":{"StreamMode":"ON_DEMAND"}}`)
	call("TagResource", `{"ResourceARN":"arn:aws:kinesis:us-east-1:000000000000:stream/s","Tags":{"a":"b"}}`)
	call("ListTagsForResource", `{"ResourceARN":"arn:aws:kinesis:us-east-1:000000000000:stream/s"}`)
	call("UntagResource", `{"ResourceARN":"arn:aws:kinesis:us-east-1:000000000000:stream/s","TagKeys":["a"]}`)
	call("DecreaseStreamRetentionPeriod", `{"StreamName":"s","RetentionPeriodHours":24}`)
	call("DeleteResourcePolicy", `{"ResourceARN":"arn:aws:kinesis:us-east-1:000000000000:stream/s"}`)
	call("DisableEnhancedMonitoring", `{"StreamName":"s","ShardLevelMetrics":["IncomingBytes"]}`)
	call("SplitShard", `{"StreamName":"s","ShardToSplit":"shardId-000000000000","NewStartingHashKey":"1"}`)
	call("MergeShards", `{"StreamName":"s","ShardToMerge":"shardId-000000000000","AdjacentShardToMerge":"shardId-000000000001"}`)
	call("StopStreamEncryption", `{"StreamName":"s","EncryptionType":"KMS","KeyId":"alias/aws/kinesis"}`)
	call("DescribeAccountSettings", `{}`)
	call("UpdateAccountSettings", `{}`)
	call("UpdateMaxRecordSize", `{"StreamName":"s","MaxRecordSizeInKiB":1024}`)
	call("UpdateStreamWarmThroughput", `{"StreamName":"s","WarmThroughputMiBPerSecond":1}`)
	call("DeregisterStreamConsumer", `{"ConsumerARN":"`+carn+`"}`)
}

func TestKinesisPublishesRecordsAndStartsAtTimestamp(t *testing.T) {
	id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
	deps := spitest.Deps(t)
	p := New(deps)
	request := func(operation string, input map[string]any) *spi.Response {
		t.Helper()
		response, err := p.Invoke(context.Background(), &spi.Request{Identity: id, Operation: operation, Input: input})
		if err != nil {
			t.Fatal(err)
		}
		return response
	}
	request("CreateStream", map[string]any{"StreamName": "events"})
	published := 0
	var event map[string]any
	cancel := deps.Bus.Subscribe("kinesis", func(_ context.Context, payload []byte) {
		published++
		_ = json.Unmarshal(payload, &event)
	})
	defer cancel()
	request("PutRecord", map[string]any{"StreamName": "events", "PartitionKey": "one", "Data": []byte("one")})
	if err := deps.Clock.Advance(time.Second); err != nil {
		t.Fatal(err)
	}
	request("PutRecord", map[string]any{"StreamName": "events", "PartitionKey": "two", "Data": []byte("two")})
	iterator := request("GetShardIterator", map[string]any{
		"StreamName": "events", "ShardIteratorType": "AT_TIMESTAMP", "Timestamp": float64(deps.Clock.Now().Add(-time.Millisecond).UnixMilli()) / 1000,
	}).Output["ShardIterator"]
	records := request("GetRecords", map[string]any{"ShardIterator": iterator}).Output["Records"].([]any)
	if published != 2 || event["Account"] != id.Account || event["Region"] != id.Region || event["StreamName"] != "events" || len(records) != 1 || records[0].(map[string]any)["PartitionKey"] != "two" {
		t.Fatalf("published=%d event=%#v records=%#v", published, event, records)
	}
}

func TestKinesisHTTPProvenOps(t *testing.T) {
	p := New(spitest.Deps(t))
	if n := len(p.Operations()); n != 39 {
		t.Fatalf("kinesis Operations() %d want 39", n)
	}
}
