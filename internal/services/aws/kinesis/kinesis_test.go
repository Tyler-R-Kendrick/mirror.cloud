package kinesis

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/bundled"
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
	_, cons := call("RegisterStreamConsumer", `{"StreamARN":"arn:aws:kinesis:us-east-1:000000000000:stream/s","ConsumerName":"c"}`)
	cm, _ := cons["Consumer"].(map[string]any)
	carn, _ := cm["ConsumerARN"].(string)
	if carn == "" {
		t.Fatalf("consumer %v", cons)
	}
	call("ListStreamConsumers", `{"StreamARN":"arn:aws:kinesis:us-east-1:000000000000:stream/s"}`)
	call("DescribeStreamConsumer", `{"ConsumerARN":"`+carn+`"}`)
	call("SubscribeToShard", `{"ConsumerARN":"`+carn+`","ShardId":"shardId-000000000000","StartingPosition":{"Type":"LATEST"}}`)
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
	call("UpdateAccountSettings", `{"MinimumThroughputBillingCommitment":{"Status":"DISABLED"}}`)
	call("UpdateMaxRecordSize", `{"StreamName":"s","MaxRecordSizeInKiB":1024}`)
	call("UpdateStreamWarmThroughput", `{"StreamName":"s","WarmThroughputMiBps":1}`)
	call("DeregisterStreamConsumer", `{"ConsumerARN":"`+carn+`"}`)
}

func TestKinesisPublishesRecordsAndStartsAtTimestamp(t *testing.T) {
	id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
	deps := spitest.Deps(t)
	p := bundled.Handler("aws.kinesis", deps)
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

	// A recreated stream restarts at sequence 0; its predecessor's records
	// must not answer from the horizon.
	request("DeleteStream", map[string]any{"StreamName": "events"})
	request("CreateStream", map[string]any{"StreamName": "events"})
	iterator = request("GetShardIterator", map[string]any{"StreamName": "events", "ShardIteratorType": "TRIM_HORIZON"}).Output["ShardIterator"]
	if stale, _ := request("GetRecords", map[string]any{"ShardIterator": iterator}).Output["Records"].([]any); len(stale) != 0 {
		t.Fatalf("recreated stream answered %#v", stale)
	}
}

// A stream has the shards it was created with. CreateStream used to accept a
// ShardCount and drop it, so a stream made with three shards said it had
// one, and UpdateShardCount answered the new count as the current one.
//
// CreateStream: "ShardCount: The number of shards that the stream will use."
// DescribeStreamSummary: "OpenShardCount: The number of open shards in the
// stream." UpdateShardCount: "CurrentShardCount: The current number of
// shards. TargetShardCount: The updated number of shards." DescribeLimits:
// "OpenShardCount: The number of open shards." (Kinesis API reference.)
func TestKinesisAStreamHasTheShardsItWasCreatedWith(t *testing.T) {
	call := kinesisCalls(t)
	open := func(stream string) any {
		t.Helper()
		summary, _ := call("DescribeStreamSummary", `{"StreamName":"`+stream+`"}`)["StreamDescriptionSummary"].(map[string]any)
		return summary["OpenShardCount"]
	}
	if got := call("DescribeLimits", `{}`)["OpenShardCount"]; got != float64(0) {
		t.Errorf("an account with no stream reports %v open shards", got)
	}
	call("CreateStream", `{"StreamName":"three","ShardCount":3}`)
	call("CreateStream", `{"StreamName":"plain"}`)
	if got := open("three"); got != float64(3) {
		t.Errorf("a stream created with three shards reports %v open", got)
	}
	if got := open("plain"); got != float64(1) {
		t.Errorf("a stream created with no count reports %v open; want the one shard it has", got)
	}
	if got := call("DescribeLimits", `{}`)["OpenShardCount"]; got != float64(4) {
		t.Errorf("the account reports %v open shards; want the four its two streams have", got)
	}
	moved := call("UpdateShardCount", `{"StreamName":"three","TargetShardCount":5,"ScalingType":"UNIFORM_SCALING"}`)
	if moved["CurrentShardCount"] != float64(3) || moved["TargetShardCount"] != float64(5) {
		t.Errorf("UpdateShardCount answered %v; want the count it found, 3, and the count asked for, 5", moved)
	}
	call("SplitShard", `{"StreamName":"three","ShardToSplit":"shardId-000000000000","NewStartingHashKey":"1"}`)
	if got := open("three"); got != float64(6) {
		t.Errorf("after an update to five and a split the stream reports %v open", got)
	}
	// Creating over the name replaces the stream, count included.
	call("CreateStream", `{"StreamName":"three","ShardCount":2}`)
	if got := open("three"); got != float64(2) {
		t.Errorf("a stream created again with two shards reports %v open", got)
	}
}

// kinesisCalls boots the service alone and answers a call that fails the test on an error.
func kinesisCalls(t *testing.T) func(op, body string) map[string]any {
	try := kinesisTries(t)
	return func(op, body string) map[string]any {
		t.Helper()
		status, out := try(op, body)
		if status >= 300 {
			t.Fatalf("%s answered %d %v", op, status, out)
		}
		return out
	}
}

// kinesisTries boots the service alone and answers a call's status and body.
func kinesisTries(t *testing.T) func(op, body string) (int, map[string]any) {
	cfg := config.Default()
	cfg.Services = []string{"aws.kinesis"}
	cfg.Seed = "kin-" + t.Name()
	rt, err := rtpkg.Boot(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(rt.Handler())
	t.Cleanup(ts.Close)
	return func(op, body string) (int, map[string]any) {
		t.Helper()
		req, _ := http.NewRequest(http.MethodPost, ts.URL+"/", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/x-amz-json-1.1")
		req.Header.Set("X-Amz-Target", "Kinesis_20131202."+op)
		req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=test/20200101/us-east-1/kinesis/aws4_request, SignedHeaders=host, Signature=00")
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer res.Body.Close()
		raw, _ := io.ReadAll(res.Body)
		out := map[string]any{}
		_ = json.Unmarshal(raw, &out)
		return res.StatusCode, out
	}
}

// An on-demand stream is recorded as one. CreateStream dropped StreamModeDetails, so a stream created
// on-demand was a provisioned stream of one shard, no summary said which mode a stream has, and the
// account's count of on-demand streams was a constant 0.
//
// CreateStream: "StreamModeDetails: Indicates the capacity mode of the data stream. Currently, in Kinesis
// Data Streams, you can choose between an on-demand capacity mode and a provisioned capacity mode".
// StreamDescriptionSummary: "StreamModeDetails: Specifies the capacity mode to which you want to set your
// data stream." DescribeLimits: "OnDemandStreamCount: Indicates the number of data streams with the
// on-demand capacity mode." (Kinesis API reference, as remembered.) That a new on-demand stream has four
// open shards is the Developer Guide's, also as remembered: "by default ... 4 MB/s write and 8 MB/s read
// throughput", one shard a megabyte of writes.
func TestKinesisAnOnDemandStreamIsRecordedAsOne(t *testing.T) {
	call := kinesisCalls(t)
	summary := func(stream string) string {
		t.Helper()
		s, _ := call("DescribeStreamSummary", `{"StreamName":"`+stream+`"}`)["StreamDescriptionSummary"].(map[string]any)
		mode, _ := s["StreamModeDetails"].(map[string]any)
		return fmt.Sprint(mode["StreamMode"], " ", s["OpenShardCount"])
	}
	limits := func() string {
		l := call("DescribeLimits", `{}`)
		return fmt.Sprint("open=", l["OpenShardCount"], " on-demand=", l["OnDemandStreamCount"])
	}
	call("CreateStream", `{"StreamName":"od","StreamModeDetails":{"StreamMode":"ON_DEMAND"}}`)
	call("CreateStream", `{"StreamName":"od6","ShardCount":6,"StreamModeDetails":{"StreamMode":"ON_DEMAND"}}`)
	call("CreateStream", `{"StreamName":"named","ShardCount":2,"StreamModeDetails":{"StreamMode":"PROVISIONED"}}`)
	call("CreateStream", `{"StreamName":"plain","ShardCount":3}`)
	for stream, want := range map[string]string{"od": "ON_DEMAND 4", "od6": "ON_DEMAND 6", "named": "PROVISIONED 2", "plain": "PROVISIONED 3"} {
		if got := summary(stream); got != want {
			t.Errorf("stream %s is summarized %s; want %s", stream, got, want)
		}
	}
	if got := limits(); got != "open=5 on-demand=2" {
		t.Errorf("the account's limits read %s; want the 5 open shards of its provisioned streams and 2 on-demand streams", got)
	}
	call("UpdateStreamMode", `{"StreamARN":"arn:aws:kinesis:us-east-1:000000000000:stream/plain","StreamModeDetails":{"StreamMode":"ON_DEMAND"}}`)
	call("UpdateStreamMode", `{"StreamARN":"arn:aws:kinesis:us-east-1:000000000000:stream/od","StreamModeDetails":{"StreamMode":"PROVISIONED"}}`)
	if got := summary("plain") + ", " + summary("od") + ", " + limits(); got != "ON_DEMAND 3, PROVISIONED 4, open=6 on-demand=2" {
		t.Errorf("after each of two streams changed mode: %s", got)
	}
	call("DeleteStream", `{"StreamName":"od6"}`)
	if got := limits(); got != "open=6 on-demand=1" {
		t.Errorf("after an on-demand stream is deleted the limits read %s", got)
	}
}

// A shard count is an existing stream's, within the account's limit. UpdateShardCount on a stream that
// does not exist answered 200 and made a record of one; and nothing held the ShardLimit DescribeLimits
// reports. UpdateShardCount lists ResourceNotFoundException and LimitExceededException among its errors,
// CreateStream LimitExceededException: "The requested resource exceeds the maximum number allowed"
// (Kinesis API reference). The message here is this emulator's. The limit is the provisioned streams':
// the Developer Guide's quotas give "the default shard quota" under provisioned mode alone, and under
// on-demand "There's no upper limit ... Kinesis Data Streams automatically scales the number of shards",
// so an on-demand stream's shards are neither held to it nor counted in it. That DescribeLimits'
// OpenShardCount, "The number of open shards", is the count held against ShardLimit, the provisioned
// streams', is a reading: the operation "Describes the shard limits and usage for the account".
func TestKinesisAShardCountIsAnExistingStreamsWithinTheLimit(t *testing.T) {
	try := kinesisTries(t)
	code := func(op, body string) string { // of try as it is when called: a second service is booted below
		t.Helper()
		status, out := try(op, body)
		return fmt.Sprint(status, " ", out["__type"])
	}
	if got := code("UpdateShardCount", `{"StreamName":"ghost","TargetShardCount":2,"ScalingType":"UNIFORM_SCALING"}`); got != "400 ResourceNotFoundException" {
		t.Errorf("UpdateShardCount on a stream that does not exist answered %s", got)
	}
	if got := code("DescribeStreamSummary", `{"StreamName":"ghost"}`); got != "400 ResourceNotFoundException" {
		t.Errorf("the stream that does not exist is described: %s", got)
	}
	for _, step := range []struct{ op, body, want string }{
		{"CreateStream", `{"StreamName":"a","ShardCount":300}`, "200 <nil>"},
		{"CreateStream", `{"StreamName":"b","ShardCount":201}`, "400 LimitExceededException"},
		{"CreateStream", `{"StreamName":"b","ShardCount":200}`, "200 <nil>"},
		{"CreateStream", `{"StreamName":"c"}`, "400 LimitExceededException"},
		{"CreateStream", `{"StreamName":"od","ShardCount":400,"StreamModeDetails":{"StreamMode":"ON_DEMAND"}}`, "200 <nil>"}, // on-demand: not the limit's
		{"UpdateShardCount", `{"StreamName":"a","TargetShardCount":301,"ScalingType":"UNIFORM_SCALING"}`, "400 LimitExceededException"},
		{"UpdateShardCount", `{"StreamName":"a","TargetShardCount":296,"ScalingType":"UNIFORM_SCALING"}`, "200 <nil>"},
		{"CreateStream", `{"StreamName":"c","ShardCount":4}`, "200 <nil>"},
		{"CreateStream", `{"StreamName":"a","ShardCount":296}`, "200 <nil>"}, // over its own name: its 296 shards are not counted twice
		{"CreateStream", `{"StreamName":"a","ShardCount":297}`, "400 LimitExceededException"},
	} {
		if got := code(step.op, step.body); got != step.want {
			t.Errorf("%s %s answered %s; want %s", step.op, step.body, got, step.want)
		}
	}
	if _, limits := try("DescribeLimits", `{}`); fmt.Sprint(limits["OpenShardCount"], limits["ShardLimit"]) != "500 500" {
		t.Errorf("the limits read %v; want every one of the 500 shards open, the on-demand stream's 400 apart", limits)
	}

	// The on-demand streams have their own limit, the OnDemandStreamCountLimit DescribeLimits reports.
	try = kinesisTries(t)
	for i := range 50 {
		if got := code("CreateStream", fmt.Sprintf(`{"StreamName":"od%d","ShardCount":1,"StreamModeDetails":{"StreamMode":"ON_DEMAND"}}`, i)); got != "200 <nil>" {
			t.Fatalf("on-demand stream %d of 50 answered %s", i+1, got)
		}
	}
	if got := code("CreateStream", `{"StreamName":"od50","ShardCount":1,"StreamModeDetails":{"StreamMode":"ON_DEMAND"}}`); got != "400 LimitExceededException" {
		t.Errorf("a 51st on-demand stream answered %s", got)
	}
	if got := code("CreateStream", `{"StreamName":"od7","ShardCount":2,"StreamModeDetails":{"StreamMode":"ON_DEMAND"}}`) + ", " + code("CreateStream", `{"StreamName":"p","ShardCount":1}`); got != "200 <nil>, 200 <nil>" {
		t.Errorf("an on-demand stream created over its own name, and a provisioned one beside fifty on-demand, answered %s", got)
	}
}
