package firehose

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
	rtpkg "github.com/tyler-r-kendrick/mirror.cloud/internal/runtime"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spitest"

	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/s3"
)

func TestFirehoseHTTPProvenOps(t *testing.T) {
	p := New(spitest.Deps(t))
	if n := len(p.Operations()); n != 12 {
		t.Fatalf("firehose Operations() %d want 12", n)
	}
}

func TestBootedServerFirehosePutRecord(t *testing.T) {
	cfg := config.Default()
	cfg.Services = []string{"aws.firehose"}
	cfg.Seed = "fh-1"
	rt, err := rtpkg.Boot(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(rt.Handler())
	defer ts.Close()
	auth := "AWS4-HMAC-SHA256 Credential=test/20200101/us-east-1/firehose/aws4_request, SignedHeaders=host, Signature=00"
	call := func(op, body string) map[string]any {
		t.Helper()
		req, _ := http.NewRequest(http.MethodPost, ts.URL+"/", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/x-amz-json-1.1")
		req.Header.Set("X-Amz-Target", "Firehose_20150804."+op)
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
	created := call("CreateDeliveryStream", `{"DeliveryStreamName":"ds1"}`)
	if created["DeliveryStreamARN"] == nil {
		t.Fatalf("create %v", created)
	}
	data := base64.StdEncoding.EncodeToString([]byte("hello"))
	put := call("PutRecord", `{"DeliveryStreamName":"ds1","Record":{"Data":"`+data+`"}}`)
	if put["RecordId"] == nil {
		t.Fatalf("put %v", put)
	}
	desc := call("DescribeDeliveryStream", `{"DeliveryStreamName":"ds1"}`)
	if desc["DeliveryStreamDescription"] == nil {
		t.Fatalf("describe %v", desc)
	}
	listed := call("ListDeliveryStreams", `{}`)
	raw, _ := json.Marshal(listed)
	if !strings.Contains(string(raw), "ds1") {
		t.Fatalf("list %s", raw)
	}
	call("DeleteDeliveryStream", `{"DeliveryStreamName":"ds1"}`)
	gone := call("ListDeliveryStreams", `{}`)
	raw, _ = json.Marshal(gone)
	if strings.Contains(string(raw), `"ds1"`) {
		t.Fatalf("stream still present %s", raw)
	}
}

func TestBootedServerFirehoseDeliversToS3(t *testing.T) {
	cfg := config.Default()
	cfg.Services = []string{"aws.firehose", "aws.s3"}
	cfg.Seed = "fh-s3-1"
	rt, err := rtpkg.Boot(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(rt.Handler())
	defer ts.Close()
	fhAuth := "AWS4-HMAC-SHA256 Credential=test/20200101/us-east-1/firehose/aws4_request, SignedHeaders=host, Signature=00"
	s3Auth := "AWS4-HMAC-SHA256 Credential=test/20200101/us-east-1/s3/aws4_request, SignedHeaders=host, Signature=00"
	call := func(op, body string) map[string]any {
		t.Helper()
		req, _ := http.NewRequest(http.MethodPost, ts.URL+"/", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/x-amz-json-1.1")
		req.Header.Set("X-Amz-Target", "Firehose_20150804."+op)
		req.Header.Set("Authorization", fhAuth)
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
	s3 := func(method, path, body string) (int, []byte) {
		t.Helper()
		req, _ := http.NewRequest(method, ts.URL+path, strings.NewReader(body))
		req.Header.Set("Authorization", s3Auth)
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		raw, _ := io.ReadAll(res.Body)
		res.Body.Close()
		return res.StatusCode, raw
	}
	if code, b := s3(http.MethodPut, "/fhout", ""); code >= 300 {
		t.Fatalf("bucket %d %s", code, b)
	}
	call("CreateDeliveryStream", `{"DeliveryStreamName":"ds1","S3DestinationConfiguration":{"RoleARN":"arn:aws:iam::000000000000:role/fh","BucketARN":"arn:aws:s3:::fhout","Prefix":"fh/"}}`)
	data := base64.StdEncoding.EncodeToString([]byte("hello-firehose"))
	put := call("PutRecord", `{"DeliveryStreamName":"ds1","Record":{"Data":"`+data+`"}}`)
	id, _ := put["RecordId"].(string)
	if id == "" {
		t.Fatalf("put %v", put)
	}
	code, listed := s3(http.MethodGet, "/fhout?list-type=2&prefix=fh/", "")
	start, end := strings.Index(string(listed), "<Key>"), strings.Index(string(listed), "</Key>")
	if code >= 300 || start < 0 || end < start {
		t.Fatalf("list objects %d %s", code, listed)
	}
	key := string(listed)[start+len("<Key>") : end]
	code, body := s3(http.MethodGet, "/fhout/"+key, "")
	if code >= 300 {
		t.Fatalf("get object %d %s", code, body)
	}
	if string(body) != "hello-firehose" {
		t.Fatalf("s3 body %q", body)
	}
}

func TestFirehoseS3ObjectNameFormat(t *testing.T) {
	deps := spitest.Deps(t)
	p := New(deps)
	id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
	invoke := func(operation string, input map[string]any) *spi.Response {
		t.Helper()
		response, err := p.Invoke(context.Background(), &spi.Request{Identity: id, Operation: operation, Input: input})
		if err != nil {
			t.Fatal(err)
		}
		return response
	}
	invoke("CreateDeliveryStream", map[string]any{
		"DeliveryStreamName": "delivery", "S3DestinationConfiguration": map[string]any{"BucketARN": "arn:aws:s3:::out", "Prefix": "logs/"},
	})
	response := invoke("PutRecord", map[string]any{
		"DeliveryStreamName": "delivery", "Record": map[string]any{"Data": base64.StdEncoding.EncodeToString([]byte("payload"))},
	})
	recordID := response.Output["RecordId"].(string)
	key := id.Account + "/" + id.Region + "/out/logs/1970/01/01/00/delivery-1-1970-01-01-00-00-00-" + recordID
	reader, _, err := deps.Blobs.Get(context.Background(), key)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	body, _ := io.ReadAll(reader)
	if string(body) != "payload" {
		t.Fatalf("S3 object body %q", body)
	}
	invoke("CreateDeliveryStream", map[string]any{
		"DeliveryStreamName": "expressions", "ExtendedS3DestinationConfiguration": map[string]any{
			"BucketARN": "arn:aws:s3:::out", "Prefix": "year=!{timestamp:yyyy}/month=!{timestamp:MM}/day=!{timestamp:dd}/hour=!{timestamp:HH}/", "FileExtension": ".jsonl",
		},
	})
	response = invoke("PutRecord", map[string]any{"DeliveryStreamName": "expressions", "Record": map[string]any{"Data": base64.StdEncoding.EncodeToString([]byte("expression"))}})
	recordID = response.Output["RecordId"].(string)
	key = id.Account + "/" + id.Region + "/out/year=1970/month=01/day=01/hour=00/expressions-1-1970-01-01-00-00-00-" + recordID + ".jsonl"
	if _, _, err := deps.Blobs.Get(context.Background(), key); err != nil {
		t.Fatal(err)
	}
	randomPrefix := p.evaluatedS3Prefix("random=!{firehose:random-string}/again=!{firehose:random-string}/!{timestamp:yyyy}/", deps.Clock.Now())
	parts := strings.Split(randomPrefix, "/")
	first, second := strings.TrimPrefix(parts[0], "random="), strings.TrimPrefix(parts[1], "again=")
	if len(first) != 11 || len(second) != 11 || first == second || !strings.HasSuffix(randomPrefix, "/1970/") {
		t.Fatalf("random prefix %q", randomPrefix)
	}
	invoke("CreateDeliveryStream", map[string]any{
		"DeliveryStreamName": "timezone", "ExtendedS3DestinationConfiguration": map[string]any{
			"BucketARN": "arn:aws:s3:::out", "Prefix": "hour=!{timestamp:HH}/", "CustomTimeZone": "Asia/Tokyo",
		},
	})
	response = invoke("PutRecord", map[string]any{"DeliveryStreamName": "timezone", "Record": map[string]any{"Data": base64.StdEncoding.EncodeToString([]byte("timezone"))}})
	recordID = response.Output["RecordId"].(string)
	key = id.Account + "/" + id.Region + "/out/hour=09/timezone-1-1970-01-01-09-00-00-" + recordID
	if _, _, err := deps.Blobs.Get(context.Background(), key); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Invoke(context.Background(), &spi.Request{Identity: id, Operation: "CreateDeliveryStream", Input: map[string]any{
		"DeliveryStreamName": "bad-timezone", "ExtendedS3DestinationConfiguration": map[string]any{
			"BucketARN": "arn:aws:s3:::out", "CustomTimeZone": "Mars/Olympus_Mons",
		},
	}}); err == nil {
		t.Fatal("accepted invalid CustomTimeZone")
	}
	for _, extension := range []string{"jsonl", ".UPPER", "." + strings.Repeat("a", 128)} {
		if _, err := p.Invoke(context.Background(), &spi.Request{Identity: id, Operation: "CreateDeliveryStream", Input: map[string]any{
			"DeliveryStreamName": "bad-extension", "ExtendedS3DestinationConfiguration": map[string]any{
				"BucketARN": "arn:aws:s3:::out", "FileExtension": extension,
			},
		}}); err == nil {
			t.Fatalf("accepted invalid FileExtension %q", extension)
		}
	}
}

func TestFirehoseControlPlaneAndBatch(t *testing.T) {
	deps := spitest.Deps(t)
	p := New(deps)
	id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
	call := func(operation string, input map[string]any) (*spi.Response, error) {
		t.Helper()
		return p.Invoke(context.Background(), &spi.Request{Identity: id, Operation: operation, Input: input})
	}
	if _, err := call("CreateDeliveryStream", map[string]any{}); err == nil {
		t.Fatal("created unnamed stream")
	}
	if _, err := call("PutRecord", map[string]any{"DeliveryStreamName": "missing"}); err == nil {
		t.Fatal("put to missing stream")
	}
	if _, err := call("CreateDeliveryStream", map[string]any{
		"DeliveryStreamName": "control", "ExtendedS3DestinationConfiguration": map[string]any{
			"BucketARN": "arn:aws:s3:::original", "Prefix": "kept/",
		},
	}); err != nil {
		t.Fatal(err)
	}
	record := map[string]any{"Data": base64.StdEncoding.EncodeToString([]byte("batch"))}
	batch, err := call("PutRecordBatch", map[string]any{"DeliveryStreamName": "control", "Records": []any{record, record}})
	if err != nil || batch.Output["FailedPutCount"] != 0 || len(batch.Output["RequestResponses"].([]any)) != 2 {
		t.Fatalf("batch %#v, %v", batch, err)
	}
	described, err := call("DescribeDeliveryStream", map[string]any{"DeliveryStreamName": "control"})
	description := described.Output["DeliveryStreamDescription"].(map[string]any)
	if err != nil || described.Output["RecordCount"] != 2 || description["VersionId"] != "1" {
		t.Fatalf("describe %#v, %v", described, err)
	}
	if _, err := call("UpdateDestination", map[string]any{"DeliveryStreamName": "control"}); err == nil {
		t.Fatal("updated without version and destination IDs")
	}
	if _, err := call("UpdateDestination", map[string]any{
		"DeliveryStreamName": "control", "CurrentDeliveryStreamVersionId": "1", "DestinationId": "destinationId-000000000001",
		"ExtendedS3DestinationUpdate": map[string]any{"BucketARN": "arn:aws:s3:::updated"},
	}); err != nil {
		t.Fatal(err)
	}
	_, staleErr := call("UpdateDestination", map[string]any{
		"DeliveryStreamName": "control", "CurrentDeliveryStreamVersionId": "1", "DestinationId": "destinationId-000000000001",
	})
	fault, ok := staleErr.(*spi.Fault)
	if !ok || fault.Code != "ConcurrentModificationException" {
		t.Fatalf("stale update error %v", staleErr)
	}
	described, err = call("DescribeDeliveryStream", map[string]any{"DeliveryStreamName": "control"})
	description = described.Output["DeliveryStreamDescription"].(map[string]any)
	if err != nil || description["VersionId"] != "2" {
		t.Fatalf("updated description %#v, %v", described, err)
	}
	put, err := call("PutRecord", map[string]any{"DeliveryStreamName": "control", "Record": record})
	if err != nil {
		t.Fatal(err)
	}
	key := id.Account + "/" + id.Region + "/updated/kept/1970/01/01/00/control-2-1970-01-01-00-00-00-" + put.Output["RecordId"].(string)
	if _, _, err := deps.Blobs.Get(context.Background(), key); err != nil {
		t.Fatal(err)
	}
	tags := []any{map[string]any{"Key": "env", "Value": "test"}}
	if _, err := call("TagDeliveryStream", map[string]any{"DeliveryStreamName": "control", "Tags": tags}); err != nil {
		t.Fatal(err)
	}
	listed, _ := call("ListTagsForDeliveryStream", map[string]any{"DeliveryStreamName": "control"})
	if len(listed.Output["Tags"].([]any)) != 1 {
		t.Fatalf("tags %#v", listed.Output)
	}
	for _, operation := range []string{"UntagDeliveryStream", "StartDeliveryStreamEncryption", "StopDeliveryStreamEncryption", "DeleteDeliveryStream"} {
		if _, err := call(operation, map[string]any{"DeliveryStreamName": "control"}); err != nil {
			t.Fatalf("%s: %v", operation, err)
		}
	}
	if _, err := call("DescribeDeliveryStream", map[string]any{"DeliveryStreamName": "control"}); err == nil {
		t.Fatal("described deleted stream")
	}
	if bucketFromARN("arn:aws:s3:::bucket") != "bucket" || bucketFromARN("arn:test:bucket") != "bucket" || bucketFromARN("bucket") != "bucket" {
		t.Fatal("bucket ARN parsing")
	}
}
