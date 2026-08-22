package firehose

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/config"
	rtpkg "github.com/tyler-r-kendrick/mirror.cloud/internal/runtime"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spitest"

	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/s3"
)

const testRoleARN = "arn:aws:iam::123456789012:role/firehose"

func testS3Destination() map[string]any {
	return map[string]any{"BucketARN": "arn:aws:s3:::out", "RoleARN": testRoleARN}
}

func testKinesisSource() map[string]any {
	return map[string]any{"KinesisStreamARN": "arn:aws:kinesis:us-east-1:123456789012:stream/source", "RoleARN": testRoleARN}
}

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
	created := call("CreateDeliveryStream", `{"DeliveryStreamName":"ds1","S3DestinationConfiguration":{"RoleARN":"arn:aws:iam::000000000000:role/fh","BucketARN":"arn:aws:s3:::out"}}`)
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
	s3 := func(method, path, body string) (int, []byte, http.Header) {
		t.Helper()
		req, _ := http.NewRequest(method, ts.URL+path, strings.NewReader(body))
		req.Header.Set("Authorization", s3Auth)
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		raw, _ := io.ReadAll(res.Body)
		res.Body.Close()
		return res.StatusCode, raw, res.Header
	}
	if code, b, _ := s3(http.MethodPut, "/fhout", ""); code >= 300 {
		t.Fatalf("bucket %d %s", code, b)
	}
	kmsARN := "arn:aws:kms:us-east-1:000000000000:key/firehose"
	call("CreateDeliveryStream", `{"DeliveryStreamName":"ds1","S3DestinationConfiguration":{"RoleARN":"arn:aws:iam::000000000000:role/fh","BucketARN":"arn:aws:s3:::fhout","Prefix":"fh/","EncryptionConfiguration":{"KMSEncryptionConfig":{"AWSKMSKeyARN":"`+kmsARN+`"}}}}`)
	data := base64.StdEncoding.EncodeToString([]byte("hello-firehose"))
	put := call("PutRecord", `{"DeliveryStreamName":"ds1","Record":{"Data":"`+data+`"}}`)
	id, _ := put["RecordId"].(string)
	if id == "" {
		t.Fatalf("put %v", put)
	}
	code, listed, _ := s3(http.MethodGet, "/fhout?list-type=2&prefix=fh/", "")
	start, end := strings.Index(string(listed), "<Key>"), strings.Index(string(listed), "</Key>")
	if code >= 300 || start < 0 || end < start {
		t.Fatalf("list objects %d %s", code, listed)
	}
	key := string(listed)[start+len("<Key>") : end]
	code, body, headers := s3(http.MethodGet, "/fhout/"+key, "")
	if code >= 300 {
		t.Fatalf("get object %d %s", code, body)
	}
	if string(body) != "hello-firehose" {
		t.Fatalf("s3 body %q", body)
	}
	if headers.Get("x-amz-server-side-encryption") != "aws:kms" || headers.Get("x-amz-server-side-encryption-aws-kms-key-id") != kmsARN {
		t.Fatalf("S3 encryption headers %#v", headers)
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
		"DeliveryStreamName": "delivery", "S3DestinationConfiguration": map[string]any{"BucketARN": "arn:aws:s3:::out", "RoleARN": testRoleARN, "Prefix": "logs/"},
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
			"BucketARN": "arn:aws:s3:::out", "RoleARN": testRoleARN, "Prefix": "year=!{timestamp:yyyy}/month=!{timestamp:MM}/day=!{timestamp:dd}/hour=!{timestamp:HH}/", "FileExtension": ".jsonl",
			"ErrorOutputPrefix": "errors/!{firehose:error-output-type}/",
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
			"BucketARN": "arn:aws:s3:::out", "RoleARN": testRoleARN, "Prefix": "hour=!{timestamp:HH}/", "ErrorOutputPrefix": "errors/!{firehose:error-output-type}/", "CustomTimeZone": "Asia/Tokyo",
		},
	})
	response = invoke("PutRecord", map[string]any{"DeliveryStreamName": "timezone", "Record": map[string]any{"Data": base64.StdEncoding.EncodeToString([]byte("timezone"))}})
	recordID = response.Output["RecordId"].(string)
	key = id.Account + "/" + id.Region + "/out/hour=09/timezone-1-1970-01-01-09-00-00-" + recordID
	if _, _, err := deps.Blobs.Get(context.Background(), key); err != nil {
		t.Fatal(err)
	}
	for _, timezone := range []string{"Mars/Olympus_Mons", "Etc/GMT+1"} {
		if _, err := p.Invoke(context.Background(), &spi.Request{Identity: id, Operation: "CreateDeliveryStream", Input: map[string]any{
			"DeliveryStreamName": "bad-timezone", "ExtendedS3DestinationConfiguration": map[string]any{
				"BucketARN": "arn:aws:s3:::out", "RoleARN": testRoleARN, "Prefix": "hour=!{timestamp:HH}/", "ErrorOutputPrefix": "errors/!{firehose:error-output-type}/", "CustomTimeZone": timezone,
			},
		}}); err == nil {
			t.Fatalf("accepted invalid CustomTimeZone %q", timezone)
		}
	}
	for _, extension := range []string{"jsonl", ".UPPER", "." + strings.Repeat("a", 128)} {
		if _, err := p.Invoke(context.Background(), &spi.Request{Identity: id, Operation: "CreateDeliveryStream", Input: map[string]any{
			"DeliveryStreamName": "bad-extension", "ExtendedS3DestinationConfiguration": map[string]any{
				"BucketARN": "arn:aws:s3:::out", "RoleARN": testRoleARN, "FileExtension": extension,
			},
		}}); err == nil {
			t.Fatalf("accepted invalid FileExtension %q", extension)
		}
	}
	invalidPrefixes := []map[string]any{
		{"Prefix": "year=!{timestamp:yyyy}/"},
		{"Prefix": "!{firehose:error-output-type}/", "ErrorOutputPrefix": "errors/!{firehose:error-output-type}/"},
		{"Prefix": "year=!{timestamp:yyyy}/", "ErrorOutputPrefix": "errors/!{timestamp:yyyy}/"},
		{"Prefix": "year=!{timestamp:yyyy", "ErrorOutputPrefix": "errors/!{firehose:error-output-type}/"},
		{"Prefix": "key=!{partitionKeyFromQuery:id}/", "ErrorOutputPrefix": "errors/!{firehose:error-output-type}/"},
	}
	for _, destination := range invalidPrefixes {
		destination["BucketARN"] = "arn:aws:s3:::out"
		destination["RoleARN"] = testRoleARN
		if _, err := p.Invoke(context.Background(), &spi.Request{Identity: id, Operation: "CreateDeliveryStream", Input: map[string]any{
			"DeliveryStreamName": "bad-prefix", "ExtendedS3DestinationConfiguration": destination,
		}}); err == nil {
			t.Fatalf("accepted invalid prefixes %#v", destination)
		}
	}
	invoke("CreateDeliveryStream", map[string]any{
		"DeliveryStreamName": "gzip", "ExtendedS3DestinationConfiguration": map[string]any{
			"BucketARN": "arn:aws:s3:::out", "RoleARN": testRoleARN, "Prefix": "gzip/", "CompressionFormat": "GZIP",
		},
	})
	response = invoke("PutRecord", map[string]any{"DeliveryStreamName": "gzip", "Record": map[string]any{"Data": base64.StdEncoding.EncodeToString([]byte("compressed"))}})
	recordID = response.Output["RecordId"].(string)
	key = id.Account + "/" + id.Region + "/out/gzip/1970/01/01/00/gzip-1-1970-01-01-00-00-00-" + recordID + ".gz"
	reader, _, err = deps.Blobs.Get(context.Background(), key)
	if err != nil {
		t.Fatal(err)
	}
	compressed, err := gzip.NewReader(reader)
	if err != nil {
		t.Fatal(err)
	}
	body, _ = io.ReadAll(compressed)
	_ = compressed.Close()
	_ = reader.Close()
	if string(body) != "compressed" {
		t.Fatalf("gzip body %q", body)
	}
	invoke("CreateDeliveryStream", map[string]any{
		"DeliveryStreamName": "gzip-custom", "ExtendedS3DestinationConfiguration": map[string]any{
			"BucketARN": "arn:aws:s3:::out", "RoleARN": testRoleARN, "Prefix": "gzip-custom/", "CompressionFormat": "GZIP", "FileExtension": ".custom",
		},
	})
	response = invoke("PutRecord", map[string]any{"DeliveryStreamName": "gzip-custom", "Record": map[string]any{"Data": base64.StdEncoding.EncodeToString([]byte("compressed"))}})
	recordID = response.Output["RecordId"].(string)
	key = id.Account + "/" + id.Region + "/out/gzip-custom/1970/01/01/00/gzip-custom-1-1970-01-01-00-00-00-" + recordID + ".custom"
	if _, _, err := deps.Blobs.Get(context.Background(), key); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Invoke(context.Background(), &spi.Request{Identity: id, Operation: "CreateDeliveryStream", Input: map[string]any{
		"DeliveryStreamName": "zip", "ExtendedS3DestinationConfiguration": map[string]any{
			"BucketARN": "arn:aws:s3:::out", "RoleARN": testRoleARN, "CompressionFormat": "ZIP",
		},
	}}); err == nil {
		t.Fatal("accepted unsupported ZIP compression")
	}
	if err := validatePrefixes(strings.Repeat("p", 1024), strings.Repeat("e", 1024)); err != nil {
		t.Fatalf("rejected maximum-length prefixes: %v", err)
	}
	for _, prefixes := range [][2]string{{strings.Repeat("p", 1025), ""}, {"", strings.Repeat("e", 1025)}} {
		if err := validatePrefixes(prefixes[0], prefixes[1]); err == nil {
			t.Fatal("accepted oversized prefix")
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
	for _, name := range []string{"bad/name", strings.Repeat("a", 65)} {
		if _, err := call("CreateDeliveryStream", map[string]any{"DeliveryStreamName": name, "S3DestinationConfiguration": testS3Destination()}); err == nil {
			t.Fatalf("created invalid stream %q", name)
		}
	}
	if _, err := call("PutRecord", map[string]any{"DeliveryStreamName": "missing"}); err == nil {
		t.Fatal("put to missing stream")
	}
	if _, err := call("CreateDeliveryStream", map[string]any{
		"DeliveryStreamName": "control", "ExtendedS3DestinationConfiguration": map[string]any{
			"BucketARN": "arn:aws:s3:::original", "RoleARN": testRoleARN, "Prefix": "kept/",
		},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := call("CreateDeliveryStream", map[string]any{"DeliveryStreamName": "control", "S3DestinationConfiguration": testS3Destination()}); err == nil {
		t.Fatal("recreated existing stream")
	} else if fault, ok := err.(*spi.Fault); !ok || fault.Code != "ResourceInUseException" {
		t.Fatalf("duplicate create error %v", err)
	}
	record := map[string]any{"Data": base64.StdEncoding.EncodeToString([]byte("batch"))}
	batch, err := call("PutRecordBatch", map[string]any{"DeliveryStreamName": "control", "Records": []any{record, record}})
	responses := batch.Output["RequestResponses"].([]any)
	_, hasErrorCode := responses[0].(map[string]any)["ErrorCode"]
	if err != nil || batch.Output["Encrypted"] != false || batch.Output["FailedPutCount"] != 0 || len(responses) != 2 || hasErrorCode {
		t.Fatalf("batch %#v, %v", batch, err)
	}
	if decoded, valid := recordData(map[string]any{"Data": make([]byte, maxRecordBytes)}); !valid || len(decoded) != maxRecordBytes {
		t.Fatal("rejected maximum-size record")
	}
	storedBefore, _, _ := p.col(&spi.Request{Identity: id}, "fhrec:control").List(context.Background(), "", "", 0)
	for _, input := range []map[string]any{
		{"DeliveryStreamName": "control"},
		{"DeliveryStreamName": "control", "Record": map[string]any{}},
		{"DeliveryStreamName": "control", "Record": map[string]any{"Data": "not base64"}},
		{"DeliveryStreamName": "control", "Record": map[string]any{"Data": base64.StdEncoding.EncodeToString(make([]byte, maxRecordBytes+1))}},
		{"DeliveryStreamName": "control", "Record": map[string]any{"Data": make([]byte, maxRecordBytes+1)}},
	} {
		if _, err := call("PutRecord", input); err == nil {
			t.Fatalf("accepted invalid record %#v", input)
		}
	}
	large := map[string]any{"Data": make([]byte, 900*1024)}
	tooManyRecords := make([]any, 501)
	for i := range tooManyRecords {
		tooManyRecords[i] = record
	}
	for _, records := range []any{
		[]any{},
		tooManyRecords,
		[]any{record, map[string]any{}},
		[]any{large, large, large, large, large},
	} {
		if _, err := call("PutRecordBatch", map[string]any{"DeliveryStreamName": "control", "Records": records}); err == nil {
			t.Fatalf("accepted invalid batch with %d records", len(records.([]any)))
		}
	}
	storedAfter, _, _ := p.col(&spi.Request{Identity: id}, "fhrec:control").List(context.Background(), "", "", 0)
	if len(storedAfter) != len(storedBefore) {
		t.Fatalf("invalid input wrote records: before %d after %d", len(storedBefore), len(storedAfter))
	}
	described, err := call("DescribeDeliveryStream", map[string]any{"DeliveryStreamName": "control"})
	description := described.Output["DeliveryStreamDescription"].(map[string]any)
	if err != nil || description["VersionId"] != "1" {
		t.Fatalf("describe %#v, %v", described, err)
	}
	if _, ok := described.Output["RecordCount"]; ok {
		t.Fatalf("describe leaked non-AWS RecordCount: %#v", described.Output)
	}
	destinations, ok := description["Destinations"].([]any)
	if !ok || len(destinations) != 1 {
		t.Fatalf("destinations %#v", description["Destinations"])
	}
	destination := destinations[0].(map[string]any)
	configuration := destination["ExtendedS3DestinationDescription"].(map[string]any)
	if destination["DestinationId"] != "destinationId-000000000001" || configuration["BucketARN"] != "arn:aws:s3:::original" || configuration["Prefix"] != "kept/" {
		t.Fatalf("destination description %#v", destination)
	}
	if description["ExtendedS3DestinationConfiguration"] != nil {
		t.Fatalf("describe leaked stored configuration %#v", description)
	}
	if _, err := call("UpdateDestination", map[string]any{"DeliveryStreamName": "control"}); err == nil {
		t.Fatal("updated without version and destination IDs")
	}
	if _, err := call("UpdateDestination", map[string]any{
		"DeliveryStreamName": "control", "CurrentDeliveryStreamVersionId": "1", "DestinationId": "destinationId-000000000002",
		"ExtendedS3DestinationUpdate": map[string]any{"BucketARN": "arn:aws:s3:::wrong"},
	}); err == nil {
		t.Fatal("updated unknown destination")
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
	destination = description["Destinations"].([]any)[0].(map[string]any)
	configuration = destination["ExtendedS3DestinationDescription"].(map[string]any)
	if err != nil || description["VersionId"] != "2" || configuration["BucketARN"] != "arn:aws:s3:::updated" || configuration["Prefix"] != "kept/" {
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
	if _, err := call("UntagDeliveryStream", map[string]any{"DeliveryStreamName": "control", "TagKeys": []any{"env"}}); err != nil {
		t.Fatal(err)
	}
	for _, operation := range []string{"StartDeliveryStreamEncryption", "StopDeliveryStreamEncryption", "DeleteDeliveryStream"} {
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

func TestFirehoseListDeliveryStreamsPagination(t *testing.T) {
	p := New(spitest.Deps(t))
	id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
	call := func(operation string, input map[string]any) (*spi.Response, error) {
		t.Helper()
		return p.Invoke(context.Background(), &spi.Request{Identity: id, Operation: operation, Input: input})
	}
	for _, name := range []string{"direct-10", "direct-03", "direct-00", "direct-08", "direct-01", "direct-09", "direct-04", "direct-07", "direct-02", "direct-06", "direct-05"} {
		if _, err := call("CreateDeliveryStream", map[string]any{"DeliveryStreamName": name, "DeliveryStreamType": "DirectPut", "S3DestinationConfiguration": testS3Destination()}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := call("CreateDeliveryStream", map[string]any{
		"DeliveryStreamName": "source", "DeliveryStreamType": "KinesisStreamAsSource",
		"KinesisStreamSourceConfiguration": testKinesisSource(), "S3DestinationConfiguration": testS3Destination(),
	}); err != nil {
		t.Fatal(err)
	}

	firstPage, err := call("ListDeliveryStreams", map[string]any{})
	firstNames := firstPage.Output["DeliveryStreamNames"].([]any)
	if err != nil || len(firstNames) != 10 || firstNames[0] != "direct-00" || firstNames[9] != "direct-09" || firstPage.Output["HasMoreDeliveryStreams"] != true {
		t.Fatalf("first page %#v, %v", firstPage, err)
	}
	lastPage, err := call("ListDeliveryStreams", map[string]any{"ExclusiveStartDeliveryStreamName": "direct-09"})
	lastNames := lastPage.Output["DeliveryStreamNames"].([]any)
	if err != nil || len(lastNames) != 2 || lastNames[0] != "direct-10" || lastNames[1] != "source" || lastPage.Output["HasMoreDeliveryStreams"] != false {
		t.Fatalf("last page %#v, %v", lastPage, err)
	}
	filtered, err := call("ListDeliveryStreams", map[string]any{
		"DeliveryStreamType": "DirectPut", "ExclusiveStartDeliveryStreamName": "direct-07", "Limit": float64(2),
	})
	filteredNames := filtered.Output["DeliveryStreamNames"].([]any)
	if err != nil || len(filteredNames) != 2 || filteredNames[0] != "direct-08" || filteredNames[1] != "direct-09" || filtered.Output["HasMoreDeliveryStreams"] != true {
		t.Fatalf("filtered page %#v, %v", filtered, err)
	}
	source, err := call("ListDeliveryStreams", map[string]any{"DeliveryStreamType": "KinesisStreamAsSource"})
	if err != nil || len(source.Output["DeliveryStreamNames"].([]any)) != 1 || source.Output["DeliveryStreamNames"].([]any)[0] != "source" {
		t.Fatalf("source page %#v, %v", source, err)
	}

	for _, input := range []map[string]any{
		{"Limit": 0}, {"Limit": 10001}, {"Limit": 1.5}, {"Limit": "2"},
		{"ExclusiveStartDeliveryStreamName": "bad/name"}, {"DeliveryStreamType": "Unknown"},
	} {
		if _, err := call("ListDeliveryStreams", input); err == nil {
			t.Fatalf("accepted invalid list input %#v", input)
		}
	}
}

func TestFirehoseTagsMergeRemoveAndPaginate(t *testing.T) {
	p := New(spitest.Deps(t))
	id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
	call := func(operation string, input map[string]any) (*spi.Response, error) {
		t.Helper()
		return p.Invoke(context.Background(), &spi.Request{Identity: id, Operation: operation, Input: input})
	}
	if _, err := call("CreateDeliveryStream", map[string]any{
		"DeliveryStreamName": "tagged", "Tags": []any{
			map[string]any{"Key": "z", "Value": "old"}, map[string]any{"Key": "a", "Value": "first"},
		},
		"S3DestinationConfiguration": testS3Destination(),
	}); err != nil {
		t.Fatal(err)
	}
	firstPage, err := call("ListTagsForDeliveryStream", map[string]any{"DeliveryStreamName": "tagged", "Limit": float64(1)})
	firstTags := firstPage.Output["Tags"].([]any)
	if err != nil || len(firstTags) != 1 || firstTags[0].(map[string]any)["Key"] != "a" || firstPage.Output["HasMoreTags"] != true {
		t.Fatalf("first tag page %#v, %v", firstPage, err)
	}
	if _, err := call("TagDeliveryStream", map[string]any{
		"DeliveryStreamName": "tagged", "Tags": []any{
			map[string]any{"Key": "z", "Value": "new"}, map[string]any{"Key": "m", "Value": "middle"},
		},
	}); err != nil {
		t.Fatal(err)
	}
	nextPage, err := call("ListTagsForDeliveryStream", map[string]any{
		"DeliveryStreamName": "tagged", "ExclusiveStartTagKey": "a", "Limit": 1,
	})
	nextTags := nextPage.Output["Tags"].([]any)
	if err != nil || len(nextTags) != 1 || nextTags[0].(map[string]any)["Key"] != "m" || nextPage.Output["HasMoreTags"] != true {
		t.Fatalf("next tag page %#v, %v", nextPage, err)
	}
	if _, err := call("UntagDeliveryStream", map[string]any{"DeliveryStreamName": "tagged", "TagKeys": []any{"m", "missing"}}); err != nil {
		t.Fatal(err)
	}
	remaining, err := call("ListTagsForDeliveryStream", map[string]any{"DeliveryStreamName": "tagged"})
	remainingTags := remaining.Output["Tags"].([]any)
	if err != nil || len(remainingTags) != 2 || remainingTags[0].(map[string]any)["Key"] != "a" || remainingTags[1].(map[string]any)["Value"] != "new" || remaining.Output["HasMoreTags"] != false {
		t.Fatalf("remaining tags %#v, %v", remaining, err)
	}

	tooMany := make([]any, 49)
	for i := range tooMany {
		tooMany[i] = map[string]any{"Key": fmt.Sprintf("k%02d", i), "Value": "value"}
	}
	if _, err := call("TagDeliveryStream", map[string]any{"DeliveryStreamName": "tagged", "Tags": tooMany}); err == nil {
		t.Fatal("exceeded 50-tag stream limit")
	} else if fault, ok := err.(*spi.Fault); !ok || fault.Code != "LimitExceededException" {
		t.Fatalf("tag limit error %v", err)
	}
	for _, input := range []map[string]any{
		{"DeliveryStreamName": "tagged", "Tags": []any{}},
		{"DeliveryStreamName": "tagged", "Tags": []any{map[string]any{"Key": "aws:reserved"}}},
		{"DeliveryStreamName": "tagged", "Tags": []any{map[string]any{"Key": "bad", "Value": "bad!"}}},
		{"DeliveryStreamName": "tagged", "Tags": []any{map[string]any{"Key": "same"}, map[string]any{"Key": "same"}}},
	} {
		if _, err := call("TagDeliveryStream", input); err == nil {
			t.Fatalf("accepted invalid tags %#v", input)
		}
	}
	for _, input := range []map[string]any{
		{"DeliveryStreamName": "tagged", "TagKeys": []any{}},
		{"DeliveryStreamName": "tagged", "TagKeys": []any{"aws:reserved"}},
	} {
		if _, err := call("UntagDeliveryStream", input); err == nil {
			t.Fatalf("accepted invalid tag keys %#v", input)
		}
	}
	for _, input := range []map[string]any{
		{"DeliveryStreamName": "tagged", "Limit": 0},
		{"DeliveryStreamName": "tagged", "Limit": 51},
		{"DeliveryStreamName": "tagged", "ExclusiveStartTagKey": "aws:reserved"},
	} {
		if _, err := call("ListTagsForDeliveryStream", input); err == nil {
			t.Fatalf("accepted invalid tag list %#v", input)
		}
	}
	for _, operation := range []string{"TagDeliveryStream", "UntagDeliveryStream", "ListTagsForDeliveryStream"} {
		if _, err := call(operation, map[string]any{"DeliveryStreamName": "missing"}); err == nil {
			t.Fatalf("%s accepted missing stream", operation)
		}
	}
	if _, err := call("DeleteDeliveryStream", map[string]any{"DeliveryStreamName": "tagged"}); err != nil {
		t.Fatal(err)
	}
	if _, err := call("CreateDeliveryStream", map[string]any{"DeliveryStreamName": "tagged", "S3DestinationConfiguration": testS3Destination()}); err != nil {
		t.Fatal(err)
	}
	cleared, err := call("ListTagsForDeliveryStream", map[string]any{"DeliveryStreamName": "tagged"})
	if err != nil || len(cleared.Output["Tags"].([]any)) != 0 {
		t.Fatalf("tags survived stream recreation %#v, %v", cleared, err)
	}
}

func TestFirehoseEncryptionState(t *testing.T) {
	p := New(spitest.Deps(t))
	id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
	call := func(operation string, input map[string]any) (*spi.Response, error) {
		t.Helper()
		return p.Invoke(context.Background(), &spi.Request{Identity: id, Operation: operation, Input: input})
	}
	describeEncryption := func(name string) map[string]any {
		t.Helper()
		response, err := call("DescribeDeliveryStream", map[string]any{"DeliveryStreamName": name})
		if err != nil {
			t.Fatal(err)
		}
		description := response.Output["DeliveryStreamDescription"].(map[string]any)
		return description["DeliveryStreamEncryptionConfiguration"].(map[string]any)
	}
	record := map[string]any{"Data": base64.StdEncoding.EncodeToString([]byte("encrypted"))}

	if _, err := call("CreateDeliveryStream", map[string]any{"DeliveryStreamName": "plain", "S3DestinationConfiguration": testS3Destination()}); err != nil {
		t.Fatal(err)
	}
	if encryption := describeEncryption("plain"); encryption["Status"] != "DISABLED" || encryption["KeyType"] != nil {
		t.Fatalf("plain encryption %#v", encryption)
	}
	put, err := call("PutRecord", map[string]any{"DeliveryStreamName": "plain", "Record": record})
	if err != nil || put.Output["Encrypted"] != false {
		t.Fatalf("plain put %#v, %v", put, err)
	}
	if _, err := call("StartDeliveryStreamEncryption", map[string]any{"DeliveryStreamName": "plain"}); err != nil {
		t.Fatal(err)
	}
	if encryption := describeEncryption("plain"); encryption["Status"] != "ENABLED" || encryption["KeyType"] != "AWS_OWNED_CMK" || encryption["KeyARN"] != nil {
		t.Fatalf("AWS-owned encryption %#v", encryption)
	}
	batch, err := call("PutRecordBatch", map[string]any{"DeliveryStreamName": "plain", "Records": []any{record}})
	if err != nil || batch.Output["Encrypted"] != true {
		t.Fatalf("encrypted batch %#v, %v", batch, err)
	}

	keyARN := "arn:aws:kms:us-east-1:123456789012:key/example_key-1"
	configuration := map[string]any{"KeyType": "CUSTOMER_MANAGED_CMK", "KeyARN": keyARN}
	if _, err := call("StartDeliveryStreamEncryption", map[string]any{
		"DeliveryStreamName": "plain", "DeliveryStreamEncryptionConfigurationInput": configuration,
	}); err != nil {
		t.Fatal(err)
	}
	if encryption := describeEncryption("plain"); encryption["Status"] != "ENABLED" || encryption["KeyType"] != "CUSTOMER_MANAGED_CMK" || encryption["KeyARN"] != keyARN {
		t.Fatalf("customer encryption %#v", encryption)
	}
	if _, err := call("StopDeliveryStreamEncryption", map[string]any{"DeliveryStreamName": "plain"}); err != nil {
		t.Fatal(err)
	}
	if encryption := describeEncryption("plain"); encryption["Status"] != "DISABLED" || encryption["KeyType"] != nil || encryption["KeyARN"] != nil {
		t.Fatalf("stopped encryption %#v", encryption)
	}

	if _, err := call("CreateDeliveryStream", map[string]any{
		"DeliveryStreamName": "created-encrypted", "DeliveryStreamEncryptionConfigurationInput": configuration,
		"S3DestinationConfiguration": testS3Destination(),
	}); err != nil {
		t.Fatal(err)
	}
	if encryption := describeEncryption("created-encrypted"); encryption["Status"] != "ENABLED" || encryption["KeyARN"] != keyARN {
		t.Fatalf("create encryption %#v", encryption)
	}
	if _, err := call("CreateDeliveryStream", map[string]any{
		"DeliveryStreamName": "source", "DeliveryStreamType": "KinesisStreamAsSource",
		"KinesisStreamSourceConfiguration": testKinesisSource(), "S3DestinationConfiguration": testS3Destination(),
	}); err != nil {
		t.Fatal(err)
	}
	for _, input := range []map[string]any{
		{"DeliveryStreamName": "invalid-type", "DeliveryStreamType": "Unknown", "S3DestinationConfiguration": testS3Destination()},
		{"DeliveryStreamName": "encrypted-source", "DeliveryStreamType": "KinesisStreamAsSource", "KinesisStreamSourceConfiguration": testKinesisSource(), "S3DestinationConfiguration": testS3Destination(), "DeliveryStreamEncryptionConfigurationInput": configuration},
	} {
		if _, err := call("CreateDeliveryStream", input); err == nil {
			t.Fatalf("accepted invalid create input %#v", input)
		}
	}
	if _, err := call("StartDeliveryStreamEncryption", map[string]any{"DeliveryStreamName": "source"}); err == nil {
		t.Fatal("encrypted non-DirectPut stream")
	}
	for _, input := range []any{
		map[string]any{},
		map[string]any{"KeyType": "UNKNOWN"},
		map[string]any{"KeyType": "AWS_OWNED_CMK", "KeyARN": keyARN},
		map[string]any{"KeyType": "CUSTOMER_MANAGED_CMK"},
		map[string]any{"KeyType": "CUSTOMER_MANAGED_CMK", "KeyARN": "not-an-arn"},
		map[string]any{"KeyType": "CUSTOMER_MANAGED_CMK", "KeyARN": "arn:aws:kms:us-east-1:123456789012:key/" + strings.Repeat("a", 480)},
	} {
		if _, err := call("StartDeliveryStreamEncryption", map[string]any{
			"DeliveryStreamName": "plain", "DeliveryStreamEncryptionConfigurationInput": input,
		}); err == nil {
			t.Fatalf("accepted invalid encryption input %#v", input)
		}
	}
	for _, operation := range []string{"StartDeliveryStreamEncryption", "StopDeliveryStreamEncryption"} {
		if _, err := call(operation, map[string]any{"DeliveryStreamName": "missing"}); err == nil {
			t.Fatalf("%s accepted missing stream", operation)
		}
	}
}

func TestFirehoseLifecycleMetadataAndValidation(t *testing.T) {
	deps := spitest.Deps(t)
	p := New(deps)
	id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
	call := func(operation string, input map[string]any) (*spi.Response, error) {
		t.Helper()
		return p.Invoke(context.Background(), &spi.Request{Identity: id, Operation: operation, Input: input})
	}
	describe := func() map[string]any {
		t.Helper()
		response, err := call("DescribeDeliveryStream", map[string]any{"DeliveryStreamName": "lifecycle"})
		if err != nil {
			t.Fatal(err)
		}
		return response.Output["DeliveryStreamDescription"].(map[string]any)
	}

	if _, err := call("CreateDeliveryStream", map[string]any{"DeliveryStreamName": "lifecycle", "S3DestinationConfiguration": testS3Destination()}); err != nil {
		t.Fatal(err)
	}
	created := describe()
	if created["CreateTimestamp"] != float64(0) || created["LastUpdateTimestamp"] != float64(0) {
		t.Fatalf("create timestamps %#v", created)
	}
	if err := deps.Clock.Advance(1500 * time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if _, err := call("UpdateDestination", map[string]any{
		"DeliveryStreamName": "lifecycle", "CurrentDeliveryStreamVersionId": "1", "DestinationId": destinationID,
	}); err != nil {
		t.Fatal(err)
	}
	updated := describe()
	if updated["CreateTimestamp"] != float64(0) || updated["LastUpdateTimestamp"] != 1.5 {
		t.Fatalf("update timestamps %#v", updated)
	}
	if err := deps.Clock.Advance(500 * time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if _, err := call("StartDeliveryStreamEncryption", map[string]any{"DeliveryStreamName": "lifecycle"}); err != nil {
		t.Fatal(err)
	}
	if encrypted := describe(); encrypted["CreateTimestamp"] != float64(0) || encrypted["LastUpdateTimestamp"] != float64(2) {
		t.Fatalf("encryption timestamp %#v", encrypted)
	}

	for _, operation := range p.Operations() {
		if operation == "ListDeliveryStreams" {
			continue
		}
		if _, err := call(operation, map[string]any{"DeliveryStreamName": "bad/name"}); err == nil {
			t.Fatalf("%s accepted invalid stream name", operation)
		}
	}
	if _, err := call("DeleteDeliveryStream", map[string]any{"DeliveryStreamName": "missing"}); err == nil {
		t.Fatal("deleted missing stream")
	}
	if _, err := call("DeleteDeliveryStream", map[string]any{"DeliveryStreamName": "lifecycle"}); err != nil {
		t.Fatal(err)
	}
	if _, err := call("DeleteDeliveryStream", map[string]any{"DeliveryStreamName": "lifecycle"}); err == nil {
		t.Fatal("deleted stream twice")
	}
}

func TestFirehoseCreateConfiguration(t *testing.T) {
	p := New(spitest.Deps(t))
	id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
	call := func(operation string, input map[string]any) (*spi.Response, error) {
		t.Helper()
		return p.Invoke(context.Background(), &spi.Request{Identity: id, Operation: operation, Input: input})
	}

	if _, err := call("CreateDeliveryStream", map[string]any{
		"DeliveryStreamName": "source", "DeliveryStreamType": "KinesisStreamAsSource",
		"KinesisStreamSourceConfiguration": testKinesisSource(), "S3DestinationConfiguration": testS3Destination(),
	}); err != nil {
		t.Fatal(err)
	}
	response, err := call("DescribeDeliveryStream", map[string]any{"DeliveryStreamName": "source"})
	if err != nil {
		t.Fatal(err)
	}
	description := response.Output["DeliveryStreamDescription"].(map[string]any)
	source := description["Source"].(map[string]any)["KinesisStreamSourceDescription"].(map[string]any)
	if source["KinesisStreamARN"] != testKinesisSource()["KinesisStreamARN"] || source["RoleARN"] != testRoleARN || source["DeliveryStartTimestamp"] != float64(0) || description["KinesisStreamSourceConfiguration"] != nil {
		t.Fatalf("source description %#v", description)
	}
	if _, err := call("UpdateDestination", map[string]any{
		"DeliveryStreamName": "source", "CurrentDeliveryStreamVersionId": "1", "DestinationId": destinationID,
		"ExtendedS3DestinationUpdate": testS3Destination(),
	}); err == nil {
		t.Fatal("changed destination type")
	}

	for _, test := range []struct {
		name  string
		input map[string]any
		code  string
	}{
		{"missing destination", map[string]any{"DeliveryStreamName": "no-destination"}, "InvalidArgumentException"},
		{"multiple destinations", map[string]any{"DeliveryStreamName": "two-destinations", "S3DestinationConfiguration": testS3Destination(), "ExtendedS3DestinationConfiguration": testS3Destination()}, "InvalidArgumentException"},
		{"missing bucket ARN", map[string]any{"DeliveryStreamName": "no-bucket", "S3DestinationConfiguration": map[string]any{"RoleARN": testRoleARN}}, "InvalidArgumentException"},
		{"malformed bucket ARN", map[string]any{"DeliveryStreamName": "bad-bucket", "S3DestinationConfiguration": map[string]any{"BucketARN": "bucket", "RoleARN": testRoleARN}}, "InvalidArgumentException"},
		{"long bucket ARN", map[string]any{"DeliveryStreamName": "long-bucket", "S3DestinationConfiguration": map[string]any{"BucketARN": "arn:" + strings.Repeat("a", 2048) + ":s3:::out", "RoleARN": testRoleARN}}, "InvalidArgumentException"},
		{"missing role ARN", map[string]any{"DeliveryStreamName": "no-role", "S3DestinationConfiguration": map[string]any{"BucketARN": "arn:aws:s3:::out"}}, "InvalidArgumentException"},
		{"malformed role ARN", map[string]any{"DeliveryStreamName": "bad-role", "S3DestinationConfiguration": map[string]any{"BucketARN": "arn:aws:s3:::out", "RoleARN": "role"}}, "InvalidArgumentException"},
		{"long role ARN", map[string]any{"DeliveryStreamName": "long-role", "S3DestinationConfiguration": map[string]any{"BucketARN": "arn:aws:s3:::out", "RoleARN": "arn:aws:iam::123456789012:role/" + strings.Repeat("a", 500)}}, "InvalidArgumentException"},
		{"unsupported destination", map[string]any{"DeliveryStreamName": "redshift", "RedshiftDestinationConfiguration": map[string]any{}}, "MirrorNotImplemented"},
		{"direct put with Kinesis source", map[string]any{"DeliveryStreamName": "direct-source", "KinesisStreamSourceConfiguration": testKinesisSource(), "S3DestinationConfiguration": testS3Destination()}, "InvalidArgumentException"},
		{"missing Kinesis source", map[string]any{"DeliveryStreamName": "no-source", "DeliveryStreamType": "KinesisStreamAsSource", "S3DestinationConfiguration": testS3Destination()}, "InvalidArgumentException"},
		{"malformed Kinesis ARN", map[string]any{"DeliveryStreamName": "bad-source", "DeliveryStreamType": "KinesisStreamAsSource", "KinesisStreamSourceConfiguration": map[string]any{"KinesisStreamARN": "stream", "RoleARN": testRoleARN}, "S3DestinationConfiguration": testS3Destination()}, "InvalidArgumentException"},
		{"long Kinesis ARN", map[string]any{"DeliveryStreamName": "long-source", "DeliveryStreamType": "KinesisStreamAsSource", "KinesisStreamSourceConfiguration": map[string]any{"KinesisStreamARN": "arn:aws:kinesis:us-east-1:123456789012:stream/" + strings.Repeat("a", 500), "RoleARN": testRoleARN}, "S3DestinationConfiguration": testS3Destination()}, "InvalidArgumentException"},
		{"malformed Kinesis role", map[string]any{"DeliveryStreamName": "bad-source-role", "DeliveryStreamType": "KinesisStreamAsSource", "KinesisStreamSourceConfiguration": map[string]any{"KinesisStreamARN": testKinesisSource()["KinesisStreamARN"], "RoleARN": "role"}, "S3DestinationConfiguration": testS3Destination()}, "InvalidArgumentException"},
		{"unsupported MSK source", map[string]any{"DeliveryStreamName": "msk", "DeliveryStreamType": "MSKAsSource", "S3DestinationConfiguration": testS3Destination()}, "MirrorNotImplemented"},
		{"unsupported database source", map[string]any{"DeliveryStreamName": "database", "DeliveryStreamType": "DatabaseAsSource", "S3DestinationConfiguration": testS3Destination()}, "MirrorNotImplemented"},
	} {
		_, err := call("CreateDeliveryStream", test.input)
		fault, ok := err.(*spi.Fault)
		if !ok || fault.Code != test.code {
			t.Errorf("%s: error %v, want %s", test.name, err, test.code)
		}
	}
}

func TestFirehoseDirectPutSourceConfiguration(t *testing.T) {
	p := New(spitest.Deps(t))
	id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
	call := func(operation string, input map[string]any) (*spi.Response, error) {
		t.Helper()
		return p.Invoke(context.Background(), &spi.Request{Identity: id, Operation: operation, Input: input})
	}

	if _, err := call("CreateDeliveryStream", map[string]any{
		"DeliveryStreamName": "direct", "DirectPutSourceConfiguration": map[string]any{"ThroughputHintInMBs": float64(100)},
		"S3DestinationConfiguration": testS3Destination(),
	}); err != nil {
		t.Fatal(err)
	}
	response, err := call("DescribeDeliveryStream", map[string]any{"DeliveryStreamName": "direct"})
	if err != nil {
		t.Fatal(err)
	}
	description := response.Output["DeliveryStreamDescription"].(map[string]any)
	source := description["Source"].(map[string]any)["DirectPutSourceDescription"].(map[string]any)
	if source["ThroughputHintInMBs"] != float64(100) || description["DirectPutSourceConfiguration"] != nil {
		t.Fatalf("direct source description %#v", description)
	}

	for _, input := range []map[string]any{
		{"DeliveryStreamName": "wrong-type", "DirectPutSourceConfiguration": "invalid", "S3DestinationConfiguration": testS3Destination()},
		{"DeliveryStreamName": "missing-throughput", "DirectPutSourceConfiguration": map[string]any{}, "S3DestinationConfiguration": testS3Destination()},
		{"DeliveryStreamName": "zero-throughput", "DirectPutSourceConfiguration": map[string]any{"ThroughputHintInMBs": 0}, "S3DestinationConfiguration": testS3Destination()},
		{"DeliveryStreamName": "high-throughput", "DirectPutSourceConfiguration": map[string]any{"ThroughputHintInMBs": 101}, "S3DestinationConfiguration": testS3Destination()},
		{"DeliveryStreamName": "fractional-throughput", "DirectPutSourceConfiguration": map[string]any{"ThroughputHintInMBs": 1.5}, "S3DestinationConfiguration": testS3Destination()},
		{"DeliveryStreamName": "string-throughput", "DirectPutSourceConfiguration": map[string]any{"ThroughputHintInMBs": "1"}, "S3DestinationConfiguration": testS3Destination()},
		{"DeliveryStreamName": "kinesis-direct", "DeliveryStreamType": "KinesisStreamAsSource", "DirectPutSourceConfiguration": map[string]any{"ThroughputHintInMBs": 1}, "KinesisStreamSourceConfiguration": testKinesisSource(), "S3DestinationConfiguration": testS3Destination()},
	} {
		if _, err := call("CreateDeliveryStream", input); err == nil {
			t.Errorf("accepted invalid DirectPut source %#v", input)
		}
	}
}

func TestFirehoseBufferingHints(t *testing.T) {
	p := New(spitest.Deps(t))
	id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
	call := func(operation string, input map[string]any) (*spi.Response, error) {
		t.Helper()
		return p.Invoke(context.Background(), &spi.Request{Identity: id, Operation: operation, Input: input})
	}
	destination := testS3Destination()
	destination["BufferingHints"] = map[string]any{"IntervalInSeconds": float64(0), "SizeInMBs": float64(128)}
	if _, err := call("CreateDeliveryStream", map[string]any{"DeliveryStreamName": "buffered", "S3DestinationConfiguration": destination}); err != nil {
		t.Fatal(err)
	}
	response, err := call("DescribeDeliveryStream", map[string]any{"DeliveryStreamName": "buffered"})
	if err != nil {
		t.Fatal(err)
	}
	description := response.Output["DeliveryStreamDescription"].(map[string]any)
	hints := description["Destinations"].([]any)[0].(map[string]any)["S3DestinationDescription"].(map[string]any)["BufferingHints"].(map[string]any)
	if hints["IntervalInSeconds"] != float64(0) || hints["SizeInMBs"] != float64(128) {
		t.Fatalf("buffering hints %#v", hints)
	}

	invalid := []any{
		"invalid",
		map[string]any{"IntervalInSeconds": 1},
		map[string]any{"SizeInMBs": 1},
		map[string]any{"IntervalInSeconds": -1, "SizeInMBs": 1},
		map[string]any{"IntervalInSeconds": 901, "SizeInMBs": 1},
		map[string]any{"IntervalInSeconds": 1.5, "SizeInMBs": 1},
		map[string]any{"IntervalInSeconds": "1", "SizeInMBs": 1},
		map[string]any{"IntervalInSeconds": 1, "SizeInMBs": 0},
		map[string]any{"IntervalInSeconds": 1, "SizeInMBs": 129},
		map[string]any{"IntervalInSeconds": 1, "SizeInMBs": 1.5},
		map[string]any{"IntervalInSeconds": 1, "SizeInMBs": "1"},
	}
	for i, hints := range invalid {
		destination := testS3Destination()
		destination["BufferingHints"] = hints
		if _, err := call("CreateDeliveryStream", map[string]any{"DeliveryStreamName": fmt.Sprintf("invalid-buffer-%d", i), "S3DestinationConfiguration": destination}); err == nil {
			t.Errorf("accepted invalid buffering hints %#v", hints)
		}
	}
	if _, err := call("UpdateDestination", map[string]any{
		"DeliveryStreamName": "buffered", "CurrentDeliveryStreamVersionId": "1", "DestinationId": destinationID,
		"S3DestinationUpdate": map[string]any{"BufferingHints": map[string]any{"SizeInMBs": 1}},
	}); err == nil {
		t.Fatal("accepted invalid buffering hints on update")
	}
}

func TestFirehoseS3Backup(t *testing.T) {
	deps := spitest.Deps(t)
	p := New(deps)
	id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
	call := func(operation string, input map[string]any) (*spi.Response, error) {
		t.Helper()
		return p.Invoke(context.Background(), &spi.Request{Identity: id, Operation: operation, Input: input})
	}
	backup := map[string]any{
		"BucketARN": "arn:aws:s3:::backup", "RoleARN": testRoleARN, "Prefix": "raw/", "CompressionFormat": "GZIP",
	}
	if _, err := call("CreateDeliveryStream", map[string]any{
		"DeliveryStreamName": "backed-up", "ExtendedS3DestinationConfiguration": map[string]any{
			"BucketARN": "arn:aws:s3:::primary", "RoleARN": testRoleARN, "Prefix": "main/",
			"S3BackupMode": "Enabled", "S3BackupConfiguration": backup,
		},
	}); err != nil {
		t.Fatal(err)
	}
	put, err := call("PutRecord", map[string]any{
		"DeliveryStreamName": "backed-up", "Record": map[string]any{"Data": base64.StdEncoding.EncodeToString([]byte("backup payload"))},
	})
	if err != nil {
		t.Fatal(err)
	}
	recordID := put.Output["RecordId"].(string)
	primaryKey := id.Account + "/" + id.Region + "/primary/main/1970/01/01/00/backed-up-1-1970-01-01-00-00-00-" + recordID
	reader, _, err := deps.Blobs.Get(context.Background(), primaryKey)
	if err != nil {
		t.Fatal(err)
	}
	primaryBody, _ := io.ReadAll(reader)
	reader.Close()
	backupKey := id.Account + "/" + id.Region + "/backup/raw/1970/01/01/00/backed-up-1-1970-01-01-00-00-00-" + recordID + ".gz"
	reader, _, err = deps.Blobs.Get(context.Background(), backupKey)
	if err != nil {
		t.Fatal(err)
	}
	compressed, _ := io.ReadAll(reader)
	reader.Close()
	gzipReader, err := gzip.NewReader(bytes.NewReader(compressed))
	if err != nil {
		t.Fatal(err)
	}
	backupBody, _ := io.ReadAll(gzipReader)
	gzipReader.Close()
	if string(primaryBody) != "backup payload" || string(backupBody) != "backup payload" {
		t.Fatalf("primary %q backup %q", primaryBody, backupBody)
	}

	if _, err := call("UpdateDestination", map[string]any{
		"DeliveryStreamName": "backed-up", "CurrentDeliveryStreamVersionId": "1", "DestinationId": destinationID,
		"ExtendedS3DestinationUpdate": map[string]any{"S3BackupMode": "Disabled"},
	}); err == nil {
		t.Fatal("disabled an enabled S3 backup")
	}
	if _, err := call("UpdateDestination", map[string]any{
		"DeliveryStreamName": "backed-up", "CurrentDeliveryStreamVersionId": "1", "DestinationId": destinationID,
		"ExtendedS3DestinationUpdate": map[string]any{"S3BackupUpdate": map[string]any{"Prefix": "updated/"}},
	}); err != nil {
		t.Fatal(err)
	}
	response, err := call("DescribeDeliveryStream", map[string]any{"DeliveryStreamName": "backed-up"})
	if err != nil {
		t.Fatal(err)
	}
	description := response.Output["DeliveryStreamDescription"].(map[string]any)["Destinations"].([]any)[0].(map[string]any)["ExtendedS3DestinationDescription"].(map[string]any)
	backupDescription := description["S3BackupDescription"].(map[string]any)
	if description["S3BackupMode"] != "Enabled" || backupDescription["Prefix"] != "updated/" || description["S3BackupConfiguration"] != nil || description["S3BackupUpdate"] != nil {
		t.Fatalf("backup description %#v", description)
	}
	put, err = call("PutRecord", map[string]any{
		"DeliveryStreamName": "backed-up", "Record": map[string]any{"Data": base64.StdEncoding.EncodeToString([]byte("updated backup"))},
	})
	if err != nil {
		t.Fatal(err)
	}
	updatedBackupKey := id.Account + "/" + id.Region + "/backup/updated/1970/01/01/00/backed-up-2-1970-01-01-00-00-00-" + put.Output["RecordId"].(string) + ".gz"
	if _, _, err := deps.Blobs.Get(context.Background(), updatedBackupKey); err != nil {
		t.Fatal(err)
	}

	for i, destination := range []map[string]any{
		{"BucketARN": "arn:aws:s3:::out", "RoleARN": testRoleARN, "S3BackupMode": "Unknown"},
		{"BucketARN": "arn:aws:s3:::out", "RoleARN": testRoleARN, "S3BackupMode": "Enabled"},
		{"BucketARN": "arn:aws:s3:::out", "RoleARN": testRoleARN, "S3BackupConfiguration": "invalid"},
		{"BucketARN": "arn:aws:s3:::out", "RoleARN": testRoleARN, "S3BackupConfiguration": map[string]any{"BucketARN": "arn:aws:s3:::backup"}},
	} {
		if _, err := call("CreateDeliveryStream", map[string]any{"DeliveryStreamName": fmt.Sprintf("bad-backup-%d", i), "ExtendedS3DestinationConfiguration": destination}); err == nil {
			t.Errorf("accepted invalid backup %#v", destination)
		}
	}
}

func TestFirehoseDestinationDescriptionDefaults(t *testing.T) {
	p := New(spitest.Deps(t))
	id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
	for _, test := range []struct {
		name, configuration, description string
	}{
		{"s3-defaults", "S3DestinationConfiguration", "S3DestinationDescription"},
		{"extended-defaults", "ExtendedS3DestinationConfiguration", "ExtendedS3DestinationDescription"},
	} {
		input := map[string]any{"DeliveryStreamName": test.name, test.configuration: testS3Destination()}
		if _, err := p.Invoke(context.Background(), &spi.Request{Identity: id, Operation: "CreateDeliveryStream", Input: input}); err != nil {
			t.Fatal(err)
		}
		response, err := p.Invoke(context.Background(), &spi.Request{Identity: id, Operation: "DescribeDeliveryStream", Input: map[string]any{"DeliveryStreamName": test.name}})
		if err != nil {
			t.Fatal(err)
		}
		description := response.Output["DeliveryStreamDescription"].(map[string]any)["Destinations"].([]any)[0].(map[string]any)[test.description].(map[string]any)
		hints := description["BufferingHints"].(map[string]any)
		encryption := description["EncryptionConfiguration"].(map[string]any)
		if hints["IntervalInSeconds"] != 300 || hints["SizeInMBs"] != 5 || description["CompressionFormat"] != "UNCOMPRESSED" || encryption["NoEncryptionConfig"] != "NoEncryption" || (test.configuration == "ExtendedS3DestinationConfiguration" && description["S3BackupMode"] != "Disabled") {
			t.Errorf("%s defaults %#v", test.name, description)
		}
	}
}

func TestFirehoseDestinationEncryption(t *testing.T) {
	p := New(spitest.Deps(t))
	id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
	call := func(name string, encryption any) (*spi.Response, error) {
		t.Helper()
		destination := testS3Destination()
		destination["EncryptionConfiguration"] = encryption
		return p.Invoke(context.Background(), &spi.Request{Identity: id, Operation: "CreateDeliveryStream", Input: map[string]any{
			"DeliveryStreamName": name, "S3DestinationConfiguration": destination,
		}})
	}

	for _, test := range []struct {
		name       string
		encryption map[string]any
	}{
		{"unencrypted", map[string]any{"NoEncryptionConfig": "NoEncryption"}},
		{"kms-alias", map[string]any{"KMSEncryptionConfig": map[string]any{"AWSKMSKeyARN": "arn:aws:kms:us-east-1:123456789012:alias/firehose"}}},
	} {
		if _, err := call(test.name, test.encryption); err != nil {
			t.Fatal(err)
		}
		response, err := p.Invoke(context.Background(), &spi.Request{Identity: id, Operation: "DescribeDeliveryStream", Input: map[string]any{"DeliveryStreamName": test.name}})
		if err != nil {
			t.Fatal(err)
		}
		described := response.Output["DeliveryStreamDescription"].(map[string]any)["Destinations"].([]any)[0].(map[string]any)["S3DestinationDescription"].(map[string]any)["EncryptionConfiguration"]
		if !reflect.DeepEqual(described, test.encryption) {
			t.Errorf("%s encryption %#v", test.name, described)
		}
	}

	for i, encryption := range []any{
		"invalid",
		map[string]any{},
		map[string]any{"NoEncryptionConfig": "invalid"},
		map[string]any{"NoEncryptionConfig": "NoEncryption", "KMSEncryptionConfig": map[string]any{"AWSKMSKeyARN": "arn:aws:kms:us-east-1:123456789012:key/firehose"}},
		map[string]any{"KMSEncryptionConfig": "invalid"},
		map[string]any{"KMSEncryptionConfig": map[string]any{}},
		map[string]any{"KMSEncryptionConfig": map[string]any{"AWSKMSKeyARN": "key"}},
		map[string]any{"KMSEncryptionConfig": map[string]any{"AWSKMSKeyARN": "arn:aws:kms:us-east-1:123:key/firehose"}},
		map[string]any{"KMSEncryptionConfig": map[string]any{"AWSKMSKeyARN": "arn:aws:kms:us-west-2:123456789012:key/firehose"}},
		map[string]any{"KMSEncryptionConfig": map[string]any{"AWSKMSKeyARN": "arn:aws:kms:us-east-1:123456789012:key/" + strings.Repeat("a", 480)}},
	} {
		if _, err := call(fmt.Sprintf("invalid-encryption-%d", i), encryption); err == nil {
			t.Errorf("accepted invalid destination encryption %#v", encryption)
		}
	}
}

func TestFirehoseDescribeDestinationPagination(t *testing.T) {
	p := New(spitest.Deps(t))
	id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
	call := func(input map[string]any) (*spi.Response, error) {
		t.Helper()
		return p.Invoke(context.Background(), &spi.Request{Identity: id, Operation: "DescribeDeliveryStream", Input: input})
	}
	if _, err := p.Invoke(context.Background(), &spi.Request{Identity: id, Operation: "CreateDeliveryStream", Input: map[string]any{
		"DeliveryStreamName": "described", "S3DestinationConfiguration": testS3Destination(),
	}}); err != nil {
		t.Fatal(err)
	}
	destinations := func(input map[string]any) []any {
		t.Helper()
		input["DeliveryStreamName"] = "described"
		response, err := call(input)
		if err != nil {
			t.Fatal(err)
		}
		description := response.Output["DeliveryStreamDescription"].(map[string]any)
		if description["HasMoreDestinations"] != false {
			t.Fatalf("HasMoreDestinations %#v", description)
		}
		return description["Destinations"].([]any)
	}
	if page := destinations(map[string]any{}); len(page) != 1 || page[0].(map[string]any)["DestinationId"] != destinationID {
		t.Fatalf("default page %#v", page)
	}
	if page := destinations(map[string]any{"Limit": float64(1), "ExclusiveStartDestinationId": "destinationId-000000000000"}); len(page) != 1 {
		t.Fatalf("page before destination %#v", page)
	}
	for _, after := range []string{destinationID, "destinationId-000000000002"} {
		if page := destinations(map[string]any{"ExclusiveStartDestinationId": after}); len(page) != 0 {
			t.Fatalf("page after %q %#v", after, page)
		}
	}
	for _, input := range []map[string]any{
		{"DeliveryStreamName": "described", "Limit": 0},
		{"DeliveryStreamName": "described", "Limit": 10001},
		{"DeliveryStreamName": "described", "Limit": 1.5},
		{"DeliveryStreamName": "described", "Limit": "1"},
		{"DeliveryStreamName": "described", "ExclusiveStartDestinationId": ""},
		{"DeliveryStreamName": "described", "ExclusiveStartDestinationId": 1},
		{"DeliveryStreamName": "described", "ExclusiveStartDestinationId": "bad_id"},
		{"DeliveryStreamName": "described", "ExclusiveStartDestinationId": strings.Repeat("a", 101)},
	} {
		if _, err := call(input); err == nil {
			t.Fatalf("accepted invalid describe input %#v", input)
		}
	}
}
