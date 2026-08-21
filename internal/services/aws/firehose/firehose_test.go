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
}
