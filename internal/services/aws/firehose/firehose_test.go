package firehose

import (
	"archive/zip"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/md5"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"math"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os/exec"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang/snappy"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/config"
	rtpkg "github.com/tyler-r-kendrick/mirror.cloud/internal/runtime"
	kafkaservice "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/kafka"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/kinesis"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/kms"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/lambda"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/logs"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/opensearch"
	redshiftservice "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/redshift"
	s3tablesservice "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/s3tables"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/secretsmanager"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spitest"

	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/s3"
)

const testRoleARN = "arn:aws:iam::123456789012:role/firehose"

// pollBudget is the wall-clock budget for this file's waits on delivery,
// retry and buffer-flush work the pack performs on its own goroutines after a
// test advances the controllable clock.
//
// Each such wait is on the wall clock for something driven by simulated time,
// which is the fragility here: the clock jump is synchronous and the work it
// releases is not, so a test can only poll. The budget is therefore generous
// rather than tight -- a passing run reaches its condition in milliseconds and
// pays nothing, while a loaded machine running under -race no longer fails a
// correct implementation for being slow. Making these deterministic needs the
// pack to expose a "due work flushed" observation point, which is a change to
// a pack that is scheduled for extraction.
//
// Waits that assert something does *not* happen are deliberately not on this
// budget: there the timeout is the success path, and lengthening it would only
// slow the suite down.
const pollBudget = 60 * time.Second

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) { return f(request) }

func testS3Destination() map[string]any {
	return map[string]any{"BucketARN": "arn:aws:s3:::out", "RoleARN": testRoleARN}
}

func testHTTPEndpointDestination(endpoint string) map[string]any {
	return map[string]any{
		"EndpointConfiguration": map[string]any{"Url": endpoint},
		"S3Configuration":       testS3Destination(),
	}
}

func testKinesisSource() map[string]any {
	return map[string]any{"KinesisStreamARN": "arn:aws:kinesis:us-east-1:123456789012:stream/source", "RoleARN": testRoleARN}
}

func testMSKSource() map[string]any {
	return map[string]any{
		"MSKClusterARN": "arn:aws:kafka:us-east-1:123456789012:cluster/source/uuid", "TopicName": "events.v1",
		"AuthenticationConfiguration": map[string]any{"Connectivity": "PRIVATE", "RoleARN": testRoleARN},
	}
}

func testIcebergDestination() map[string]any {
	return map[string]any{
		"CatalogConfiguration": map[string]any{"CatalogARN": "arn:aws:glue:us-east-1:123456789012:catalog/s3tablescatalog/warehouse"},
		"RoleARN":              testRoleARN, "S3Configuration": testS3Destination(),
		"DestinationTableConfigurationList": []any{map[string]any{
			"DestinationDatabaseName": "analytics", "DestinationTableName": "events", "UniqueKeys": []any{"id"},
		}},
	}
}

func testSnowflakeDestination() map[string]any {
	return map[string]any{
		"AccountUrl": "account.us-east-1.snowflakecomputing.com", "Database": "ANALYTICS", "Schema": "PUBLIC", "Table": "EVENTS",
		"RoleARN": testRoleARN, "User": "firehose", "PrivateKey": base64.StdEncoding.EncodeToString(make([]byte, 192)), "S3Configuration": testS3Destination(),
	}
}

func testOpenSearchServerlessDestination() map[string]any {
	return map[string]any{
		"CollectionEndpoint": "https://collection.us-east-1.aoss.amazonaws.com", "IndexName": "events", "RoleARN": testRoleARN,
		"S3Configuration": testS3Destination(),
	}
}

func testDatabaseSource() map[string]any {
	return map[string]any{
		"Type": "PostgreSQL", "Endpoint": "database.internal", "Port": float64(5432), "SSLMode": "Enabled",
		"Databases": map[string]any{"Include": []any{"app"}}, "Tables": map[string]any{"Include": []any{"public.*"}},
		"Columns": map[string]any{"Exclude": []any{"public.users.password"}}, "SurrogateKeys": []any{"public.events.id"},
		"SnapshotWatermarkTable": "public.firehose_watermark",
		"DatabaseSourceAuthenticationConfiguration": map[string]any{"SecretsManagerConfiguration": map[string]any{
			"Enabled": true, "RoleARN": testRoleARN, "SecretARN": "arn:aws:secretsmanager:us-east-1:123456789012:secret:database",
		}},
		"DatabaseSourceVPCConfiguration": map[string]any{"VpcEndpointServiceName": "com.amazonaws.vpce.us-east-1.vpce-svc-12345678901234567"},
	}
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
	call("CreateDeliveryStream", `{"DeliveryStreamName":"decompressed","ExtendedS3DestinationConfiguration":{"RoleARN":"arn:aws:iam::000000000000:role/fh","BucketARN":"arn:aws:s3:::out","ErrorOutputPrefix":"errors/!{firehose:error-output-type}/","ProcessingConfiguration":{"Enabled":true,"Processors":[{"Type":"Decompression"}]}}}`)
	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/", strings.NewReader(`{"DeliveryStreamName":"decompressed","Record":{"Data":"aGVsbG8="}}`))
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", "Firehose_20150804.PutRecord")
	req.Header.Set("Authorization", auth)
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	raw, _ = io.ReadAll(res.Body)
	res.Body.Close()
	if res.StatusCode != http.StatusBadRequest || res.Header.Get("x-amzn-errortype") != "InvalidSourceException" || !strings.Contains(string(raw), `"__type":"InvalidSourceException"`) {
		t.Fatalf("decompression source response %d %s %s", res.StatusCode, res.Header.Get("x-amzn-errortype"), raw)
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

func TestFirehoseMSKSourceConfiguration(t *testing.T) {
	deps := spitest.Deps(t)
	_ = deps.Clock.Advance(time.Hour)
	p := New(deps)
	id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
	call := func(operation string, input map[string]any) (*spi.Response, error) {
		return p.Invoke(context.Background(), &spi.Request{Identity: id, Operation: operation, Input: input})
	}
	create := func(name string, source map[string]any) {
		t.Helper()
		if _, err := call("CreateDeliveryStream", map[string]any{
			"DeliveryStreamName": name, "DeliveryStreamType": "MSKAsSource", "MSKSourceConfiguration": source,
			"ExtendedS3DestinationConfiguration": testS3Destination(),
		}); err != nil {
			t.Fatal(err)
		}
	}
	source := testMSKSource()
	source["ReadFromTimestamp"] = float64(123)
	create("msk-explicit", source)
	create("msk-default", testMSKSource())
	for name, start := range map[string]float64{"msk-explicit": 123, "msk-default": 3600} {
		described, err := call("DescribeDeliveryStream", map[string]any{"DeliveryStreamName": name})
		if err != nil {
			t.Fatal(err)
		}
		description := described.Output["DeliveryStreamDescription"].(map[string]any)
		msk := description["Source"].(map[string]any)["MSKSourceDescription"].(map[string]any)
		if description["DeliveryStreamType"] != "MSKAsSource" || msk["MSKClusterARN"] != testMSKSource()["MSKClusterARN"] || msk["TopicName"] != "events.v1" || msk["DeliveryStartTimestamp"] != start || msk["ReadFromTimestamp"] != nil || description["MSKSourceConfiguration"] != nil {
			t.Fatalf("MSK source description %#v", description)
		}
		if !reflect.DeepEqual(msk["AuthenticationConfiguration"], testMSKSource()["AuthenticationConfiguration"]) {
			t.Fatalf("MSK authentication description %#v", msk["AuthenticationConfiguration"])
		}
	}
	listed, err := call("ListDeliveryStreams", map[string]any{"DeliveryStreamType": "MSKAsSource"})
	if err != nil || !reflect.DeepEqual(listed.Output["DeliveryStreamNames"], []any{"msk-default", "msk-explicit"}) {
		t.Fatalf("MSK stream listing %#v, %v", listed, err)
	}

	invalid := []any{
		"invalid",
		map[string]any{},
		map[string]any{"MSKClusterARN": "arn:aws:kinesis:us-east-1:123456789012:cluster/source/uuid", "TopicName": "events", "AuthenticationConfiguration": testMSKSource()["AuthenticationConfiguration"]},
		map[string]any{"MSKClusterARN": testMSKSource()["MSKClusterARN"], "TopicName": "events/invalid", "AuthenticationConfiguration": testMSKSource()["AuthenticationConfiguration"]},
		map[string]any{"MSKClusterARN": testMSKSource()["MSKClusterARN"], "TopicName": strings.Repeat("t", 256), "AuthenticationConfiguration": testMSKSource()["AuthenticationConfiguration"]},
		map[string]any{"MSKClusterARN": testMSKSource()["MSKClusterARN"], "TopicName": "events", "AuthenticationConfiguration": "invalid"},
		map[string]any{"MSKClusterARN": testMSKSource()["MSKClusterARN"], "TopicName": "events", "AuthenticationConfiguration": map[string]any{"Connectivity": "VPC", "RoleARN": testRoleARN}},
		map[string]any{"MSKClusterARN": testMSKSource()["MSKClusterARN"], "TopicName": "events", "AuthenticationConfiguration": map[string]any{"Connectivity": "PUBLIC", "RoleARN": "role"}},
		map[string]any{"MSKClusterARN": testMSKSource()["MSKClusterARN"], "TopicName": "events", "AuthenticationConfiguration": testMSKSource()["AuthenticationConfiguration"], "ReadFromTimestamp": "epoch"},
		map[string]any{"MSKClusterARN": testMSKSource()["MSKClusterARN"], "TopicName": "events", "AuthenticationConfiguration": testMSKSource()["AuthenticationConfiguration"], "ReadFromTimestamp": math.NaN()},
		map[string]any{"MSKClusterARN": testMSKSource()["MSKClusterARN"], "TopicName": "events", "AuthenticationConfiguration": testMSKSource()["AuthenticationConfiguration"], "ReadFromTimestamp": math.Inf(1)},
	}
	for index, source := range invalid {
		if _, err := call("CreateDeliveryStream", map[string]any{
			"DeliveryStreamName": fmt.Sprintf("invalid-msk-%d", index), "DeliveryStreamType": "MSKAsSource", "MSKSourceConfiguration": source,
			"ExtendedS3DestinationConfiguration": testS3Destination(),
		}); err == nil {
			t.Fatalf("accepted invalid MSK source %#v", source)
		}
	}
	for index, input := range []map[string]any{
		{"DeliveryStreamName": "direct-with-msk", "MSKSourceConfiguration": testMSKSource(), "ExtendedS3DestinationConfiguration": testS3Destination()},
		{"DeliveryStreamName": "kinesis-with-msk", "DeliveryStreamType": "KinesisStreamAsSource", "KinesisStreamSourceConfiguration": testKinesisSource(), "MSKSourceConfiguration": testMSKSource(), "ExtendedS3DestinationConfiguration": testS3Destination()},
		{"DeliveryStreamName": "msk-with-kinesis", "DeliveryStreamType": "MSKAsSource", "MSKSourceConfiguration": testMSKSource(), "KinesisStreamSourceConfiguration": testKinesisSource(), "ExtendedS3DestinationConfiguration": testS3Destination()},
	} {
		if _, err := call("CreateDeliveryStream", input); err == nil {
			t.Fatalf("accepted mismatched source %d", index)
		}
	}
}

func TestFirehoseDatabaseSourceConfiguration(t *testing.T) {
	p := New(spitest.Deps(t))
	id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
	call := func(operation string, input map[string]any) (*spi.Response, error) {
		return p.Invoke(context.Background(), &spi.Request{Identity: id, Operation: operation, Input: input})
	}
	for _, name := range []string{"database-z", "database-a"} {
		if _, err := call("CreateDeliveryStream", map[string]any{
			"DeliveryStreamName": name, "DeliveryStreamType": "DatabaseAsSource", "DatabaseSourceConfiguration": testDatabaseSource(),
			"ExtendedS3DestinationConfiguration": testS3Destination(),
		}); err != nil {
			t.Fatal(err)
		}
	}
	mysql := testDatabaseSource()
	mysql["Type"], mysql["Port"] = "MySQL", float64(3306)
	if _, err := call("CreateDeliveryStream", map[string]any{
		"DeliveryStreamName": "database-mysql", "DeliveryStreamType": "DatabaseAsSource", "DatabaseSourceConfiguration": mysql,
		"ExtendedS3DestinationConfiguration": testS3Destination(),
	}); err != nil {
		t.Fatal(err)
	}
	minimal := testDatabaseSource()
	delete(minimal, "Columns")
	delete(minimal, "SSLMode")
	delete(minimal, "SurrogateKeys")
	minimal["DatabaseSourceAuthenticationConfiguration"] = map[string]any{"SecretsManagerConfiguration": map[string]any{"Enabled": false}}
	if _, err := call("CreateDeliveryStream", map[string]any{
		"DeliveryStreamName": "database-minimal", "DeliveryStreamType": "DatabaseAsSource", "DatabaseSourceConfiguration": minimal,
		"ExtendedS3DestinationConfiguration": testS3Destination(),
	}); err != nil {
		t.Fatal(err)
	}
	described, err := call("DescribeDeliveryStream", map[string]any{"DeliveryStreamName": "database-a"})
	if err != nil {
		t.Fatal(err)
	}
	description := described.Output["DeliveryStreamDescription"].(map[string]any)
	database := description["Source"].(map[string]any)["DatabaseSourceDescription"].(map[string]any)
	if description["DeliveryStreamType"] != "DatabaseAsSource" || description["DatabaseSourceConfiguration"] != nil || !reflect.DeepEqual(database, testDatabaseSource()) {
		t.Fatalf("database source description %#v", description)
	}
	listed, err := call("ListDeliveryStreams", map[string]any{"DeliveryStreamType": "DatabaseAsSource"})
	if err != nil || !reflect.DeepEqual(listed.Output["DeliveryStreamNames"], []any{"database-a", "database-minimal", "database-mysql", "database-z"}) {
		t.Fatalf("database stream listing %#v, %v", listed, err)
	}

	invalid := []any{"invalid", map[string]any{}}
	for _, key := range []string{"Type", "Endpoint", "Port", "Databases", "Tables", "SnapshotWatermarkTable", "DatabaseSourceAuthenticationConfiguration", "DatabaseSourceVPCConfiguration"} {
		source := testDatabaseSource()
		delete(source, key)
		invalid = append(invalid, source)
	}
	for _, change := range []struct {
		key   string
		value any
	}{
		{"Type", "Oracle"}, {"Endpoint", "   "}, {"Endpoint", strings.Repeat("e", 256)}, {"Port", "5432"}, {"Port", float64(3306)}, {"Port", float64(65536)},
		{"SSLMode", "Required"}, {"SnapshotWatermarkTable", ""}, {"SnapshotWatermarkTable", "table\x00name"}, {"SnapshotWatermarkTable", "table😀"},
		{"Databases", "app"}, {"Tables", map[string]any{"Include": []any{""}}}, {"Columns", map[string]any{"Include": []any{strings.Repeat("c", 195)}}},
		{"SurrogateKeys", "public.events.id"}, {"SurrogateKeys", []any{"public.events event_id"}}, {"DatabaseSourceAuthenticationConfiguration", "invalid"}, {"DatabaseSourceVPCConfiguration", "invalid"},
	} {
		source := testDatabaseSource()
		source[change.key] = change.value
		invalid = append(invalid, source)
	}
	for _, authentication := range []any{
		map[string]any{},
		map[string]any{"SecretsManagerConfiguration": "invalid"},
		map[string]any{"SecretsManagerConfiguration": map[string]any{"Enabled": "true"}},
		map[string]any{"SecretsManagerConfiguration": map[string]any{"Enabled": true, "RoleARN": testRoleARN}},
		map[string]any{"SecretsManagerConfiguration": map[string]any{"Enabled": true, "RoleARN": "role", "SecretARN": "arn:aws:secretsmanager:us-east-1:123456789012:secret:database"}},
		map[string]any{"SecretsManagerConfiguration": map[string]any{"Enabled": true, "RoleARN": testRoleARN, "SecretARN": "secret"}},
	} {
		source := testDatabaseSource()
		source["DatabaseSourceAuthenticationConfiguration"] = authentication
		invalid = append(invalid, source)
	}
	for _, vpc := range []any{
		map[string]any{},
		map[string]any{"VpcEndpointServiceName": "com.amazonaws.vpce.us-east-1.vpce-svc-short"},
	} {
		source := testDatabaseSource()
		source["DatabaseSourceVPCConfiguration"] = vpc
		invalid = append(invalid, source)
	}
	for index, source := range invalid {
		if _, err := call("CreateDeliveryStream", map[string]any{
			"DeliveryStreamName": fmt.Sprintf("invalid-database-%d", index), "DeliveryStreamType": "DatabaseAsSource", "DatabaseSourceConfiguration": source,
			"ExtendedS3DestinationConfiguration": testS3Destination(),
		}); err == nil {
			t.Fatalf("accepted invalid database source %#v", source)
		}
	}
	for index, input := range []map[string]any{
		{"DeliveryStreamName": "direct-with-database", "DatabaseSourceConfiguration": testDatabaseSource(), "ExtendedS3DestinationConfiguration": testS3Destination()},
		{"DeliveryStreamName": "kinesis-with-database", "DeliveryStreamType": "KinesisStreamAsSource", "KinesisStreamSourceConfiguration": testKinesisSource(), "DatabaseSourceConfiguration": testDatabaseSource(), "ExtendedS3DestinationConfiguration": testS3Destination()},
		{"DeliveryStreamName": "msk-with-database", "DeliveryStreamType": "MSKAsSource", "MSKSourceConfiguration": testMSKSource(), "DatabaseSourceConfiguration": testDatabaseSource(), "ExtendedS3DestinationConfiguration": testS3Destination()},
		{"DeliveryStreamName": "database-with-msk", "DeliveryStreamType": "DatabaseAsSource", "DatabaseSourceConfiguration": testDatabaseSource(), "MSKSourceConfiguration": testMSKSource(), "ExtendedS3DestinationConfiguration": testS3Destination()},
	} {
		if _, err := call("CreateDeliveryStream", input); err == nil {
			t.Fatalf("accepted mismatched database source %d", index)
		}
	}
}

func TestFirehoseRejectsDirectPutForSourceStreams(t *testing.T) {
	p := New(spitest.Deps(t))
	id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
	call := func(operation string, input map[string]any) (*spi.Response, error) {
		return p.Invoke(context.Background(), &spi.Request{Identity: id, Operation: operation, Input: input})
	}
	for _, source := range []struct {
		name, streamType, key string
		configuration         map[string]any
	}{
		{"kinesis-guard", "KinesisStreamAsSource", "KinesisStreamSourceConfiguration", testKinesisSource()},
		{"msk-guard", "MSKAsSource", "MSKSourceConfiguration", testMSKSource()},
		{"database-guard", "DatabaseAsSource", "DatabaseSourceConfiguration", testDatabaseSource()},
	} {
		input := map[string]any{
			"DeliveryStreamName": source.name, "DeliveryStreamType": source.streamType, source.key: source.configuration,
			"ExtendedS3DestinationConfiguration": testS3Destination(),
		}
		if _, err := call("CreateDeliveryStream", input); err != nil {
			t.Fatal(err)
		}
		record := map[string]any{"Data": base64.StdEncoding.EncodeToString([]byte("payload"))}
		for operation, putInput := range map[string]map[string]any{
			"PutRecord":      {"DeliveryStreamName": source.name, "Record": record},
			"PutRecordBatch": {"DeliveryStreamName": source.name, "Records": []any{record}},
		} {
			_, err := call(operation, putInput)
			if fault, ok := err.(*spi.Fault); !ok || fault.Code != "InvalidArgumentException" {
				t.Fatalf("%s accepted %s stream: %#v", operation, source.streamType, err)
			}
		}
		stored, _, _ := p.col(&spi.Request{Identity: id}, "fhrec:"+source.name).List(context.Background(), "", "", 0)
		if len(stored) != 0 {
			t.Fatalf("source-backed put stored %d records", len(stored))
		}
	}
}

func TestFirehoseConsumesFutureKinesisRecords(t *testing.T) {
	deps := spitest.Deps(t)
	firehose := New(deps)
	defer firehose.Close()
	kinesis := kinesis.New(deps)
	id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
	invoke := func(pack spi.BehaviorPack, operation string, input map[string]any) *spi.Response {
		t.Helper()
		response, err := pack.Invoke(context.Background(), &spi.Request{Identity: id, Operation: operation, Input: input})
		if err != nil {
			t.Fatal(err)
		}
		return response
	}
	invoke(kinesis, "CreateStream", map[string]any{"StreamName": "source"})
	invoke(kinesis, "PutRecord", map[string]any{"StreamName": "source", "PartitionKey": "before", "Data": []byte("before")})
	otherSource := testKinesisSource()
	otherSource["KinesisStreamARN"] = "arn:aws:kinesis:us-east-1:123456789012:stream/other"
	invoke(firehose, "CreateDeliveryStream", map[string]any{
		"DeliveryStreamName": "from-other", "DeliveryStreamType": "KinesisStreamAsSource",
		"KinesisStreamSourceConfiguration": otherSource, "S3DestinationConfiguration": testS3Destination(),
	})
	invoke(firehose, "CreateDeliveryStream", map[string]any{
		"DeliveryStreamName": "from-kinesis", "DeliveryStreamType": "KinesisStreamAsSource",
		"KinesisStreamSourceConfiguration": testKinesisSource(), "S3DestinationConfiguration": testS3Destination(),
	})
	invoke(kinesis, "PutRecord", map[string]any{"StreamName": "source", "PartitionKey": "after", "Data": []byte("after")})
	invoke(kinesis, "PutRecord", map[string]any{"StreamName": "source", "PartitionKey": "aggregate", "Data": kplAggregate([]byte("one"), []byte("two"))})

	records, _, err := firehose.col(&spi.Request{Identity: id}, "fhrec:from-kinesis").List(context.Background(), "", "", 0)
	if err != nil || len(records) != 3 {
		t.Fatalf("retained records=%d err=%v", len(records), err)
	}
	seen := map[string]bool{}
	for _, record := range records {
		var retained map[string]any
		if json.Unmarshal(record.Value, &retained) != nil {
			t.Fatalf("invalid retained record %q", record.Value)
		}
		decoded := first(retained, "Decoded")
		key := id.Account + "/" + id.Region + "/out/1970/01/01/00/from-kinesis-1-1970-01-01-00-00-00-" + record.Key
		reader, _, err := deps.Blobs.Get(context.Background(), key)
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(reader)
		_ = reader.Close()
		if string(body) != decoded {
			t.Fatalf("S3 object body %q want %q", body, decoded)
		}
		seen[decoded] = true
	}
	if !seen["after"] || !seen["one"] || !seen["two"] {
		t.Fatalf("delivered records %#v", seen)
	}
	unmatched, _, _ := firehose.col(&spi.Request{Identity: id}, "fhrec:from-other").List(context.Background(), "", "", 0)
	if len(unmatched) != 0 {
		t.Fatalf("unmatched source retained %d records", len(unmatched))
	}
}

func TestFirehoseConsumesMSKMessages(t *testing.T) {
	deps := spitest.Deps(t)
	firehose := New(deps)
	defer firehose.Close()
	kafka := kafkaservice.New(deps)
	ctx := context.Background()
	id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
	created, err := kafka.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateCluster", Input: map[string]any{"ClusterName": "source"}})
	if err != nil {
		t.Fatal(err)
	}
	clusterARN := created.Output["ClusterArn"].(string)
	if err := kafka.Publish(ctx, id, clusterARN, "events", []byte("before")); err != nil {
		t.Fatal(err)
	}
	if err := deps.Clock.Advance(time.Second); err != nil {
		t.Fatal(err)
	}
	source := testMSKSource()
	source["MSKClusterARN"], source["TopicName"], source["ReadFromTimestamp"] = clusterARN, "events", float64(0)
	if _, err := firehose.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateDeliveryStream", Input: map[string]any{
		"DeliveryStreamName": "from-msk", "DeliveryStreamType": "MSKAsSource", "MSKSourceConfiguration": source, "S3DestinationConfiguration": testS3Destination(),
	}}); err != nil {
		t.Fatal(err)
	}
	defaultSource := maps.Clone(source)
	delete(defaultSource, "ReadFromTimestamp")
	if _, err := firehose.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateDeliveryStream", Input: map[string]any{
		"DeliveryStreamName": "from-msk-now", "DeliveryStreamType": "MSKAsSource", "MSKSourceConfiguration": defaultSource, "S3DestinationConfiguration": testS3Destination(),
	}}); err != nil {
		t.Fatal(err)
	}
	if err := kafka.Publish(ctx, id, clusterARN, "other", []byte("ignored")); err != nil {
		t.Fatal(err)
	}
	if err := kafka.Publish(ctx, id, clusterARN, "events", []byte("after")); err != nil {
		t.Fatal(err)
	}
	for stream, want := range map[string]map[string]bool{
		"from-msk": {"before": true, "after": true}, "from-msk-now": {"after": true},
	} {
		records, _, err := firehose.col(&spi.Request{Identity: id}, "fhrec:"+stream).List(ctx, "", "", 0)
		if err != nil || len(records) != len(want) {
			t.Fatalf("%s retained records=%d err=%v", stream, len(records), err)
		}
		seen := map[string]bool{}
		for _, record := range records {
			var retained map[string]any
			_ = json.Unmarshal(record.Value, &retained)
			decoded := first(retained, "Decoded")
			key := id.Account + "/" + id.Region + "/out/1970/01/01/00/" + stream + "-1-1970-01-01-00-00-01-" + record.Key
			reader, _, err := deps.Blobs.Get(ctx, key)
			if err != nil {
				t.Fatal(err)
			}
			body, _ := io.ReadAll(reader)
			_ = reader.Close()
			if string(body) != decoded {
				t.Fatalf("MSK S3 body %q want %q", body, decoded)
			}
			seen[decoded] = true
		}
		if !reflect.DeepEqual(seen, want) {
			t.Fatalf("%s delivered %#v", stream, seen)
		}
	}
}

func TestFirehoseOpenSearchDestination(t *testing.T) {
	deps := spitest.Deps(t)
	firehose := New(deps)
	search := opensearch.New(deps)
	id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
	invoke := func(pack spi.BehaviorPack, operation string, input map[string]any) *spi.Response {
		t.Helper()
		response, err := pack.Invoke(context.Background(), &spi.Request{Identity: id, Operation: operation, Input: input})
		if err != nil {
			t.Fatal(err)
		}
		return response
	}
	invoke(search, "CreateDomain", map[string]any{"DomainName": "logs"})
	destination := map[string]any{
		"DomainARN": "arn:aws:es:us-east-1:123456789012:domain/logs", "IndexName": "events", "RoleARN": testRoleARN,
		"BufferingHints": map[string]any{"IntervalInSeconds": 0, "SizeInMBs": 1},
		"S3BackupMode":   "AllDocuments", "S3Configuration": map[string]any{
			"BucketARN": "arn:aws:s3:::out", "RoleARN": testRoleARN, "Prefix": "backup/",
		},
	}
	invoke(firehose, "CreateDeliveryStream", map[string]any{"DeliveryStreamName": "search", "ElasticsearchDestinationConfiguration": destination})
	put := invoke(firehose, "PutRecord", map[string]any{
		"DeliveryStreamName": "search", "Record": map[string]any{"Data": base64.StdEncoding.EncodeToString([]byte(`{"city":"Austin"}`))},
	})
	recordID := first(put.Output, "RecordId")
	result := invoke(search, "Search", map[string]any{"DomainName": "logs", "Index": "events-1970-01-01", "query": map[string]any{"match": map[string]any{"city": "austin"}}})
	hits := result.Output["hits"].(map[string]any)["hits"].([]any)
	if len(hits) != 1 || first(hits[0].(map[string]any), "_id") != recordID {
		t.Fatalf("OpenSearch hits %#v", hits)
	}
	key := id.Account + "/" + id.Region + "/out/backup/1970/01/01/00/search-1-1970-01-01-00-00-00-" + recordID
	reader, _, err := deps.Blobs.Get(context.Background(), key)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(reader)
	_ = reader.Close()
	if string(body) != `{"city":"Austin"}` {
		t.Fatalf("backup body %q", body)
	}
	description := invoke(firehose, "DescribeDeliveryStream", map[string]any{"DeliveryStreamName": "search"}).Output["DeliveryStreamDescription"].(map[string]any)
	described := description["Destinations"].([]any)[0].(map[string]any)["ElasticsearchDestinationDescription"].(map[string]any)
	if first(described, "IndexRotationPeriod") != "OneDay" || first(described, "S3BackupMode") != "AllDocuments" {
		t.Fatalf("OpenSearch description %#v", described)
	}
}

func TestFirehoseAmazonOpenSearchServiceDestination(t *testing.T) {
	ctx := context.Background()
	deps := spitest.Deps(t)
	p := New(deps)
	defer func() { _ = p.Close() }()
	search := opensearch.New(deps)
	id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
	call := func(pack spi.BehaviorPack, operation string, input map[string]any) *spi.Response {
		t.Helper()
		response, err := pack.Invoke(ctx, &spi.Request{Identity: id, Operation: operation, Input: input})
		if err != nil {
			t.Fatal(err)
		}
		return response
	}
	call(search, "CreateDomain", map[string]any{"DomainName": "modern"})
	destination := map[string]any{
		"DomainARN": "arn:aws:es:us-east-1:123456789012:domain/modern", "IndexName": "events", "RoleARN": testRoleARN,
		"BufferingHints":  map[string]any{"IntervalInSeconds": 0, "SizeInMBs": 1},
		"S3Configuration": testS3Destination(),
		"VpcConfiguration": map[string]any{
			"RoleARN": testRoleARN, "SecurityGroupIds": []any{"sg-123"}, "SubnetIds": []any{"subnet-123"},
		},
	}
	invalid := maps.Clone(destination)
	invalid["VpcConfiguration"] = map[string]any{"RoleARN": testRoleARN, "SecurityGroupIds": []any{}, "SubnetIds": []any{"subnet-123"}}
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateDeliveryStream", Input: map[string]any{
		"DeliveryStreamName": "invalid-modern-search", "AmazonopensearchserviceDestinationConfiguration": invalid,
	}}); err == nil {
		t.Fatal("accepted invalid modern OpenSearch VPC configuration")
	}
	call(p, "CreateDeliveryStream", map[string]any{
		"DeliveryStreamName": "modern-search", "AmazonopensearchserviceDestinationConfiguration": destination,
	})
	description := call(p, "DescribeDeliveryStream", map[string]any{"DeliveryStreamName": "modern-search"}).Output["DeliveryStreamDescription"].(map[string]any)
	described := description["Destinations"].([]any)[0].(map[string]any)["AmazonopensearchserviceDestinationDescription"].(map[string]any)
	if described["VpcConfiguration"] != nil || described["VpcConfigurationDescription"] == nil || described["S3Configuration"] != nil || described["S3DestinationDescription"] == nil {
		t.Fatalf("modern OpenSearch description %#v", described)
	}
	call(p, "UpdateDestination", map[string]any{
		"DeliveryStreamName": "modern-search", "CurrentDeliveryStreamVersionId": "1", "DestinationId": destinationID,
		"AmazonopensearchserviceDestinationUpdate": map[string]any{"IndexName": "events-v2"},
	})
	put := call(p, "PutRecord", map[string]any{
		"DeliveryStreamName": "modern-search", "Record": map[string]any{"Data": base64.StdEncoding.EncodeToString([]byte(`{"city":"Austin"}`))},
	})
	result := call(search, "GetDocument", map[string]any{
		"DomainName": "modern", "Index": "events-v2-1970-01-01", "Id": first(put.Output, "RecordId"),
	})
	if result.Output["found"] != true {
		t.Fatalf("modern OpenSearch document %#v", result.Output)
	}
}

func TestOpenSearchBufferRetryPersistence(t *testing.T) {
	deps := spitest.Deps(t)
	id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
	search := opensearch.New(deps)
	p := New(deps)
	defer func() { _ = p.Close() }()
	call := func(pack spi.BehaviorPack, operation string, input map[string]any) *spi.Response {
		t.Helper()
		response, err := pack.Invoke(context.Background(), &spi.Request{Identity: id, Operation: operation, Input: input})
		if err != nil {
			t.Fatal(err)
		}
		return response
	}
	wait := func(message string, ready func() bool) {
		t.Helper()
		for deadline := time.Now().Add(pollBudget); time.Now().Before(deadline); {
			if ready() {
				return
			}
			time.Sleep(time.Millisecond)
		}
		objects, _, _ := deps.Store.Scope(id.Account, id.Region).Collection("objects").List(context.Background(), "", "", 0)
		t.Fatalf("%s: objects %#v", message, objects)
	}
	destination := func(domain string, interval, retry int) map[string]any {
		return map[string]any{
			"DomainARN": "arn:aws:es:us-east-1:123456789012:domain/" + domain, "IndexName": "events", "RoleARN": testRoleARN,
			"BufferingHints": map[string]any{"IntervalInSeconds": interval, "SizeInMBs": 1}, "RetryOptions": map[string]any{"DurationInSeconds": retry},
			"S3Configuration": map[string]any{"BucketARN": "arn:aws:s3:::out", "RoleARN": testRoleARN, "Prefix": "backup/"},
		}
	}
	create := func(name string, destination map[string]any) {
		call(p, "CreateDeliveryStream", map[string]any{
			"DeliveryStreamName": name, "ElasticsearchDestinationConfiguration": destination,
			"DeliveryStreamEncryptionConfigurationInput": map[string]any{"KeyType": "AWS_OWNED_CMK"},
		})
	}
	work := deps.Store.Scope(id.Account, id.Region).Collection("fh-search-work")

	call(search, "CreateDomain", map[string]any{"DomainName": "buffered"})
	create("buffered", destination("buffered", 10, 4))
	batch := call(p, "PutRecordBatch", map[string]any{"DeliveryStreamName": "buffered", "Records": []any{
		map[string]any{"Data": base64.StdEncoding.EncodeToString([]byte(`{"city":"Austin"}`))},
		map[string]any{"Data": base64.StdEncoding.EncodeToString([]byte(`not-json`))},
	}})
	responses := batch.Output["RequestResponses"].([]any)
	validID := first(responses[0].(map[string]any), "RecordId")
	invalidID := first(responses[1].(map[string]any), "RecordId")
	items, _, _ := work.List(context.Background(), "buffered/buffer/", "", 0)
	if len(items) != 1 {
		t.Fatalf("persisted OpenSearch buffer %#v", items)
	}
	var buffered searchWork
	_ = json.Unmarshal(items[0].Value, &buffered)
	reader, _, err := deps.Blobs.Get(context.Background(), buffered.DataKey)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(reader)
	_ = reader.Close()
	if !bytes.HasPrefix(body, firehoseEncryptedPrefix) || bytes.Contains(body, []byte("Austin")) {
		t.Fatalf("unencrypted OpenSearch buffer %q", body)
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	p = New(deps)
	if err := deps.Clock.Advance(10 * time.Second); err != nil {
		t.Fatal(err)
	}
	wait("persisted OpenSearch buffer did not flush", func() bool {
		result := call(search, "Search", map[string]any{"DomainName": "buffered", "Index": "events-1970-01-01", "query": map[string]any{"match_all": map[string]any{}}})
		return len(result.Output["hits"].(map[string]any)["hits"].([]any)) == 1
	})
	result := call(search, "GetDocument", map[string]any{"DomainName": "buffered", "Index": "events-1970-01-01", "Id": validID})
	if result.Output["found"] != true {
		t.Fatalf("buffered document %#v", result.Output)
	}
	failedKey := id.Account + "/" + id.Region + "/out/backup/AmazonOpenSearchService-failed/1970/01/01/00/buffered-1-1970-01-01-00-00-10-" + invalidID
	wait("malformed OpenSearch document was not backed up", func() bool {
		_, _, err := deps.Blobs.Get(context.Background(), failedKey)
		return err == nil
	})
	wait("delivered OpenSearch buffer remained persisted", func() bool {
		items, _, _ := work.List(context.Background(), "buffered/", "", 0)
		return len(items) == 0
	})
	if _, _, err := deps.Blobs.Get(context.Background(), buffered.DataKey); err == nil {
		t.Fatal("delivered OpenSearch buffer blob remained persisted")
	}

	create("retrying", destination("retrying", 0, 10))
	retryPut := call(p, "PutRecord", map[string]any{"DeliveryStreamName": "retrying", "Record": map[string]any{"Data": base64.StdEncoding.EncodeToString([]byte(`{"retry":true}`))}})
	retryID := first(retryPut.Output, "RecordId")
	items, _, _ = work.List(context.Background(), "retrying/retry/", "", 0)
	if len(items) != 1 {
		t.Fatalf("persisted OpenSearch retry %#v", items)
	}
	var retry searchWork
	_ = json.Unmarshal(items[0].Value, &retry)
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	if err := deps.Clock.Advance(2 * time.Second); err != nil {
		t.Fatal(err)
	}
	(&Pack{deps: deps}).runSearchWork(context.Background())
	items, _, _ = work.List(context.Background(), "retrying/retry/", "", 0)
	var updated searchWork
	if len(items) != 1 || json.Unmarshal(items[0].Value, &updated) != nil || updated.Retries != 1 {
		t.Fatalf("failed OpenSearch retry state was not updated: %#v", items)
	}
	call(search, "CreateDomain", map[string]any{"DomainName": "retrying"})
	p = New(deps)
	if err := deps.Clock.Advance(3 * time.Second); err != nil {
		t.Fatal(err)
	}
	wait("persisted OpenSearch retry did not run", func() bool {
		result := call(search, "GetDocument", map[string]any{"DomainName": "retrying", "Index": "events-1970-01-01", "Id": retryID})
		return result.Output["found"] == true
	})
	wait("successful OpenSearch retry remained persisted", func() bool {
		items, _, _ := work.List(context.Background(), "retrying/", "", 0)
		return len(items) == 0
	})
	if _, _, err := deps.Blobs.Get(context.Background(), retry.DataKey); err == nil {
		t.Fatal("successful OpenSearch retry blob remained persisted")
	}

	create("expired", destination("expired", 0, 2))
	expiredPut := call(p, "PutRecord", map[string]any{"DeliveryStreamName": "expired", "Record": map[string]any{"Data": base64.StdEncoding.EncodeToString([]byte(`{"expired":true}`))}})
	expiredID := first(expiredPut.Output, "RecordId")
	items, _, _ = work.List(context.Background(), "expired/retry/", "", 0)
	if len(items) != 1 {
		t.Fatalf("persisted expiring OpenSearch retry %#v", items)
	}
	var expired searchWork
	_ = json.Unmarshal(items[0].Value, &expired)
	if err := deps.Clock.Advance(2 * time.Second); err != nil {
		t.Fatal(err)
	}
	expiredKey := id.Account + "/" + id.Region + "/out/backup/AmazonOpenSearchService-failed/1970/01/01/00/expired-1-1970-01-01-00-00-17-" + expiredID
	wait("expired OpenSearch document was not backed up", func() bool {
		_, _, err := deps.Blobs.Get(context.Background(), expiredKey)
		return err == nil
	})
	reader, _, err = deps.Blobs.Get(context.Background(), expiredKey)
	if err != nil {
		t.Fatal(err)
	}
	body, _ = io.ReadAll(reader)
	_ = reader.Close()
	var failure map[string]any
	_ = json.Unmarshal(body, &failure)
	if first(failure, "errorCode") != "400" || first(failure, "esDocumentId") != expiredID || first(failure, "esIndexName") != "events-1970-01-01" || first(failure, "rawData") != base64.StdEncoding.EncodeToString([]byte(`{"expired":true}`)) || first(failure, "arrivalTimestamp") != "1970-01-01T00:00:15Z" {
		t.Fatalf("OpenSearch failure envelope %#v", failure)
	}
	wait("expired OpenSearch retry remained persisted", func() bool {
		items, _, _ := work.List(context.Background(), "expired/", "", 0)
		return len(items) == 0
	})
	if _, _, err := deps.Blobs.Get(context.Background(), expired.DataKey); err == nil {
		t.Fatal("expired OpenSearch retry blob remained persisted")
	}

	call(search, "CreateDomain", map[string]any{"DomainName": "sized"})
	create("sized", destination("sized", 900, 0))
	padding := strings.Repeat("x", 600*1024)
	for index := range 2 {
		call(p, "PutRecord", map[string]any{"DeliveryStreamName": "sized", "Record": map[string]any{"Data": base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf(`{"index":%d,"padding":"%s"}`, index, padding)))}})
	}
	wait("OpenSearch size threshold did not flush", func() bool {
		result := call(search, "Search", map[string]any{"DomainName": "sized", "Index": "events-1970-01-01", "query": map[string]any{"match_all": map[string]any{}}})
		return len(result.Output["hits"].(map[string]any)["hits"].([]any)) == 2
	})

	create("deleted", destination("deleted", 900, 4))
	call(p, "PutRecord", map[string]any{"DeliveryStreamName": "deleted", "Record": map[string]any{"Data": base64.StdEncoding.EncodeToString([]byte(`{"deleted":true}`))}})
	items, _, _ = work.List(context.Background(), "deleted/buffer/", "", 0)
	if len(items) != 1 {
		t.Fatalf("persisted deletable OpenSearch buffer %#v", items)
	}
	var deleted searchWork
	_ = json.Unmarshal(items[0].Value, &deleted)
	call(p, "DeleteDeliveryStream", map[string]any{"DeliveryStreamName": "deleted"})
	items, _, _ = work.List(context.Background(), "deleted/", "", 0)
	if len(items) != 0 {
		t.Fatalf("deleted stream retained OpenSearch work %#v", items)
	}
	if _, _, err := deps.Blobs.Get(context.Background(), deleted.DataKey); err == nil {
		t.Fatal("deleted stream retained OpenSearch work blob")
	}
}

func TestOpenSearchHelpers(t *testing.T) {
	now := time.Date(2024, time.February, 29, 15, 4, 5, 0, time.UTC)
	for _, test := range []struct {
		rotation, expected string
	}{
		{"NoRotation", "events"},
		{"OneHour", "events-2024-02-29-15"},
		{"OneDay", "events-2024-02-29"},
		{"OneWeek", "events-2024-w09"},
		{"OneMonth", "events-2024-02"},
	} {
		if index := elasticsearchIndex(map[string]any{"IndexName": "events", "IndexRotationPeriod": test.rotation}, now); index != test.expected {
			t.Errorf("%s index %q", test.rotation, index)
		}
	}
	endpoint := map[string]any{"ClusterEndpoint": "https://logs.us-east-1.es.localhost.localstack.cloud"}
	if domain := elasticsearchDomain(endpoint); domain != "logs" {
		t.Fatalf("endpoint domain %q", domain)
	}
	if interval, size := searchBufferingHints(nil); interval != 300*time.Second || size != 5*1024*1024 {
		t.Fatalf("default buffering hints %v %d", interval, size)
	}
}

func TestOpenSearchServerlessDestinationValidationAndDescription(t *testing.T) {
	destination := testOpenSearchServerlessDestination()
	destination["VpcConfiguration"] = map[string]any{
		"RoleARN": testRoleARN, "SecurityGroupIds": []any{"sg-123"}, "SubnetIds": []any{"subnet-123"},
	}
	if err := validateOpenSearchServerlessDestination(destination, "us-east-1"); err != nil {
		t.Fatal(err)
	}
	for index, change := range []func(map[string]any){
		func(value map[string]any) { value["IndexName"] = "" },
		func(value map[string]any) { value["CollectionEndpoint"] = "http://collection.example" },
		func(value map[string]any) { value["S3BackupMode"] = "Enabled" },
		func(value map[string]any) { value["BufferingHints"] = map[string]any{"SizeInMBs": 101} },
		func(value map[string]any) {
			value["VpcConfiguration"] = map[string]any{"RoleARN": testRoleARN, "SecurityGroupIds": []any{}, "SubnetIds": []any{"subnet-123"}}
		},
	} {
		candidate := maps.Clone(destination)
		change(candidate)
		if err := validateOpenSearchServerlessDestination(candidate, "us-east-1"); err == nil {
			t.Fatalf("accepted invalid OpenSearch Serverless destination %d: %#v", index, candidate)
		}
	}
	p := New(spitest.Deps(t))
	id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
	call := func(operation string, input map[string]any) (*spi.Response, error) {
		return p.Invoke(context.Background(), &spi.Request{Identity: id, Operation: operation, Input: input})
	}
	if _, err := call("CreateDeliveryStream", map[string]any{"DeliveryStreamName": "serverless", "AmazonOpenSearchServerlessDestinationConfiguration": destination}); err != nil {
		t.Fatal(err)
	}
	response, err := call("DescribeDeliveryStream", map[string]any{"DeliveryStreamName": "serverless"})
	if err != nil {
		t.Fatal(err)
	}
	description := response.Output["DeliveryStreamDescription"].(map[string]any)["Destinations"].([]any)[0].(map[string]any)["AmazonOpenSearchServerlessDestinationDescription"].(map[string]any)
	if description["S3Configuration"] != nil || description["S3DestinationDescription"] == nil || description["VpcConfiguration"] != nil || description["VpcConfigurationDescription"] == nil || first(description, "S3BackupMode") != "FailedDocumentsOnly" || !reflect.DeepEqual(description["BufferingHints"], map[string]any{"IntervalInSeconds": 300, "SizeInMBs": 5}) {
		t.Fatalf("OpenSearch Serverless description %#v", description)
	}
	if _, err := call("UpdateDestination", map[string]any{
		"DeliveryStreamName": "serverless", "CurrentDeliveryStreamVersionId": "1", "DestinationId": destinationID,
		"AmazonOpenSearchServerlessDestinationUpdate": map[string]any{"IndexName": "events-v2"},
	}); err != nil {
		t.Fatal(err)
	}
	response, _ = call("DescribeDeliveryStream", map[string]any{"DeliveryStreamName": "serverless"})
	description = response.Output["DeliveryStreamDescription"].(map[string]any)["Destinations"].([]any)[0].(map[string]any)["AmazonOpenSearchServerlessDestinationDescription"].(map[string]any)
	if first(description, "IndexName") != "events-v2" {
		t.Fatalf("updated OpenSearch Serverless description %#v", description)
	}
}

func TestFirehoseOpenSearchServerlessDestination(t *testing.T) {
	ctx := context.Background()
	deps := spitest.Deps(t)
	p := New(deps)
	defer func() { _ = p.Close() }()
	search := opensearch.New(deps)
	id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
	destination := testOpenSearchServerlessDestination()
	destination["BufferingHints"] = map[string]any{"IntervalInSeconds": 0, "SizeInMBs": 5}
	destination["S3BackupMode"] = "AllDocuments"
	destination["S3Configuration"] = map[string]any{"BucketARN": "arn:aws:s3:::out", "RoleARN": testRoleARN, "Prefix": "backup/"}
	call := func(pack spi.BehaviorPack, operation string, input map[string]any) *spi.Response {
		t.Helper()
		response, err := pack.Invoke(ctx, &spi.Request{Identity: id, Operation: operation, Input: input})
		if err != nil {
			t.Fatal(err)
		}
		return response
	}
	call(p, "CreateDeliveryStream", map[string]any{"DeliveryStreamName": "serverless-delivery", "AmazonOpenSearchServerlessDestinationConfiguration": destination})
	records := []string{`{"id":"1","name":"alice"}`, `["invalid"]`}
	response := call(p, "PutRecordBatch", map[string]any{"DeliveryStreamName": "serverless-delivery", "Records": []any{
		map[string]any{"Data": base64.StdEncoding.EncodeToString([]byte(records[0]))},
		map[string]any{"Data": base64.StdEncoding.EncodeToString([]byte(records[1]))},
	}})
	var hits []any
	for deadline := time.Now().Add(pollBudget); time.Now().Before(deadline); {
		result := call(search, "Search", map[string]any{"Index": "collection/events", "query": map[string]any{"match_all": map[string]any{}}})
		hits = result.Output["hits"].(map[string]any)["hits"].([]any)
		if len(hits) == 1 {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if len(hits) != 1 || hits[0].(map[string]any)["_source"].(map[string]any)["name"] != "alice" {
		t.Fatalf("OpenSearch Serverless hits %#v", hits)
	}
	responses := response.Output["RequestResponses"].([]any)
	for index := range records {
		recordID := first(responses[index].(map[string]any), "RecordId")
		key := id.Account + "/" + id.Region + "/out/backup/1970/01/01/00/serverless-delivery-1-1970-01-01-00-00-00-" + recordID
		reader, _, err := deps.Blobs.Get(ctx, key)
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(reader)
		_ = reader.Close()
		if string(body) != records[index] {
			t.Fatalf("OpenSearch Serverless backup %q", body)
		}
	}
	failedID := first(responses[1].(map[string]any), "RecordId")
	failureKey := id.Account + "/" + id.Region + "/out/backup/AmazonOpenSearchService-failed/1970/01/01/00/serverless-delivery-1-1970-01-01-00-00-00-" + failedID
	var failureBody []byte
	for deadline := time.Now().Add(pollBudget); time.Now().Before(deadline); {
		reader, _, err := deps.Blobs.Get(ctx, failureKey)
		if err == nil {
			failureBody, _ = io.ReadAll(reader)
			_ = reader.Close()
			break
		}
		time.Sleep(time.Millisecond)
	}
	failure := map[string]any{}
	if json.Unmarshal(failureBody, &failure) != nil || first(failure, "esDocumentId") != failedID || first(failure, "esIndexName") != "events" || first(failure, "rawData") != base64.StdEncoding.EncodeToString([]byte(records[1])) {
		t.Fatalf("OpenSearch Serverless failure envelope %s", failureBody)
	}
}

func TestFirehoseOpenSearchServerlessPersistentBuffer(t *testing.T) {
	ctx := context.Background()
	deps := spitest.Deps(t)
	id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
	destination := testOpenSearchServerlessDestination()
	destination["BufferingHints"] = map[string]any{"IntervalInSeconds": 5, "SizeInMBs": 5}
	p := New(deps)
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateDeliveryStream", Input: map[string]any{
		"DeliveryStreamName": "serverless-buffer", "AmazonOpenSearchServerlessDestinationConfiguration": destination,
		"DeliveryStreamEncryptionConfigurationInput": map[string]any{"KeyType": "AWS_OWNED_CMK"},
	}}); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "PutRecord", Input: map[string]any{
		"DeliveryStreamName": "serverless-buffer", "Record": map[string]any{"Data": base64.StdEncoding.EncodeToString([]byte(`{"id":"persisted"}`))},
	}}); err != nil {
		t.Fatal(err)
	}
	work := deps.Store.Scope(id.Account, id.Region).Collection("fh-search-work")
	items, _, _ := work.List(ctx, "serverless-buffer/buffer/", "", 0)
	if len(items) != 1 {
		t.Fatalf("OpenSearch Serverless buffer %#v", items)
	}
	var buffered searchWork
	_ = json.Unmarshal(items[0].Value, &buffered)
	if buffered.Destination != "AmazonOpenSearchServerlessDestinationConfiguration" {
		t.Fatalf("OpenSearch Serverless persisted destination %#v", buffered)
	}
	reader, _, err := deps.Blobs.Get(ctx, buffered.DataKey)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(reader)
	_ = reader.Close()
	if !bytes.HasPrefix(body, firehoseEncryptedPrefix) || bytes.Contains(body, []byte("persisted")) {
		t.Fatalf("OpenSearch Serverless buffer was not encrypted %q", body)
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	p = New(deps)
	defer func() { _ = p.Close() }()
	if err := deps.Clock.Advance(5 * time.Second); err != nil {
		t.Fatal(err)
	}
	search := opensearch.New(deps)
	for deadline := time.Now().Add(pollBudget); time.Now().Before(deadline); {
		result, err := search.Invoke(ctx, &spi.Request{Identity: id, Operation: "Search", Input: map[string]any{"Index": "collection/events", "query": map[string]any{"match_all": map[string]any{}}}})
		if err == nil && len(result.Output["hits"].(map[string]any)["hits"].([]any)) == 1 {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("persisted OpenSearch Serverless buffer did not resume")
}

func TestRedshiftDestinationValidationAndDescription(t *testing.T) {
	valid := map[string]any{
		"ClusterJDBCURL": "jdbc:redshift://cluster.abc.us-east-1.redshift.amazonaws.com:5439/dev", "RoleARN": testRoleARN,
		"CopyCommand": map[string]any{"DataTableName": "events", "DataTableColumns": "id,payload", "CopyOptions": "JSON 'auto'"},
		"Username":    "firehose", "Password": "secret-password", "S3Configuration": testS3Destination(),
	}
	if err := validateRedshiftDestination(valid, "us-east-1"); err != nil {
		t.Fatal(err)
	}
	secret := maps.Clone(valid)
	delete(secret, "Username")
	delete(secret, "Password")
	secret["SecretsManagerConfiguration"] = map[string]any{
		"Enabled": true, "RoleARN": testRoleARN, "SecretARN": "arn:aws:secretsmanager:us-east-1:123456789012:secret:redshift",
	}
	if err := validateRedshiftDestination(secret, "us-east-1"); err != nil {
		t.Fatal(err)
	}
	invalid := []map[string]any{}
	add := func(key string, value any) {
		candidate := maps.Clone(valid)
		if value == nil {
			delete(candidate, key)
		} else {
			candidate[key] = value
		}
		invalid = append(invalid, candidate)
	}
	add("ClusterJDBCURL", "jdbc:redshift://localhost:5439/dev")
	add("RoleARN", "role")
	add("CopyCommand", map[string]any{})
	add("CopyCommand", map[string]any{"DataTableName": "events", "DataTableColumns": 1})
	add("Username", nil)
	add("Password", "short")
	add("RetryOptions", map[string]any{"DurationInSeconds": 7201})
	add("S3BackupMode", "Everything")
	add("S3BackupMode", "Enabled")
	for _, compression := range []string{"ZIP", "Snappy", "HADOOP_SNAPPY"} {
		s3 := testS3Destination()
		s3["CompressionFormat"] = compression
		add("S3Configuration", s3)
	}
	for index, candidate := range invalid {
		if err := validateRedshiftDestination(candidate, "us-east-1"); err == nil {
			t.Fatalf("accepted invalid Redshift destination %d: %#v", index, candidate)
		}
	}

	deps := spitest.Deps(t)
	p := New(deps)
	id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
	call := func(operation string, input map[string]any) (*spi.Response, error) {
		return p.Invoke(context.Background(), &spi.Request{Identity: id, Operation: operation, Input: input})
	}
	if _, err := call("CreateDeliveryStream", map[string]any{"DeliveryStreamName": "redshift", "RedshiftDestinationConfiguration": valid}); err != nil {
		t.Fatal(err)
	}
	response, err := call("DescribeDeliveryStream", map[string]any{"DeliveryStreamName": "redshift"})
	if err != nil {
		t.Fatal(err)
	}
	description := response.Output["DeliveryStreamDescription"].(map[string]any)["Destinations"].([]any)[0].(map[string]any)["RedshiftDestinationDescription"].(map[string]any)
	if description["Password"] != nil || description["Username"] != "firehose" || description["RetryOptions"].(map[string]any)["DurationInSeconds"] != 3600 || first(description, "S3BackupMode") != "Disabled" || description["S3DestinationDescription"] == nil {
		t.Fatalf("Redshift description %#v", description)
	}
	if _, err := call("UpdateDestination", map[string]any{
		"DeliveryStreamName": "redshift", "CurrentDeliveryStreamVersionId": "1", "DestinationId": destinationID,
		"RedshiftDestinationUpdate": map[string]any{"S3BackupMode": "Enabled", "S3BackupUpdate": testS3Destination()},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := call("UpdateDestination", map[string]any{
		"DeliveryStreamName": "redshift", "CurrentDeliveryStreamVersionId": "2", "DestinationId": destinationID,
		"RedshiftDestinationUpdate": map[string]any{"S3BackupMode": "Disabled"},
	}); err == nil {
		t.Fatal("disabled Redshift S3 backup")
	}
}

func TestFirehoseRedshiftDestination(t *testing.T) {
	deps := spitest.Deps(t)
	id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
	redshift := redshiftservice.New(deps)
	if _, err := redshift.Invoke(context.Background(), &spi.Request{Identity: id, Operation: "CreateCluster", Input: map[string]any{
		"ClusterIdentifier": "warehouse", "DBName": "analytics", "MasterUsername": "firehose", "MasterUserPassword": "secret-password",
	}}); err != nil {
		t.Fatal(err)
	}
	if err := redshift.CreateTable(context.Background(), id, "warehouse", "analytics", "events", []string{"id", "payload"}); err != nil {
		t.Fatal(err)
	}
	p := New(deps)
	defer func() { _ = p.Close() }()
	call := func(operation string, input map[string]any) (*spi.Response, error) {
		t.Helper()
		return p.Invoke(context.Background(), &spi.Request{Identity: id, Operation: operation, Input: input})
	}
	destination := map[string]any{
		"ClusterJDBCURL": "jdbc:redshift://warehouse.abc.us-east-1.redshift.amazonaws.com:5439/analytics", "RoleARN": testRoleARN,
		"CopyCommand": map[string]any{"DataTableName": "events", "DataTableColumns": "id,payload", "CopyOptions": "delimiter '|'"},
		"Username":    "firehose", "Password": "secret-password", "S3BackupMode": "Enabled",
		"S3Configuration":         map[string]any{"BucketARN": "arn:aws:s3:::out", "RoleARN": testRoleARN, "Prefix": "staging/"},
		"S3BackupConfiguration":   map[string]any{"BucketARN": "arn:aws:s3:::out", "RoleARN": testRoleARN, "Prefix": "source/"},
		"ProcessingConfiguration": map[string]any{"Enabled": true, "Processors": []any{map[string]any{"Type": "AppendDelimiterToRecord"}}},
	}
	if _, err := call("CreateDeliveryStream", map[string]any{"DeliveryStreamName": "redshift-delivery", "RedshiftDestinationConfiguration": destination}); err != nil {
		t.Fatal(err)
	}
	response, err := call("PutRecordBatch", map[string]any{"DeliveryStreamName": "redshift-delivery", "Records": []any{
		map[string]any{"Data": base64.StdEncoding.EncodeToString([]byte("1|one"))},
		map[string]any{"Data": base64.StdEncoding.EncodeToString([]byte("2|two"))},
	}})
	if err != nil {
		t.Fatal(err)
	}
	rows, err := redshift.TableRows(context.Background(), id, "warehouse", "analytics", "events")
	if err != nil || !reflect.DeepEqual(rows, []map[string]any{{"id": "1", "payload": "one"}, {"id": "2", "payload": "two"}}) {
		t.Fatalf("Firehose Redshift rows %#v, %v", rows, err)
	}
	for index, rawResponse := range response.Output["RequestResponses"].([]any) {
		recordID := first(rawResponse.(map[string]any), "RecordId")
		for prefix, expected := range map[string]string{"staging": []string{"1|one\n", "2|two\n"}[index], "source": []string{"1|one", "2|two"}[index]} {
			key := id.Account + "/" + id.Region + "/out/" + prefix + "/1970/01/01/00/redshift-delivery-1-1970-01-01-00-00-00-" + recordID
			reader, _, err := deps.Blobs.Get(context.Background(), key)
			if err != nil {
				t.Fatal(err)
			}
			body, _ := io.ReadAll(reader)
			_ = reader.Close()
			if string(body) != expected {
				t.Fatalf("Redshift %s object %q", prefix, body)
			}
		}
	}
}

func TestFirehoseRedshiftPersistentRetry(t *testing.T) {
	deps := spitest.Deps(t)
	id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
	redshift := redshiftservice.New(deps)
	if _, err := redshift.Invoke(context.Background(), &spi.Request{Identity: id, Operation: "CreateCluster", Input: map[string]any{
		"ClusterIdentifier": "retry-warehouse", "DBName": "analytics", "MasterUsername": "firehose", "MasterUserPassword": "secret-password",
	}}); err != nil {
		t.Fatal(err)
	}
	if err := redshift.CreateTable(context.Background(), id, "retry-warehouse", "analytics", "events", []string{"id", "payload"}); err != nil {
		t.Fatal(err)
	}
	secretPack := secretsmanager.New(deps)
	secret, err := secretPack.Invoke(context.Background(), &spi.Request{Identity: id, Operation: "CreateSecret", Input: map[string]any{
		"Name": "redshift-credentials", "SecretString": `{"username":"firehose","password":"wrong-password"}`,
	}})
	if err != nil {
		t.Fatal(err)
	}
	p := New(deps)
	destination := map[string]any{
		"ClusterJDBCURL": "jdbc:redshift://retry-warehouse.abc.us-east-1.redshift.amazonaws.com:5439/analytics", "RoleARN": testRoleARN,
		"CopyCommand":  map[string]any{"DataTableName": "events", "DataTableColumns": "id,payload", "CopyOptions": "delimiter '|'"},
		"RetryOptions": map[string]any{"DurationInSeconds": 600}, "S3Configuration": testS3Destination(),
		"ProcessingConfiguration":     map[string]any{"Enabled": true, "Processors": []any{map[string]any{"Type": "AppendDelimiterToRecord"}}},
		"SecretsManagerConfiguration": map[string]any{"Enabled": true, "RoleARN": testRoleARN, "SecretARN": secret.Output["ARN"]},
	}
	call := func(operation string, input map[string]any) (*spi.Response, error) {
		return p.Invoke(context.Background(), &spi.Request{Identity: id, Operation: operation, Input: input})
	}
	if _, err := call("CreateDeliveryStream", map[string]any{
		"DeliveryStreamName": "redshift-retry", "RedshiftDestinationConfiguration": destination,
		"DeliveryStreamEncryptionConfigurationInput": map[string]any{"KeyType": "AWS_OWNED_CMK"},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := call("PutRecord", map[string]any{"DeliveryStreamName": "redshift-retry", "Record": map[string]any{"Data": base64.StdEncoding.EncodeToString([]byte("3|three"))}}); err != nil {
		t.Fatal(err)
	}
	collection := deps.Store.Scope(id.Account, id.Region).Collection("fh-redshift-work")
	items, _, _ := collection.List(context.Background(), "redshift-retry/", "", 0)
	var work redshiftWork
	if len(items) != 1 || json.Unmarshal(items[0].Value, &work) != nil || work.Next != time.Unix(300, 0).UTC() || work.Expires != time.Unix(600, 0).UTC() || work.ErrorMessage == "" {
		t.Fatalf("persisted Redshift retry %#v", items)
	}
	reader, _, err := deps.Blobs.Get(context.Background(), work.DataKey)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(reader)
	_ = reader.Close()
	if !bytes.HasPrefix(body, firehoseEncryptedPrefix) || bytes.Contains(body, []byte("three")) {
		t.Fatalf("unencrypted Redshift retry payload %q", body)
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := secretPack.Invoke(context.Background(), &spi.Request{Identity: id, Operation: "PutSecretValue", Input: map[string]any{
		"SecretId": secret.Output["ARN"], "SecretString": `{"username":"firehose","password":"secret-password"}`,
	}}); err != nil {
		t.Fatal(err)
	}
	p = New(deps)
	defer func() { _ = p.Close() }()
	if err := deps.Clock.Advance(5 * time.Minute); err != nil {
		t.Fatal(err)
	}
	for deadline := time.Now().Add(pollBudget); time.Now().Before(deadline); {
		rows, _ := redshift.TableRows(context.Background(), id, "retry-warehouse", "analytics", "events")
		if len(rows) == 1 {
			if !reflect.DeepEqual(rows[0], map[string]any{"id": "3", "payload": "three"}) {
				t.Fatalf("retried Redshift row %#v", rows)
			}
			if workItems, _, _ := collection.List(context.Background(), "redshift-retry/", "", 0); len(workItems) != 0 {
				t.Fatalf("successful Redshift retry remained persisted %#v", workItems)
			}
			if _, _, err := deps.Blobs.Get(context.Background(), work.DataKey); err == nil {
				t.Fatal("successful Redshift retry payload remained persisted")
			}
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("persisted Redshift COPY did not retry")
}

func TestFirehoseRedshiftRetryExpiryAndDelete(t *testing.T) {
	deps := spitest.Deps(t)
	id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
	redshift := redshiftservice.New(deps)
	if _, err := redshift.Invoke(context.Background(), &spi.Request{Identity: id, Operation: "CreateCluster", Input: map[string]any{
		"ClusterIdentifier": "failed-warehouse", "DBName": "analytics", "MasterUsername": "firehose", "MasterUserPassword": "secret-password",
	}}); err != nil {
		t.Fatal(err)
	}
	if err := redshift.CreateTable(context.Background(), id, "failed-warehouse", "analytics", "events", []string{"id"}); err != nil {
		t.Fatal(err)
	}
	p := New(deps)
	defer func() { _ = p.Close() }()
	call := func(operation string, input map[string]any) (*spi.Response, error) {
		return p.Invoke(context.Background(), &spi.Request{Identity: id, Operation: operation, Input: input})
	}
	destination := func(table string, duration int) map[string]any {
		return map[string]any{
			"ClusterJDBCURL": "jdbc:redshift://failed-warehouse.abc.us-east-1.redshift.amazonaws.com:5439/analytics", "RoleARN": testRoleARN,
			"CopyCommand": map[string]any{"DataTableName": table}, "Username": "firehose", "Password": "wrong-password",
			"RetryOptions": map[string]any{"DurationInSeconds": duration}, "S3Configuration": testS3Destination(),
		}
	}
	if _, err := call("CreateDeliveryStream", map[string]any{"DeliveryStreamName": "redshift-expiry", "RedshiftDestinationConfiguration": destination("events", 300)}); err != nil {
		t.Fatal(err)
	}
	if _, err := call("PutRecord", map[string]any{"DeliveryStreamName": "redshift-expiry", "Record": map[string]any{"Data": base64.StdEncoding.EncodeToString([]byte("1\n"))}}); err != nil {
		t.Fatal(err)
	}
	collection := deps.Store.Scope(id.Account, id.Region).Collection("fh-redshift-work")
	items, _, _ := collection.List(context.Background(), "redshift-expiry/", "", 0)
	var expired redshiftWork
	if len(items) != 1 || json.Unmarshal(items[0].Value, &expired) != nil {
		t.Fatalf("Redshift expiry work %#v", items)
	}
	if err := deps.Clock.Advance(5 * time.Minute); err != nil {
		t.Fatal(err)
	}
	for deadline := time.Now().Add(pollBudget); ; {
		if work, _, _ := collection.List(context.Background(), "redshift-expiry/", "", 0); len(work) == 0 {
			break
		}
		if !time.Now().Before(deadline) {
			t.Fatal("expired Redshift retry remained persisted")
		}
		time.Sleep(time.Millisecond)
	}
	if _, _, err := deps.Blobs.Get(context.Background(), expired.DataKey); err == nil {
		t.Fatal("expired Redshift retry payload remained persisted")
	}
	rows, _ := redshift.TableRows(context.Background(), id, "failed-warehouse", "analytics", "events")
	if len(rows) != 0 {
		t.Fatalf("expired Redshift retry loaded rows %#v", rows)
	}

	if _, err := call("CreateDeliveryStream", map[string]any{"DeliveryStreamName": "redshift-delete", "RedshiftDestinationConfiguration": destination("missing", 600)}); err != nil {
		t.Fatal(err)
	}
	if _, err := call("PutRecord", map[string]any{"DeliveryStreamName": "redshift-delete", "Record": map[string]any{"Data": base64.StdEncoding.EncodeToString([]byte("2\n"))}}); err != nil {
		t.Fatal(err)
	}
	items, _, _ = collection.List(context.Background(), "redshift-delete/", "", 0)
	var deleted redshiftWork
	if len(items) != 1 || json.Unmarshal(items[0].Value, &deleted) != nil {
		t.Fatalf("Redshift deletion work %#v", items)
	}
	if _, err := call("DeleteDeliveryStream", map[string]any{"DeliveryStreamName": "redshift-delete"}); err != nil {
		t.Fatal(err)
	}
	if work, _, _ := collection.List(context.Background(), "redshift-delete/", "", 0); len(work) != 0 {
		t.Fatalf("deleted stream retained Redshift work %#v", work)
	}
	if _, _, err := deps.Blobs.Get(context.Background(), deleted.DataKey); err == nil {
		t.Fatal("deleted stream retained Redshift retry payload")
	}
}

func TestIcebergDestinationValidationAndDescription(t *testing.T) {
	valid := testIcebergDestination()
	if err := validateIcebergDestination(valid, "us-east-1"); err != nil {
		t.Fatal(err)
	}
	invalid := []map[string]any{}
	add := func(key string, value any) {
		candidate := maps.Clone(valid)
		candidate[key] = value
		invalid = append(invalid, candidate)
	}
	add("CatalogConfiguration", map[string]any{"CatalogARN": "catalog"})
	add("RoleARN", "role")
	add("S3Configuration", nil)
	add("BufferingHints", map[string]any{"IntervalInSeconds": 901, "SizeInMBs": 5})
	add("RetryOptions", map[string]any{"DurationInSeconds": 7201})
	add("S3BackupMode", "Everything")
	add("AppendOnly", "true")
	add("DestinationTableConfigurationList", "events")
	add("DestinationTableConfigurationList", []any{map[string]any{"DestinationDatabaseName": "bad-name", "DestinationTableName": "events"}})
	add("DestinationTableConfigurationList", []any{
		map[string]any{"DestinationDatabaseName": "analytics", "DestinationTableName": "events"},
		map[string]any{"DestinationDatabaseName": "analytics", "DestinationTableName": "events"},
	})
	add("DestinationTableConfigurationList", []any{map[string]any{"DestinationDatabaseName": "analytics", "DestinationTableName": "events", "UniqueKeys": []any{""}}})
	for index, destination := range invalid {
		if err := validateIcebergDestination(destination, "us-east-1"); err == nil {
			t.Fatalf("accepted invalid Iceberg destination %d: %#v", index, destination)
		}
	}
	p := New(spitest.Deps(t))
	id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
	call := func(operation string, input map[string]any) (*spi.Response, error) {
		return p.Invoke(context.Background(), &spi.Request{Identity: id, Operation: operation, Input: input})
	}
	if _, err := call("CreateDeliveryStream", map[string]any{"DeliveryStreamName": "iceberg", "IcebergDestinationConfiguration": valid}); err != nil {
		t.Fatal(err)
	}
	described, err := call("DescribeDeliveryStream", map[string]any{"DeliveryStreamName": "iceberg"})
	if err != nil {
		t.Fatal(err)
	}
	description := described.Output["DeliveryStreamDescription"].(map[string]any)["Destinations"].([]any)[0].(map[string]any)["IcebergDestinationDescription"].(map[string]any)
	if first(description, "S3BackupMode") != "FailedDataOnly" || !reflect.DeepEqual(description["BufferingHints"], map[string]any{"IntervalInSeconds": 300, "SizeInMBs": 5}) || !reflect.DeepEqual(description["RetryOptions"], map[string]any{"DurationInSeconds": 300}) || description["S3DestinationDescription"] == nil {
		t.Fatalf("Iceberg destination description %#v", description)
	}
	if _, err := call("UpdateDestination", map[string]any{
		"DeliveryStreamName": "iceberg", "CurrentDeliveryStreamVersionId": "1", "DestinationId": destinationID,
		"IcebergDestinationUpdate": map[string]any{"AppendOnly": true},
	}); err != nil {
		t.Fatal(err)
	}
}

func TestFirehoseIcebergDestination(t *testing.T) {
	deps := spitest.Deps(t)
	ctx := context.Background()
	id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
	tables := s3tablesservice.New(deps)
	if _, err := tables.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateTableBucket", Input: map[string]any{"name": "warehouse"}}); err != nil {
		t.Fatal(err)
	}
	if err := tables.CreateTable(ctx, id, "warehouse", "analytics", "events", []string{"id", "name", "op"}); err != nil {
		t.Fatal(err)
	}
	p := New(deps)
	defer func() { _ = p.Close() }()
	destination := testIcebergDestination()
	destination["S3BackupMode"] = "AllData"
	destination["S3Configuration"] = map[string]any{
		"BucketARN": "arn:aws:s3:::out", "RoleARN": testRoleARN, "Prefix": "source/", "ErrorOutputPrefix": "errors/!{firehose:error-output-type}/",
	}
	destination["ProcessingConfiguration"] = map[string]any{"Enabled": true, "Processors": []any{map[string]any{
		"Type": "MetadataExtraction", "Parameters": []any{
			map[string]any{"ParameterName": "MetadataExtractionQuery", "ParameterValue": "{operation: .op}"},
			map[string]any{"ParameterName": "JsonParsingEngine", "ParameterValue": "JQ-1.6"},
		},
	}}}
	call := func(operation string, input map[string]any) (*spi.Response, error) {
		return p.Invoke(ctx, &spi.Request{Identity: id, Operation: operation, Input: input})
	}
	if _, err := call("CreateDeliveryStream", map[string]any{"DeliveryStreamName": "iceberg-delivery", "IcebergDestinationConfiguration": destination}); err != nil {
		t.Fatal(err)
	}
	data := []string{
		`{"id":"1","name":"alice","op":"insert"}`,
		`{"id":"2","name":"bob","op":"insert"}`,
		`{"id":"2","name":"robert","op":"update"}`,
		`{"id":"1","op":"delete"}`,
	}
	response, err := call("PutRecordBatch", map[string]any{"DeliveryStreamName": "iceberg-delivery", "Records": []any{
		map[string]any{"Data": base64.StdEncoding.EncodeToString([]byte(data[0]))},
		map[string]any{"Data": base64.StdEncoding.EncodeToString([]byte(data[1]))},
		map[string]any{"Data": base64.StdEncoding.EncodeToString([]byte(data[2]))},
		map[string]any{"Data": base64.StdEncoding.EncodeToString([]byte(data[3]))},
	}})
	if err != nil {
		t.Fatal(err)
	}
	rows, err := tables.TableRows(ctx, id, "warehouse", "analytics", "events")
	want := []map[string]any{{"id": "2", "name": "robert", "op": "update"}}
	if err != nil || !reflect.DeepEqual(rows, want) {
		t.Fatalf("Iceberg rows %#v, %v", rows, err)
	}
	for index, raw := range response.Output["RequestResponses"].([]any) {
		recordID := first(raw.(map[string]any), "RecordId")
		key := id.Account + "/" + id.Region + "/out/source/1970/01/01/00/iceberg-delivery-1-1970-01-01-00-00-00-" + recordID
		reader, _, err := deps.Blobs.Get(ctx, key)
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(reader)
		_ = reader.Close()
		if string(body) != data[index] {
			t.Fatalf("Iceberg source backup %q", body)
		}
	}

	appendOnly := maps.Clone(destination)
	appendOnly["AppendOnly"], appendOnly["S3BackupMode"] = true, "FailedDataOnly"
	if _, err := call("CreateDeliveryStream", map[string]any{"DeliveryStreamName": "iceberg-append", "IcebergDestinationConfiguration": appendOnly}); err != nil {
		t.Fatal(err)
	}
	failed, err := call("PutRecord", map[string]any{
		"DeliveryStreamName": "iceberg-append", "Record": map[string]any{"Data": base64.StdEncoding.EncodeToString([]byte(`{"id":"2","name":"wrong","op":"update"}`))},
	})
	if err != nil {
		t.Fatal(err)
	}
	failedID := first(failed.Output, "RecordId")
	failureKey := id.Account + "/" + id.Region + "/out/errors/iceberg-failed/1970/01/01/00/iceberg-append-1-1970-01-01-00-00-00-" + failedID
	reader, _, err := deps.Blobs.Get(ctx, failureKey)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(reader)
	_ = reader.Close()
	if !bytes.Contains(body, []byte("append-only")) {
		t.Fatalf("Iceberg failure envelope %q", body)
	}
	rows, err = tables.TableRows(ctx, id, "warehouse", "analytics", "events")
	if err != nil || !reflect.DeepEqual(rows, want) {
		t.Fatalf("append-only Iceberg mutation changed rows %#v, %v", rows, err)
	}
}

func TestSnowflakeDestinationValidationAndDescription(t *testing.T) {
	destination := testSnowflakeDestination()
	if err := validateSnowflakeDestination(destination, "us-east-1"); err != nil {
		t.Fatal(err)
	}
	if interval, size := endpointBufferingHints("SnowflakeDestinationConfiguration", destination); interval != 0 || size != 128*1024*1024 || endpointRetryDuration("SnowflakeDestinationConfiguration", destination) != 60*time.Second {
		t.Fatalf("Snowflake delivery defaults %s %d %s", interval, size, endpointRetryDuration("SnowflakeDestinationConfiguration", destination))
	}
	secret := maps.Clone(destination)
	delete(secret, "User")
	delete(secret, "PrivateKey")
	secret["SecretsManagerConfiguration"] = map[string]any{
		"Enabled": true, "SecretARN": "arn:aws:secretsmanager:us-east-1:123456789012:secret:snowflake",
	}
	if err := validateSnowflakeDestination(secret, "us-east-1"); err != nil {
		t.Fatal(err)
	}
	for index, change := range []func(map[string]any){
		func(value map[string]any) { value["AccountUrl"] = "https://example.com" },
		func(value map[string]any) { value["Database"] = "" },
		func(value map[string]any) { value["DataLoadingOption"] = "CSV" },
		func(value map[string]any) { value["DataLoadingOption"] = "VARIANT_CONTENT_MAPPING" },
		func(value map[string]any) { value["PrivateKey"] = "not-base64" },
		func(value map[string]any) { value["KeyPassphrase"] = "short" },
		func(value map[string]any) { value["S3BackupMode"] = "Enabled" },
	} {
		candidate := maps.Clone(destination)
		change(candidate)
		if err := validateSnowflakeDestination(candidate, "us-east-1"); err == nil {
			t.Fatalf("accepted invalid Snowflake destination %d: %#v", index, candidate)
		}
	}

	p := New(spitest.Deps(t))
	id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
	call := func(operation string, input map[string]any) (*spi.Response, error) {
		t.Helper()
		return p.Invoke(context.Background(), &spi.Request{Identity: id, Operation: operation, Input: input})
	}
	if _, err := call("CreateDeliveryStream", map[string]any{"DeliveryStreamName": "snowflake", "SnowflakeDestinationConfiguration": destination}); err != nil {
		t.Fatal(err)
	}
	response, err := call("DescribeDeliveryStream", map[string]any{"DeliveryStreamName": "snowflake"})
	if err != nil {
		t.Fatal(err)
	}
	description := response.Output["DeliveryStreamDescription"].(map[string]any)["Destinations"].([]any)[0].(map[string]any)["SnowflakeDestinationDescription"].(map[string]any)
	if description["PrivateKey"] != nil || description["KeyPassphrase"] != nil || description["S3Configuration"] != nil || description["S3DestinationDescription"] == nil || first(description, "DataLoadingOption") != "JSON_MAPPING" || first(description, "S3BackupMode") != "FailedDataOnly" || !reflect.DeepEqual(description["BufferingHints"], map[string]any{"IntervalInSeconds": 0, "SizeInMBs": 128}) || !reflect.DeepEqual(description["RetryOptions"], map[string]any{"DurationInSeconds": 60}) {
		t.Fatalf("Snowflake description %#v", description)
	}
	if _, err := call("UpdateDestination", map[string]any{
		"DeliveryStreamName": "snowflake", "CurrentDeliveryStreamVersionId": "1", "DestinationId": destinationID,
		"SnowflakeDestinationUpdate": map[string]any{"Table": "EVENTS_V2", "RetryOptions": map[string]any{"DurationInSeconds": 30}},
	}); err != nil {
		t.Fatal(err)
	}
	response, _ = call("DescribeDeliveryStream", map[string]any{"DeliveryStreamName": "snowflake"})
	description = response.Output["DeliveryStreamDescription"].(map[string]any)["Destinations"].([]any)[0].(map[string]any)["SnowflakeDestinationDescription"].(map[string]any)
	if first(description, "Table") != "EVENTS_V2" || description["RetryOptions"].(map[string]any)["DurationInSeconds"] != float64(30) {
		t.Fatalf("updated Snowflake description %#v", description)
	}
}

func TestFirehoseSnowflakeDestination(t *testing.T) {
	ctx := context.Background()
	deps := spitest.Deps(t)
	p := New(deps)
	defer func() { _ = p.Close() }()
	id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
	destination := testSnowflakeDestination()
	destination["BufferingHints"] = map[string]any{"IntervalInSeconds": 0, "SizeInMBs": 128}
	destination["DataLoadingOption"] = "VARIANT_CONTENT_AND_METADATA_MAPPING"
	destination["ContentColumnName"] = "payload"
	destination["MetaDataColumnName"] = "metadata"
	destination["S3BackupMode"] = "AllData"
	destination["S3Configuration"] = map[string]any{
		"BucketARN": "arn:aws:s3:::out", "RoleARN": testRoleARN, "Prefix": "source/", "ErrorOutputPrefix": "errors/!{firehose:error-output-type}/",
	}
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateDeliveryStream", Input: map[string]any{
		"DeliveryStreamName": "snowflake-delivery", "SnowflakeDestinationConfiguration": destination,
	}}); err != nil {
		t.Fatal(err)
	}
	records := []string{`{"id":"1","name":"alice"}`, `{"id":"2","name":"bob"}`, `["not-an-object"]`}
	input := make([]any, len(records))
	for index := range records {
		input[index] = map[string]any{"Data": base64.StdEncoding.EncodeToString([]byte(records[index]))}
	}
	response, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "PutRecordBatch", Input: map[string]any{
		"DeliveryStreamName": "snowflake-delivery", "Records": input,
	}})
	if err != nil {
		t.Fatal(err)
	}
	var rows []map[string]any
	for deadline := time.Now().Add(pollBudget); time.Now().Before(deadline); {
		rows, err = p.SnowflakeRows(ctx, id, first(destination, "AccountUrl"), "ANALYTICS", "PUBLIC", "EVENTS")
		if err == nil && len(rows) == 2 {
			break
		}
		time.Sleep(time.Millisecond)
	}
	want := []map[string]any{
		{"payload": map[string]any{"id": "1", "name": "alice"}, "metadata": map[string]any{"IngestionTime": "1970-01-01T00:00:00Z", "firehoseDeliveryStreamName": "snowflake-delivery"}},
		{"payload": map[string]any{"id": "2", "name": "bob"}, "metadata": map[string]any{"IngestionTime": "1970-01-01T00:00:00Z", "firehoseDeliveryStreamName": "snowflake-delivery"}},
	}
	if err != nil || !reflect.DeepEqual(rows, want) {
		t.Fatalf("Snowflake rows %#v, %v", rows, err)
	}
	responses := response.Output["RequestResponses"].([]any)
	for index := range records {
		recordID := first(responses[index].(map[string]any), "RecordId")
		key := id.Account + "/" + id.Region + "/out/source/1970/01/01/00/snowflake-delivery-1-1970-01-01-00-00-00-" + recordID
		reader, _, err := deps.Blobs.Get(ctx, key)
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(reader)
		_ = reader.Close()
		if string(body) != records[index] {
			t.Fatalf("Snowflake source backup %q", body)
		}
	}
	failedID := first(responses[2].(map[string]any), "RecordId")
	failureKey := id.Account + "/" + id.Region + "/out/errors/snowflake-failed/1970/01/01/00/snowflake-delivery-1-1970-01-01-00-00-00-" + failedID
	reader, _, err := deps.Blobs.Get(ctx, failureKey)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(reader)
	_ = reader.Close()
	failure := map[string]any{}
	if json.Unmarshal(body, &failure) != nil || failure["attemptsMade"] != float64(1) || failure["arrivalTimestamp"] != float64(0) || failure["errorCode"] != "Snowflake.InvalidInput" || failure["rawData"] != base64.StdEncoding.EncodeToString([]byte(records[2])) {
		t.Fatalf("Snowflake failure envelope %s", body)
	}
}

func TestFirehoseSnowflakeSecretAndPersistentBuffer(t *testing.T) {
	ctx := context.Background()
	deps := spitest.Deps(t)
	id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
	secretBody := fmt.Sprintf(`{"user":"firehose","private_key":%q}`, base64.StdEncoding.EncodeToString(make([]byte, 192)))
	secretPack := secretsmanager.New(deps)
	secret, err := secretPack.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateSecret", Input: map[string]any{"Name": "snowflake-key", "SecretString": secretBody}})
	if err != nil {
		t.Fatal(err)
	}
	destination := testSnowflakeDestination()
	delete(destination, "User")
	delete(destination, "PrivateKey")
	destination["BufferingHints"] = map[string]any{"IntervalInSeconds": 5, "SizeInMBs": 128}
	destination["RetryOptions"] = map[string]any{"DurationInSeconds": 0}
	destination["S3Configuration"] = map[string]any{"BucketARN": "arn:aws:s3:::out", "RoleARN": testRoleARN, "ErrorOutputPrefix": "errors/!{firehose:error-output-type}/"}
	destination["SecretsManagerConfiguration"] = map[string]any{"Enabled": true, "SecretARN": secret.Output["ARN"]}
	p := New(deps)
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateDeliveryStream", Input: map[string]any{
		"DeliveryStreamName": "snowflake-buffer", "SnowflakeDestinationConfiguration": destination,
		"DeliveryStreamEncryptionConfigurationInput": map[string]any{"KeyType": "AWS_OWNED_CMK"},
	}}); err != nil {
		t.Fatal(err)
	}
	_, err = p.Invoke(ctx, &spi.Request{Identity: id, Operation: "PutRecord", Input: map[string]any{
		"DeliveryStreamName": "snowflake-buffer", "Record": map[string]any{"Data": base64.StdEncoding.EncodeToString([]byte(`{"id":"buffered"}`))},
	}})
	if err != nil {
		t.Fatal(err)
	}
	buffers := deps.Store.Scope(id.Account, id.Region).Collection("fh-http-buffers")
	items, _, _ := buffers.List(ctx, "snowflake-buffer/", "", 0)
	if len(items) != 1 {
		t.Fatalf("persisted Snowflake buffers %#v", items)
	}
	var buffered httpBuffer
	_ = json.Unmarshal(items[0].Value, &buffered)
	reader, _, err := deps.Blobs.Get(ctx, buffered.DataKey)
	if err != nil {
		t.Fatal(err)
	}
	encrypted, _ := io.ReadAll(reader)
	_ = reader.Close()
	if !bytes.HasPrefix(encrypted, firehoseEncryptedPrefix) || bytes.Contains(encrypted, []byte("buffered")) {
		t.Fatalf("Snowflake buffer was not encrypted at rest: %q", encrypted)
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := secretPack.Invoke(ctx, &spi.Request{Identity: id, Operation: "PutSecretValue", Input: map[string]any{"SecretId": secret.Output["ARN"], "SecretString": secretBody}}); err != nil {
		t.Fatal(err)
	}
	p = New(deps)
	defer func() { _ = p.Close() }()
	if err := deps.Clock.Advance(5 * time.Second); err != nil {
		t.Fatal(err)
	}
	var rows []map[string]any
	for deadline := time.Now().Add(pollBudget); time.Now().Before(deadline); {
		rows, err = p.SnowflakeRows(ctx, id, first(destination, "AccountUrl"), "ANALYTICS", "PUBLIC", "EVENTS")
		if err == nil && len(rows) == 1 {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if err != nil || !reflect.DeepEqual(rows, []map[string]any{{"id": "buffered"}}) {
		t.Fatalf("restarted Snowflake buffer rows %#v, %v", rows, err)
	}
	if remaining, _, _ := buffers.List(ctx, "snowflake-buffer/", "", 0); len(remaining) != 0 {
		t.Fatalf("successful Snowflake buffer remained %#v", remaining)
	}
	if _, _, err := deps.Blobs.Get(ctx, buffered.DataKey); err == nil {
		t.Fatal("successful Snowflake buffer payload remained")
	}

	if _, err := secretPack.Invoke(ctx, &spi.Request{Identity: id, Operation: "PutSecretValue", Input: map[string]any{"SecretId": secret.Output["ARN"], "SecretString": `{}`}}); err != nil {
		t.Fatal(err)
	}
	immediate := maps.Clone(destination)
	immediate["BufferingHints"] = map[string]any{"IntervalInSeconds": 0, "SizeInMBs": 128}
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateDeliveryStream", Input: map[string]any{"DeliveryStreamName": "snowflake-rotated", "SnowflakeDestinationConfiguration": immediate}}); err != nil {
		t.Fatal(err)
	}
	failed, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "PutRecord", Input: map[string]any{"DeliveryStreamName": "snowflake-rotated", "Record": map[string]any{"Data": base64.StdEncoding.EncodeToString([]byte(`{"id":"failed"}`))}}})
	if err != nil {
		t.Fatal(err)
	}
	failureKey := id.Account + "/" + id.Region + "/out/errors/snowflake-failed/1970/01/01/00/snowflake-rotated-1-1970-01-01-00-00-05-" + first(failed.Output, "RecordId")
	for deadline := time.Now().Add(pollBudget); time.Now().Before(deadline); {
		reader, _, err = deps.Blobs.Get(ctx, failureKey)
		if err == nil {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if err != nil {
		t.Fatal(err)
	}
	failureBody, _ := io.ReadAll(reader)
	_ = reader.Close()
	if !bytes.Contains(failureBody, []byte("Snowflake.InvalidPrivateKeyOrPassphrase")) {
		t.Fatalf("rotated Snowflake secret failure %s", failureBody)
	}
}

func TestSplunkDestinationValidationAndDescription(t *testing.T) {
	valid := map[string]any{
		"HECEndpoint": "https://splunk.example.com:8088", "HECEndpointType": "Raw", "HECToken": "token",
		"S3Configuration": testS3Destination(),
	}
	if err := validateSplunkDestination(valid, "us-east-1"); err != nil {
		t.Fatal(err)
	}
	secret := maps.Clone(valid)
	delete(secret, "HECToken")
	secret["SecretsManagerConfiguration"] = map[string]any{
		"Enabled": true, "RoleARN": testRoleARN, "SecretARN": "arn:aws:secretsmanager:us-east-1:123456789012:secret:splunk",
	}
	if err := validateSplunkDestination(secret, "us-east-1"); err != nil {
		t.Fatal(err)
	}
	invalid := []map[string]any{}
	add := func(key string, value any) {
		candidate := maps.Clone(valid)
		if value == nil {
			delete(candidate, key)
		} else {
			candidate[key] = value
		}
		invalid = append(invalid, candidate)
	}
	add("HECEndpoint", "http://splunk.example.com")
	add("HECEndpointType", "Other")
	add("HECToken", "bad\ntoken")
	add("HECToken", nil)
	add("BufferingHints", map[string]any{"IntervalInSeconds": 61, "SizeInMBs": 5})
	add("BufferingHints", map[string]any{"IntervalInSeconds": 60, "SizeInMBs": 6})
	add("RetryOptions", map[string]any{"DurationInSeconds": 7201})
	add("HECAcknowledgmentTimeoutInSeconds", 179)
	add("S3BackupMode", "Everything")
	add("S3Configuration", nil)
	for index, candidate := range invalid {
		if err := validateSplunkDestination(candidate, "us-east-1"); err == nil {
			t.Fatalf("accepted invalid Splunk destination %d: %#v", index, candidate)
		}
	}

	deps := spitest.Deps(t)
	p := New(deps)
	id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
	call := func(operation string, input map[string]any) (*spi.Response, error) {
		return p.Invoke(context.Background(), &spi.Request{Identity: id, Operation: operation, Input: input})
	}
	if _, err := call("CreateDeliveryStream", map[string]any{"DeliveryStreamName": "splunk", "SplunkDestinationConfiguration": valid}); err != nil {
		t.Fatal(err)
	}
	response, err := call("DescribeDeliveryStream", map[string]any{"DeliveryStreamName": "splunk"})
	if err != nil {
		t.Fatal(err)
	}
	description := response.Output["DeliveryStreamDescription"].(map[string]any)["Destinations"].([]any)[0].(map[string]any)["SplunkDestinationDescription"].(map[string]any)
	if description["HECToken"] != nil || first(description, "S3BackupMode") != "FailedEventsOnly" || description["HECAcknowledgmentTimeoutInSeconds"] != 300 {
		t.Fatalf("Splunk description %#v", description)
	}
	if _, err := call("UpdateDestination", map[string]any{
		"DeliveryStreamName": "splunk", "CurrentDeliveryStreamVersionId": "1", "DestinationId": destinationID,
		"SplunkDestinationUpdate": map[string]any{"S3BackupMode": "AllEvents"},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := call("UpdateDestination", map[string]any{
		"DeliveryStreamName": "splunk", "CurrentDeliveryStreamVersionId": "2", "DestinationId": destinationID,
		"SplunkDestinationUpdate": map[string]any{"S3BackupMode": "FailedEventsOnly"},
	}); err == nil {
		t.Fatal("disabled Splunk AllEvents backup")
	}
}

func TestFirehoseSplunkDestination(t *testing.T) {
	type capturedRequest struct {
		path, authorization, channel, body string
	}
	captured := make(chan capturedRequest, 2)
	server := httptest.NewTLSServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		body, _ := io.ReadAll(request.Body)
		captured <- capturedRequest{request.URL.Path, request.Header.Get("Authorization"), request.Header.Get("X-Splunk-Request-Channel"), string(body)}
		writer.Header().Set("Content-Type", "application/json")
		if request.URL.Path == "/services/collector/ack" {
			_, _ = writer.Write([]byte(`{"acks":{"7":true}}`))
			return
		}
		_, _ = writer.Write([]byte(`{"ackID":7}`))
	}))
	defer server.Close()

	deps := spitest.Deps(t)
	p := New(deps)
	defer func() { _ = p.Close() }()
	p.httpClient = server.Client()
	id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
	call := func(operation string, input map[string]any) (*spi.Response, error) {
		t.Helper()
		return p.Invoke(context.Background(), &spi.Request{Identity: id, Operation: operation, Input: input})
	}
	destination := map[string]any{
		"HECEndpoint": server.URL, "HECEndpointType": "Raw", "HECToken": "token",
		"BufferingHints": map[string]any{"IntervalInSeconds": 0, "SizeInMBs": 1}, "S3BackupMode": "AllEvents",
		"S3Configuration": map[string]any{"BucketARN": "arn:aws:s3:::out", "RoleARN": testRoleARN, "Prefix": "all/"},
	}
	if _, err := call("CreateDeliveryStream", map[string]any{"DeliveryStreamName": "splunk-delivery", "SplunkDestinationConfiguration": destination}); err != nil {
		t.Fatal(err)
	}
	response, err := call("PutRecordBatch", map[string]any{"DeliveryStreamName": "splunk-delivery", "Records": []any{
		map[string]any{"Data": base64.StdEncoding.EncodeToString([]byte("one"))},
		map[string]any{"Data": base64.StdEncoding.EncodeToString([]byte("two"))},
	}})
	if err != nil {
		t.Fatal(err)
	}
	recordIDs := response.Output["RequestResponses"].([]any)
	firstID := first(recordIDs[0].(map[string]any), "RecordId")
	requests := make([]capturedRequest, 2)
	for index := range requests {
		select {
		case requests[index] = <-captured:
		case <-time.After(pollBudget):
			t.Fatal("Splunk HEC delivery timed out")
		}
	}
	if requests[0].path != "/services/collector/raw" || requests[0].authorization != "Splunk token" || requests[0].channel != firstID || requests[0].body != "onetwo" {
		t.Fatalf("Splunk HEC request %#v", requests[0])
	}
	if requests[1].path != "/services/collector/ack" || requests[1].authorization != "Splunk token" || requests[1].channel != firstID || requests[1].body != `{"acks":[7]}` {
		t.Fatalf("Splunk HEC acknowledgment request %#v", requests[1])
	}
	for index, value := range []string{"one", "two"} {
		recordID := first(recordIDs[index].(map[string]any), "RecordId")
		key := id.Account + "/" + id.Region + "/out/all/1970/01/01/00/splunk-delivery-1-1970-01-01-00-00-00-" + recordID
		reader, _, err := deps.Blobs.Get(context.Background(), key)
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(reader)
		_ = reader.Close()
		if string(body) != value {
			t.Fatalf("Splunk backup %q", body)
		}
	}
}

func TestFirehoseSplunkFailureBackup(t *testing.T) {
	requests := make(chan string, 2)
	server := httptest.NewTLSServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		body, _ := io.ReadAll(request.Body)
		requests <- request.URL.Path + ":" + string(body)
		writer.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()
	deps := spitest.Deps(t)
	p := New(deps)
	defer func() { _ = p.Close() }()
	p.httpClient = server.Client()
	id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
	destination := map[string]any{
		"HECEndpoint": server.URL, "HECEndpointType": "Event", "HECToken": "token", "S3BackupMode": "AllEvents",
		"BufferingHints": map[string]any{"IntervalInSeconds": 0, "SizeInMBs": 1}, "RetryOptions": map[string]any{"DurationInSeconds": 0},
		"S3Configuration": map[string]any{"BucketARN": "arn:aws:s3:::out", "RoleARN": testRoleARN, "Prefix": "all/", "ErrorOutputPrefix": "errors/!{firehose:error-output-type}/"},
	}
	if _, err := p.Invoke(context.Background(), &spi.Request{Identity: id, Operation: "CreateDeliveryStream", Input: map[string]any{"DeliveryStreamName": "splunk-failure", "SplunkDestinationConfiguration": destination}}); err != nil {
		t.Fatal(err)
	}
	response, err := p.Invoke(context.Background(), &spi.Request{Identity: id, Operation: "PutRecord", Input: map[string]any{"DeliveryStreamName": "splunk-failure", "Record": map[string]any{"Data": base64.StdEncoding.EncodeToString([]byte("failed"))}}})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case request := <-requests:
		if request != "/services/collector/event:failed" {
			t.Fatalf("Splunk event request %q", request)
		}
	case <-time.After(pollBudget):
		t.Fatal("Splunk failure request timed out")
	}
	recordID := response.Output["RecordId"].(string)
	failureKey := id.Account + "/" + id.Region + "/out/errors/splunk-failed/1970/01/01/00/splunk-failure-1-1970-01-01-00-00-00-" + recordID
	var failureBody []byte
	for deadline := time.Now().Add(pollBudget); time.Now().Before(deadline); {
		reader, _, err := deps.Blobs.Get(context.Background(), failureKey)
		if err == nil {
			failureBody, _ = io.ReadAll(reader)
			_ = reader.Close()
			break
		}
		time.Sleep(time.Millisecond)
	}
	failure := map[string]any{}
	if json.Unmarshal(failureBody, &failure) != nil || failure["attemptsMade"] != float64(0) || failure["arrivalTimestamp"] != float64(0) || failure["attemptEndingTimestamp"] != float64(0) || failure["errorCode"] != "Splunk.ServerError" || failure["rawData"] != base64.StdEncoding.EncodeToString([]byte("failed")) || failure["EventId"] != recordID {
		t.Fatalf("Splunk failure envelope %s", failureBody)
	}
	rawKey := id.Account + "/" + id.Region + "/out/all/1970/01/01/00/splunk-failure-1-1970-01-01-00-00-00-" + recordID
	reader, _, err := deps.Blobs.Get(context.Background(), rawKey)
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := io.ReadAll(reader)
	_ = reader.Close()
	if string(raw) != "failed" {
		t.Fatalf("Splunk AllEvents backup %q", raw)
	}
}

func TestFirehoseSplunkSecretAndPersistentRetry(t *testing.T) {
	var deliveries atomic.Int32
	requests := make(chan http.Header, 4)
	server := httptest.NewTLSServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		requests <- request.Header.Clone()
		writer.Header().Set("Content-Type", "application/json")
		if request.URL.Path != "/services/collector/ack" && deliveries.Add(1) == 1 {
			writer.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		if request.URL.Path == "/services/collector/ack" {
			_, _ = writer.Write([]byte(`{"acks":{"11":true}}`))
			return
		}
		_, _ = writer.Write([]byte(`{"ackId":"11"}`))
	}))
	defer server.Close()
	deps := spitest.Deps(t)
	id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
	secret, err := secretsmanager.New(deps).Invoke(context.Background(), &spi.Request{Identity: id, Operation: "CreateSecret", Input: map[string]any{"Name": "splunk-token", "SecretString": `{"hec_token":"from-secret"}`}})
	if err != nil {
		t.Fatal(err)
	}
	p := New(deps)
	p.httpClient = server.Client()
	destination := map[string]any{
		"HECEndpoint": server.URL, "HECEndpointType": "Raw", "BufferingHints": map[string]any{"IntervalInSeconds": 0, "SizeInMBs": 1},
		"RetryOptions": map[string]any{"DurationInSeconds": 10}, "S3Configuration": testS3Destination(),
		"SecretsManagerConfiguration": map[string]any{"Enabled": true, "RoleARN": testRoleARN, "SecretARN": secret.Output["ARN"]},
	}
	if _, err := p.Invoke(context.Background(), &spi.Request{Identity: id, Operation: "CreateDeliveryStream", Input: map[string]any{"DeliveryStreamName": "splunk-retry", "SplunkDestinationConfiguration": destination}}); err != nil {
		t.Fatal(err)
	}
	response, err := p.Invoke(context.Background(), &spi.Request{Identity: id, Operation: "PutRecord", Input: map[string]any{"DeliveryStreamName": "splunk-retry", "Record": map[string]any{"Data": base64.StdEncoding.EncodeToString([]byte("retry"))}}})
	if err != nil {
		t.Fatal(err)
	}
	initial := <-requests
	requestID := response.Output["RecordId"].(string)
	if initial.Get("Authorization") != "Splunk from-secret" || initial.Get("X-Splunk-Request-Channel") != requestID {
		t.Fatalf("initial Splunk secret request %#v", initial)
	}
	retryCollection := deps.Store.Scope(id.Account, id.Region).Collection("fh-http-retries")
	var retryKey, dataKey string
	for deadline := time.Now().Add(pollBudget); time.Now().Before(deadline); {
		items, _, _ := retryCollection.List(context.Background(), "", "", 0)
		if len(items) == 1 {
			var retry httpRetry
			if json.Unmarshal(items[0].Value, &retry) == nil && retry.Destination == "SplunkDestinationConfiguration" && retry.ErrorCode == "Splunk.ServerError" {
				retryKey, dataKey = items[0].Key, retry.DataKey
				break
			}
		}
		time.Sleep(time.Millisecond)
	}
	if retryKey == "" {
		t.Fatal("Splunk retry was not persisted with its destination and error")
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	p = New(deps)
	p.httpClient = server.Client()
	defer func() { _ = p.Close() }()
	if err := deps.Clock.Advance(2 * time.Second); err != nil {
		t.Fatal(err)
	}
	for index := 0; index < 2; index++ {
		select {
		case request := <-requests:
			if request.Get("Authorization") != "Splunk from-secret" || request.Get("X-Splunk-Request-Channel") != requestID {
				t.Fatalf("retried Splunk request %#v", request)
			}
		case <-time.After(pollBudget):
			t.Fatal("persisted Splunk retry did not complete")
		}
	}
	for deadline := time.Now().Add(pollBudget); time.Now().Before(deadline); {
		if _, ok, _ := retryCollection.Get(context.Background(), retryKey); !ok {
			if _, _, err := deps.Blobs.Get(context.Background(), dataKey); err == nil {
				t.Fatal("successful Splunk retry payload remained persisted")
			}
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("successful Splunk retry metadata remained persisted")
}

func TestFirehoseSplunkAcknowledgmentTimeout(t *testing.T) {
	acknowledgments := make(chan struct{}, 2)
	server := httptest.NewTLSServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		if request.URL.Path == "/services/collector/ack" {
			acknowledgments <- struct{}{}
			_, _ = writer.Write([]byte(`{"acks":{"13":false}}`))
			return
		}
		_, _ = writer.Write([]byte(`{"ackID":13}`))
	}))
	defer server.Close()
	deps := spitest.Deps(t)
	p := New(deps)
	defer func() { _ = p.Close() }()
	p.httpClient = server.Client()
	id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
	destination := map[string]any{
		"HECEndpoint": server.URL, "HECEndpointType": "Raw", "HECToken": "token", "HECAcknowledgmentTimeoutInSeconds": 180,
		"BufferingHints": map[string]any{"IntervalInSeconds": 0, "SizeInMBs": 1}, "RetryOptions": map[string]any{"DurationInSeconds": 0},
		"S3Configuration": map[string]any{"BucketARN": "arn:aws:s3:::out", "RoleARN": testRoleARN, "Prefix": "failed/"},
	}
	if _, err := p.Invoke(context.Background(), &spi.Request{Identity: id, Operation: "CreateDeliveryStream", Input: map[string]any{"DeliveryStreamName": "splunk-timeout", "SplunkDestinationConfiguration": destination}}); err != nil {
		t.Fatal(err)
	}
	response, err := p.Invoke(context.Background(), &spi.Request{Identity: id, Operation: "PutRecord", Input: map[string]any{"DeliveryStreamName": "splunk-timeout", "Record": map[string]any{"Data": base64.StdEncoding.EncodeToString([]byte("timeout"))}}})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-acknowledgments:
	case <-time.After(pollBudget):
		t.Fatal("Splunk acknowledgment polling did not start")
	}
	if err := deps.Clock.Advance(180 * time.Second); err != nil {
		t.Fatal(err)
	}
	recordID := response.Output["RecordId"].(string)
	key := id.Account + "/" + id.Region + "/out/failed/splunk-failed/1970/01/01/00/splunk-timeout-1-1970-01-01-00-03-00-" + recordID
	for deadline := time.Now().Add(pollBudget); time.Now().Before(deadline); {
		reader, _, err := deps.Blobs.Get(context.Background(), key)
		if err == nil {
			body, _ := io.ReadAll(reader)
			_ = reader.Close()
			failure := map[string]any{}
			if json.Unmarshal(body, &failure) != nil || failure["errorCode"] != "Splunk.AckTimeout" || failure["arrivalTimestamp"] != float64(0) || failure["attemptEndingTimestamp"] != float64(180000) {
				t.Fatalf("Splunk acknowledgment timeout %s", body)
			}
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("Splunk acknowledgment timeout was not backed up")
}

func TestSplunkProtocolHelpers(t *testing.T) {
	for _, test := range []struct {
		body string
		id   any
		key  string
		ok   bool
	}{
		{`{"ackID":7}`, int64(7), "7", true},
		{`{"ackId":"8"}`, "8", "8", true},
		{`{"ackID":-1}`, nil, "", false},
		{`{"ackID":1.5}`, nil, "", false},
		{`{"ackId":"bad"}`, nil, "", false},
		{`{}`, nil, "", false},
		{`not-json`, nil, "", false},
	} {
		id, key, ok := splunkAckID([]byte(test.body))
		if !reflect.DeepEqual(id, test.id) || key != test.key || ok != test.ok {
			t.Errorf("splunkAckID(%q) = %#v, %q, %v", test.body, id, key, ok)
		}
	}
	for status, expected := range map[int]string{
		http.StatusUnauthorized:          "Splunk.InvalidToken",
		http.StatusForbidden:             "Splunk.InvalidToken",
		http.StatusNotFound:              "Splunk.URLNotFound",
		http.StatusRequestEntityTooLarge: "Splunk.ServerError.ContentTooLarge",
		http.StatusInternalServerError:   "Splunk.ServerError",
	} {
		if code := splunkStatusError(status); code != expected {
			t.Errorf("splunkStatusError(%d) = %q", status, code)
		}
	}
	if endpointDestinationKey("") != "HttpEndpointDestinationConfiguration" || endpointDestinationKey("SplunkDestinationConfiguration") != "SplunkDestinationConfiguration" {
		t.Fatal("endpoint destination compatibility mapping changed")
	}
}

func TestOpenSearchDestinationValidation(t *testing.T) {
	valid := map[string]any{
		"DomainARN": "arn:aws:es:us-east-1:123456789012:domain/logs", "IndexName": "events", "RoleARN": testRoleARN,
		"S3Configuration": testS3Destination(),
	}
	if err := validateElasticsearchDestination(valid, "us-east-1"); err != nil {
		t.Fatal(err)
	}
	endpoint := maps.Clone(valid)
	delete(endpoint, "DomainARN")
	endpoint["ClusterEndpoint"] = "https://logs.us-east-1.es.localhost.localstack.cloud"
	if err := validateElasticsearchDestination(endpoint, "us-east-1"); err != nil {
		t.Fatal(err)
	}
	invalid := []map[string]any{}
	add := func(key string, value any) {
		candidate := maps.Clone(valid)
		if value == nil {
			delete(candidate, key)
		} else {
			candidate[key] = value
		}
		invalid = append(invalid, candidate)
	}
	add("DomainARN", nil)
	add("ClusterEndpoint", "https://also.example.com")
	add("IndexName", "")
	add("RoleARN", "invalid")
	add("DomainARN", "arn:aws:es:us-east-1:123456789012:domain/INVALID")
	add("IndexRotationPeriod", "EveryMinute")
	add("BufferingHints", map[string]any{"SizeInMBs": float64(101)})
	add("RetryOptions", map[string]any{"DurationInSeconds": float64(7201)})
	add("S3BackupMode", "Everything")
	add("DocumentIdOptions", map[string]any{"DefaultDocumentIdFormat": "RANDOM"})
	add("S3Configuration", nil)
	for i, candidate := range invalid {
		if err := validateElasticsearchDestination(candidate, "us-east-1"); err == nil {
			t.Fatalf("accepted invalid OpenSearch destination %d: %#v", i, candidate)
		}
	}
}

func kplAggregate(records ...[]byte) []byte {
	message := appendProtoBytes(nil, 1, []byte("partition"))
	for _, data := range records {
		record := appendProtoBytes([]byte{0x08, 0x00}, 3, data)
		message = appendProtoBytes(message, 3, record)
	}
	digest := md5.Sum(message)
	return append(append(append([]byte{}, kplMagic...), message...), digest[:]...)
}

func appendProtoBytes(message []byte, field byte, value []byte) []byte {
	message = append(message, field<<3|2)
	message = binary.AppendUvarint(message, uint64(len(value)))
	return append(message, value...)
}

func TestKPLDeaggregation(t *testing.T) {
	aggregate := kplAggregate([]byte("one"), []byte("two"))
	if records := deaggregateKPL(aggregate); len(records) != 2 || string(records[0]) != "one" || string(records[1]) != "two" {
		t.Fatalf("deaggregated records %#v", records)
	}
	corrupt := bytes.Clone(aggregate)
	corrupt[len(corrupt)-1] ^= 1
	if records := deaggregateKPL(corrupt); len(records) != 1 || !bytes.Equal(records[0], corrupt) {
		t.Fatalf("corrupt aggregate changed %#v", records)
	}
	invalidIndex := bytes.Clone(aggregate)
	index := bytes.Index(invalidIndex, []byte{0x08, 0x00})
	if index < 0 {
		t.Fatal("aggregate lacks partition index")
	}
	invalidIndex[index+1] = 1
	digest := md5.Sum(invalidIndex[len(kplMagic) : len(invalidIndex)-md5.Size])
	copy(invalidIndex[len(invalidIndex)-md5.Size:], digest[:])
	if records := deaggregateKPL(invalidIndex); len(records) != 1 || !bytes.Equal(records[0], invalidIndex) {
		t.Fatalf("invalid partition index changed %#v", records)
	}
}

func FuzzKPLDeaggregation(f *testing.F) {
	f.Add([]byte("plain"))
	f.Add(kplAggregate([]byte("one"), []byte("two")))
	f.Add([]byte{0xf3, 0x89, 0x9a, 0xc2})
	f.Fuzz(func(t *testing.T, data []byte) {
		if records := deaggregateKPL(data); len(records) == 0 {
			t.Fatal("deaggregation returned no records")
		}
	})
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
	invoke("CreateDeliveryStream", map[string]any{
		"DeliveryStreamName": "zip", "ExtendedS3DestinationConfiguration": map[string]any{
			"BucketARN": "arn:aws:s3:::out", "RoleARN": testRoleARN, "Prefix": "zip/", "CompressionFormat": "ZIP",
		},
	})
	response = invoke("PutRecord", map[string]any{"DeliveryStreamName": "zip", "Record": map[string]any{"Data": base64.StdEncoding.EncodeToString([]byte("archived"))}})
	recordID = response.Output["RecordId"].(string)
	key = id.Account + "/" + id.Region + "/out/zip/1970/01/01/00/zip-1-1970-01-01-00-00-00-" + recordID + ".zip"
	reader, _, err = deps.Blobs.Get(context.Background(), key)
	if err != nil {
		t.Fatal(err)
	}
	archiveBody, _ := io.ReadAll(reader)
	_ = reader.Close()
	archive, err := zip.NewReader(bytes.NewReader(archiveBody), int64(len(archiveBody)))
	if err != nil || len(archive.File) != 1 || archive.File[0].Name != "zip" {
		t.Fatalf("ZIP archive %#v: %v", archive, err)
	}
	entry, err := archive.File[0].Open()
	if err != nil {
		t.Fatal(err)
	}
	body, _ = io.ReadAll(entry)
	_ = entry.Close()
	if string(body) != "archived" {
		t.Fatalf("ZIP body %q", body)
	}
	for _, test := range []struct{ compression, extension string }{{"Snappy", ".snappy"}, {"HADOOP_SNAPPY", ".hsnappy"}} {
		stream := strings.ToLower(test.compression)
		invoke("CreateDeliveryStream", map[string]any{"DeliveryStreamName": stream, "ExtendedS3DestinationConfiguration": map[string]any{
			"BucketARN": "arn:aws:s3:::out", "RoleARN": testRoleARN, "Prefix": stream + "/", "CompressionFormat": test.compression,
		}})
		response = invoke("PutRecord", map[string]any{"DeliveryStreamName": stream, "Record": map[string]any{"Data": base64.StdEncoding.EncodeToString([]byte("snappy data"))}})
		recordID = response.Output["RecordId"].(string)
		key = id.Account + "/" + id.Region + "/out/" + stream + "/1970/01/01/00/" + stream + "-1-1970-01-01-00-00-00-" + recordID + test.extension
		reader, _, err = deps.Blobs.Get(context.Background(), key)
		if err != nil {
			t.Fatal(err)
		}
		body, _ = io.ReadAll(reader)
		_ = reader.Close()
		if test.compression == "HADOOP_SNAPPY" {
			decodedSize, compressedSize := binary.BigEndian.Uint32(body), binary.BigEndian.Uint32(body[4:])
			var prefix [binary.MaxVarintLen32]byte
			n := binary.PutUvarint(prefix[:], uint64(decodedSize))
			body = append(prefix[:n], body[8:8+compressedSize]...)
		}
		decoded, err := snappy.Decode(nil, body)
		if err != nil || string(decoded) != "snappy data" {
			t.Fatalf("%s body %q: %v", test.compression, decoded, err)
		}
	}
	large := bytes.Repeat([]byte("incompressible-ish-data-"), 15000)
	hadoop := hadoopSnappy(large)
	decoded := make([]byte, 0, len(large))
	blocks := 0
	for payload := hadoop[4:]; len(payload) > 0; {
		blocks++
		size := binary.BigEndian.Uint32(payload)
		payload = payload[4:]
		var prefix [binary.MaxVarintLen32]byte
		n := binary.PutUvarint(prefix[:], uint64(min(len(large)-len(decoded), 262144-262144/6-32)))
		block, err := snappy.Decode(nil, append(prefix[:n], payload[:size]...))
		if err != nil {
			t.Fatal(err)
		}
		decoded = append(decoded, block...)
		payload = payload[size:]
	}
	if blocks < 2 || !bytes.Equal(decoded, large) || binary.BigEndian.Uint32(hadoop) != uint32(len(large)) {
		t.Fatal("Hadoop Snappy multi-block framing did not round trip")
	}
	if _, err := p.Invoke(context.Background(), &spi.Request{Identity: id, Operation: "CreateDeliveryStream", Input: map[string]any{
		"DeliveryStreamName": "bad-snappy", "ExtendedS3DestinationConfiguration": map[string]any{
			"BucketARN": "arn:aws:s3:::out", "RoleARN": testRoleARN, "CompressionFormat": "SNAPPY",
		},
	}}); err == nil {
		t.Fatal("accepted invalid SNAPPY capitalization")
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

func TestFirehoseAppendDelimiterProcessing(t *testing.T) {
	deps := spitest.Deps(t)
	p := New(deps)
	id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
	call := func(operation string, input map[string]any) (*spi.Response, error) {
		return p.Invoke(context.Background(), &spi.Request{Identity: id, Operation: operation, Input: input})
	}
	processing := func(enabled any) map[string]any {
		return map[string]any{"Enabled": enabled, "Processors": []any{map[string]any{"Type": "AppendDelimiterToRecord"}}}
	}
	read := func(key string, compressed bool) string {
		t.Helper()
		reader, _, err := deps.Blobs.Get(context.Background(), id.Account+"/"+id.Region+"/out/"+key)
		if err != nil {
			t.Fatal(err)
		}
		defer reader.Close()
		var bodyReader io.Reader = reader
		if compressed {
			gzipReader, err := gzip.NewReader(reader)
			if err != nil {
				t.Fatal(err)
			}
			defer gzipReader.Close()
			bodyReader = gzipReader
		}
		body, _ := io.ReadAll(bodyReader)
		return string(body)
	}

	if _, err := call("CreateDeliveryStream", map[string]any{
		"DeliveryStreamName": "processed", "ExtendedS3DestinationConfiguration": map[string]any{
			"BucketARN": "arn:aws:s3:::out", "RoleARN": testRoleARN, "Prefix": "processed/", "CompressionFormat": "GZIP", "ProcessingConfiguration": processing(true),
		},
	}); err != nil {
		t.Fatal(err)
	}
	put, err := call("PutRecord", map[string]any{"DeliveryStreamName": "processed", "Record": map[string]any{"Data": base64.StdEncoding.EncodeToString([]byte("one"))}})
	if err != nil {
		t.Fatal(err)
	}
	id1 := put.Output["RecordId"].(string)
	if body := read("processed/1970/01/01/00/processed-1-1970-01-01-00-00-00-"+id1+".gz", true); body != "one\n" {
		t.Fatalf("processed body %q", body)
	}
	described, err := call("DescribeDeliveryStream", map[string]any{"DeliveryStreamName": "processed"})
	if err != nil {
		t.Fatal(err)
	}
	destination := described.Output["DeliveryStreamDescription"].(map[string]any)["Destinations"].([]any)[0].(map[string]any)["ExtendedS3DestinationDescription"].(map[string]any)
	if !reflect.DeepEqual(destination["ProcessingConfiguration"], processing(true)) {
		t.Fatalf("processing description %#v", destination["ProcessingConfiguration"])
	}

	if _, err := call("CreateDeliveryStream", map[string]any{
		"DeliveryStreamName": "disabled-processing", "ExtendedS3DestinationConfiguration": map[string]any{
			"BucketARN": "arn:aws:s3:::out", "RoleARN": testRoleARN, "Prefix": "disabled/", "ProcessingConfiguration": processing(false),
		},
	}); err != nil {
		t.Fatal(err)
	}
	put, err = call("PutRecord", map[string]any{"DeliveryStreamName": "disabled-processing", "Record": map[string]any{"Data": base64.StdEncoding.EncodeToString([]byte("two"))}})
	if err != nil {
		t.Fatal(err)
	}
	id2 := put.Output["RecordId"].(string)
	if body := read("disabled/1970/01/01/00/disabled-processing-1-1970-01-01-00-00-00-"+id2, false); body != "two" {
		t.Fatalf("disabled body %q", body)
	}
	if _, err := call("UpdateDestination", map[string]any{
		"DeliveryStreamName": "disabled-processing", "CurrentDeliveryStreamVersionId": "1", "DestinationId": destinationID,
		"ExtendedS3DestinationUpdate": map[string]any{"ProcessingConfiguration": processing(true)},
	}); err != nil {
		t.Fatal(err)
	}
	put, err = call("PutRecord", map[string]any{"DeliveryStreamName": "disabled-processing", "Record": map[string]any{"Data": base64.StdEncoding.EncodeToString([]byte("three"))}})
	if err != nil {
		t.Fatal(err)
	}
	id3 := put.Output["RecordId"].(string)
	if body := read("disabled/1970/01/01/00/disabled-processing-2-1970-01-01-00-00-00-"+id3, false); body != "three\n" {
		t.Fatalf("updated body %q", body)
	}

	invalid := []any{
		"invalid",
		map[string]any{"Enabled": "true"},
		map[string]any{"Enabled": true},
		map[string]any{"Enabled": true, "Processors": "invalid"},
		map[string]any{"Enabled": true, "Processors": []any{}},
		map[string]any{"Enabled": true, "Processors": []any{"invalid"}},
		map[string]any{"Enabled": true, "Processors": []any{map[string]any{}}},
		map[string]any{"Enabled": true, "Processors": []any{map[string]any{"Type": "Unknown"}}},
		map[string]any{"Enabled": true, "Processors": []any{map[string]any{"Type": "AppendDelimiterToRecord", "Parameters": "invalid"}}},
		map[string]any{"Enabled": true, "Processors": []any{map[string]any{"Type": "AppendDelimiterToRecord", "Parameters": []any{map[string]any{"ParameterName": "Unknown", "ParameterValue": "value"}}}}},
		map[string]any{"Enabled": true, "Processors": []any{map[string]any{"Type": "AppendDelimiterToRecord", "Parameters": []any{map[string]any{"ParameterName": "Delimiter", "ParameterValue": " "}}}}},
	}
	for i, configuration := range invalid {
		destination := testS3Destination()
		destination["ProcessingConfiguration"] = configuration
		if _, err := call("CreateDeliveryStream", map[string]any{"DeliveryStreamName": fmt.Sprintf("invalid-processing-%d", i), "ExtendedS3DestinationConfiguration": destination}); err == nil {
			t.Errorf("accepted processing configuration %#v", configuration)
		}
	}
	destination = testS3Destination()
	destination["ProcessingConfiguration"] = map[string]any{"Enabled": true, "Processors": []any{map[string]any{"Type": "MetadataExtraction"}}}
	if _, err := call("CreateDeliveryStream", map[string]any{"DeliveryStreamName": "unsupported-processing", "ExtendedS3DestinationConfiguration": destination}); err == nil {
		t.Fatal("accepted unsupported metadata processor")
	}
	processedDestination := testS3Destination()
	processedDestination["ProcessingConfiguration"] = processing(true)
	for name, destination := range map[string]map[string]any{"basic": {"S3DestinationConfiguration": processedDestination}, "backup": {"ExtendedS3DestinationConfiguration": map[string]any{"BucketARN": "arn:aws:s3:::out", "RoleARN": testRoleARN, "S3BackupMode": "Enabled", "S3BackupConfiguration": processedDestination}}} {
		destination["DeliveryStreamName"] = "processing-on-" + name
		if _, err := call("CreateDeliveryStream", destination); err == nil {
			t.Errorf("accepted processing configuration on %s S3 configuration", name)
		}
	}
}

func TestFirehoseDataFormatConversionGuard(t *testing.T) {
	deps := spitest.Deps(t)
	p := New(deps)
	id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
	call := func(operation string, input map[string]any) (*spi.Response, error) {
		return p.Invoke(context.Background(), &spi.Request{Identity: id, Operation: operation, Input: input})
	}
	disabled := map[string]any{"Enabled": false, "SchemaConfiguration": map[string]any{"DatabaseName": "db", "TableName": "table"}}
	destination := testS3Destination()
	destination["Prefix"] = "raw/"
	destination["DataFormatConversionConfiguration"] = disabled
	if _, err := call("CreateDeliveryStream", map[string]any{"DeliveryStreamName": "disabled-conversion", "ExtendedS3DestinationConfiguration": destination}); err != nil {
		t.Fatal(err)
	}
	described, err := call("DescribeDeliveryStream", map[string]any{"DeliveryStreamName": "disabled-conversion"})
	if err != nil {
		t.Fatal(err)
	}
	configuration := described.Output["DeliveryStreamDescription"].(map[string]any)["Destinations"].([]any)[0].(map[string]any)["ExtendedS3DestinationDescription"].(map[string]any)
	if !reflect.DeepEqual(configuration["DataFormatConversionConfiguration"], disabled) {
		t.Fatalf("disabled conversion description %#v", configuration["DataFormatConversionConfiguration"])
	}
	put, err := call("PutRecord", map[string]any{"DeliveryStreamName": "disabled-conversion", "Record": map[string]any{"Data": base64.StdEncoding.EncodeToString([]byte(`{"raw":true}`))}})
	if err != nil {
		t.Fatal(err)
	}
	key := id.Account + "/" + id.Region + "/out/raw/1970/01/01/00/disabled-conversion-1-1970-01-01-00-00-00-" + put.Output["RecordId"].(string)
	reader, _, err := deps.Blobs.Get(context.Background(), key)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(reader)
	_ = reader.Close()
	if string(body) != `{"raw":true}` {
		t.Fatalf("disabled conversion body %q", body)
	}
	_, err = call("UpdateDestination", map[string]any{
		"DeliveryStreamName": "disabled-conversion", "CurrentDeliveryStreamVersionId": "1", "DestinationId": destinationID,
		"ExtendedS3DestinationUpdate": map[string]any{"DataFormatConversionConfiguration": map[string]any{"Enabled": true}},
	})
	if fault, ok := err.(*spi.Fault); !ok || fault.Code != "MirrorNotImplemented" {
		t.Fatalf("enabled conversion update fault %#v", err)
	}
	described, err = call("DescribeDeliveryStream", map[string]any{"DeliveryStreamName": "disabled-conversion"})
	if err != nil || described.Output["DeliveryStreamDescription"].(map[string]any)["VersionId"] != "1" {
		t.Fatalf("failed conversion update changed stream: %#v, %v", described, err)
	}

	for i, conversion := range []any{map[string]any{}, map[string]any{"Enabled": true}} {
		candidate := testS3Destination()
		candidate["DataFormatConversionConfiguration"] = conversion
		_, err := call("CreateDeliveryStream", map[string]any{"DeliveryStreamName": fmt.Sprintf("enabled-conversion-%d", i), "ExtendedS3DestinationConfiguration": candidate})
		fault, ok := err.(*spi.Fault)
		if !ok || fault.Code != "MirrorNotImplemented" {
			t.Fatalf("enabled conversion fault %#v", err)
		}
	}
	for i, conversion := range []any{"invalid", map[string]any{"Enabled": "false"}} {
		candidate := testS3Destination()
		candidate["DataFormatConversionConfiguration"] = conversion
		if _, err := call("CreateDeliveryStream", map[string]any{"DeliveryStreamName": fmt.Sprintf("invalid-conversion-%d", i), "ExtendedS3DestinationConfiguration": candidate}); err == nil {
			t.Fatalf("accepted invalid conversion %#v", conversion)
		}
	}
	basic := testS3Destination()
	basic["DataFormatConversionConfiguration"] = disabled
	if _, err := call("CreateDeliveryStream", map[string]any{"DeliveryStreamName": "basic-conversion", "S3DestinationConfiguration": basic}); err == nil {
		t.Fatal("accepted conversion configuration on basic S3")
	}
}

func TestFirehoseDecompressionProcessing(t *testing.T) {
	deps := spitest.Deps(t)
	p := New(deps)
	id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
	call := func(operation string, input map[string]any) (*spi.Response, error) {
		return p.Invoke(context.Background(), &spi.Request{Identity: id, Operation: operation, Input: input})
	}
	cloudWatchCall := func(operation string, input map[string]any) (*spi.Response, error) {
		return p.Invoke(context.Background(), &spi.Request{Identity: id, SourceService: "aws.logs", Operation: operation, Input: input})
	}
	compress := func(data string) []byte {
		t.Helper()
		var compressed bytes.Buffer
		writer := gzip.NewWriter(&compressed)
		if _, err := writer.Write([]byte(data)); err != nil {
			t.Fatal(err)
		}
		if err := writer.Close(); err != nil {
			t.Fatal(err)
		}
		return compressed.Bytes()
	}
	read := func(key string) ([]byte, bool) {
		t.Helper()
		reader, _, err := deps.Blobs.Get(context.Background(), id.Account+"/"+id.Region+"/out/"+key)
		if err != nil {
			return nil, false
		}
		defer reader.Close()
		body, _ := io.ReadAll(reader)
		return body, true
	}
	processing := map[string]any{"Enabled": true, "Processors": []any{
		map[string]any{"Type": "Decompression", "Parameters": []any{map[string]any{"ParameterName": "CompressionFormat", "ParameterValue": "GZIP"}}},
		map[string]any{"Type": "AppendDelimiterToRecord"},
	}}
	if _, err := call("CreateDeliveryStream", map[string]any{
		"DeliveryStreamName": "decompressed", "ExtendedS3DestinationConfiguration": map[string]any{
			"BucketARN": "arn:aws:s3:::out", "RoleARN": testRoleARN, "Prefix": "success/", "ErrorOutputPrefix": "errors/!{firehose:error-output-type}/", "ProcessingConfiguration": processing,
		},
	}); err != nil {
		t.Fatal(err)
	}
	put := func(stream string, data []byte) string {
		t.Helper()
		response, err := cloudWatchCall("PutRecord", map[string]any{"DeliveryStreamName": stream, "Record": map[string]any{"Data": base64.StdEncoding.EncodeToString(data)}})
		if err != nil {
			t.Fatal(err)
		}
		return response.Output["RecordId"].(string)
	}
	putInput := map[string]any{"DeliveryStreamName": "decompressed", "Record": map[string]any{"Data": base64.StdEncoding.EncodeToString(compress("unauthorized"))}}
	for _, operation := range []string{"PutRecord", "PutRecordBatch"} {
		input := putInput
		if operation == "PutRecordBatch" {
			input = map[string]any{"DeliveryStreamName": "decompressed", "Records": []any{putInput["Record"]}}
		}
		_, err := call(operation, input)
		fault, ok := err.(*spi.Fault)
		if !ok || fault.Code != "InvalidSourceException" || !strings.Contains(fault.Message, "AccountId: 123456789012, FirehoseName: decompressed") {
			t.Fatalf("%s source fault %#v", operation, err)
		}
	}
	if records, _, err := p.col(&spi.Request{Identity: id}, "fhrec:decompressed").List(context.Background(), "", "", 1); err != nil || len(records) != 0 {
		t.Fatalf("unauthorized records stored: %#v, %v", records, err)
	}
	body := `{"owner":"123456789012","messageType":"DATA_MESSAGE"}`
	okID := put("decompressed", compress(body))
	okKey := "success/1970/01/01/00/decompressed-1-1970-01-01-00-00-00-" + okID
	if delivered, found := read(okKey); !found || string(delivered) != body+"\n" {
		t.Fatalf("decompressed body %q found %v", delivered, found)
	}
	described, err := call("DescribeDeliveryStream", map[string]any{"DeliveryStreamName": "decompressed"})
	if err != nil {
		t.Fatal(err)
	}
	destination := described.Output["DeliveryStreamDescription"].(map[string]any)["Destinations"].([]any)[0].(map[string]any)["ExtendedS3DestinationDescription"].(map[string]any)
	if !reflect.DeepEqual(destination["ProcessingConfiguration"], processing) {
		t.Fatalf("decompression description %#v", destination["ProcessingConfiguration"])
	}

	for name, invalid := range map[string][]byte{"header": []byte("not gzip"), "checksum": compress("corrupt")} {
		if name == "checksum" {
			invalid[len(invalid)-1]++
		}
		failureID := put("decompressed", invalid)
		failureKey := "errors/decompression-failed/1970/01/01/00/decompressed-1-1970-01-01-00-00-00-" + failureID
		failureBody, found := read(failureKey)
		if !found {
			t.Fatalf("missing %s decompression failure", name)
		}
		var failure map[string]any
		if json.Unmarshal(failureBody, &failure) != nil || failure["errorCode"] != "Decompression.Failed" || failure["rawData"] != base64.StdEncoding.EncodeToString(invalid) || failure["errorMessage"] == "" || failure["lambdaArn"] != nil {
			t.Fatalf("%s decompression failure %s", name, failureBody)
		}
	}

	defaultProcessing := map[string]any{"Enabled": true, "Processors": []any{map[string]any{"Type": "Decompression"}}}
	for name, configuration := range map[string]map[string]any{
		"default":  defaultProcessing,
		"disabled": {"Enabled": false, "Processors": []any{map[string]any{"Type": "Decompression"}}},
	} {
		destination := testS3Destination()
		destination["Prefix"] = name + "/"
		destination["ProcessingConfiguration"] = configuration
		if _, err := call("CreateDeliveryStream", map[string]any{"DeliveryStreamName": name + "-decompression", "ExtendedS3DestinationConfiguration": destination}); err != nil {
			t.Fatal(err)
		}
		input := compress(name)
		recordID := put(name+"-decompression", input)
		key := name + "/1970/01/01/00/" + name + "-decompression-1-1970-01-01-00-00-00-" + recordID
		delivered, found := read(key)
		if want := map[string][]byte{"default": []byte(name), "disabled": input}[name]; !found || !bytes.Equal(delivered, want) {
			t.Fatalf("%s decompression body %x found %v", name, delivered, found)
		}
	}

	for i, processor := range []map[string]any{
		{"Type": "Decompression", "Parameters": []any{map[string]any{"ParameterName": "CompressionFormat", "ParameterValue": "ZIP"}}},
		{"Type": "Decompression", "Parameters": []any{map[string]any{"ParameterName": "LambdaArn", "ParameterValue": "arn:aws:lambda:us-east-1:123456789012:function:ignored"}}},
	} {
		destination := testS3Destination()
		destination["ProcessingConfiguration"] = map[string]any{"Enabled": true, "Processors": []any{processor}}
		if _, err := call("CreateDeliveryStream", map[string]any{"DeliveryStreamName": fmt.Sprintf("invalid-decompression-%d", i), "ExtendedS3DestinationConfiguration": destination}); err == nil {
			t.Errorf("accepted invalid decompression processor %#v", processor)
		}
	}
}

func TestFirehoseCloudWatchMessageExtraction(t *testing.T) {
	deps := spitest.Deps(t)
	p := New(deps)
	id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
	call := func(operation string, input map[string]any) (*spi.Response, error) {
		return p.Invoke(context.Background(), &spi.Request{Identity: id, SourceService: "aws.logs", Operation: operation, Input: input})
	}
	compress := func(data string) []byte {
		t.Helper()
		var compressed bytes.Buffer
		writer := gzip.NewWriter(&compressed)
		_, _ = writer.Write([]byte(data))
		_ = writer.Close()
		return compressed.Bytes()
	}
	read := func(key string) ([]byte, bool) {
		t.Helper()
		reader, _, err := deps.Blobs.Get(context.Background(), id.Account+"/"+id.Region+"/out/"+key)
		if err != nil {
			return nil, false
		}
		defer reader.Close()
		body, _ := io.ReadAll(reader)
		return body, true
	}
	decompression := map[string]any{"Type": "Decompression"}
	extraction := map[string]any{"Type": "CloudWatchLogProcessing", "Parameters": []any{map[string]any{"ParameterName": "DataMessageExtraction", "ParameterValue": "true"}}}
	processing := map[string]any{"Enabled": true, "Processors": []any{decompression, extraction}}
	if _, err := call("CreateDeliveryStream", map[string]any{
		"DeliveryStreamName": "message-extraction", "ExtendedS3DestinationConfiguration": map[string]any{
			"BucketARN": "arn:aws:s3:::out", "RoleARN": testRoleARN, "Prefix": "messages/", "ErrorOutputPrefix": "errors/!{firehose:error-output-type}/", "ProcessingConfiguration": processing,
		},
	}); err != nil {
		t.Fatal(err)
	}
	put := func(data []byte) string {
		t.Helper()
		response, err := call("PutRecord", map[string]any{"DeliveryStreamName": "message-extraction", "Record": map[string]any{"Data": base64.StdEncoding.EncodeToString(data)}})
		if err != nil {
			t.Fatal(err)
		}
		return response.Output["RecordId"].(string)
	}
	dataMessage := `{"owner":"123456789012","messageType":"DATA_MESSAGE","logEvents":[{"message":"first"},{"message":"{\"second\":true}"}]}`
	recordID := put(compress(dataMessage))
	key := "messages/1970/01/01/00/message-extraction-1-1970-01-01-00-00-00-" + recordID
	if body, found := read(key); !found || string(body) != "first\n{\"second\":true}\n" {
		t.Fatalf("extracted messages %q found %v", body, found)
	}

	controlID := put(compress(`{"messageType":"CONTROL_MESSAGE"}`))
	controlKey := "messages/1970/01/01/00/message-extraction-1-1970-01-01-00-00-00-" + controlID
	if _, found := read(controlKey); found {
		t.Fatal("delivered CloudWatch control message")
	}

	for name, invalid := range map[string][]byte{
		"json":   compress(`{"messageType":`),
		"events": compress(`{"messageType":"DATA_MESSAGE","logEvents":[]}`),
	} {
		failureID := put(invalid)
		failureKey := "errors/processing-failed/1970/01/01/00/message-extraction-1-1970-01-01-00-00-00-" + failureID
		failureBody, found := read(failureKey)
		var failure map[string]any
		if !found || json.Unmarshal(failureBody, &failure) != nil || failure["errorCode"] != "CloudWatchLogProcessing.Failed" || failure["rawData"] != base64.StdEncoding.EncodeToString(invalid) {
			t.Fatalf("%s extraction failure %s found %v", name, failureBody, found)
		}
	}

	for _, configuration := range []map[string]any{
		{"Enabled": true, "Processors": []any{extraction}},
		{"Enabled": true, "Processors": []any{extraction, decompression}},
		{"Enabled": true, "Processors": []any{decompression, map[string]any{"Type": "CloudWatchLogProcessing"}}},
		{"Enabled": true, "Processors": []any{decompression, map[string]any{"Type": "CloudWatchLogProcessing", "Parameters": []any{map[string]any{"ParameterName": "DataMessageExtraction", "ParameterValue": "false"}}}}},
		{"Enabled": true, "Processors": []any{decompression, map[string]any{"Type": "CloudWatchLogProcessing", "Parameters": []any{map[string]any{"ParameterName": "CompressionFormat", "ParameterValue": "GZIP"}}}}},
	} {
		if err := validateProcessingConfiguration(configuration); err == nil {
			t.Errorf("accepted invalid CloudWatch Logs processing %#v", configuration)
		}
	}
}

func TestFirehoseRecordDeAggregation(t *testing.T) {
	deps := spitest.Deps(t)
	p := New(deps)
	id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
	call := func(operation string, input map[string]any) (*spi.Response, error) {
		return p.Invoke(context.Background(), &spi.Request{Identity: id, Operation: operation, Input: input})
	}
	read := func(key string) ([]byte, bool) {
		t.Helper()
		reader, _, err := deps.Blobs.Get(context.Background(), id.Account+"/"+id.Region+"/out/"+key)
		if err != nil {
			return nil, false
		}
		defer reader.Close()
		body, _ := io.ReadAll(reader)
		return body, true
	}
	put := func(stream string, data []byte) string {
		t.Helper()
		response, err := call("PutRecord", map[string]any{"DeliveryStreamName": stream, "Record": map[string]any{"Data": base64.StdEncoding.EncodeToString(data)}})
		if err != nil {
			t.Fatal(err)
		}
		return response.Output["RecordId"].(string)
	}
	processor := func(subRecordType, delimiter string) map[string]any {
		parameters := []any{map[string]any{"ParameterName": "SubRecordType", "ParameterValue": subRecordType}}
		if delimiter != "" {
			parameters = append(parameters, map[string]any{"ParameterName": "Delimiter", "ParameterValue": delimiter})
		}
		return map[string]any{"Type": "RecordDeAggregation", "Parameters": parameters}
	}
	create := func(name string, deaggregation map[string]any) map[string]any {
		t.Helper()
		processing := map[string]any{"Enabled": true, "Processors": []any{deaggregation, map[string]any{"Type": "AppendDelimiterToRecord"}}}
		if _, err := call("CreateDeliveryStream", map[string]any{
			"DeliveryStreamName": name, "ExtendedS3DestinationConfiguration": map[string]any{
				"BucketARN": "arn:aws:s3:::out", "RoleARN": testRoleARN, "Prefix": name + "/", "ErrorOutputPrefix": "errors/!{firehose:error-output-type}/", "ProcessingConfiguration": processing,
			},
		}); err != nil {
			t.Fatal(err)
		}
		return processing
	}
	processing := create("json-deaggregation", processor("JSON", ""))
	for name, input := range map[string]string{
		"consecutive": `{"a":1}{"a":2}`,
		"jsonl":       "{\"a\":1}\n{\"a\":2}\n",
	} {
		recordID := put("json-deaggregation", []byte(input))
		key := "json-deaggregation/1970/01/01/00/json-deaggregation-1-1970-01-01-00-00-00-" + recordID
		if body, found := read(key); !found || string(body) != "{\"a\":1}\n{\"a\":2}\n" {
			t.Fatalf("%s JSON deaggregation %q found %v", name, body, found)
		}
	}
	described, err := call("DescribeDeliveryStream", map[string]any{"DeliveryStreamName": "json-deaggregation"})
	if err != nil {
		t.Fatal(err)
	}
	destination := described.Output["DeliveryStreamDescription"].(map[string]any)["Destinations"].([]any)[0].(map[string]any)["ExtendedS3DestinationDescription"].(map[string]any)
	if !reflect.DeepEqual(destination["ProcessingConfiguration"], processing) {
		t.Fatalf("deaggregation description %#v", destination["ProcessingConfiguration"])
	}
	for name, invalid := range map[string][]byte{"array": []byte(`[{"a":1},{"a":2}]`), "truncated": []byte(`{"a":`)} {
		failureID := put("json-deaggregation", invalid)
		failureKey := "errors/processing-failed/1970/01/01/00/json-deaggregation-1-1970-01-01-00-00-00-" + failureID
		failureBody, found := read(failureKey)
		var failure map[string]any
		if !found || json.Unmarshal(failureBody, &failure) != nil || failure["errorCode"] != "RecordDeAggregation.Failed" || failure["rawData"] != base64.StdEncoding.EncodeToString(invalid) {
			t.Fatalf("%s JSON deaggregation failure %s found %v", name, failureBody, found)
		}
	}

	delimiter := base64.StdEncoding.EncodeToString([]byte("####"))
	create("delimited-deaggregation", processor("DELIMITED", delimiter))
	delimitedID := put("delimited-deaggregation", []byte("one####two####"))
	delimitedKey := "delimited-deaggregation/1970/01/01/00/delimited-deaggregation-1-1970-01-01-00-00-00-" + delimitedID
	if body, found := read(delimitedKey); !found || string(body) != "one\ntwo\n" {
		t.Fatalf("delimited deaggregation %q found %v", body, found)
	}
	delimitedAggregate := []byte(strings.Repeat("x####", 501))
	delimitedOverflowID := put("delimited-deaggregation", delimitedAggregate)
	delimitedOverflowKey := "delimited-deaggregation/1970/01/01/00/delimited-deaggregation-1-1970-01-01-00-00-00-" + delimitedOverflowID
	if body, found := read(delimitedOverflowKey); !found || !bytes.Equal(body, append(delimitedAggregate, '\n')) {
		t.Fatalf("delimited overflow length %d found %v", len(body), found)
	}

	aggregated := []byte(strings.Repeat(`{"a":1}`, 501))
	overflowID := put("json-deaggregation", aggregated)
	overflowKey := "json-deaggregation/1970/01/01/00/json-deaggregation-1-1970-01-01-00-00-00-" + overflowID
	if body, found := read(overflowKey); !found || !bytes.Equal(body, append(aggregated, '\n')) {
		t.Fatalf("overflow deaggregation length %d found %v", len(body), found)
	}

	for _, invalidProcessor := range []map[string]any{
		{"Type": "RecordDeAggregation"},
		processor("XML", ""),
		processor("JSON", delimiter),
		processor("DELIMITED", ""),
		processor("DELIMITED", "not base64"),
		{"Type": "RecordDeAggregation", "Parameters": []any{map[string]any{"ParameterName": "LambdaArn", "ParameterValue": "ignored"}}},
	} {
		configuration := map[string]any{"Enabled": true, "Processors": []any{invalidProcessor}}
		if err := validateProcessingConfiguration(configuration); err == nil {
			t.Errorf("accepted invalid record deaggregation %#v", invalidProcessor)
		}
	}

	if _, err := exec.LookPath("python3"); err == nil {
		t.Run("isolates downstream Lambda failures", func(t *testing.T) {
			function := lambda.New(deps)
			code := `import base64
def lambda_handler(event, context):
    output = []
    for record in event['records']:
        data = base64.b64decode(record['data'])
        result = 'ProcessingFailed' if b'"a":2' in data else 'Ok'
        output.append({'recordId': record['recordId'], 'result': result, 'data': base64.b64encode(data.upper()).decode()})
    return {'records': output}
`
			if _, err := function.Invoke(context.Background(), &spi.Request{Identity: id, Operation: "CreateFunction", Input: map[string]any{
				"FunctionName": "deaggregate", "Runtime": "python3.12", "Handler": "lambda_function.lambda_handler", "Code": map[string]any{"ZipFile": base64.StdEncoding.EncodeToString([]byte(code))},
			}}); err != nil {
				t.Fatal(err)
			}
			processing := map[string]any{"Enabled": true, "Processors": []any{
				processor("JSON", ""),
				map[string]any{"Type": "Lambda", "Parameters": []any{map[string]any{"ParameterName": "LambdaArn", "ParameterValue": "arn:aws:lambda:us-east-1:123456789012:function:deaggregate"}, map[string]any{"ParameterName": "NumberOfRetries", "ParameterValue": "0"}}},
				map[string]any{"Type": "AppendDelimiterToRecord"},
			}}
			if _, err := call("CreateDeliveryStream", map[string]any{"DeliveryStreamName": "deaggregate-lambda", "ExtendedS3DestinationConfiguration": map[string]any{
				"BucketARN": "arn:aws:s3:::out", "RoleARN": testRoleARN, "Prefix": "lambda/", "ErrorOutputPrefix": "errors/!{firehose:error-output-type}/", "ProcessingConfiguration": processing,
			}}); err != nil {
				t.Fatal(err)
			}
			recordID := put("deaggregate-lambda", []byte(`{"a":1}{"a":2}`))
			successKey := "lambda/1970/01/01/00/deaggregate-lambda-1-1970-01-01-00-00-00-" + recordID
			if body, found := read(successKey); !found || string(body) != "{\"A\":1}\n" {
				t.Fatalf("deaggregated Lambda success %q found %v", body, found)
			}
			failureKey := "errors/processing-failed/1970/01/01/00/deaggregate-lambda-1-1970-01-01-00-00-00-" + recordID + ".1"
			if body, found := read(failureKey); !found || !strings.Contains(string(body), base64.StdEncoding.EncodeToString([]byte(`{"a":2}`))) {
				t.Fatalf("deaggregated Lambda failure %q found %v", body, found)
			}
		})
	}
}

func TestFirehoseLambdaProcessing(t *testing.T) {
	if _, err := exec.LookPath("python3"); err != nil {
		t.Skip("python3 not installed")
	}
	deps := spitest.Deps(t)
	id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
	p, function, logService := New(deps), lambda.New(deps), logs.New(deps)
	for operation, input := range map[string]map[string]any{
		"CreateLogGroup":  {"logGroupName": "firehose"},
		"CreateLogStream": {"logGroupName": "firehose", "logStreamName": "errors"},
	} {
		if _, err := logService.Invoke(context.Background(), &spi.Request{Identity: id, Operation: operation, Input: input}); err != nil {
			t.Fatal(err)
		}
	}
	code := `import base64
def lambda_handler(event, context):
    assert event['deliveryStreamArn'].endswith('/transformed') and event['region'] == 'us-east-1' and event['invocationId']
    output = []
    for record in event['records']:
        data = base64.b64decode(record['data'])
        result = 'Dropped' if data == b'drop' else ('ProcessingFailed' if data == b'fail' else 'Ok')
        output.append({'recordId': record['recordId'], 'result': result, 'data': base64.b64encode(data.upper()).decode()})
    return {'records': output}
`
	if _, err := function.Invoke(context.Background(), &spi.Request{Identity: id, Operation: "CreateFunction", Input: map[string]any{
		"FunctionName": "transform", "Runtime": "python3.12", "Handler": "lambda_function.lambda_handler", "Code": map[string]any{"ZipFile": base64.StdEncoding.EncodeToString([]byte(code))},
	}}); err != nil {
		t.Fatal(err)
	}
	call := func(operation string, input map[string]any) (*spi.Response, error) {
		return p.Invoke(context.Background(), &spi.Request{Identity: id, Operation: operation, Input: input})
	}
	lambdaARN := "arn:aws:lambda:us-east-1:123456789012:function:transform"
	processing := map[string]any{"Enabled": true, "Processors": []any{
		map[string]any{"Type": "Lambda", "Parameters": []any{
			map[string]any{"ParameterName": "LambdaArn", "ParameterValue": lambdaARN},
			map[string]any{"ParameterName": "NumberOfRetries", "ParameterValue": "0"},
			map[string]any{"ParameterName": "RoleArn", "ParameterValue": testRoleARN},
			map[string]any{"ParameterName": "BufferSizeInMBs", "ParameterValue": "0.2"},
			map[string]any{"ParameterName": "BufferIntervalInSeconds", "ParameterValue": "900"},
		}},
		map[string]any{"Type": "AppendDelimiterToRecord"},
	}}
	if _, err := call("CreateDeliveryStream", map[string]any{
		"DeliveryStreamName": "transformed", "ExtendedS3DestinationConfiguration": map[string]any{
			"BucketARN": "arn:aws:s3:::out", "RoleARN": testRoleARN, "Prefix": "success/", "ErrorOutputPrefix": "errors/!{firehose:error-output-type}/", "ProcessingConfiguration": processing,
			"CloudWatchLoggingOptions": map[string]any{"Enabled": true, "LogGroupName": "firehose", "LogStreamName": "errors"},
		},
	}); err != nil {
		t.Fatal(err)
	}
	put := func(data string) string {
		t.Helper()
		response, err := call("PutRecord", map[string]any{"DeliveryStreamName": "transformed", "Record": map[string]any{"Data": base64.StdEncoding.EncodeToString([]byte(data))}})
		if err != nil {
			t.Fatal(err)
		}
		return response.Output["RecordId"].(string)
	}
	read := func(key string) ([]byte, bool) {
		t.Helper()
		reader, _, err := deps.Blobs.Get(context.Background(), id.Account+"/"+id.Region+"/out/"+key)
		if err != nil {
			return nil, false
		}
		defer reader.Close()
		body, _ := io.ReadAll(reader)
		return body, true
	}

	okID := put("hello")
	okKey := "success/1970/01/01/00/transformed-1-1970-01-01-00-00-00-" + okID
	if body, found := read(okKey); !found || string(body) != "HELLO\n" {
		t.Fatalf("transformed body %q found %v", body, found)
	}
	droppedID := put("drop")
	if _, found := read("success/1970/01/01/00/transformed-1-1970-01-01-00-00-00-" + droppedID); found {
		t.Fatal("delivered dropped Lambda record")
	}
	failedID := put("fail")
	failureKey := "errors/processing-failed/1970/01/01/00/transformed-1-1970-01-01-00-00-00-" + failedID
	failureBody, found := read(failureKey)
	if !found {
		t.Fatal("missing Lambda processing failure")
	}
	var failure map[string]any
	if json.Unmarshal(failureBody, &failure) != nil || failure["attemptsMade"] != "1" || failure["rawData"] != base64.StdEncoding.EncodeToString([]byte("fail")) || failure["lambdaArn"] != lambdaARN {
		t.Fatalf("Lambda processing failure %s", failureBody)
	}
	logged, err := logService.Invoke(context.Background(), &spi.Request{Identity: id, Operation: "GetLogEvents", Input: map[string]any{"logGroupName": "firehose", "logStreamName": "errors"}})
	if err != nil {
		t.Fatal(err)
	}
	events := logged.Output["events"].([]any)
	if len(events) != 1 || !strings.Contains(events[0].(map[string]any)["message"].(string), "Lambda processing failed") || events[0].(map[string]any)["timestamp"] != float64(0) {
		t.Fatalf("CloudWatch events %#v", events)
	}
	described, err := call("DescribeDeliveryStream", map[string]any{"DeliveryStreamName": "transformed"})
	if err != nil {
		t.Fatal(err)
	}
	destination := described.Output["DeliveryStreamDescription"].(map[string]any)["Destinations"].([]any)[0].(map[string]any)["ExtendedS3DestinationDescription"].(map[string]any)
	if !reflect.DeepEqual(destination["ProcessingConfiguration"], processing) {
		t.Fatalf("Lambda processing description %#v", destination["ProcessingConfiguration"])
	}
	invalidCode := "def lambda_handler(event, context):\n    return {'records': []}\n"
	if _, err := function.Invoke(context.Background(), &spi.Request{Identity: id, Operation: "UpdateFunctionCode", Input: map[string]any{"FunctionName": "transform", "ZipFile": base64.StdEncoding.EncodeToString([]byte(invalidCode))}}); err != nil {
		t.Fatal(err)
	}
	invalidID := put("invalid-response")
	invalidKey := "errors/processing-failed/1970/01/01/00/transformed-1-1970-01-01-00-00-00-" + invalidID
	if body, found := read(invalidKey); !found || !strings.Contains(string(body), "invalid record count") {
		t.Fatalf("invalid Lambda response body %q found %v", body, found)
	}

	retryDestination := testS3Destination()
	retryDestination["ErrorOutputPrefix"] = "retry/!{firehose:error-output-type}/"
	retryDestination["ProcessingConfiguration"] = map[string]any{"Enabled": true, "Processors": []any{map[string]any{"Type": "Lambda", "Parameters": []any{
		map[string]any{"ParameterName": "LambdaArn", "ParameterValue": "arn:aws:lambda:us-east-1:123456789012:function:missing"},
		map[string]any{"ParameterName": "NumberOfRetries", "ParameterValue": "2"},
	}}}}
	if _, err := call("CreateDeliveryStream", map[string]any{"DeliveryStreamName": "retry-failure", "ExtendedS3DestinationConfiguration": retryDestination}); err != nil {
		t.Fatal(err)
	}
	retryResponse, err := call("PutRecord", map[string]any{"DeliveryStreamName": "retry-failure", "Record": map[string]any{"Data": base64.StdEncoding.EncodeToString([]byte("retry"))}})
	if err != nil {
		t.Fatal(err)
	}
	retryKey := "retry/processing-failed/1970/01/01/00/retry-failure-1-1970-01-01-00-00-00-" + retryResponse.Output["RecordId"].(string)
	if body, found := read(retryKey); !found || !strings.Contains(string(body), `"attemptsMade":"3"`) {
		t.Fatalf("Lambda retry failure body %q found %v", body, found)
	}
	if err := validateProcessingConfiguration(map[string]any{"Enabled": true, "Processors": []any{map[string]any{"Type": "Lambda", "Parameters": []any{
		map[string]any{"ParameterName": "LambdaArn", "ParameterValue": lambdaARN},
		map[string]any{"ParameterName": "NumberOfRetries", "ParameterValue": "300"},
		map[string]any{"ParameterName": "BufferSizeInMBs", "ParameterValue": "3"},
		map[string]any{"ParameterName": "BufferIntervalInSeconds", "ParameterValue": "0"},
	}}}}); err != nil {
		t.Fatalf("rejected Lambda processor boundaries: %v", err)
	}

	for i, parameters := range [][]any{
		{},
		{map[string]any{"ParameterName": "LambdaArn", "ParameterValue": "function"}},
		{map[string]any{"ParameterName": "LambdaArn", "ParameterValue": lambdaARN}, map[string]any{"ParameterName": "NumberOfRetries", "ParameterValue": "301"}},
		{map[string]any{"ParameterName": "LambdaArn", "ParameterValue": lambdaARN}, map[string]any{"ParameterName": "NumberOfRetries", "ParameterValue": "one"}},
		{map[string]any{"ParameterName": "LambdaArn", "ParameterValue": lambdaARN}, map[string]any{"ParameterName": "RoleArn", "ParameterValue": "role"}},
		{map[string]any{"ParameterName": "LambdaArn", "ParameterValue": lambdaARN}, map[string]any{"ParameterName": "BufferSizeInMBs", "ParameterValue": "0.19"}},
		{map[string]any{"ParameterName": "LambdaArn", "ParameterValue": lambdaARN}, map[string]any{"ParameterName": "BufferSizeInMBs", "ParameterValue": "3.01"}},
		{map[string]any{"ParameterName": "LambdaArn", "ParameterValue": lambdaARN}, map[string]any{"ParameterName": "BufferSizeInMBs", "ParameterValue": "NaN"}},
		{map[string]any{"ParameterName": "LambdaArn", "ParameterValue": lambdaARN}, map[string]any{"ParameterName": "BufferIntervalInSeconds", "ParameterValue": "-1"}},
		{map[string]any{"ParameterName": "LambdaArn", "ParameterValue": lambdaARN}, map[string]any{"ParameterName": "BufferIntervalInSeconds", "ParameterValue": "901"}},
		{map[string]any{"ParameterName": "LambdaArn", "ParameterValue": lambdaARN}, map[string]any{"ParameterName": "BufferIntervalInSeconds", "ParameterValue": "0.5"}},
		{map[string]any{"ParameterName": "LambdaArn", "ParameterValue": lambdaARN}, map[string]any{"ParameterName": "JsonParsingEngine", "ParameterValue": "JQ-1.6"}},
	} {
		destination := testS3Destination()
		destination["ProcessingConfiguration"] = map[string]any{"Enabled": true, "Processors": []any{map[string]any{"Type": "Lambda", "Parameters": parameters}}}
		if _, err := call("CreateDeliveryStream", map[string]any{"DeliveryStreamName": fmt.Sprintf("invalid-lambda-%d", i), "ExtendedS3DestinationConfiguration": destination}); err == nil {
			t.Errorf("accepted invalid Lambda parameters %#v", parameters)
		}
	}
}

func TestFirehoseLambdaDynamicPartitioning(t *testing.T) {
	if _, err := exec.LookPath("python3"); err != nil {
		t.Skip("python3 not installed")
	}
	deps := spitest.Deps(t)
	id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
	p, function := New(deps), lambda.New(deps)
	call := func(operation string, input map[string]any) (*spi.Response, error) {
		return p.Invoke(context.Background(), &spi.Request{Identity: id, Operation: operation, Input: input})
	}
	code := `import base64
import json
def lambda_handler(event, context):
    output = []
    for record in event['records']:
        value = json.loads(base64.b64decode(record['data']))
        keys = {} if 'customer' not in value else {'customer': value['customer']}
        output.append({'recordId': record['recordId'], 'result': 'Ok', 'data': record['data'], 'metadata': {'partitionKeys': keys}})
    return {'records': output}
`
	if _, err := function.Invoke(context.Background(), &spi.Request{Identity: id, Operation: "CreateFunction", Input: map[string]any{
		"FunctionName": "partition", "Runtime": "python3.12", "Handler": "lambda_function.lambda_handler", "Code": map[string]any{"ZipFile": base64.StdEncoding.EncodeToString([]byte(code))},
	}}); err != nil {
		t.Fatal(err)
	}
	lambdaARN := "arn:aws:lambda:us-east-1:123456789012:function:partition"
	processing := map[string]any{"Enabled": true, "Processors": []any{
		map[string]any{"Type": "RecordDeAggregation", "Parameters": []any{map[string]any{"ParameterName": "SubRecordType", "ParameterValue": "JSON"}}},
		map[string]any{"Type": "Lambda", "Parameters": []any{
			map[string]any{"ParameterName": "LambdaArn", "ParameterValue": lambdaARN},
			map[string]any{"ParameterName": "NumberOfRetries", "ParameterValue": "0"},
		}},
	}}
	destination := map[string]any{
		"BucketARN": "arn:aws:s3:::out", "RoleARN": testRoleARN,
		"Prefix": "customer=!{partitionKeyFromLambda:customer}/", "ErrorOutputPrefix": "errors/!{firehose:error-output-type}/",
		"DynamicPartitioningConfiguration": map[string]any{"Enabled": true}, "ProcessingConfiguration": processing,
	}
	if _, err := call("CreateDeliveryStream", map[string]any{"DeliveryStreamName": "partitioned", "ExtendedS3DestinationConfiguration": destination}); err != nil {
		t.Fatal(err)
	}
	put, err := call("PutRecord", map[string]any{"DeliveryStreamName": "partitioned", "Record": map[string]any{"Data": base64.StdEncoding.EncodeToString([]byte(`{"customer":"acme"}{"customer":"beta"}`))}})
	if err != nil {
		t.Fatal(err)
	}
	recordID := put.Output["RecordId"].(string)
	key := id.Account + "/" + id.Region + "/out/customer=acme/1970/01/01/00/partitioned-1-1970-01-01-00-00-00-" + recordID + ".0"
	reader, _, err := deps.Blobs.Get(context.Background(), key)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(reader)
	_ = reader.Close()
	if string(body) != `{"customer":"acme"}` {
		t.Fatalf("partitioned body %q", body)
	}
	if _, _, err := deps.Blobs.Get(context.Background(), id.Account+"/"+id.Region+"/out/customer=beta/1970/01/01/00/partitioned-1-1970-01-01-00-00-00-"+recordID+".1"); err != nil {
		t.Fatal(err)
	}
	described, err := call("DescribeDeliveryStream", map[string]any{"DeliveryStreamName": "partitioned"})
	configuration := described.Output["DeliveryStreamDescription"].(map[string]any)["Destinations"].([]any)[0].(map[string]any)["ExtendedS3DestinationDescription"].(map[string]any)
	dynamic := configuration["DynamicPartitioningConfiguration"].(map[string]any)
	if err != nil || dynamic["Enabled"] != true || dynamic["RetryOptions"].(map[string]any)["DurationInSeconds"] != 300 {
		t.Fatalf("default dynamic partition description %#v, %v", dynamic, err)
	}

	missing, err := call("PutRecord", map[string]any{"DeliveryStreamName": "partitioned", "Record": map[string]any{"Data": base64.StdEncoding.EncodeToString([]byte(`{"event":"missing"}`))}})
	if err != nil {
		t.Fatal(err)
	}
	failureKey := id.Account + "/" + id.Region + "/out/errors/processing-failed/1970/01/01/00/partitioned-1-1970-01-01-00-00-00-" + missing.Output["RecordId"].(string)
	reader, _, err = deps.Blobs.Get(context.Background(), failureKey)
	if err != nil {
		t.Fatal(err)
	}
	body, _ = io.ReadAll(reader)
	_ = reader.Close()
	if !strings.Contains(string(body), "missing partition key: customer") || !strings.Contains(string(body), base64.StdEncoding.EncodeToString([]byte(`{"event":"missing"}`))) {
		t.Fatalf("dynamic partition failure %s", body)
	}
	invalidMetadataCode := "import base64, json\ndef lambda_handler(event, context):\n    r = event['records'][0]\n    value = json.loads(base64.b64decode(r['data']))\n    metadata = {} if value['customer'] == 'shape' else {'partitionKeys': {'customer': 1}}\n    return {'records': [{'recordId': r['recordId'], 'result': 'Ok', 'data': r['data'], 'metadata': metadata}]}\n"
	if _, err := function.Invoke(context.Background(), &spi.Request{Identity: id, Operation: "UpdateFunctionCode", Input: map[string]any{"FunctionName": "partition", "ZipFile": base64.StdEncoding.EncodeToString([]byte(invalidMetadataCode))}}); err != nil {
		t.Fatal(err)
	}
	for _, customer := range []string{"invalid", "shape"} {
		invalid, err := call("PutRecord", map[string]any{"DeliveryStreamName": "partitioned", "Record": map[string]any{"Data": base64.StdEncoding.EncodeToString([]byte(`{"customer":"` + customer + `"}`))}})
		if err != nil {
			t.Fatal(err)
		}
		invalidKey := id.Account + "/" + id.Region + "/out/errors/processing-failed/1970/01/01/00/partitioned-1-1970-01-01-00-00-00-" + invalid.Output["RecordId"].(string)
		reader, _, err = deps.Blobs.Get(context.Background(), invalidKey)
		if err != nil {
			t.Fatal(err)
		}
		body, _ = io.ReadAll(reader)
		_ = reader.Close()
		if !strings.Contains(string(body), "invalid partition keys") {
			t.Fatalf("invalid Lambda metadata failure %s", body)
		}
	}

	if _, err := call("UpdateDestination", map[string]any{
		"DeliveryStreamName": "partitioned", "CurrentDeliveryStreamVersionId": "1", "DestinationId": destinationID,
		"ExtendedS3DestinationUpdate": map[string]any{"DynamicPartitioningConfiguration": map[string]any{"RetryOptions": map[string]any{"DurationInSeconds": 12}}},
	}); err != nil {
		t.Fatal(err)
	}
	described, err = call("DescribeDeliveryStream", map[string]any{"DeliveryStreamName": "partitioned"})
	configuration = described.Output["DeliveryStreamDescription"].(map[string]any)["Destinations"].([]any)[0].(map[string]any)["ExtendedS3DestinationDescription"].(map[string]any)
	dynamic = configuration["DynamicPartitioningConfiguration"].(map[string]any)
	if err != nil || dynamic["Enabled"] != true || dynamic["RetryOptions"].(map[string]any)["DurationInSeconds"] != float64(12) {
		t.Fatalf("dynamic partition description %#v, %v", dynamic, err)
	}
	if _, err := call("UpdateDestination", map[string]any{
		"DeliveryStreamName": "partitioned", "CurrentDeliveryStreamVersionId": "2", "DestinationId": destinationID,
		"ExtendedS3DestinationUpdate": map[string]any{"DynamicPartitioningConfiguration": map[string]any{"Enabled": false}},
	}); err == nil {
		t.Fatal("disabled dynamic partitioning")
	}

	if _, err := call("CreateDeliveryStream", map[string]any{"DeliveryStreamName": "plain", "ExtendedS3DestinationConfiguration": testS3Destination()}); err != nil {
		t.Fatal(err)
	}
	if _, err := call("UpdateDestination", map[string]any{
		"DeliveryStreamName": "plain", "CurrentDeliveryStreamVersionId": "1", "DestinationId": destinationID,
		"ExtendedS3DestinationUpdate": map[string]any{"DynamicPartitioningConfiguration": map[string]any{"Enabled": true}, "Prefix": destination["Prefix"], "ErrorOutputPrefix": destination["ErrorOutputPrefix"], "ProcessingConfiguration": processing},
	}); err == nil {
		t.Fatal("enabled dynamic partitioning after stream creation")
	}

	for i, invalid := range []map[string]any{
		{"DynamicPartitioningConfiguration": "invalid"},
		{"DynamicPartitioningConfiguration": map[string]any{"Enabled": "true"}},
		{"DynamicPartitioningConfiguration": map[string]any{"RetryOptions": "invalid"}},
		{"DynamicPartitioningConfiguration": map[string]any{"RetryOptions": map[string]any{"DurationInSeconds": -1}}},
		{"DynamicPartitioningConfiguration": map[string]any{"RetryOptions": map[string]any{"DurationInSeconds": 7201}}},
		{"DynamicPartitioningConfiguration": map[string]any{"Enabled": true}, "Prefix": "static/", "ErrorOutputPrefix": destination["ErrorOutputPrefix"]},
		{"DynamicPartitioningConfiguration": map[string]any{"Enabled": true}, "Prefix": destination["Prefix"], "ProcessingConfiguration": processing},
		{"DynamicPartitioningConfiguration": map[string]any{"Enabled": true}, "Prefix": destination["Prefix"], "ErrorOutputPrefix": destination["ErrorOutputPrefix"]},
	} {
		candidate := testS3Destination()
		maps.Copy(candidate, invalid)
		if _, err := call("CreateDeliveryStream", map[string]any{"DeliveryStreamName": fmt.Sprintf("invalid-dynamic-%d", i), "ExtendedS3DestinationConfiguration": candidate}); err == nil {
			t.Errorf("accepted invalid dynamic partitioning %#v", invalid)
		}
	}
	for _, duration := range []any{0, 7200} {
		candidate := testS3Destination()
		candidate["DynamicPartitioningConfiguration"] = map[string]any{"Enabled": false, "RetryOptions": map[string]any{"DurationInSeconds": duration}}
		if _, err := call("CreateDeliveryStream", map[string]any{"DeliveryStreamName": fmt.Sprintf("retry-%d", duration), "ExtendedS3DestinationConfiguration": candidate}); err != nil {
			t.Fatalf("rejected retry boundary %v: %v", duration, err)
		}
	}
	if err := validatePrefixes("key=!{partitionKeyFromQuery:id}/", "errors/!{firehose:error-output-type}/", true); err != nil {
		t.Fatalf("rejected inline metadata prefix: %v", err)
	}
	basic := testS3Destination()
	basic["DynamicPartitioningConfiguration"] = map[string]any{"Enabled": false}
	if _, err := call("CreateDeliveryStream", map[string]any{"DeliveryStreamName": "basic-dynamic", "S3DestinationConfiguration": basic}); err == nil {
		t.Fatal("accepted dynamic partitioning on a basic S3 destination")
	}
}

func TestFirehoseMetadataExtraction(t *testing.T) {
	deps := spitest.Deps(t)
	id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
	p := New(deps)
	call := func(operation string, input map[string]any) (*spi.Response, error) {
		return p.Invoke(context.Background(), &spi.Request{Identity: id, Operation: operation, Input: input})
	}
	metadata := func(query, engine string) map[string]any {
		return map[string]any{"Type": "MetadataExtraction", "Parameters": []any{
			map[string]any{"ParameterName": "MetadataExtractionQuery", "ParameterValue": query},
			map[string]any{"ParameterName": "JsonParsingEngine", "ParameterValue": engine},
		}}
	}
	query := `{"customer": .customer_id, "year": (.event_timestamp | strftime("%Y")), "active": .active}`
	processing := map[string]any{"Enabled": true, "Processors": []any{
		map[string]any{"Type": "RecordDeAggregation", "Parameters": []any{map[string]any{"ParameterName": "SubRecordType", "ParameterValue": "JSON"}}},
		metadata(query, "JQ-1.6"),
	}}
	destination := map[string]any{
		"BucketARN": "arn:aws:s3:::out", "RoleARN": testRoleARN,
		"Prefix":            "customer=!{partitionKeyFromQuery:customer}/year=!{partitionKeyFromQuery:year}/active=!{partitionKeyFromQuery:active}/",
		"ErrorOutputPrefix": "errors/!{firehose:error-output-type}/", "DynamicPartitioningConfiguration": map[string]any{"Enabled": true}, "ProcessingConfiguration": processing,
	}
	if _, err := call("CreateDeliveryStream", map[string]any{"DeliveryStreamName": "queried", "ExtendedS3DestinationConfiguration": destination}); err != nil {
		t.Fatal(err)
	}
	records := `{"customer_id":"acme","event_timestamp":0,"active":true}{"customer_id":"beta","event_timestamp":31536000,"active":false}`
	put, err := call("PutRecord", map[string]any{"DeliveryStreamName": "queried", "Record": map[string]any{"Data": base64.StdEncoding.EncodeToString([]byte(records))}})
	if err != nil {
		t.Fatal(err)
	}
	recordID := put.Output["RecordId"].(string)
	for _, expected := range []struct {
		key, body string
	}{
		{"customer=acme/year=1970/active=true/1970/01/01/00/queried-1-1970-01-01-00-00-00-" + recordID + ".0", `{"customer_id":"acme","event_timestamp":0,"active":true}`},
		{"customer=beta/year=1971/active=false/1970/01/01/00/queried-1-1970-01-01-00-00-00-" + recordID + ".1", `{"customer_id":"beta","event_timestamp":31536000,"active":false}`},
	} {
		reader, _, err := deps.Blobs.Get(context.Background(), id.Account+"/"+id.Region+"/out/"+expected.key)
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(reader)
		_ = reader.Close()
		if string(body) != expected.body {
			t.Fatalf("metadata-partitioned body %q", body)
		}
	}

	missing, err := call("PutRecord", map[string]any{"DeliveryStreamName": "queried", "Record": map[string]any{"Data": base64.StdEncoding.EncodeToString([]byte(`{"customer_id":null,"event_timestamp":0,"active":true}`))}})
	if err != nil {
		t.Fatal(err)
	}
	failureKey := id.Account + "/" + id.Region + "/out/errors/processing-failed/1970/01/01/00/queried-1-1970-01-01-00-00-00-" + missing.Output["RecordId"].(string)
	reader, _, err := deps.Blobs.Get(context.Background(), failureKey)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(reader)
	_ = reader.Close()
	if !strings.Contains(string(body), "metadata extraction values must be scalar") || !strings.Contains(string(body), `"errorCode":"MetadataExtraction.Failed"`) {
		t.Fatalf("metadata extraction failure %s", body)
	}

	if prefix, err := p.evaluatedDynamicS3Prefix("lambda=!{partitionKeyFromLambda:l}/query=!{partitionKeyFromQuery:q}/", deps.Clock.Now(), map[string]string{"l": "L"}, map[string]string{"q": "Q"}); err != nil || !strings.HasPrefix(prefix, "lambda=L/query=Q/") {
		t.Fatalf("combined partition prefix %q, %v", prefix, err)
	}
	if _, err := p.evaluatedDynamicS3Prefix("query=!{partitionKeyFromQuery:q}/", deps.Clock.Now(), nil, nil); err == nil || !strings.Contains(err.Error(), "missing partition key: q") {
		t.Fatalf("missing query partition key error %v", err)
	}
	for _, invalid := range []struct {
		query, data, message string
	}{
		{query, `not json`, "invalid character"},
		{`empty`, `{"customer_id":"acme"}`, "no result"},
		{`error("bad query")`, `{"customer_id":"acme"}`, "bad query"},
		{`.customer_id, .active`, `{"customer_id":"acme","active":true}`, "multiple results"},
		{`[.customer_id]`, `{"customer_id":"acme"}`, "must return an object"},
		{`{}`, `{"customer_id":"acme"}`, "must return an object"},
		{`{"": "value"}`, `{"customer_id":"acme"}`, "must be non-empty"},
		{`{"key": ""}`, `{"customer_id":"acme"}`, "must be non-empty"},
	} {
		if _, err := extractMetadata(metadata(invalid.query, "JQ-1.6"), []byte(invalid.data)); err == nil || !strings.Contains(err.Error(), invalid.message) {
			t.Errorf("metadata result from %q: %v", invalid.query, err)
		}
	}
	if keys, err := extractMetadata(metadata(`{"number": .number}`, "JQ-1.6"), []byte(`{"number":42}`)); err != nil || keys["number"] != "42" {
		t.Fatalf("numeric metadata keys %#v, %v", keys, err)
	}

	validMetadata := metadata(`{"customer": .customer_id}`, "JQ-1.6")
	extraParameter := metadata(`{"customer": .customer_id}`, "JQ-1.6")
	extraParameter["Parameters"] = append(extraParameter["Parameters"].([]any), map[string]any{"ParameterName": "RoleArn", "ParameterValue": testRoleARN})
	for i, invalid := range []map[string]any{
		{"ProcessingConfiguration": map[string]any{"Enabled": true, "Processors": []any{validMetadata}}},
		{"DynamicPartitioningConfiguration": map[string]any{"Enabled": true}, "Prefix": "key=!{partitionKeyFromQuery:customer}/", "ErrorOutputPrefix": destination["ErrorOutputPrefix"]},
		{"DynamicPartitioningConfiguration": map[string]any{"Enabled": true}, "Prefix": "key=!{partitionKeyFromQuery:customer}/", "ErrorOutputPrefix": destination["ErrorOutputPrefix"], "ProcessingConfiguration": map[string]any{"Enabled": true, "Processors": []any{metadata(`{`, "JQ-1.6")}}},
		{"DynamicPartitioningConfiguration": map[string]any{"Enabled": true}, "Prefix": "key=!{partitionKeyFromQuery:customer}/", "ErrorOutputPrefix": destination["ErrorOutputPrefix"], "ProcessingConfiguration": map[string]any{"Enabled": true, "Processors": []any{metadata(`unknown_function`, "JQ-1.6")}}},
		{"DynamicPartitioningConfiguration": map[string]any{"Enabled": true}, "Prefix": "key=!{partitionKeyFromQuery:customer}/", "ErrorOutputPrefix": destination["ErrorOutputPrefix"], "ProcessingConfiguration": map[string]any{"Enabled": true, "Processors": []any{metadata(`{"customer": .customer_id}`, "JQ-1.5")}}},
		{"DynamicPartitioningConfiguration": map[string]any{"Enabled": true}, "Prefix": "key=!{partitionKeyFromQuery:customer}/", "ErrorOutputPrefix": destination["ErrorOutputPrefix"], "ProcessingConfiguration": map[string]any{"Enabled": true, "Processors": []any{map[string]any{"Type": "MetadataExtraction", "Parameters": []any{map[string]any{"ParameterName": "JsonParsingEngine", "ParameterValue": "JQ-1.6"}}}}}},
		{"DynamicPartitioningConfiguration": map[string]any{"Enabled": true}, "Prefix": "key=!{partitionKeyFromQuery:customer}/", "ErrorOutputPrefix": destination["ErrorOutputPrefix"], "ProcessingConfiguration": map[string]any{"Enabled": true, "Processors": []any{extraParameter}}},
	} {
		candidate := testS3Destination()
		maps.Copy(candidate, invalid)
		if _, err := call("CreateDeliveryStream", map[string]any{"DeliveryStreamName": fmt.Sprintf("invalid-metadata-%d", i), "ExtendedS3DestinationConfiguration": candidate}); err == nil {
			t.Errorf("accepted invalid metadata configuration %#v", invalid)
		}
	}
}

func TestFirehoseCloudWatchLoggingValidation(t *testing.T) {
	deps := spitest.Deps(t)
	p := New(deps)
	id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
	call := func(name string, options any) error {
		destination := testS3Destination()
		destination["CloudWatchLoggingOptions"] = options
		_, err := p.Invoke(context.Background(), &spi.Request{Identity: id, Operation: "CreateDeliveryStream", Input: map[string]any{"DeliveryStreamName": name, "S3DestinationConfiguration": destination}})
		return err
	}
	if err := call("logging-disabled", map[string]any{"Enabled": false}); err != nil {
		t.Fatal(err)
	}
	p.logDeliveryError(context.Background(), &spi.Request{Identity: id}, map[string]any{"CloudWatchLoggingOptions": map[string]any{"Enabled": false, "LogGroupName": "disabled", "LogStreamName": "errors"}}, "logging-disabled", "ignored", time.Unix(0, 0))
	logged, err := logs.New(deps).Invoke(context.Background(), &spi.Request{Identity: id, Operation: "GetLogEvents", Input: map[string]any{"logGroupName": "disabled", "logStreamName": "errors"}})
	if err != nil || len(logged.Output["events"].([]any)) != 0 {
		t.Fatalf("disabled CloudWatch logging %#v %v", logged, err)
	}
	for i, options := range []any{
		"invalid",
		map[string]any{"Enabled": "true"},
		map[string]any{"Enabled": true},
		map[string]any{"Enabled": true, "LogGroupName": "group", "LogStreamName": 1},
		map[string]any{"Enabled": true, "LogGroupName": "bad?group", "LogStreamName": "stream"},
		map[string]any{"Enabled": true, "LogGroupName": "group", "LogStreamName": "bad:stream"},
		map[string]any{"Enabled": true, "LogGroupName": strings.Repeat("g", 513), "LogStreamName": "stream"},
	} {
		if err := call(fmt.Sprintf("invalid-logging-%d", i), options); err == nil {
			t.Errorf("accepted CloudWatch logging options %#v", options)
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
	deps := spitest.Deps(t)
	p := New(deps)
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
	storedRecord := func(name, recordID string) []byte {
		t.Helper()
		stored, ok, err := p.col(&spi.Request{Identity: id}, "fhrec:"+name).Get(context.Background(), recordID)
		if err != nil || !ok {
			t.Fatalf("stored record %q: %v", recordID, err)
		}
		return stored
	}

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
	if stored := storedRecord("plain", put.Output["RecordId"].(string)); !bytes.Contains(stored, []byte("encrypted")) || bytes.HasPrefix(stored, firehoseEncryptedPrefix) {
		t.Fatalf("plain stored record %q", stored)
	}
	if _, err := call("StartDeliveryStreamEncryption", map[string]any{"DeliveryStreamName": "plain"}); err != nil {
		t.Fatal(err)
	}
	if encryption := describeEncryption("plain"); encryption["Status"] != "ENABLED" || encryption["KeyType"] != "AWS_OWNED_CMK" || encryption["KeyARN"] != nil {
		t.Fatalf("AWS-owned encryption %#v", encryption)
	}
	put, err = call("PutRecord", map[string]any{"DeliveryStreamName": "plain", "Record": record})
	if err != nil || put.Output["Encrypted"] != true {
		t.Fatalf("encrypted put %#v, %v", put, err)
	}
	stored := storedRecord("plain", put.Output["RecordId"].(string))
	if !bytes.HasPrefix(stored, firehoseEncryptedPrefix) || bytes.Contains(stored, []byte("encrypted")) {
		t.Fatalf("unencrypted retained record %q", stored)
	}
	plaintext, err := p.decryptAtRest(context.Background(), &spi.Request{Identity: id}, stored)
	if err != nil || !bytes.Contains(plaintext, []byte("encrypted")) {
		t.Fatalf("retained record decrypt %q, %v", plaintext, err)
	}
	batch, err := call("PutRecordBatch", map[string]any{"DeliveryStreamName": "plain", "Records": []any{record}})
	if err != nil || batch.Output["Encrypted"] != true {
		t.Fatalf("encrypted batch %#v, %v", batch, err)
	}

	missingConfiguration := map[string]any{"KeyType": "CUSTOMER_MANAGED_CMK", "KeyARN": "arn:aws:kms:us-east-1:123456789012:key/missing"}
	if _, err := call("StartDeliveryStreamEncryption", map[string]any{"DeliveryStreamName": "plain", "DeliveryStreamEncryptionConfigurationInput": missingConfiguration}); err == nil {
		t.Fatal("started encryption with a missing KMS key")
	} else if fault, ok := err.(*spi.Fault); !ok || fault.Code != "InvalidKMSResourceException" {
		t.Fatalf("missing KMS key fault %#v", err)
	}
	keyService := kms.New(deps)
	createdKey, err := keyService.Invoke(context.Background(), &spi.Request{Identity: id, Operation: "CreateKey", Input: map[string]any{}})
	if err != nil {
		t.Fatal(err)
	}
	keyARN := first(createdKey.Output["KeyMetadata"].(map[string]any), "Arn")
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
	if _, err := keyService.Invoke(context.Background(), &spi.Request{Identity: id, Operation: "DisableKey", Input: map[string]any{"KeyId": keyARN}}); err != nil {
		t.Fatal(err)
	}
	if _, err := call("StartDeliveryStreamEncryption", map[string]any{"DeliveryStreamName": "plain", "DeliveryStreamEncryptionConfigurationInput": configuration}); err == nil {
		t.Fatal("started encryption with a disabled KMS key")
	} else if fault, ok := err.(*spi.Fault); !ok || fault.Code != "InvalidKMSResourceException" {
		t.Fatalf("disabled KMS key fault %#v", err)
	}
	if _, err := keyService.Invoke(context.Background(), &spi.Request{Identity: id, Operation: "EnableKey", Input: map[string]any{"KeyId": keyARN}}); err != nil {
		t.Fatal(err)
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
	if _, err := keyService.Invoke(context.Background(), &spi.Request{Identity: id, Operation: "DisableKey", Input: map[string]any{"KeyId": keyARN}}); err != nil {
		t.Fatal(err)
	}
	storedBefore, _, _ := p.col(&spi.Request{Identity: id}, "fhrec:created-encrypted").List(context.Background(), "", "", 0)
	if _, err := call("PutRecord", map[string]any{"DeliveryStreamName": "created-encrypted", "Record": record}); err == nil {
		t.Fatal("stored data with a disabled KMS key")
	} else if fault, ok := err.(*spi.Fault); !ok || fault.Code != "InvalidKMSResourceException" {
		t.Fatalf("disabled KMS put fault %#v", err)
	}
	storedAfter, _, _ := p.col(&spi.Request{Identity: id}, "fhrec:created-encrypted").List(context.Background(), "", "", 0)
	if len(storedAfter) != len(storedBefore) {
		t.Fatal("disabled KMS put retained plaintext")
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
		} else if fault, ok := err.(*spi.Fault); !ok || fault.Code != "InvalidArgumentException" {
			t.Fatalf("invalid create fault %#v for %#v", err, input)
		}
	}
	if _, err := call("StartDeliveryStreamEncryption", map[string]any{"DeliveryStreamName": "source"}); err == nil {
		t.Fatal("encrypted non-DirectPut stream")
	} else if fault, ok := err.(*spi.Fault); !ok || fault.Code != "InvalidArgumentException" {
		t.Fatalf("source encryption fault %#v", err)
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
		} else if fault, ok := err.(*spi.Fault); !ok || fault.Code != "InvalidArgumentException" {
			t.Fatalf("invalid encryption fault %#v for %#v", err, input)
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
		{"invalid Snowflake destination", map[string]any{"DeliveryStreamName": "snowflake", "SnowflakeDestinationConfiguration": map[string]any{}}, "InvalidArgumentException"},
		{"direct put with Kinesis source", map[string]any{"DeliveryStreamName": "direct-source", "KinesisStreamSourceConfiguration": testKinesisSource(), "S3DestinationConfiguration": testS3Destination()}, "InvalidArgumentException"},
		{"missing Kinesis source", map[string]any{"DeliveryStreamName": "no-source", "DeliveryStreamType": "KinesisStreamAsSource", "S3DestinationConfiguration": testS3Destination()}, "InvalidArgumentException"},
		{"malformed Kinesis ARN", map[string]any{"DeliveryStreamName": "bad-source", "DeliveryStreamType": "KinesisStreamAsSource", "KinesisStreamSourceConfiguration": map[string]any{"KinesisStreamARN": "stream", "RoleARN": testRoleARN}, "S3DestinationConfiguration": testS3Destination()}, "InvalidArgumentException"},
		{"long Kinesis ARN", map[string]any{"DeliveryStreamName": "long-source", "DeliveryStreamType": "KinesisStreamAsSource", "KinesisStreamSourceConfiguration": map[string]any{"KinesisStreamARN": "arn:aws:kinesis:us-east-1:123456789012:stream/" + strings.Repeat("a", 500), "RoleARN": testRoleARN}, "S3DestinationConfiguration": testS3Destination()}, "InvalidArgumentException"},
		{"malformed Kinesis role", map[string]any{"DeliveryStreamName": "bad-source-role", "DeliveryStreamType": "KinesisStreamAsSource", "KinesisStreamSourceConfiguration": map[string]any{"KinesisStreamARN": testKinesisSource()["KinesisStreamARN"], "RoleARN": "role"}, "S3DestinationConfiguration": testS3Destination()}, "InvalidArgumentException"},
		{"missing MSK source", map[string]any{"DeliveryStreamName": "msk", "DeliveryStreamType": "MSKAsSource", "S3DestinationConfiguration": testS3Destination()}, "InvalidArgumentException"},
		{"missing database source", map[string]any{"DeliveryStreamName": "database", "DeliveryStreamType": "DatabaseAsSource", "S3DestinationConfiguration": testS3Destination()}, "InvalidArgumentException"},
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

func TestFirehoseHTTPEndpointDestination(t *testing.T) {
	type capturedRequest struct {
		path    string
		header  http.Header
		payload map[string]any
	}
	captured := make(chan capturedRequest, 8)
	releaseBlocked := make(chan struct{}, 2)
	immediateDestination := func(endpoint string) map[string]any {
		destination := testHTTPEndpointDestination(endpoint)
		destination["BufferingHints"] = map[string]any{"IntervalInSeconds": 0, "SizeInMBs": 1}
		return destination
	}
	server := httptest.NewTLSServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		body, _ := io.ReadAll(request.Body)
		if request.Header.Get("Content-Encoding") == "gzip" {
			reader, err := gzip.NewReader(bytes.NewReader(body))
			if err != nil {
				t.Error(err)
				writer.WriteHeader(http.StatusBadRequest)
				return
			}
			body, _ = io.ReadAll(reader)
			_ = reader.Close()
		}
		payload := map[string]any{}
		if err := json.Unmarshal(body, &payload); err != nil {
			t.Error(err)
		}
		captured <- capturedRequest{path: request.URL.RequestURI(), header: request.Header.Clone(), payload: payload}
		if request.URL.Path == "/blocked" {
			<-releaseBlocked
		}
		if request.URL.Path == "/redirect" {
			writer.Header().Set("Location", "https://example.test/ok")
			writer.WriteHeader(http.StatusFound)
			return
		}
		if request.URL.Path == "/permanent" {
			writer.WriteHeader(http.StatusRequestEntityTooLarge)
			return
		}
		if request.URL.Path == "/exhaust" || (request.URL.Path == "/retry" && request.Header.Get("X-Amz-Firehose-Access-Key") != "retry-success") {
			writer.WriteHeader(http.StatusInternalServerError)
			return
		}
		requestID := first(payload, "requestId")
		if request.URL.Path == "/failure" {
			requestID = "wrong-request"
		}
		writer.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(writer).Encode(map[string]any{"requestId": requestID, "timestamp": 1})
	}))
	defer server.Close()

	target, _ := url.Parse(server.URL)
	transport := server.Client().Transport
	deps := spitest.Deps(t)
	p := New(deps)
	defer func() { _ = p.Close() }()
	defer close(releaseBlocked)
	checkRedirect := p.httpClient.CheckRedirect
	httpClient := &http.Client{
		Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
			clone := request.Clone(request.Context())
			requestURL := *request.URL
			requestURL.Scheme, requestURL.Host = target.Scheme, target.Host
			clone.URL = &requestURL
			return transport.RoundTrip(clone)
		}),
		CheckRedirect: checkRedirect,
	}
	p.httpClient = httpClient
	id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
	call := func(operation string, input map[string]any) (*spi.Response, error) {
		t.Helper()
		return p.Invoke(context.Background(), &spi.Request{Identity: id, Operation: operation, Input: input})
	}
	waitBlob := func(key string) {
		t.Helper()
		for deadline := time.Now().Add(pollBudget); time.Now().Before(deadline); {
			if reader, _, err := deps.Blobs.Get(context.Background(), key); err == nil {
				_ = reader.Close()
				return
			}
			time.Sleep(time.Millisecond)
		}
		t.Fatalf("blob not found: %s", key)
	}

	destination := immediateDestination("https://example.test/ok?tenant=1")
	destination["EndpointConfiguration"] = map[string]any{"Url": "https://example.test/ok?tenant=1", "Name": "collector", "AccessKey": "secret"}
	destination["S3Configuration"].(map[string]any)["Prefix"] = "all/"
	destination["S3BackupMode"] = "AllData"
	destination["RequestConfiguration"] = map[string]any{
		"ContentEncoding":  "GZIP",
		"CommonAttributes": []any{map[string]any{"AttributeName": "environment", "AttributeValue": "test"}},
	}
	if _, err := call("CreateDeliveryStream", map[string]any{"DeliveryStreamName": "http-endpoint", "HttpEndpointDestinationConfiguration": destination}); err != nil {
		t.Fatal(err)
	}
	put, err := call("PutRecord", map[string]any{"DeliveryStreamName": "http-endpoint", "Record": map[string]any{"Data": base64.StdEncoding.EncodeToString([]byte("hello"))}})
	if err != nil {
		t.Fatal(err)
	}
	request := <-captured
	records, _ := request.payload["records"].([]any)
	record, _ := records[0].(map[string]any)
	common := map[string]any{}
	_ = json.Unmarshal([]byte(request.header.Get("X-Amz-Firehose-Common-Attributes")), &common)
	if request.path != "/ok?tenant=1" || request.header.Get("Content-Type") != "application/json" || request.header.Get("Content-Encoding") != "gzip" || request.header.Get("X-Amz-Firehose-Protocol-Version") != "1.0" || request.header.Get("X-Amz-Firehose-Request-Id") != first(request.payload, "requestId") || request.header.Get("X-Amz-Firehose-Source-Arn") != "arn:aws:firehose:us-east-1:123456789012:deliverystream/http-endpoint" || request.header.Get("X-Amz-Firehose-Access-Key") != "secret" || request.payload["timestamp"] != float64(0) || first(record, "data") != base64.StdEncoding.EncodeToString([]byte("hello")) || common["commonAttributes"].(map[string]any)["environment"] != "test" {
		t.Fatalf("HTTP request %#v %#v", request, common)
	}
	recordID := put.Output["RecordId"].(string)
	backupKey := id.Account + "/" + id.Region + "/out/all/1970/01/01/00/http-endpoint-1-1970-01-01-00-00-00-" + recordID
	reader, _, err := deps.Blobs.Get(context.Background(), backupKey)
	if err != nil {
		t.Fatal(err)
	}
	backup, _ := io.ReadAll(reader)
	_ = reader.Close()
	if string(backup) != "hello" {
		t.Fatalf("backup %q", backup)
	}

	described, err := call("DescribeDeliveryStream", map[string]any{"DeliveryStreamName": "http-endpoint"})
	if err != nil {
		t.Fatal(err)
	}
	description := described.Output["DeliveryStreamDescription"].(map[string]any)["Destinations"].([]any)[0].(map[string]any)["HttpEndpointDestinationDescription"].(map[string]any)
	endpoint := description["EndpointConfiguration"].(map[string]any)
	hints := description["BufferingHints"].(map[string]any)
	retry := description["RetryOptions"].(map[string]any)
	if endpoint["Name"] != "collector" || endpoint["Url"] != "https://example.test/ok?tenant=1" || endpoint["AccessKey"] != nil || description["S3Configuration"] != nil || description["S3DestinationDescription"].(map[string]any)["Prefix"] != "all/" || hints["IntervalInSeconds"] != float64(0) || hints["SizeInMBs"] != float64(1) || retry["DurationInSeconds"] != 300 {
		t.Fatalf("HTTP description %#v", description)
	}

	if _, err := call("UpdateDestination", map[string]any{
		"DeliveryStreamName": "http-endpoint", "CurrentDeliveryStreamVersionId": "1", "DestinationId": destinationID,
		"HttpEndpointDestinationUpdate": map[string]any{
			"EndpointConfiguration": map[string]any{"Url": "https://example.test/ok?tenant=2", "Name": "updated", "AccessKey": "updated-secret"},
			"RequestConfiguration":  map[string]any{"ContentEncoding": "NONE"},
			"S3BackupMode":          "FailedDataOnly",
			"S3Update":              map[string]any{"Prefix": "updated/"},
		},
	}); err != nil {
		t.Fatal(err)
	}
	described, err = call("DescribeDeliveryStream", map[string]any{"DeliveryStreamName": "http-endpoint"})
	if err != nil {
		t.Fatal(err)
	}
	description = described.Output["DeliveryStreamDescription"].(map[string]any)["Destinations"].([]any)[0].(map[string]any)["HttpEndpointDestinationDescription"].(map[string]any)
	endpoint = description["EndpointConfiguration"].(map[string]any)
	if endpoint["Name"] != "updated" || endpoint["Url"] != "https://example.test/ok?tenant=2" || description["S3BackupMode"] != "FailedDataOnly" || description["S3DestinationDescription"].(map[string]any)["Prefix"] != "updated/" {
		t.Fatalf("updated HTTP description %#v", description)
	}
	put, err = call("PutRecord", map[string]any{"DeliveryStreamName": "http-endpoint", "Record": map[string]any{"Data": base64.StdEncoding.EncodeToString([]byte("updated"))}})
	if err != nil {
		t.Fatal(err)
	}
	request = <-captured
	if request.path != "/ok?tenant=2" || request.header.Get("Content-Encoding") != "" || request.header.Get("X-Amz-Firehose-Access-Key") != "updated-secret" {
		t.Fatalf("updated HTTP request %#v", request)
	}
	updatedBackupKey := id.Account + "/" + id.Region + "/out/updated/1970/01/01/00/http-endpoint-2-1970-01-01-00-00-00-" + put.Output["RecordId"].(string)
	if _, _, err := deps.Blobs.Get(context.Background(), updatedBackupKey); err == nil {
		t.Fatal("backed up successfully delivered FailedDataOnly record")
	}
	batch, err := call("PutRecordBatch", map[string]any{
		"DeliveryStreamName": "http-endpoint",
		"Records": []any{
			map[string]any{"Data": base64.StdEncoding.EncodeToString([]byte("batch-one"))},
			map[string]any{"Data": base64.StdEncoding.EncodeToString([]byte("batch-two"))},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	request = <-captured
	records, _ = request.payload["records"].([]any)
	responses := batch.Output["RequestResponses"].([]any)
	if len(records) != 2 || len(responses) != 2 {
		t.Fatalf("HTTP batch request %#v response %#v", request, batch.Output)
	}
	firstBatchRecord := records[0].(map[string]any)
	secondBatchRecord := records[1].(map[string]any)
	if first(request.payload, "requestId") != responses[0].(map[string]any)["RecordId"] || first(firstBatchRecord, "data") != base64.StdEncoding.EncodeToString([]byte("batch-one")) || first(secondBatchRecord, "data") != base64.StdEncoding.EncodeToString([]byte("batch-two")) || len(captured) != 0 {
		t.Fatalf("HTTP batch request %#v response %#v queued %d", request, batch.Output, len(captured))
	}

	processing := map[string]any{"Enabled": true, "Processors": []any{map[string]any{"Type": "AppendDelimiterToRecord"}}}
	processedDestination := immediateDestination("https://example.test/ok")
	processedDestination["ProcessingConfiguration"] = processing
	if _, err := call("CreateDeliveryStream", map[string]any{"DeliveryStreamName": "http-processed", "HttpEndpointDestinationConfiguration": processedDestination}); err != nil {
		t.Fatal(err)
	}
	if _, err := call("PutRecordBatch", map[string]any{
		"DeliveryStreamName": "http-processed",
		"Records": []any{
			map[string]any{"Data": base64.StdEncoding.EncodeToString([]byte("processed-one"))},
			map[string]any{"Data": base64.StdEncoding.EncodeToString([]byte("processed-two"))},
		},
	}); err != nil {
		t.Fatal(err)
	}
	request = <-captured
	records, _ = request.payload["records"].([]any)
	if len(records) != 2 {
		t.Fatalf("processed HTTP records %#v", records)
	}
	processedOne, _ := base64.StdEncoding.DecodeString(first(records[0].(map[string]any), "data"))
	processedTwo, _ := base64.StdEncoding.DecodeString(first(records[1].(map[string]any), "data"))
	if string(processedOne) != "processed-one\n" || string(processedTwo) != "processed-two\n" {
		t.Fatalf("processed HTTP data %q %q", processedOne, processedTwo)
	}
	described, err = call("DescribeDeliveryStream", map[string]any{"DeliveryStreamName": "http-processed"})
	if err != nil {
		t.Fatal(err)
	}
	description = described.Output["DeliveryStreamDescription"].(map[string]any)["Destinations"].([]any)[0].(map[string]any)["HttpEndpointDestinationDescription"].(map[string]any)
	if !reflect.DeepEqual(description["ProcessingConfiguration"], processing) {
		t.Fatalf("HTTP processing description %#v", description["ProcessingConfiguration"])
	}

	failureProcessing := map[string]any{"Enabled": true, "Processors": []any{map[string]any{"Type": "Decompression"}}}
	processingFailureDestination := immediateDestination("https://example.test/ok")
	processingFailureDestination["ProcessingConfiguration"] = failureProcessing
	processingFailureDestination["S3BackupMode"] = "AllData"
	processingFailureDestination["S3Configuration"].(map[string]any)["Prefix"] = "all-failed/"
	processingFailureDestination["S3Configuration"].(map[string]any)["ErrorOutputPrefix"] = "errors/!{firehose:error-output-type}/"
	if _, err := call("CreateDeliveryStream", map[string]any{"DeliveryStreamName": "http-processing-failure", "HttpEndpointDestinationConfiguration": processingFailureDestination}); err != nil {
		t.Fatal(err)
	}
	failedPut, err := p.Invoke(context.Background(), &spi.Request{Identity: id, SourceService: "aws.logs", Operation: "PutRecord", Input: map[string]any{"DeliveryStreamName": "http-processing-failure", "Record": map[string]any{"Data": base64.StdEncoding.EncodeToString([]byte("not-gzip"))}}})
	if err != nil {
		t.Fatal(err)
	}
	if len(captured) != 0 {
		t.Fatal("sent an HTTP request containing only processing failures")
	}
	failureKey := id.Account + "/" + id.Region + "/out/errors/decompression-failed/1970/01/01/00/http-processing-failure-1-1970-01-01-00-00-00-" + failedPut.Output["RecordId"].(string)
	reader, _, err = deps.Blobs.Get(context.Background(), failureKey)
	if err != nil {
		t.Fatal(err)
	}
	failureBody, _ := io.ReadAll(reader)
	_ = reader.Close()
	failureEnvelope := map[string]any{}
	_ = json.Unmarshal(failureBody, &failureEnvelope)
	if failureEnvelope["errorCode"] != "Decompression.Failed" || failureEnvelope["rawData"] != base64.StdEncoding.EncodeToString([]byte("not-gzip")) {
		t.Fatalf("HTTP processing failure %#v", failureEnvelope)
	}
	rawBackupKey := id.Account + "/" + id.Region + "/out/all-failed/1970/01/01/00/http-processing-failure-1-1970-01-01-00-00-00-" + failedPut.Output["RecordId"].(string)
	reader, _, err = deps.Blobs.Get(context.Background(), rawBackupKey)
	if err != nil {
		t.Fatal(err)
	}
	rawBackup, _ := io.ReadAll(reader)
	_ = reader.Close()
	if string(rawBackup) != "not-gzip" {
		t.Fatalf("HTTP AllData processing-failure backup %q", rawBackup)
	}

	secretPack := secretsmanager.New(deps)
	createdSecret, err := secretPack.Invoke(context.Background(), &spi.Request{Identity: id, Operation: "CreateSecret", Input: map[string]any{"Name": "http-key", "SecretString": `{"api_key":"from-secret"}`}})
	if err != nil {
		t.Fatal(err)
	}
	secretDestination := immediateDestination("https://example.test/ok")
	secretDestination["EndpointConfiguration"].(map[string]any)["AccessKey"] = "ignored"
	secretDestination["SecretsManagerConfiguration"] = map[string]any{"Enabled": true, "RoleARN": testRoleARN, "SecretARN": createdSecret.Output["ARN"]}
	if _, err := call("CreateDeliveryStream", map[string]any{"DeliveryStreamName": "http-secret", "HttpEndpointDestinationConfiguration": secretDestination}); err != nil {
		t.Fatal(err)
	}
	if _, err := call("PutRecord", map[string]any{"DeliveryStreamName": "http-secret", "Record": map[string]any{"Data": base64.StdEncoding.EncodeToString([]byte("secret"))}}); err != nil {
		t.Fatal(err)
	}
	select {
	case request = <-captured:
	case <-time.After(pollBudget):
		t.Fatal("HTTP secret request did not run")
	}
	if request.header.Get("X-Amz-Firehose-Access-Key") != "from-secret" {
		t.Fatalf("HTTP secret access key %q", request.header.Get("X-Amz-Firehose-Access-Key"))
	}
	described, err = call("DescribeDeliveryStream", map[string]any{"DeliveryStreamName": "http-secret"})
	if err != nil {
		t.Fatal(err)
	}
	description = described.Output["DeliveryStreamDescription"].(map[string]any)["Destinations"].([]any)[0].(map[string]any)["HttpEndpointDestinationDescription"].(map[string]any)
	secretDescription := description["SecretsManagerConfiguration"].(map[string]any)
	if secretDescription["Enabled"] != true || secretDescription["SecretARN"] != createdSecret.Output["ARN"] || description["EndpointConfiguration"].(map[string]any)["AccessKey"] != nil {
		t.Fatalf("HTTP secret description %#v", description)
	}

	badSecret, err := secretPack.Invoke(context.Background(), &spi.Request{Identity: id, Operation: "CreateSecret", Input: map[string]any{"Name": "bad-http-key", "SecretString": `{}`}})
	if err != nil {
		t.Fatal(err)
	}
	badSecretDestination := immediateDestination("https://example.test/ok")
	badSecretDestination["S3Configuration"].(map[string]any)["Prefix"] = "secret-failed/"
	badSecretDestination["RetryOptions"] = map[string]any{"DurationInSeconds": 0}
	badSecretDestination["SecretsManagerConfiguration"] = map[string]any{"Enabled": true, "RoleARN": testRoleARN, "SecretARN": badSecret.Output["ARN"]}
	if _, err := call("CreateDeliveryStream", map[string]any{"DeliveryStreamName": "http-bad-secret", "HttpEndpointDestinationConfiguration": badSecretDestination}); err != nil {
		t.Fatal(err)
	}
	badSecretPut, err := call("PutRecord", map[string]any{"DeliveryStreamName": "http-bad-secret", "Record": map[string]any{"Data": base64.StdEncoding.EncodeToString([]byte("not-sent"))}})
	if err != nil {
		t.Fatal(err)
	}
	if len(captured) != 0 {
		t.Fatal("sent HTTP request with malformed secret")
	}
	badSecretBackupKey := id.Account + "/" + id.Region + "/out/secret-failed/1970/01/01/00/http-bad-secret-1-1970-01-01-00-00-00-" + badSecretPut.Output["RecordId"].(string)
	waitBlob(badSecretBackupKey)

	missingSecretDestination := immediateDestination("https://example.test/ok")
	missingSecretDestination["S3Configuration"].(map[string]any)["Prefix"] = "missing-secret/"
	missingSecretDestination["RetryOptions"] = map[string]any{"DurationInSeconds": 0}
	missingSecretDestination["SecretsManagerConfiguration"] = map[string]any{"Enabled": true, "RoleARN": testRoleARN, "SecretARN": "arn:aws:secretsmanager:us-east-1:123456789012:secret:missing-http-key"}
	if _, err := call("CreateDeliveryStream", map[string]any{"DeliveryStreamName": "http-missing-secret", "HttpEndpointDestinationConfiguration": missingSecretDestination}); err != nil {
		t.Fatal(err)
	}
	missingSecretPut, err := call("PutRecord", map[string]any{"DeliveryStreamName": "http-missing-secret", "Record": map[string]any{"Data": base64.StdEncoding.EncodeToString([]byte("not-sent"))}})
	if err != nil {
		t.Fatal(err)
	}
	if len(captured) != 0 {
		t.Fatal("sent HTTP request with missing secret")
	}
	missingSecretBackupKey := id.Account + "/" + id.Region + "/out/missing-secret/1970/01/01/00/http-missing-secret-1-1970-01-01-00-00-00-" + missingSecretPut.Output["RecordId"].(string)
	waitBlob(missingSecretBackupKey)

	for _, failure := range []struct {
		name, path, prefix string
		retrySeconds       int
	}{
		{name: "retryable", path: "/failure", prefix: "failed/"},
		{name: "redirect", path: "/redirect", prefix: "redirect/"},
		{name: "permanent", path: "/permanent", prefix: "permanent/", retrySeconds: 10},
	} {
		destination := immediateDestination("https://example.test" + failure.path)
		destination["S3Configuration"].(map[string]any)["Prefix"] = failure.prefix
		destination["S3BackupMode"] = "FailedDataOnly"
		destination["RetryOptions"] = map[string]any{"DurationInSeconds": failure.retrySeconds}
		if _, err := call("CreateDeliveryStream", map[string]any{"DeliveryStreamName": failure.name, "HttpEndpointDestinationConfiguration": destination}); err != nil {
			t.Fatal(err)
		}
		put, err := call("PutRecord", map[string]any{"DeliveryStreamName": failure.name, "Record": map[string]any{"Data": base64.StdEncoding.EncodeToString([]byte(failure.name))}})
		if err != nil {
			t.Fatal(err)
		}
		<-captured
		key := id.Account + "/" + id.Region + "/out/" + failure.prefix + "1970/01/01/00/" + failure.name + "-1-1970-01-01-00-00-00-" + put.Output["RecordId"].(string)
		waitBlob(key)
	}
	if retries, _, _ := deps.Store.Scope(id.Account, id.Region).Collection("fh-http-retries").List(context.Background(), "", "", 0); len(retries) != 0 {
		t.Fatalf("scheduled permanent HTTP failure retry %#v", retries)
	}

	retryDestination := immediateDestination("https://example.test/retry")
	retryDestination["EndpointConfiguration"].(map[string]any)["AccessKey"] = "retry-initial"
	retryDestination["S3Configuration"].(map[string]any)["Prefix"] = "retry-failed/"
	retryDestination["RetryOptions"] = map[string]any{"DurationInSeconds": 10}
	if _, err := call("CreateDeliveryStream", map[string]any{
		"DeliveryStreamName": "http-retry", "HttpEndpointDestinationConfiguration": retryDestination,
		"DeliveryStreamEncryptionConfigurationInput": map[string]any{"KeyType": "AWS_OWNED_CMK"},
	}); err != nil {
		t.Fatal(err)
	}
	retryPut, err := call("PutRecord", map[string]any{"DeliveryStreamName": "http-retry", "Record": map[string]any{"Data": base64.StdEncoding.EncodeToString([]byte("retry"))}})
	if err != nil {
		t.Fatal(err)
	}
	initialRetry := <-captured
	retryCollection := deps.Store.Scope(id.Account, id.Region).Collection("fh-http-retries")
	var storedRetries []spi.KV
	for deadline := time.Now().Add(pollBudget); time.Now().Before(deadline); {
		storedRetries, _, _ = retryCollection.List(context.Background(), "", "", 0)
		if len(storedRetries) == 1 {
			break
		}
		time.Sleep(time.Millisecond)
	}
	var storedRetry httpRetry
	if len(storedRetries) != 1 || json.Unmarshal(storedRetries[0].Value, &storedRetry) != nil || bytes.Contains(storedRetries[0].Value, []byte(base64.StdEncoding.EncodeToString([]byte("retry")))) {
		t.Fatalf("persisted HTTP retry metadata %#v", storedRetries)
	}
	retryData, _, err := deps.Blobs.Get(context.Background(), storedRetry.DataKey)
	if err != nil {
		t.Fatal(err)
	}
	retryPayload, _ := io.ReadAll(retryData)
	_ = retryData.Close()
	if !bytes.HasPrefix(retryPayload, firehoseEncryptedPrefix) || bytes.Contains(retryPayload, []byte(base64.StdEncoding.EncodeToString([]byte("retry")))) {
		t.Fatalf("unencrypted HTTP retry payload %q", retryPayload)
	}
	if _, err := call("UpdateDestination", map[string]any{
		"DeliveryStreamName": "http-retry", "CurrentDeliveryStreamVersionId": "1", "DestinationId": destinationID,
		"HttpEndpointDestinationUpdate": map[string]any{"EndpointConfiguration": map[string]any{"Url": "https://example.test/retry", "AccessKey": "retry-success"}},
	}); err != nil {
		t.Fatal(err)
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	p = New(deps)
	p.httpClient = httpClient
	if err := deps.Clock.Advance(2 * time.Second); err != nil {
		t.Fatal(err)
	}
	var retried capturedRequest
	select {
	case retried = <-captured:
	case <-time.After(pollBudget):
		t.Fatal("persisted HTTP retry did not run")
	}
	if first(retried.payload, "requestId") != first(initialRetry.payload, "requestId") || retried.header.Get("X-Amz-Firehose-Access-Key") != "retry-success" {
		t.Fatalf("HTTP retry %#v initial %#v", retried, initialRetry)
	}
	for deadline := time.Now().Add(pollBudget); ; {
		retries, _, _ := retryCollection.List(context.Background(), "", "", 0)
		if len(retries) == 0 {
			break
		}
		if !time.Now().Before(deadline) {
			t.Fatal("successful HTTP retry remained persisted")
		}
		time.Sleep(time.Millisecond)
	}
	if _, _, err := deps.Blobs.Get(context.Background(), storedRetry.DataKey); err == nil {
		t.Fatal("successful HTTP retry payload remained persisted")
	}
	retryBackupKey := id.Account + "/" + id.Region + "/out/retry-failed/1970/01/01/00/http-retry-2-1970-01-01-00-00-02-" + retryPut.Output["RecordId"].(string)
	if _, _, err := deps.Blobs.Get(context.Background(), retryBackupKey); err == nil {
		t.Fatal("backed up successfully retried HTTP record")
	}

	exhaustedDestination := immediateDestination("https://example.test/exhaust")
	exhaustedDestination["S3Configuration"].(map[string]any)["Prefix"] = "exhausted/"
	exhaustedDestination["RetryOptions"] = map[string]any{"DurationInSeconds": 4}
	if _, err := call("CreateDeliveryStream", map[string]any{"DeliveryStreamName": "http-exhaust", "HttpEndpointDestinationConfiguration": exhaustedDestination}); err != nil {
		t.Fatal(err)
	}
	exhaustedPut, err := call("PutRecord", map[string]any{"DeliveryStreamName": "http-exhaust", "Record": map[string]any{"Data": base64.StdEncoding.EncodeToString([]byte("exhaust"))}})
	if err != nil {
		t.Fatal(err)
	}
	initialExhausted := <-captured
	if err := deps.Clock.Advance(2 * time.Second); err != nil {
		t.Fatal(err)
	}
	var retriedExhausted capturedRequest
	select {
	case retriedExhausted = <-captured:
	case <-time.After(pollBudget):
		t.Fatal("HTTP retry did not run before expiration")
	}
	if first(retriedExhausted.payload, "requestId") != first(initialExhausted.payload, "requestId") {
		t.Fatalf("HTTP retry request IDs %#v %#v", initialExhausted.payload, retriedExhausted.payload)
	}
	for deadline := time.Now().Add(pollBudget); ; {
		retries, _, _ := retryCollection.List(context.Background(), "", "", 0)
		if len(retries) == 1 {
			var retry httpRetry
			_ = json.Unmarshal(retries[0].Value, &retry)
			if retry.Retries == 1 {
				break
			}
		}
		if !time.Now().Before(deadline) {
			t.Fatal("HTTP retry state was not updated")
		}
		time.Sleep(time.Millisecond)
	}
	if err := deps.Clock.Advance(2 * time.Second); err != nil {
		t.Fatal(err)
	}
	select {
	case <-captured:
		t.Fatal("sent HTTP request after retry duration expired")
	case <-time.After(10 * time.Millisecond):
	}
	exhaustedBackupKey := id.Account + "/" + id.Region + "/out/exhausted/1970/01/01/00/http-exhaust-1-1970-01-01-00-00-06-" + exhaustedPut.Output["RecordId"].(string)
	waitBlob(exhaustedBackupKey)

	bufferedDestination := testHTTPEndpointDestination("https://example.test/ok")
	bufferedDestination["BufferingHints"] = map[string]any{"IntervalInSeconds": 10, "SizeInMBs": 64}
	if _, err := call("CreateDeliveryStream", map[string]any{
		"DeliveryStreamName": "http-buffered", "HttpEndpointDestinationConfiguration": bufferedDestination,
		"DeliveryStreamEncryptionConfigurationInput": map[string]any{"KeyType": "AWS_OWNED_CMK"},
	}); err != nil {
		t.Fatal(err)
	}
	bufferedFirst, err := call("PutRecord", map[string]any{"DeliveryStreamName": "http-buffered", "Record": map[string]any{"Data": base64.StdEncoding.EncodeToString([]byte("buffer-one"))}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := call("PutRecord", map[string]any{"DeliveryStreamName": "http-buffered", "Record": map[string]any{"Data": base64.StdEncoding.EncodeToString([]byte("buffer-two"))}}); err != nil {
		t.Fatal(err)
	}
	if len(captured) != 0 {
		t.Fatal("flushed HTTP records before the buffering interval")
	}
	bufferCollection := deps.Store.Scope(id.Account, id.Region).Collection("fh-http-buffers")
	bufferItems, _, _ := bufferCollection.List(context.Background(), "http-buffered/", "", 0)
	if len(bufferItems) != 2 {
		t.Fatalf("persisted HTTP buffer %#v", bufferItems)
	}
	bufferDataKeys := make([]string, len(bufferItems))
	for i, item := range bufferItems {
		var buffer httpBuffer
		_ = json.Unmarshal(item.Value, &buffer)
		bufferDataKeys[i] = buffer.DataKey
		reader, _, err := deps.Blobs.Get(context.Background(), buffer.DataKey)
		if err != nil {
			t.Fatal(err)
		}
		payload, _ := io.ReadAll(reader)
		_ = reader.Close()
		if !bytes.HasPrefix(payload, firehoseEncryptedPrefix) || bytes.Contains(payload, []byte(base64.StdEncoding.EncodeToString([]byte("buffer-one")))) || bytes.Contains(payload, []byte(base64.StdEncoding.EncodeToString([]byte("buffer-two")))) {
			t.Fatalf("unencrypted HTTP buffer payload %q", payload)
		}
	}
	if !p.hasHTTPWork(context.Background()) {
		t.Fatal("persisted HTTP buffer was not discoverable for restart")
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	p = New(deps)
	p.httpClient = httpClient
	if err := deps.Clock.Advance(10 * time.Second); err != nil {
		t.Fatal(err)
	}
	var bufferedRequest capturedRequest
	select {
	case bufferedRequest = <-captured:
	case <-time.After(pollBudget):
		t.Fatal("persisted HTTP buffer did not flush")
	}
	bufferedRecords := bufferedRequest.payload["records"].([]any)
	if first(bufferedRequest.payload, "requestId") != bufferedFirst.Output["RecordId"] || len(bufferedRecords) != 2 || first(bufferedRecords[0].(map[string]any), "data") != base64.StdEncoding.EncodeToString([]byte("buffer-one")) || first(bufferedRecords[1].(map[string]any), "data") != base64.StdEncoding.EncodeToString([]byte("buffer-two")) {
		t.Fatalf("buffered HTTP request %#v", bufferedRequest.payload)
	}
	for deadline := time.Now().Add(pollBudget); ; {
		items, _, _ := bufferCollection.List(context.Background(), "http-buffered/", "", 0)
		if len(items) == 0 {
			break
		}
		if !time.Now().Before(deadline) {
			t.Fatal("delivered HTTP buffer remained persisted")
		}
		time.Sleep(time.Millisecond)
	}
	for _, dataKey := range bufferDataKeys {
		if _, _, err := deps.Blobs.Get(context.Background(), dataKey); err == nil {
			t.Fatalf("delivered HTTP buffer payload remained persisted: %s", dataKey)
		}
	}

	sizeDestination := testHTTPEndpointDestination("https://example.test/ok")
	sizeDestination["BufferingHints"] = map[string]any{"IntervalInSeconds": 900, "SizeInMBs": 1}
	if _, err := call("CreateDeliveryStream", map[string]any{"DeliveryStreamName": "http-size-buffered", "HttpEndpointDestinationConfiguration": sizeDestination}); err != nil {
		t.Fatal(err)
	}
	largeA, largeB := bytes.Repeat([]byte("a"), 600*1024), bytes.Repeat([]byte("b"), 600*1024)
	sizeFirst, err := call("PutRecord", map[string]any{"DeliveryStreamName": "http-size-buffered", "Record": map[string]any{"Data": base64.StdEncoding.EncodeToString(largeA)}})
	if err != nil {
		t.Fatal(err)
	}
	if len(captured) != 0 {
		t.Fatal("flushed HTTP records below the size hint")
	}
	if _, err := call("PutRecord", map[string]any{"DeliveryStreamName": "http-size-buffered", "Record": map[string]any{"Data": base64.StdEncoding.EncodeToString(largeB)}}); err != nil {
		t.Fatal(err)
	}
	select {
	case bufferedRequest = <-captured:
	case <-time.After(pollBudget):
		t.Fatal("size-threshold HTTP buffer did not flush")
	}
	bufferedRecords = bufferedRequest.payload["records"].([]any)
	if first(bufferedRequest.payload, "requestId") != sizeFirst.Output["RecordId"] || len(bufferedRecords) != 2 || first(bufferedRecords[0].(map[string]any), "data") != base64.StdEncoding.EncodeToString(largeA) || first(bufferedRecords[1].(map[string]any), "data") != base64.StdEncoding.EncodeToString(largeB) {
		t.Fatal("size-threshold HTTP buffer changed record order or data")
	}

	concurrentDestination := immediateDestination("https://example.test/blocked")
	if _, err := call("CreateDeliveryStream", map[string]any{"DeliveryStreamName": "http-concurrent-buffer", "HttpEndpointDestinationConfiguration": concurrentDestination}); err != nil {
		t.Fatal(err)
	}
	if _, err := call("PutRecord", map[string]any{"DeliveryStreamName": "http-concurrent-buffer", "Record": map[string]any{"Data": base64.StdEncoding.EncodeToString([]byte("in-flight"))}}); err != nil {
		t.Fatal(err)
	}
	firstInFlight := <-captured
	if _, err := call("PutRecord", map[string]any{"DeliveryStreamName": "http-concurrent-buffer", "Record": map[string]any{"Data": base64.StdEncoding.EncodeToString([]byte("queued"))}}); err != nil {
		t.Fatal(err)
	}
	queued, _, _ := bufferCollection.List(context.Background(), "http-concurrent-buffer/", "", 0)
	if len(queued) != 2 {
		t.Fatalf("concurrent HTTP buffer %#v", queued)
	}
	releaseBlocked <- struct{}{}
	var secondInFlight capturedRequest
	select {
	case secondInFlight = <-captured:
	case <-time.After(pollBudget):
		t.Fatal("concurrent HTTP buffer was erased")
	}
	releaseBlocked <- struct{}{}
	firstData := firstInFlight.payload["records"].([]any)[0].(map[string]any)
	secondData := secondInFlight.payload["records"].([]any)[0].(map[string]any)
	if first(firstData, "data") != base64.StdEncoding.EncodeToString([]byte("in-flight")) || first(secondData, "data") != base64.StdEncoding.EncodeToString([]byte("queued")) {
		t.Fatalf("concurrent HTTP requests %#v %#v", firstInFlight.payload, secondInFlight.payload)
	}

	deleteDestination := testHTTPEndpointDestination("https://example.test/ok")
	deleteDestination["BufferingHints"] = map[string]any{"IntervalInSeconds": 900, "SizeInMBs": 64}
	if _, err := call("CreateDeliveryStream", map[string]any{"DeliveryStreamName": "http-delete-buffer", "HttpEndpointDestinationConfiguration": deleteDestination}); err != nil {
		t.Fatal(err)
	}
	if _, err := call("PutRecord", map[string]any{"DeliveryStreamName": "http-delete-buffer", "Record": map[string]any{"Data": base64.StdEncoding.EncodeToString([]byte("delete"))}}); err != nil {
		t.Fatal(err)
	}
	deleteItems, _, _ := bufferCollection.List(context.Background(), "http-delete-buffer/", "", 0)
	var deleteBuffer httpBuffer
	if len(deleteItems) != 1 || json.Unmarshal(deleteItems[0].Value, &deleteBuffer) != nil {
		t.Fatalf("delete HTTP buffer %#v", deleteItems)
	}
	if _, err := call("DeleteDeliveryStream", map[string]any{"DeliveryStreamName": "http-delete-buffer"}); err != nil {
		t.Fatal(err)
	}
	deleteItems, _, _ = bufferCollection.List(context.Background(), "http-delete-buffer/", "", 0)
	if len(deleteItems) != 0 {
		t.Fatalf("deleted stream retained HTTP buffer %#v", deleteItems)
	}
	if _, _, err := deps.Blobs.Get(context.Background(), deleteBuffer.DataKey); err == nil {
		t.Fatal("deleted stream retained HTTP buffer payload")
	}

	deleteRetryDestination := immediateDestination("https://example.test/failure")
	if _, err := call("CreateDeliveryStream", map[string]any{"DeliveryStreamName": "http-delete-retry", "HttpEndpointDestinationConfiguration": deleteRetryDestination}); err != nil {
		t.Fatal(err)
	}
	if _, err := call("PutRecord", map[string]any{"DeliveryStreamName": "http-delete-retry", "Record": map[string]any{"Data": base64.StdEncoding.EncodeToString([]byte("delete-retry"))}}); err != nil {
		t.Fatal(err)
	}
	<-captured
	var deleteRetry httpRetry
	for deadline := time.Now().Add(pollBudget); ; {
		retries, _, _ := retryCollection.List(context.Background(), "", "", 0)
		for _, item := range retries {
			_ = json.Unmarshal(item.Value, &deleteRetry)
			if deleteRetry.Stream == "http-delete-retry" {
				break
			}
		}
		if deleteRetry.Stream == "http-delete-retry" {
			break
		}
		if !time.Now().Before(deadline) {
			t.Fatal("delete HTTP retry was not persisted")
		}
		time.Sleep(time.Millisecond)
	}
	if _, err := call("DeleteDeliveryStream", map[string]any{"DeliveryStreamName": "http-delete-retry"}); err != nil {
		t.Fatal(err)
	}
	remainingRetries, _, _ := retryCollection.List(context.Background(), "", "", 0)
	for _, item := range remainingRetries {
		var retry httpRetry
		_ = json.Unmarshal(item.Value, &retry)
		if retry.Stream == "http-delete-retry" {
			t.Fatalf("deleted stream retained HTTP retry %#v", retry)
		}
	}
	if _, _, err := deps.Blobs.Get(context.Background(), deleteRetry.DataKey); err == nil {
		t.Fatal("deleted stream retained HTTP retry payload")
	}
}

func TestFirehoseHTTPEndpointValidation(t *testing.T) {
	p := New(spitest.Deps(t))
	id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
	invalid := []map[string]any{
		testHTTPEndpointDestination("http://example.test"),
		testHTTPEndpointDestination("https://example.test:444/path"),
		{"S3Configuration": testS3Destination()},
		{"EndpointConfiguration": map[string]any{"Url": "https://example.test"}},
		testHTTPEndpointDestination("not a URL"),
		testHTTPEndpointDestination("https://example.test"),
	}
	invalid[5]["EndpointConfiguration"].(map[string]any)["Name"] = " "
	for i, destination := range invalid {
		_, err := p.Invoke(context.Background(), &spi.Request{Identity: id, Operation: "CreateDeliveryStream", Input: map[string]any{
			"DeliveryStreamName": fmt.Sprintf("invalid-http-%d", i), "HttpEndpointDestinationConfiguration": destination,
		}})
		if err == nil {
			t.Errorf("accepted invalid HTTP destination %#v", destination)
		}
	}

	for i, patch := range []map[string]any{
		{"BufferingHints": map[string]any{"SizeInMBs": 1}},
		{"BufferingHints": map[string]any{"SizeInMBs": 65, "IntervalInSeconds": 1}},
		{"RetryOptions": map[string]any{"DurationInSeconds": 7201}},
		{"S3BackupMode": "Unknown"},
		{"RequestConfiguration": map[string]any{"ContentEncoding": "ZIP"}},
		{"RequestConfiguration": map[string]any{"CommonAttributes": []any{map[string]any{"AttributeName": "", "AttributeValue": "value"}}}},
		{"EndpointConfiguration": map[string]any{"Url": "https://example.test", "AccessKey": "bad\nkey"}},
		{"ProcessingConfiguration": "invalid"},
		{"SecretsManagerConfiguration": map[string]any{"Enabled": true, "RoleARN": testRoleARN}},
	} {
		destination := testHTTPEndpointDestination("https://example.test")
		maps.Copy(destination, patch)
		_, err := p.Invoke(context.Background(), &spi.Request{Identity: id, Operation: "CreateDeliveryStream", Input: map[string]any{
			"DeliveryStreamName": fmt.Sprintf("invalid-http-option-%d", i), "HttpEndpointDestinationConfiguration": destination,
		}})
		if err == nil {
			t.Errorf("accepted invalid HTTP option %#v", patch)
		}
	}
	if _, err := p.Invoke(context.Background(), &spi.Request{Identity: id, Operation: "CreateDeliveryStream", Input: map[string]any{
		"DeliveryStreamName": "default-http-buffer", "HttpEndpointDestinationConfiguration": testHTTPEndpointDestination("https://example.test"),
	}}); err != nil {
		t.Fatal(err)
	}
	described, err := p.Invoke(context.Background(), &spi.Request{Identity: id, Operation: "DescribeDeliveryStream", Input: map[string]any{"DeliveryStreamName": "default-http-buffer"}})
	if err != nil {
		t.Fatal(err)
	}
	description := described.Output["DeliveryStreamDescription"].(map[string]any)["Destinations"].([]any)[0].(map[string]any)["HttpEndpointDestinationDescription"].(map[string]any)
	if hints := description["BufferingHints"].(map[string]any); hints["IntervalInSeconds"] != 300 || hints["SizeInMBs"] != 5 {
		t.Fatalf("default HTTP buffering hints %#v", hints)
	}
}

func TestFirehoseHTTPRetryDelay(t *testing.T) {
	p := New(spitest.Deps(t))
	defer func() { _ = p.Close() }()
	if delay := p.httpRetryDelay("request", 0); delay < 850*time.Millisecond || delay > 1150*time.Millisecond {
		t.Fatalf("initial HTTP retry delay %s", delay)
	}
	if delay := p.httpRetryDelay("request", 100); delay > 2*time.Minute {
		t.Fatalf("capped HTTP retry delay %s", delay)
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
