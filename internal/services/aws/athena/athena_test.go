package athena

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
	"github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/s3tables"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spitest"

	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/glue"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/s3"
)

func TestS3TablesCatalogDDLInsertSelectAndCTAS(t *testing.T) {
	deps := spitest.Deps(t)
	ctx := context.Background()
	id := spi.Identity{Account: "000000000000", Region: "us-east-1"}
	tables := s3tables.New(deps)
	if _, err := tables.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateTableBucket", Input: map[string]any{"name": "warehouse"}}); err != nil {
		t.Fatal(err)
	}
	p := New(deps)
	run := func(sql string) map[string]any {
		t.Helper()
		started, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "StartQueryExecution", Input: map[string]any{
			"QueryString": sql, "QueryExecutionContext": map[string]any{"Catalog": "s3tablescatalog/warehouse", "Database": "analytics"},
		}})
		if err != nil {
			t.Fatal(err)
		}
		queryID := started.Output["QueryExecutionId"].(string)
		execution, _ := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "GetQueryExecution", Input: map[string]any{"QueryExecutionId": queryID}})
		status := execution.Output["QueryExecution"].(map[string]any)["Status"].(map[string]any)
		if status["State"] != "SUCCEEDED" {
			t.Fatalf("%s: %#v", sql, status)
		}
		results, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "GetQueryResults", Input: map[string]any{"QueryExecutionId": queryID}})
		if err != nil {
			t.Fatal(err)
		}
		return results.Output
	}
	run("CREATE TABLE events (id varchar, name varchar)")
	run("INSERT INTO events VALUES ('1', 'alice'), ('2', 'bob')")
	selected := run("SELECT name FROM events WHERE id = '2'")
	raw, _ := json.Marshal(selected)
	if !strings.Contains(string(raw), "bob") || strings.Contains(string(raw), "alice") {
		t.Fatalf("selected = %s", raw)
	}
	run("CREATE TABLE copied AS SELECT * FROM events WHERE name = 'alice'")
	ctas := run("SELECT * FROM copied")
	raw, _ = json.Marshal(ctas)
	if !strings.Contains(string(raw), "alice") || strings.Contains(string(raw), "bob") {
		t.Fatalf("ctas = %s", raw)
	}
	arn := "arn:aws:s3tables:us-east-1:000000000000:bucket/warehouse"
	if _, err := tables.Invoke(ctx, &spi.Request{Identity: id, Operation: "GetTable", Input: map[string]any{"tableBucketARN": arn, "namespace": "analytics", "name": "copied"}}); err != nil {
		t.Fatal(err)
	}
}

func TestAthenaHTTPProvenOps(t *testing.T) {
	p := New(spitest.Deps(t))
	if n := len(p.Operations()); n != 9 {
		t.Fatalf("athena Operations() %d want 9", n)
	}
}

func TestBootedServerAthenaQueryAndWorkGroup(t *testing.T) {
	cfg := config.Default()
	cfg.Services = []string{"aws.athena"}
	cfg.Seed = "ath-1"
	rt, err := rtpkg.Boot(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(rt.Handler())
	defer ts.Close()
	auth := "AWS4-HMAC-SHA256 Credential=test/20200101/us-east-1/athena/aws4_request, SignedHeaders=host, Signature=00"
	call := func(op, body string) map[string]any {
		t.Helper()
		req, _ := http.NewRequest(http.MethodPost, ts.URL+"/", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/x-amz-json-1.1")
		req.Header.Set("X-Amz-Target", "AmazonAthena."+op)
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
	call("CreateWorkGroup", `{"Name":"wg1"}`)
	wg := call("GetWorkGroup", `{"WorkGroup":"wg1"}`)
	if wg["WorkGroup"] == nil {
		t.Fatalf("wg %v", wg)
	}
	started := call("StartQueryExecution", `{"QueryString":"SELECT 1","WorkGroup":"wg1"}`)
	id, _ := started["QueryExecutionId"].(string)
	if id == "" {
		t.Fatalf("start %v", started)
	}
	got := call("GetQueryExecution", `{"QueryExecutionId":"`+id+`"}`)
	qe, _ := got["QueryExecution"].(map[string]any)
	st, _ := qe["Status"].(map[string]any)
	if st["State"] != "SUCCEEDED" {
		t.Fatalf("exec %v", got)
	}
	res := call("GetQueryResults", `{"QueryExecutionId":"`+id+`"}`)
	raw, _ := json.Marshal(res)
	if !strings.Contains(string(raw), `"1"`) {
		t.Fatalf("results %s", raw)
	}
	call("DeleteWorkGroup", `{"WorkGroup":"wg1"}`)
	listed := call("ListWorkGroups", `{}`)
	raw, _ = json.Marshal(listed)
	if strings.Contains(string(raw), `"Name":"wg1"`) {
		t.Fatalf("wg still present %s", raw)
	}
}

func TestBootedServerAthenaSQLOverGlueS3(t *testing.T) {
	cfg := config.Default()
	cfg.Services = []string{"aws.athena", "aws.glue", "aws.s3"}
	cfg.Seed = "ath-sql-1"
	rt, err := rtpkg.Boot(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(rt.Handler())
	defer ts.Close()
	athAuth := "AWS4-HMAC-SHA256 Credential=test/20200101/us-east-1/athena/aws4_request, SignedHeaders=host, Signature=00"
	glueAuth := "AWS4-HMAC-SHA256 Credential=test/20200101/us-east-1/glue/aws4_request, SignedHeaders=host, Signature=00"
	s3Auth := "AWS4-HMAC-SHA256 Credential=test/20200101/us-east-1/s3/aws4_request, SignedHeaders=host, Signature=00"
	jsonCall := func(auth, target, op, body string) map[string]any {
		t.Helper()
		req, _ := http.NewRequest(http.MethodPost, ts.URL+"/", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/x-amz-json-1.1")
		req.Header.Set("X-Amz-Target", target+"."+op)
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
	s3 := func(method, path, body string) {
		t.Helper()
		req, _ := http.NewRequest(method, ts.URL+path, strings.NewReader(body))
		req.Header.Set("Authorization", s3Auth)
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		raw, _ := io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode >= 300 {
			t.Fatalf("s3 %s %s %d %s", method, path, res.StatusCode, raw)
		}
	}
	s3(http.MethodPut, "/athin", "")
	s3(http.MethodPut, "/athin/data/part.csv", "n,name\n1,alice\n2,bob\n")
	jsonCall(glueAuth, "AWSGlue", "CreateDatabase", `{"DatabaseInput":{"Name":"db1"}}`)
	jsonCall(glueAuth, "AWSGlue", "CreateTable", `{"DatabaseName":"db1","TableInput":{"Name":"t1","StorageDescriptor":{"Location":"s3://athin/data/","Columns":[{"Name":"n","Type":"string"},{"Name":"name","Type":"string"}]}}}`)
	started := jsonCall(athAuth, "AmazonAthena", "StartQueryExecution", `{"QueryString":"SELECT * FROM db1.t1"}`)
	id, _ := started["QueryExecutionId"].(string)
	if id == "" {
		t.Fatalf("start %v", started)
	}
	got := jsonCall(athAuth, "AmazonAthena", "GetQueryExecution", `{"QueryExecutionId":"`+id+`"}`)
	qe, _ := got["QueryExecution"].(map[string]any)
	st, _ := qe["Status"].(map[string]any)
	if st["State"] != "SUCCEEDED" {
		t.Fatalf("exec %v", got)
	}
	res := jsonCall(athAuth, "AmazonAthena", "GetQueryResults", `{"QueryExecutionId":"`+id+`"}`)
	raw, _ := json.Marshal(res)
	if !strings.Contains(string(raw), "alice") || !strings.Contains(string(raw), "bob") {
		t.Fatalf("select * %s", raw)
	}
	wstarted := jsonCall(athAuth, "AmazonAthena", "StartQueryExecution", `{"QueryString":"SELECT name FROM db1.t1 WHERE name = 'alice'"}`)
	wid, _ := wstarted["QueryExecutionId"].(string)
	wres := jsonCall(athAuth, "AmazonAthena", "GetQueryResults", `{"QueryExecutionId":"`+wid+`"}`)
	wraw, _ := json.Marshal(wres)
	if !strings.Contains(string(wraw), "alice") || strings.Contains(string(wraw), "bob") {
		t.Fatalf("where %s", wraw)
	}
}
