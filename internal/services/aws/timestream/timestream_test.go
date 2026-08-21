package timestream

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/config"
	rtpkg "github.com/tyler-r-kendrick/mirror.cloud/internal/runtime"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spitest"
)

func TestTimestreamHTTPProvenOps(t *testing.T) {
	p := New(spitest.Deps(t))
	if n := len(p.Operations()); n != 12 {
		t.Fatalf("timestream Operations() %d want 12", n)
	}
}

func TestBootedServerTimestreamCreateGetDelete(t *testing.T) {
	cfg := config.Default()
	cfg.Services = []string{"aws.timestream"}
	cfg.Seed = "ts-1"
	rt, err := rtpkg.Boot(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(rt.Handler())
	defer ts.Close()
	auth := "AWS4-HMAC-SHA256 Credential=test/20200101/us-east-1/timestream/aws4_request, SignedHeaders=host, Signature=00"
	call := func(op, body string) map[string]any {
		t.Helper()
		req, _ := http.NewRequest(http.MethodPost, ts.URL+"/", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/x-amz-json-1.1")
		req.Header.Set("X-Amz-Target", "Timestream_20181101."+op)
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
	created := call("CreateDatabase", `{"DatabaseName":"metrics"}`)
	if created["Database"] == nil {
		t.Fatalf("create %v", created)
	}
	got := call("DescribeDatabase", `{"DatabaseName":"metrics"}`)
	db, _ := got["Database"].(map[string]any)
	if db["DatabaseName"] != "metrics" {
		t.Fatalf("describe %v", got)
	}
	call("CreateTable", `{"DatabaseName":"metrics","TableName":"cpu"}`)
	tbl := call("DescribeTable", `{"DatabaseName":"metrics","TableName":"cpu"}`)
	if tbl["Table"] == nil {
		t.Fatalf("table %v", tbl)
	}
	wr := call("WriteRecords", `{"DatabaseName":"metrics","TableName":"cpu","Records":[{"MeasureName":"pct","MeasureValue":"1"}]}`)
	ing, _ := wr["RecordsIngested"].(map[string]any)
	if ing["Total"] == nil {
		t.Fatalf("write %v", wr)
	}
	q := call("Query", `{"QueryString":"SELECT * FROM cpu"}`)
	if q["Rows"] == nil {
		t.Fatalf("query %v", q)
	}
	call("DeleteTable", `{"DatabaseName":"metrics","TableName":"cpu"}`)
	call("DeleteDatabase", `{"DatabaseName":"metrics"}`)
	listed := call("ListDatabases", `{}`)
	raw, _ := json.Marshal(listed)
	if strings.Contains(string(raw), `"DatabaseName":"metrics"`) {
		t.Fatalf("still present %s", raw)
	}
}
