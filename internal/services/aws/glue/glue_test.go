package glue

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

func TestGlueHTTPProvenOps(t *testing.T) {
	p := New(spitest.Deps(t))
	if n := len(p.Operations()); n != 21 {
		t.Fatalf("glue Operations() %d want 21", n)
	}
}

func TestBootedServerGlueDatabaseTable(t *testing.T) {
	cfg := config.Default()
	cfg.Services = []string{"aws.glue"}
	cfg.Seed = "glue-1"
	rt, err := rtpkg.Boot(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(rt.Handler())
	defer ts.Close()
	auth := "AWS4-HMAC-SHA256 Credential=test/20200101/us-east-1/glue/aws4_request, SignedHeaders=host, Signature=00"
	call := func(op, body string) map[string]any {
		t.Helper()
		req, _ := http.NewRequest(http.MethodPost, ts.URL+"/", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/x-amz-json-1.1")
		req.Header.Set("X-Amz-Target", "AWSGlue."+op)
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
	call("CreateDatabase", `{"DatabaseInput":{"Name":"db1"}}`)
	got := call("GetDatabase", `{"Name":"db1"}`)
	db, _ := got["Database"].(map[string]any)
	if db["Name"] != "db1" {
		t.Fatalf("get db %v", got)
	}
	call("CreateTable", `{"DatabaseName":"db1","TableInput":{"Name":"t1"}}`)
	tbl := call("GetTable", `{"DatabaseName":"db1","Name":"t1"}`)
	if tbl["Table"] == nil {
		t.Fatalf("get table %v", tbl)
	}
	listed := call("GetDatabases", `{}`)
	raw, _ := json.Marshal(listed)
	if !strings.Contains(string(raw), "db1") {
		t.Fatalf("list %s", raw)
	}
	call("DeleteTable", `{"DatabaseName":"db1","Name":"t1"}`)
	call("DeleteDatabase", `{"Name":"db1"}`)
	gone := call("GetDatabases", `{}`)
	raw, _ = json.Marshal(gone)
	if strings.Contains(string(raw), `"Name":"db1"`) {
		t.Fatalf("db still present %s", raw)
	}
}
