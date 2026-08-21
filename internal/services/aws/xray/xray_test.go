package xray

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

func TestXRayHTTPProvenOps(t *testing.T) {
	p := New(spitest.Deps(t))
	if n := len(p.Operations()); n != 11 {
		t.Fatalf("xray Operations() %d want 11", n)
	}
}

func TestBootedServerXRayTraceAndGroup(t *testing.T) {
	cfg := config.Default()
	cfg.Services = []string{"aws.xray"}
	cfg.Seed = "xray-1"
	rt, err := rtpkg.Boot(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(rt.Handler())
	defer ts.Close()
	auth := "AWS4-HMAC-SHA256 Credential=test/20200101/us-east-1/xray/aws4_request, SignedHeaders=host, Signature=00"
	call := func(op, body string) map[string]any {
		t.Helper()
		req, _ := http.NewRequest(http.MethodPost, ts.URL+"/", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/x-amz-json-1.1")
		req.Header.Set("X-Amz-Target", "AWSXRay."+op)
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
	seg := `{"trace_id":"1-5f84c7a1-aaaaaaaaaaaaaaaaaaaaaaaa","id":"bbbbbbbbbbbbbbbb","name":"api"}`
	doc, _ := json.Marshal(seg)
	put := call("PutTraceSegments", `{"TraceSegmentDocuments":[`+string(doc)+`]}`)
	if put["UnprocessedTraceSegments"] == nil {
		t.Fatalf("put %v", put)
	}
	got := call("BatchGetTraces", `{"TraceIds":["1-5f84c7a1-aaaaaaaaaaaaaaaaaaaaaaaa"]}`)
	raw, _ := json.Marshal(got)
	if !strings.Contains(string(raw), "1-5f84c7a1-aaaaaaaaaaaaaaaaaaaaaaaa") {
		t.Fatalf("get traces %s", raw)
	}
	grp := call("CreateGroup", `{"GroupName":"g1","FilterExpression":"service(\"api\")"}`)
	if grp["Group"] == nil {
		t.Fatalf("group %v", grp)
	}
	listed := call("GetGroups", `{}`)
	raw, _ = json.Marshal(listed)
	if !strings.Contains(string(raw), "g1") {
		t.Fatalf("groups %s", raw)
	}
	call("DeleteGroup", `{"GroupName":"g1"}`)
	gone := call("GetGroups", `{}`)
	raw, _ = json.Marshal(gone)
	if strings.Contains(string(raw), `"GroupName":"g1"`) {
		t.Fatalf("group still present %s", raw)
	}
}
