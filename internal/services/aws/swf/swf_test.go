package swf

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

func TestSWFHTTPProvenOps(t *testing.T) {
	p := New(spitest.Deps(t))
	if n := len(p.Operations()); n != 15 {
		t.Fatalf("swf Operations() %d want 15", n)
	}
}

func TestBootedServerSWFCreateGetDelete(t *testing.T) {
	cfg := config.Default()
	cfg.Services = []string{"aws.swf"}
	cfg.Seed = "swf-1"
	rt, err := rtpkg.Boot(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(rt.Handler())
	defer ts.Close()
	auth := "AWS4-HMAC-SHA256 Credential=test/20200101/us-east-1/swf/aws4_request, SignedHeaders=host, Signature=00"
	call := func(op, body string) map[string]any {
		t.Helper()
		req, _ := http.NewRequest(http.MethodPost, ts.URL+"/", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/x-amz-json-1.1")
		req.Header.Set("X-Amz-Target", "SimpleWorkflowService."+op)
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
	call("RegisterDomain", `{"name":"d1","workflowExecutionRetentionPeriodInDays":"1"}`)
	got := call("DescribeDomain", `{"name":"d1"}`)
	info, _ := got["domainInfo"].(map[string]any)
	if info["name"] != "d1" {
		t.Fatalf("describe %v", got)
	}
	listed := call("ListDomains", `{"registrationStatus":"REGISTERED"}`)
	raw, _ := json.Marshal(listed)
	if !strings.Contains(string(raw), "d1") {
		t.Fatalf("list %s", raw)
	}
	call("RegisterWorkflowType", `{"domain":"d1","name":"wf","version":"1"}`)
	start := call("StartWorkflowExecution", `{"domain":"d1","workflowId":"w1","workflowType":{"name":"wf","version":"1"}}`)
	if start["runId"] == nil {
		t.Fatalf("start %v", start)
	}
	ex := call("DescribeWorkflowExecution", `{"domain":"d1","execution":{"workflowId":"w1"}}`)
	if ex["executionInfo"] == nil {
		t.Fatalf("exec %v", ex)
	}
	call("TerminateWorkflowExecution", `{"domain":"d1","workflowId":"w1"}`)
	call("DeprecateDomain", `{"name":"d1"}`)
	gone := call("ListDomains", `{"registrationStatus":"REGISTERED"}`)
	raw, _ = json.Marshal(gone)
	if strings.Contains(string(raw), `"name":"d1"`) {
		t.Fatalf("still present %s", raw)
	}
}
