package batch

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

func TestBatchHTTPProvenOps(t *testing.T) {
	p := New(spitest.Deps(t))
	if n := len(p.Operations()); n != 13 {
		t.Fatalf("batch Operations() %d want 13", n)
	}
}

func TestBootedServerBatchQueueAndJob(t *testing.T) {
	cfg := config.Default()
	cfg.Services = []string{"aws.batch"}
	cfg.Seed = "batch-1"
	rt, err := rtpkg.Boot(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(rt.Handler())
	defer ts.Close()
	auth := "AWS4-HMAC-SHA256 Credential=test/20200101/us-east-1/batch/aws4_request, SignedHeaders=host, Signature=00"
	call := func(op, body string) map[string]any {
		t.Helper()
		req, _ := http.NewRequest(http.MethodPost, ts.URL+"/", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/x-amz-json-1.1")
		req.Header.Set("X-Amz-Target", "AWSBatch."+op)
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
	ce := call("CreateComputeEnvironment", `{"computeEnvironmentName":"ce1","type":"MANAGED"}`)
	if ce["computeEnvironmentName"] != "ce1" {
		t.Fatalf("ce %v", ce)
	}
	jq := call("CreateJobQueue", `{"jobQueueName":"q1"}`)
	if jq["jobQueueName"] != "q1" {
		t.Fatalf("jq %v", jq)
	}
	listed := call("DescribeJobQueues", `{}`)
	raw, _ := json.Marshal(listed)
	if !strings.Contains(string(raw), "q1") {
		t.Fatalf("list %s", raw)
	}
	call("DeleteJobQueue", `{"jobQueue":"q1"}`)
	gone := call("DescribeJobQueues", `{}`)
	raw, _ = json.Marshal(gone)
	if strings.Contains(string(raw), `"q1"`) {
		t.Fatalf("still present %s", raw)
	}
}
