package codepipeline

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

func TestCodePipelineHTTPProvenOps(t *testing.T) {
	p := New(spitest.Deps(t))
	if n := len(p.Operations()); n != 10 {
		t.Fatalf("codepipeline Operations() %d want 10", n)
	}
}

func TestBootedServerCodePipelineCreateGetDelete(t *testing.T) {
	cfg := config.Default()
	cfg.Services = []string{"aws.codepipeline"}
	cfg.Seed = "cp-1"
	rt, err := rtpkg.Boot(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(rt.Handler())
	defer ts.Close()
	auth := "AWS4-HMAC-SHA256 Credential=test/20200101/us-east-1/codepipeline/aws4_request, SignedHeaders=host, Signature=00"
	call := func(op, body string) map[string]any {
		t.Helper()
		req, _ := http.NewRequest(http.MethodPost, ts.URL+"/", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/x-amz-json-1.1")
		req.Header.Set("X-Amz-Target", "CodePipeline_20150709."+op)
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
	created := call("CreatePipeline", `{"pipeline":{"name":"p1","roleArn":"arn:aws:iam::000000000000:role/x","stages":[]}}`)
	if created["pipeline"] == nil {
		t.Fatalf("create %v", created)
	}
	got := call("GetPipeline", `{"name":"p1"}`)
	pl, _ := got["pipeline"].(map[string]any)
	if pl["name"] != "p1" {
		t.Fatalf("get %v", got)
	}
	listed := call("ListPipelines", `{}`)
	raw, _ := json.Marshal(listed)
	if !strings.Contains(string(raw), "p1") {
		t.Fatalf("list %s", raw)
	}
	ex := call("StartPipelineExecution", `{"name":"p1"}`)
	if ex["pipelineExecutionId"] == nil {
		t.Fatalf("start %v", ex)
	}
	call("DeletePipeline", `{"name":"p1"}`)
	gone := call("ListPipelines", `{}`)
	raw, _ = json.Marshal(gone)
	if strings.Contains(string(raw), `"name":"p1"`) {
		t.Fatalf("still present %s", raw)
	}
}
