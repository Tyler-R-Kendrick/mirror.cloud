package amplify

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

func TestAmplifyHTTPProvenOps(t *testing.T) {
	p := New(spitest.Deps(t))
	if n := len(p.Operations()); n != 12 {
		t.Fatalf("amplify Operations() %d want 12", n)
	}
}

func TestBootedServerAmplifyCreateGetDelete(t *testing.T) {
	cfg := config.Default()
	cfg.Services = []string{"aws.amplify"}
	cfg.Seed = "amp-1"
	rt, err := rtpkg.Boot(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(rt.Handler())
	defer ts.Close()
	auth := "AWS4-HMAC-SHA256 Credential=test/20200101/us-east-1/amplify/aws4_request, SignedHeaders=host, Signature=00"
	call := func(op, body string) map[string]any {
		t.Helper()
		req, _ := http.NewRequest(http.MethodPost, ts.URL+"/", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/x-amz-json-1.1")
		req.Header.Set("X-Amz-Target", "AWSAmplify."+op)
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
	created := call("CreateApp", `{"name":"web"}`)
	app, _ := created["app"].(map[string]any)
	id, _ := app["appId"].(string)
	if id == "" {
		t.Fatalf("create %v", created)
	}
	got := call("GetApp", `{"appId":"`+id+`"}`)
	if got["app"] == nil {
		t.Fatalf("get %v", got)
	}
	listed := call("ListApps", `{}`)
	raw, _ := json.Marshal(listed)
	if !strings.Contains(string(raw), id) {
		t.Fatalf("list %s", raw)
	}
	call("CreateBranch", `{"appId":"`+id+`","branchName":"main"}`)
	call("GetBranch", `{"appId":"`+id+`","branchName":"main"}`)
	job := call("StartJob", `{"appId":"`+id+`","branchName":"main","jobType":"RELEASE"}`)
	if job["jobSummary"] == nil {
		t.Fatalf("job %v", job)
	}
	call("DeleteBranch", `{"appId":"`+id+`","branchName":"main"}`)
	call("DeleteApp", `{"appId":"`+id+`"}`)
	gone := call("ListApps", `{}`)
	raw, _ = json.Marshal(gone)
	if strings.Contains(string(raw), `"appId":"`+id+`"`) {
		t.Fatalf("still present %s", raw)
	}
}
