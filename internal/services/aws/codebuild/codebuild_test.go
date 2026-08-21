package codebuild

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

func TestCodeBuildHTTPProvenOps(t *testing.T) {
	p := New(spitest.Deps(t))
	if n := len(p.Operations()); n != 9 {
		t.Fatalf("codebuild Operations() %d want 9", n)
	}
}

func TestBootedServerCodeBuildProject(t *testing.T) {
	cfg := config.Default()
	cfg.Services = []string{"aws.codebuild"}
	cfg.Seed = "cb-1"
	rt, err := rtpkg.Boot(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(rt.Handler())
	defer ts.Close()
	auth := "AWS4-HMAC-SHA256 Credential=test/20200101/us-east-1/codebuild/aws4_request, SignedHeaders=host, Signature=00"
	call := func(op, body string) map[string]any {
		t.Helper()
		req, _ := http.NewRequest(http.MethodPost, ts.URL+"/", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/x-amz-json-1.1")
		req.Header.Set("X-Amz-Target", "CodeBuild_20161006."+op)
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
	created := call("CreateProject", `{"name":"p1","source":{"type":"NO_SOURCE"},"environment":{"type":"LINUX_CONTAINER","image":"aws/codebuild/standard:7.0","computeType":"BUILD_GENERAL1_SMALL"}}`)
	if created["project"] == nil {
		t.Fatalf("create %v", created)
	}
	got := call("BatchGetProjects", `{"names":["p1"]}`)
	if got["projects"] == nil {
		t.Fatalf("get %v", got)
	}
	listed := call("ListProjects", `{}`)
	raw, _ := json.Marshal(listed)
	if !strings.Contains(string(raw), "p1") {
		t.Fatalf("list %s", raw)
	}
	call("DeleteProject", `{"name":"p1"}`)
	gone := call("ListProjects", `{}`)
	raw, _ = json.Marshal(gone)
	if strings.Contains(string(raw), `"p1"`) {
		t.Fatalf("still present %s", raw)
	}
}
