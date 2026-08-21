package workspaces

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

func TestWorkSpacesHTTPProvenOps(t *testing.T) {
	p := New(spitest.Deps(t))
	if n := len(p.Operations()); n != 9 {
		t.Fatalf("workspaces Operations() %d want 9", n)
	}
}

func TestBootedServerWorkSpacesCreateGetDelete(t *testing.T) {
	cfg := config.Default()
	cfg.Services = []string{"aws.workspaces"}
	cfg.Seed = "ws-1"
	rt, err := rtpkg.Boot(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(rt.Handler())
	defer ts.Close()
	auth := "AWS4-HMAC-SHA256 Credential=test/20200101/us-east-1/workspaces/aws4_request, SignedHeaders=host, Signature=00"
	call := func(op, body string) map[string]any {
		t.Helper()
		req, _ := http.NewRequest(http.MethodPost, ts.URL+"/", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/x-amz-json-1.1")
		req.Header.Set("X-Amz-Target", "WorkspacesService."+op)
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
	created := call("CreateWorkspaces", `{"Workspaces":[{"DirectoryId":"d-1","BundleId":"wsb-1","UserName":"alice"}]}`)
	pending, _ := created["PendingRequests"].([]any)
	if len(pending) == 0 {
		t.Fatalf("create %v", created)
	}
	rec, _ := pending[0].(map[string]any)
	id, _ := rec["WorkspaceId"].(string)
	if id == "" {
		t.Fatalf("id %v", created)
	}
	got := call("DescribeWorkspaces", `{"WorkspaceIds":["`+id+`"]}`)
	raw, _ := json.Marshal(got)
	if !strings.Contains(string(raw), id) {
		t.Fatalf("describe %s", raw)
	}
	call("StopWorkspaces", `{"WorkspaceIds":["`+id+`"]}`)
	call("TerminateWorkspaces", `{"WorkspaceIds":["`+id+`"]}`)
	gone := call("DescribeWorkspaces", `{"WorkspaceIds":["`+id+`"]}`)
	raw, _ = json.Marshal(gone)
	if strings.Contains(string(raw), `"`+id+`"`) {
		t.Fatalf("still present %s", raw)
	}
}
