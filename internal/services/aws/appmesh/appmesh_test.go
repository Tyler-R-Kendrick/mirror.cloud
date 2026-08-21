package appmesh

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

func TestAppMeshHTTPProvenOps(t *testing.T) {
	p := New(spitest.Deps(t))
	if n := len(p.Operations()); n != 7 {
		t.Fatalf("appmesh Operations() %d want 7", n)
	}
}

func TestBootedServerAppMeshCreateGetDelete(t *testing.T) {
	cfg := config.Default()
	cfg.Services = []string{"aws.appmesh"}
	cfg.Seed = "am-1"
	rt, err := rtpkg.Boot(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(rt.Handler())
	defer ts.Close()
	auth := "AWS4-HMAC-SHA256 Credential=test/20200101/us-east-1/appmesh/aws4_request, SignedHeaders=host, Signature=00"
	call := func(op, body string) map[string]any {
		t.Helper()
		req, _ := http.NewRequest(http.MethodPost, ts.URL+"/", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/x-amz-json-1.1")
		req.Header.Set("X-Amz-Target", "AppMesh."+op)
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
	created := call("CreateMesh", `{"meshName":"mesh1"}`)
	if created["mesh"] == nil {
		t.Fatalf("create %v", created)
	}
	got := call("DescribeMesh", `{"meshName":"mesh1"}`)
	if got["mesh"] == nil {
		t.Fatalf("get %v", got)
	}
	call("DeleteMesh", `{"meshName":"mesh1"}`)
	listed := call("ListMeshes", `{}`)
	raw, _ := json.Marshal(listed)
	if strings.Contains(string(raw), `"mesh1"`) {
		t.Fatalf("still present %s", raw)
	}
}
