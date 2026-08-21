package lightsail

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

func TestLightsailHTTPProvenOps(t *testing.T) {
	p := New(spitest.Deps(t))
	if n := len(p.Operations()); n != 7 {
		t.Fatalf("lightsail Operations() %d want 7", n)
	}
}

func TestBootedServerLightsailCreateGetDelete(t *testing.T) {
	cfg := config.Default()
	cfg.Services = []string{"aws.lightsail"}
	cfg.Seed = "ls-1"
	rt, err := rtpkg.Boot(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(rt.Handler())
	defer ts.Close()
	auth := "AWS4-HMAC-SHA256 Credential=test/20200101/us-east-1/lightsail/aws4_request, SignedHeaders=host, Signature=00"
	call := func(op, body string) map[string]any {
		t.Helper()
		req, _ := http.NewRequest(http.MethodPost, ts.URL+"/", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/x-amz-json-1.1")
		req.Header.Set("X-Amz-Target", "Lightsail_20161128."+op)
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
	created := call("CreateInstances", `{"instanceNames":["i1"],"blueprintId":"amazon_linux_2023","bundleId":"nano_3_0"}`)
	if created["operations"] == nil {
		t.Fatalf("create %v", created)
	}
	got := call("GetInstance", `{"instanceName":"i1"}`)
	if got["instance"] == nil {
		t.Fatalf("get %v", got)
	}
	call("DeleteInstance", `{"instanceName":"i1"}`)
	listed := call("GetInstances", `{}`)
	raw, _ := json.Marshal(listed)
	if strings.Contains(string(raw), `"name":"i1"`) {
		t.Fatalf("still present %s", raw)
	}
}
