package cloudhsmv2

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

func TestCloudHSMv2HTTPProvenOps(t *testing.T) {
	p := New(spitest.Deps(t))
	if n := len(p.Operations()); n != 6 {
		t.Fatalf("cloudhsmv2 Operations() %d want 6", n)
	}
}

func TestBootedServerCloudHSMv2CreateGetDelete(t *testing.T) {
	cfg := config.Default()
	cfg.Services = []string{"aws.cloudhsmv2"}
	cfg.Seed = "hsm-1"
	rt, err := rtpkg.Boot(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(rt.Handler())
	defer ts.Close()
	auth := "AWS4-HMAC-SHA256 Credential=test/20200101/us-east-1/cloudhsmv2/aws4_request, SignedHeaders=host, Signature=00"
	call := func(op, body string) map[string]any {
		t.Helper()
		req, _ := http.NewRequest(http.MethodPost, ts.URL+"/", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/x-amz-json-1.1")
		req.Header.Set("X-Amz-Target", "BaldrApiService."+op)
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
	created := call("CreateCluster", `{"HsmType":"hsm1.medium","SubnetIds":["subnet-1"]}`)
	cl, _ := created["Cluster"].(map[string]any)
	id, _ := cl["ClusterId"].(string)
	if id == "" {
		t.Fatalf("create %v", created)
	}
	got := call("DescribeClusters", `{"ClusterId":"`+id+`"}`)
	raw, _ := json.Marshal(got)
	if !strings.Contains(string(raw), id) {
		t.Fatalf("describe %s", raw)
	}
	call("DeleteCluster", `{"ClusterId":"`+id+`"}`)
	gone := call("DescribeClusters", `{"ClusterId":"`+id+`"}`)
	raw, _ = json.Marshal(gone)
	if strings.Contains(string(raw), `"`+id+`"`) {
		t.Fatalf("still present %s", raw)
	}
}
