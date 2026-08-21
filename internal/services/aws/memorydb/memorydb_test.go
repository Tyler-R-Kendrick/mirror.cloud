package memorydb

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

func TestMemoryDBHTTPProvenOps(t *testing.T) {
	p := New(spitest.Deps(t))
	if n := len(p.Operations()); n != 6 {
		t.Fatalf("memorydb Operations() %d want 6", n)
	}
}

func TestBootedServerMemoryDBCreateGetDelete(t *testing.T) {
	cfg := config.Default()
	cfg.Services = []string{"aws.memorydb"}
	cfg.Seed = "mdb-1"
	rt, err := rtpkg.Boot(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(rt.Handler())
	defer ts.Close()
	auth := "AWS4-HMAC-SHA256 Credential=test/20200101/us-east-1/memorydb/aws4_request, SignedHeaders=host, Signature=00"
	call := func(op, body string) map[string]any {
		t.Helper()
		req, _ := http.NewRequest(http.MethodPost, ts.URL+"/", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/x-amz-json-1.1")
		req.Header.Set("X-Amz-Target", "AmazonMemoryDB."+op)
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
	created := call("CreateCluster", `{"ClusterName":"c1","NodeType":"db.t4g.small"}`)
	if created["Cluster"] == nil {
		t.Fatalf("create %v", created)
	}
	got := call("DescribeClusters", `{"ClusterName":"c1"}`)
	raw, _ := json.Marshal(got)
	if !strings.Contains(string(raw), `"c1"`) {
		t.Fatalf("describe %s", raw)
	}
	call("DeleteCluster", `{"ClusterName":"c1"}`)
	gone := call("DescribeClusters", `{"ClusterName":"c1"}`)
	raw, _ = json.Marshal(gone)
	if strings.Contains(string(raw), `"c1"`) {
		t.Fatalf("still present %s", raw)
	}
}
