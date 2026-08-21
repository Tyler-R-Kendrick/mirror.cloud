package emr

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

func TestEMRHTTPProvenOps(t *testing.T) {
	p := New(spitest.Deps(t))
	if n := len(p.Operations()); n != 8 {
		t.Fatalf("emr Operations() %d want 8", n)
	}
}

func TestBootedServerEMRCluster(t *testing.T) {
	cfg := config.Default()
	cfg.Services = []string{"aws.elasticmapreduce"}
	cfg.Seed = "emr-1"
	rt, err := rtpkg.Boot(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(rt.Handler())
	defer ts.Close()
	auth := "AWS4-HMAC-SHA256 Credential=test/20200101/us-east-1/elasticmapreduce/aws4_request, SignedHeaders=host, Signature=00"
	call := func(op, body string) map[string]any {
		t.Helper()
		req, _ := http.NewRequest(http.MethodPost, ts.URL+"/", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/x-amz-json-1.1")
		req.Header.Set("X-Amz-Target", "ElasticMapReduce."+op)
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
	created := call("RunJobFlow", `{"Name":"c1"}`)
	id, _ := created["JobFlowId"].(string)
	if id == "" {
		t.Fatalf("create %v", created)
	}
	got := call("DescribeCluster", `{"ClusterId":"`+id+`"}`)
	if got["Cluster"] == nil {
		t.Fatalf("describe %v", got)
	}
	listed := call("ListClusters", `{}`)
	raw, _ := json.Marshal(listed)
	if !strings.Contains(string(raw), id) {
		t.Fatalf("list %s", raw)
	}
	call("TerminateJobFlows", `{"JobFlowIds":["`+id+`"]}`)
	term := call("DescribeCluster", `{"ClusterId":"`+id+`"}`)
	cl, _ := term["Cluster"].(map[string]any)
	st, _ := cl["Status"].(map[string]any)
	if st["State"] != "TERMINATED" {
		t.Fatalf("term %v", term)
	}
}
