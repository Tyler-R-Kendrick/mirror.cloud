package dms

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

func TestDMSHTTPProvenOps(t *testing.T) {
	p := New(spitest.Deps(t))
	if n := len(p.Operations()); n != 11 {
		t.Fatalf("dms Operations() %d want 11", n)
	}
}

func TestBootedServerDMSCreateGetDelete(t *testing.T) {
	cfg := config.Default()
	cfg.Services = []string{"aws.dms"}
	cfg.Seed = "dms-1"
	rt, err := rtpkg.Boot(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(rt.Handler())
	defer ts.Close()
	auth := "AWS4-HMAC-SHA256 Credential=test/20200101/us-east-1/dms/aws4_request, SignedHeaders=host, Signature=00"
	call := func(op, body string) map[string]any {
		t.Helper()
		req, _ := http.NewRequest(http.MethodPost, ts.URL+"/", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/x-amz-json-1.1")
		req.Header.Set("X-Amz-Target", "AmazonDMS20160101."+op)
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
	created := call("CreateReplicationInstance", `{"ReplicationInstanceIdentifier":"ri1","ReplicationInstanceClass":"dms.t3.micro"}`)
	if created["ReplicationInstance"] == nil {
		t.Fatalf("create %v", created)
	}
	got := call("DescribeReplicationInstances", `{"ReplicationInstanceIdentifier":"ri1"}`)
	raw, _ := json.Marshal(got)
	if !strings.Contains(string(raw), "ri1") {
		t.Fatalf("describe %s", raw)
	}
	call("CreateEndpoint", `{"EndpointIdentifier":"ep1","EngineName":"postgres"}`)
	call("CreateReplicationTask", `{"ReplicationTaskIdentifier":"t1","MigrationType":"full-load"}`)
	start := call("StartReplicationTask", `{"ReplicationTaskIdentifier":"t1"}`)
	task, _ := start["ReplicationTask"].(map[string]any)
	if task["Status"] != "running" {
		t.Fatalf("start %v", start)
	}
	call("DeleteReplicationTask", `{"ReplicationTaskIdentifier":"t1"}`)
	call("DeleteEndpoint", `{"EndpointIdentifier":"ep1"}`)
	call("DeleteReplicationInstance", `{"ReplicationInstanceIdentifier":"ri1"}`)
	gone := call("DescribeReplicationInstances", `{"ReplicationInstanceIdentifier":"ri1"}`)
	raw, _ = json.Marshal(gone)
	if strings.Contains(string(raw), `"ReplicationInstanceIdentifier":"ri1"`) {
		t.Fatalf("still present %s", raw)
	}
}
