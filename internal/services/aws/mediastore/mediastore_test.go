package mediastore

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

func TestMediaStoreHTTPProvenOps(t *testing.T) {
	p := New(spitest.Deps(t))
	if n := len(p.Operations()); n != 6 {
		t.Fatalf("mediastore Operations() %d want 6", n)
	}
}

func TestBootedServerMediaStoreCreateGetDelete(t *testing.T) {
	cfg := config.Default()
	cfg.Services = []string{"aws.mediastore"}
	cfg.Seed = "ms-1"
	rt, err := rtpkg.Boot(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(rt.Handler())
	defer ts.Close()
	auth := "AWS4-HMAC-SHA256 Credential=test/20200101/us-east-1/mediastore/aws4_request, SignedHeaders=host, Signature=00"
	call := func(op, body string) map[string]any {
		t.Helper()
		req, _ := http.NewRequest(http.MethodPost, ts.URL+"/", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/x-amz-json-1.1")
		req.Header.Set("X-Amz-Target", "MediaStore_20170901."+op)
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
	created := call("CreateContainer", `{"ContainerName":"c1"}`)
	if created["Container"] == nil {
		t.Fatalf("create %v", created)
	}
	got := call("DescribeContainer", `{"ContainerName":"c1"}`)
	ct, _ := got["Container"].(map[string]any)
	if ct["Name"] != "c1" {
		t.Fatalf("describe %v", got)
	}
	call("PutContainerPolicy", `{"ContainerName":"c1","Policy":"{}"}`)
	pol := call("GetContainerPolicy", `{"ContainerName":"c1"}`)
	if pol["Policy"] != "{}" {
		t.Fatalf("policy %v", pol)
	}
	call("DeleteContainer", `{"ContainerName":"c1"}`)
	listed := call("ListContainers", `{}`)
	raw, _ := json.Marshal(listed)
	if strings.Contains(string(raw), `"Name":"c1"`) {
		t.Fatalf("still present %s", raw)
	}
}
