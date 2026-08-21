package servicediscovery

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

func TestServiceDiscoveryHTTPProvenOps(t *testing.T) {
	p := New(spitest.Deps(t))
	if n := len(p.Operations()); n != 13 {
		t.Fatalf("servicediscovery Operations() %d want 13", n)
	}
}

func TestBootedServerServiceDiscoveryCreateGetDelete(t *testing.T) {
	cfg := config.Default()
	cfg.Services = []string{"aws.servicediscovery"}
	cfg.Seed = "sd-1"
	rt, err := rtpkg.Boot(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(rt.Handler())
	defer ts.Close()
	auth := "AWS4-HMAC-SHA256 Credential=test/20200101/us-east-1/servicediscovery/aws4_request, SignedHeaders=host, Signature=00"
	call := func(op, body string) map[string]any {
		t.Helper()
		req, _ := http.NewRequest(http.MethodPost, ts.URL+"/", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/x-amz-json-1.1")
		req.Header.Set("X-Amz-Target", "Route53AutoNaming_v20170314."+op)
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
	created := call("CreateHttpNamespace", `{"Name":"ns1"}`)
	ns, _ := created["Namespace"].(map[string]any)
	id, _ := ns["Id"].(string)
	if id == "" {
		t.Fatalf("create %v", created)
	}
	got := call("GetNamespace", `{"Id":"`+id+`"}`)
	if got["Namespace"] == nil {
		t.Fatalf("get %v", got)
	}
	svc := call("CreateService", `{"Name":"svc","NamespaceId":"`+id+`"}`)
	s, _ := svc["Service"].(map[string]any)
	sid, _ := s["Id"].(string)
	call("RegisterInstance", `{"ServiceId":"`+sid+`","InstanceId":"i1","Attributes":{"AWS_INSTANCE_IPV4":"10.0.0.1"}}`)
	inst := call("GetInstance", `{"ServiceId":"`+sid+`","InstanceId":"i1"}`)
	if inst["Instance"] == nil {
		t.Fatalf("inst %v", inst)
	}
	call("DeregisterInstance", `{"ServiceId":"`+sid+`","InstanceId":"i1"}`)
	call("DeleteService", `{"Id":"`+sid+`"}`)
	call("DeleteNamespace", `{"Id":"`+id+`"}`)
	listed := call("ListNamespaces", `{}`)
	raw, _ := json.Marshal(listed)
	if strings.Contains(string(raw), `"Id":"`+id+`"`) {
		t.Fatalf("still present %s", raw)
	}
}
