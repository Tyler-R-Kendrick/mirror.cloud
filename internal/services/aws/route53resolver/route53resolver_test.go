package route53resolver

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

func TestRoute53ResolverHTTPProvenOps(t *testing.T) {
	p := New(spitest.Deps(t))
	if n := len(p.Operations()); n != 8 {
		t.Fatalf("route53resolver Operations() %d want 8", n)
	}
}

func TestBootedServerRoute53ResolverCreateGetDelete(t *testing.T) {
	cfg := config.Default()
	cfg.Services = []string{"aws.route53resolver"}
	cfg.Seed = "r53r-1"
	rt, err := rtpkg.Boot(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(rt.Handler())
	defer ts.Close()
	auth := "AWS4-HMAC-SHA256 Credential=test/20200101/us-east-1/route53resolver/aws4_request, SignedHeaders=host, Signature=00"
	call := func(op, body string) map[string]any {
		t.Helper()
		req, _ := http.NewRequest(http.MethodPost, ts.URL+"/", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/x-amz-json-1.1")
		req.Header.Set("X-Amz-Target", "Route53Resolver."+op)
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
	created := call("CreateResolverEndpoint", `{"Name":"ep1","Direction":"INBOUND"}`)
	ep, _ := created["ResolverEndpoint"].(map[string]any)
	id, _ := ep["Id"].(string)
	if id == "" {
		t.Fatalf("create %v", created)
	}
	got := call("GetResolverEndpoint", `{"ResolverEndpointId":"`+id+`"}`)
	if got["ResolverEndpoint"] == nil {
		t.Fatalf("get %v", got)
	}
	rule := call("CreateResolverRule", `{"Name":"rr1","DomainName":"example.com","ResolverEndpointId":"`+id+`"}`)
	if rule["ResolverRule"] == nil {
		t.Fatalf("rule %v", rule)
	}
	call("DeleteResolverEndpoint", `{"ResolverEndpointId":"`+id+`"}`)
	listed := call("ListResolverEndpoints", `{}`)
	raw, _ := json.Marshal(listed)
	if strings.Contains(string(raw), `"`+id+`"`) {
		t.Fatalf("still present %s", raw)
	}
}
