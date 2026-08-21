package apigatewayv2

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

func TestAPIGatewayV2HTTPProvenOps(t *testing.T) {
	p := New(spitest.Deps(t))
	if n := len(p.Operations()); n != 17 {
		t.Fatalf("apigatewayv2 Operations() %d want 17", n)
	}
}

func TestBootedServerAPIGatewayV2ApiRoute(t *testing.T) {
	cfg := config.Default()
	cfg.Services = []string{"aws.apigatewayv2"}
	cfg.Seed = "ag2-1"
	rt, err := rtpkg.Boot(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(rt.Handler())
	defer ts.Close()
	auth := "AWS4-HMAC-SHA256 Credential=test/20200101/us-east-1/apigateway/aws4_request, SignedHeaders=host, Signature=00"
	call := func(op, body string) map[string]any {
		t.Helper()
		req, _ := http.NewRequest(http.MethodPost, ts.URL+"/", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/x-amz-json-1.1")
		req.Header.Set("X-Amz-Target", "ApiGatewayV2."+op)
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
	created := call("CreateApi", `{"Name":"http1","ProtocolType":"HTTP"}`)
	id, _ := created["ApiId"].(string)
	if id == "" {
		t.Fatalf("create %v", created)
	}
	got := call("GetApi", `{"ApiId":"`+id+`"}`)
	if got["Name"] != "http1" {
		t.Fatalf("get %v", got)
	}
	rted := call("CreateRoute", `{"ApiId":"`+id+`","RouteKey":"GET /hello"}`)
	if rted["RouteId"] == nil {
		t.Fatalf("route %v", rted)
	}
	listed := call("GetApis", `{}`)
	raw, _ := json.Marshal(listed)
	if !strings.Contains(string(raw), id) {
		t.Fatalf("list %s", raw)
	}
	call("DeleteApi", `{"ApiId":"`+id+`"}`)
	gone := call("GetApis", `{}`)
	raw, _ = json.Marshal(gone)
	if strings.Contains(string(raw), `"`+id+`"`) {
		t.Fatalf("still present %s", raw)
	}
}
