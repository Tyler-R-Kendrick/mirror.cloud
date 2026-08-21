package cognitoidentity

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

func TestCognitoIdentityHTTPProvenOps(t *testing.T) {
	p := New(spitest.Deps(t))
	if n := len(p.Operations()); n != 10 {
		t.Fatalf("cognito-identity Operations() %d want 10", n)
	}
}

func TestBootedServerCognitoIdentityPool(t *testing.T) {
	cfg := config.Default()
	cfg.Services = []string{"aws.cognito-identity"}
	cfg.Seed = "cid-1"
	rt, err := rtpkg.Boot(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(rt.Handler())
	defer ts.Close()
	auth := "AWS4-HMAC-SHA256 Credential=test/20200101/us-east-1/cognito-identity/aws4_request, SignedHeaders=host, Signature=00"
	call := func(op, body string) map[string]any {
		t.Helper()
		req, _ := http.NewRequest(http.MethodPost, ts.URL+"/", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/x-amz-json-1.1")
		req.Header.Set("X-Amz-Target", "AWSCognitoIdentityService."+op)
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
	created := call("CreateIdentityPool", `{"IdentityPoolName":"p1","AllowUnauthenticatedIdentities":true}`)
	id, _ := created["IdentityPoolId"].(string)
	if id == "" {
		t.Fatalf("create %v", created)
	}
	got := call("DescribeIdentityPool", `{"IdentityPoolId":"`+id+`"}`)
	if got["IdentityPoolName"] != "p1" {
		t.Fatalf("describe %v", got)
	}
	gid := call("GetId", `{"IdentityPoolId":"`+id+`"}`)
	if gid["IdentityId"] == nil {
		t.Fatalf("getid %v", gid)
	}
	listed := call("ListIdentityPools", `{"MaxResults":10}`)
	raw, _ := json.Marshal(listed)
	if !strings.Contains(string(raw), "p1") {
		t.Fatalf("list %s", raw)
	}
	call("DeleteIdentityPool", `{"IdentityPoolId":"`+id+`"}`)
	gone := call("ListIdentityPools", `{"MaxResults":10}`)
	raw, _ = json.Marshal(gone)
	if strings.Contains(string(raw), `"IdentityPoolName":"p1"`) {
		t.Fatalf("still present %s", raw)
	}
}
