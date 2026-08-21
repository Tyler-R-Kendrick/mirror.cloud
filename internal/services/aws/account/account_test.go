package account

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

func TestAccountHTTPProvenOps(t *testing.T) {
	p := New(spitest.Deps(t))
	if n := len(p.Operations()); n != 6 {
		t.Fatalf("account Operations() %d want 6", n)
	}
}

func TestBootedServerAccountCreateGetDelete(t *testing.T) {
	cfg := config.Default()
	cfg.Services = []string{"aws.account"}
	cfg.Seed = "acct-1"
	rt, err := rtpkg.Boot(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(rt.Handler())
	defer ts.Close()
	auth := "AWS4-HMAC-SHA256 Credential=test/20200101/us-east-1/account/aws4_request, SignedHeaders=host, Signature=00"
	call := func(op, body string) (map[string]any, int) {
		t.Helper()
		req, _ := http.NewRequest(http.MethodPost, ts.URL+"/", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/x-amz-json-1.1")
		req.Header.Set("X-Amz-Target", "Account."+op)
		req.Header.Set("Authorization", auth)
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		raw, _ := io.ReadAll(res.Body)
		res.Body.Close()
		if res.Header.Get("x-mirror-fidelity") != "emulate" {
			t.Fatalf("fidelity %q", res.Header.Get("x-mirror-fidelity"))
		}
		out := map[string]any{}
		_ = json.Unmarshal(raw, &out)
		if res.StatusCode >= 300 && op != "GetAlternateContact" {
			t.Fatalf("%s %d %s", op, res.StatusCode, raw)
		}
		return out, res.StatusCode
	}
	_, code := call("PutAlternateContact", `{"AlternateContactType":"BILLING","EmailAddress":"b@example.com","Name":"B","PhoneNumber":"1","Title":"T"}`)
	if code >= 300 {
		t.Fatalf("put %d", code)
	}
	got, code := call("GetAlternateContact", `{"AlternateContactType":"BILLING"}`)
	if code >= 300 || got["AlternateContact"] == nil {
		t.Fatalf("get %d %v", code, got)
	}
	call("DeleteAlternateContact", `{"AlternateContactType":"BILLING"}`)
	_, code = call("GetAlternateContact", `{"AlternateContactType":"BILLING"}`)
	if code < 300 {
		t.Fatalf("still present %d", code)
	}
}
