package appconfig

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

func TestAppConfigHTTPProvenOps(t *testing.T) {
	p := New(spitest.Deps(t))
	if n := len(p.Operations()); n != 18 {
		t.Fatalf("appconfig Operations() %d want 18", n)
	}
}

func TestBootedServerAppConfigAppAndConfig(t *testing.T) {
	cfg := config.Default()
	cfg.Services = []string{"aws.appconfig"}
	cfg.Seed = "ac-1"
	rt, err := rtpkg.Boot(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(rt.Handler())
	defer ts.Close()
	auth := "AWS4-HMAC-SHA256 Credential=test/20200101/us-east-1/appconfig/aws4_request, SignedHeaders=host, Signature=00"
	call := func(op, body string) map[string]any {
		t.Helper()
		req, _ := http.NewRequest(http.MethodPost, ts.URL+"/", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/x-amz-json-1.1")
		req.Header.Set("X-Amz-Target", "AmazonAppConfig."+op)
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
	app := call("CreateApplication", `{"Name":"a1"}`)
	aid, _ := app["Id"].(string)
	if aid == "" {
		t.Fatalf("app %v", app)
	}
	got := call("GetApplication", `{"ApplicationId":"`+aid+`"}`)
	if got["Name"] != "a1" {
		t.Fatalf("get %v", got)
	}
	prof := call("CreateConfigurationProfile", `{"ApplicationId":"`+aid+`","Name":"p1","LocationUri":"hosted"}`)
	pid, _ := prof["Id"].(string)
	ver := call("CreateHostedConfigurationVersion", `{"ApplicationId":"`+aid+`","ConfigurationProfileId":"`+pid+`","Content":"hello=1","ContentType":"text/plain"}`)
	if ver["Content"] != "hello=1" {
		t.Fatalf("ver %v", ver)
	}
	latest := call("GetLatestConfiguration", `{"ApplicationId":"`+aid+`","ConfigurationProfileId":"`+pid+`"}`)
	if latest["Content"] != "hello=1" {
		t.Fatalf("latest %v", latest)
	}
	call("DeleteApplication", `{"ApplicationId":"`+aid+`"}`)
	gone := call("ListApplications", `{}`)
	raw, _ := json.Marshal(gone)
	if strings.Contains(string(raw), `"Name":"a1"`) {
		t.Fatalf("still present %s", raw)
	}
}
