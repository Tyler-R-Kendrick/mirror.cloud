package spine

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/config"
	rtpkg "github.com/tyler-r-kendrick/mirror.cloud/internal/runtime"

	// Served from behavior/cloudflare/api since the pack was deleted; this
	// import is what registers it.
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/bundled"
)

func TestBootedServerCloudflareAPI(t *testing.T) {
	cfg := config.Default()
	cfg.Services = []string{"cloudflare.api"}
	cfg.Seed = "cf-1"
	rt, err := rtpkg.Boot(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(rt.Handler())
	defer ts.Close()
	do := func(method, path, body string) (int, []byte, http.Header) {
		t.Helper()
		var rdr io.Reader
		if body != "" {
			rdr = strings.NewReader(body)
		}
		req, err := http.NewRequest(method, ts.URL+path, rdr)
		if err != nil {
			t.Fatal(err)
		}
		req.Host = "api.cloudflare.com"
		req.Header.Set("Authorization", "Bearer test")
		req.Header.Set("Content-Type", "application/json")
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		b, _ := io.ReadAll(res.Body)
		res.Body.Close()
		return res.StatusCode, b, res.Header
	}
	code, raw, hdr := do(http.MethodPost, "/client/v4/accounts/acct1/storage/kv/namespaces", `{"title":"ns"}`)
	env := map[string]any{}
	_ = json.Unmarshal(raw, &env)
	if code != 200 || env["success"] != true {
		t.Fatalf("create %d %s", code, raw)
	}
	result, _ := env["result"].(map[string]any)
	nid, _ := result["id"].(string)
	if nid == "" {
		t.Fatalf("id %s", raw)
	}
	code, raw, hdr = do(http.MethodPut, "/client/v4/accounts/acct1/storage/kv/namespaces/"+nid+"/values/k", "hello")
	_ = json.Unmarshal(raw, &env)
	if code != 200 || env["success"] != true {
		t.Fatalf("put %d %s", code, raw)
	}
	code, raw, hdr = do(http.MethodGet, "/client/v4/accounts/acct1/storage/kv/namespaces/"+nid+"/values/k", "")
	if code != 200 || string(raw) != "hello" {
		t.Fatalf("get value %d %q", code, raw)
	}
	code, _, _ = do(http.MethodDelete, "/client/v4/accounts/acct1/storage/kv/namespaces/"+nid+"/values/k", "")
	if code != 200 {
		t.Fatalf("del %d", code)
	}
	code, _, hdr = do(http.MethodGet, "/client/v4/accounts/acct1/storage/kv/namespaces/"+nid+"/values/k", "")
	if code != 404 {
		t.Fatalf("get after del %d", code)
	}
	if hdr.Get("x-amzn-errortype") != "" {
		t.Fatalf("aws fault header %q", hdr.Get("x-amzn-errortype"))
	}
	code, raw, hdr = do(http.MethodGet, "/client/v4/accounts/acct1/storage/kv/namespaces/nope", "")
	_ = json.Unmarshal(raw, &env)
	if code != 404 || env["success"] != false {
		t.Fatalf("missing ns %d %s", code, raw)
	}
	errs, _ := env["errors"].([]any)
	if len(errs) == 0 {
		t.Fatalf("errors %s", raw)
	}
	err0, _ := errs[0].(map[string]any)
	if _, ok := err0["code"].(float64); !ok {
		t.Fatalf("numeric code %#v", err0)
	}
	if hdr.Get("x-amzn-errortype") != "" {
		t.Fatalf("aws header on missing ns")
	}
}
