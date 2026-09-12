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

	// Links the bundles' registration in. Without it the registry has no pack
	// for vercel.api or vercel.kv and the edge answers from the mock tier --
	// which looks like a working service returning synthesized data, not like
	// a failure.
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/bundled"
)

func TestBootedServerVercelAPI(t *testing.T) {
	cfg := config.Default()
	cfg.Services = []string{"vercel.api", "vercel.kv"}
	cfg.Seed = "vercel-1"
	rt, err := rtpkg.Boot(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(rt.Handler())
	defer ts.Close()
	do := func(method, path, body, host string) (int, map[string]any, http.Header) {
		t.Helper()
		var rdr io.Reader
		if body != "" {
			rdr = strings.NewReader(body)
		}
		req, err := http.NewRequest(method, ts.URL+path, rdr)
		if err != nil {
			t.Fatal(err)
		}
		if host == "" {
			host = "api.vercel.com"
		}
		req.Host = host
		req.Header.Set("Authorization", "Bearer test")
		req.Header.Set("Content-Type", "application/json")
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		b, _ := io.ReadAll(res.Body)
		res.Body.Close()
		m := map[string]any{}
		_ = json.Unmarshal(b, &m)
		return res.StatusCode, m, res.Header
	}
	code, user, _ := do(http.MethodGet, "/v2/user", "", "")
	// The document wraps the user object in `user`; the pack answered the
	// members bare.
	u, _ := user["user"].(map[string]any)
	if code != 200 || u["username"] != "test" {
		t.Fatalf("user %d %#v", code, user)
	}
	code, prj, _ := do(http.MethodPost, "/v11/projects", `{"name":"app"}`, "")
	if code != 200 || prj["name"] != "app" {
		t.Fatalf("create %d %#v", code, prj)
	}
	code, got, _ := do(http.MethodGet, "/v9/projects/app", "", "")
	if code != 200 || got["id"] != prj["id"] {
		t.Fatalf("get %d %#v", code, got)
	}
	code, dpl, _ := do(http.MethodPost, "/v13/deployments", `{"name":"app","project":"app"}`, "")
	if code != 200 || dpl["readyState"] != "READY" {
		t.Fatalf("deploy %d %#v", code, dpl)
	}
	code, kv, _ := do(http.MethodPost, "/", `["SET","k","v"]`, "id.kv.vercel-storage.com")
	if code != 200 || kv["result"] != "OK" {
		t.Fatalf("kv set %d %#v", code, kv)
	}
	code, kv, _ = do(http.MethodPost, "/", `["GET","k"]`, "id.kv.vercel-storage.com")
	if code != 200 || kv["result"] != "v" {
		t.Fatalf("kv get %d %#v", code, kv)
	}
	code, kv, _ = do(http.MethodPost, "/", `["DEL","k"]`, "id.kv.vercel-storage.com")
	if code != 200 || kv["result"] != float64(1) {
		t.Fatalf("kv del %d %#v", code, kv)
	}
	code, kv, _ = do(http.MethodPost, "/", `["GET","k"]`, "id.kv.vercel-storage.com")
	if code != 200 || kv["result"] != nil {
		t.Fatalf("kv get after del %d %#v", code, kv)
	}
	code, missing, h := do(http.MethodGet, "/v9/projects/missing", "", "")
	if code != 404 || h.Get("x-amzn-errortype") != "" {
		t.Fatalf("missing %d %#v %s", code, h, missing)
	}
	errObj, _ := missing["error"].(map[string]any)
	if errObj["code"] != "not_found" {
		t.Fatalf("error shape %#v", missing)
	}
	code, missing, h = do(http.MethodDelete, "/v9/projects/missing", "", "")
	if code != 404 || h.Get("x-amzn-errortype") != "" {
		t.Fatalf("delete missing %d %#v %s", code, h, missing)
	}
	errObj, _ = missing["error"].(map[string]any)
	if errObj["code"] != "not_found" {
		t.Fatalf("delete missing shape %#v", missing)
	}
}
