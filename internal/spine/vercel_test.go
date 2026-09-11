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

	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/vercel/api"
)

func TestBootedServerVercelAPI(t *testing.T) {
	cfg := config.Default()
	cfg.Services = []string{"vercel.api"}
	cfg.Seed = "vercel-1"
	rt, err := rtpkg.Boot(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(rt.Handler())
	defer ts.Close()
	do := func(method, path, body, host string) (int, map[string]any) {
		t.Helper()
		var rdr io.Reader
		if body != "" {
			rdr = strings.NewReader(body)
		}
		req, err := http.NewRequest(method, ts.URL+path, rdr)
		if err != nil {
			t.Fatal(err)
		}
		if host != "" {
			req.Host = host
		}
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
		return res.StatusCode, m
	}
	code, user := do(http.MethodGet, "/v2/user", "", "")
	if code != 200 || user["username"] != "test" {
		t.Fatalf("user %d %#v", code, user)
	}
	code, prj := do(http.MethodPost, "/v11/projects", `{"name":"app"}`, "")
	if code != 200 || prj["name"] != "app" {
		t.Fatalf("create %d %#v", code, prj)
	}
	code, got := do(http.MethodGet, "/v9/projects/app", "", "")
	if code != 200 || got["id"] != prj["id"] {
		t.Fatalf("get %d %#v", code, got)
	}
	code, dpl := do(http.MethodPost, "/v13/deployments", `{"name":"app","project":"app"}`, "")
	if code != 200 || dpl["readyState"] != "READY" {
		t.Fatalf("deploy %d %#v", code, dpl)
	}
	code, kv := do(http.MethodPost, "/", `["SET","k","v"]`, "id.kv.vercel-storage.com")
	if code != 200 || kv["result"] != "OK" {
		t.Fatalf("kv set %d %#v", code, kv)
	}
	code, kv = do(http.MethodPost, "/", `["GET","k"]`, "id.kv.vercel-storage.com")
	if code != 200 || kv["result"] != "v" {
		t.Fatalf("kv get %d %#v", code, kv)
	}
	code, kv = do(http.MethodPost, "/", `["DEL","k"]`, "id.kv.vercel-storage.com")
	if code != 200 || kv["result"] != float64(1) {
		t.Fatalf("kv del %d %#v", code, kv)
	}
	code, kv = do(http.MethodPost, "/", `["GET","k"]`, "id.kv.vercel-storage.com")
	if code != 200 || kv["result"] != nil {
		t.Fatalf("kv get after del %d %#v", code, kv)
	}
	code, missing := do(http.MethodGet, "/v9/projects/missing", "", "")
	if code != 404 {
		t.Fatalf("missing %d %#v", code, missing)
	}
	errObj, _ := missing["error"].(map[string]any)
	if errObj["code"] != "not_found" {
		t.Fatalf("error shape %#v", missing)
	}
	code, missing = do(http.MethodDelete, "/v9/projects/missing", "", "")
	if code != 404 {
		t.Fatalf("delete missing %d %#v", code, missing)
	}
	errObj, _ = missing["error"].(map[string]any)
	if errObj["code"] != "not_found" {
		t.Fatalf("delete missing shape %#v", missing)
	}
}
