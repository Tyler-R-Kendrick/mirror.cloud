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

	// Links the bundle's registration in. Without it the registry has no pack
	// for fly.machines and the edge answers from the mock tier -- which looks
	// like a working service returning synthesized data, not like a failure.
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/bundled"
)

func TestBootedServerFlyMachines(t *testing.T) {
	cfg := config.Default()
	cfg.Services = []string{"fly.machines"}
	cfg.Seed = "fly-1"
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
		req.Host = "api.machines.dev"
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
	code, raw, _ := do(http.MethodPost, "/v1/apps", `{"app_name":"web","org_slug":"personal"}`)
	env := map[string]any{}
	_ = json.Unmarshal(raw, &env)
	if code != 201 || env["id"] == nil {
		t.Fatalf("create app %d %s", code, raw)
	}
	code, raw, _ = do(http.MethodGet, "/v1/apps?org_slug=personal", "")
	if code != 200 || !strings.Contains(string(raw), `"apps"`) || !strings.Contains(string(raw), `"total_apps"`) {
		t.Fatalf("list apps %d %s", code, raw)
	}
	code, raw, _ = do(http.MethodGet, "/v1/apps/web", "")
	_ = json.Unmarshal(raw, &env)
	if code != 200 || env["name"] != "web" {
		t.Fatalf("get app %d %s", code, raw)
	}
	code, raw, _ = do(http.MethodPost, "/v1/apps/web/machines", `{"config":{"image":"nginx"}}`)
	_ = json.Unmarshal(raw, &env)
	mid, _ := env["id"].(string)
	if code != 200 || mid == "" {
		t.Fatalf("create machine %d %s", code, raw)
	}
	code, raw, _ = do(http.MethodGet, "/v1/apps/web/machines/"+mid, "")
	_ = json.Unmarshal(raw, &env)
	if code != 200 || env["id"] != mid {
		t.Fatalf("get machine %d %s", code, raw)
	}
	code, raw, h := do(http.MethodGet, "/v1/apps/missing", "")
	miss := map[string]any{}
	_ = json.Unmarshal(raw, &miss)
	if code != 404 || miss["error"] == nil || h.Get("x-amzn-errortype") != "" {
		t.Fatalf("missing app %d %#v %s", code, h, raw)
	}
	code, raw, h = do(http.MethodDelete, "/v1/apps/missing", "")
	_ = json.Unmarshal(raw, &miss)
	if code != 404 || miss["error"] == nil || h.Get("x-amzn-errortype") != "" {
		t.Fatalf("delete missing app %d %#v %s", code, h, raw)
	}
	code, raw, h = do(http.MethodDelete, "/v1/apps/web/machines/missing", "")
	_ = json.Unmarshal(raw, &miss)
	if code != 404 || miss["error"] == nil || h.Get("x-amzn-errortype") != "" {
		t.Fatalf("delete missing machine %d %#v %s", code, h, raw)
	}
}
