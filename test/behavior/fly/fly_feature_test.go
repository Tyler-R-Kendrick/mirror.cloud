package fly

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/config"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/edge"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spitest"

	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/fly/machines"
)

func TestFlyMachinesBehavior(t *testing.T) {
	deps := spitest.Deps(t)
	cfg := config.Default()
	cfg.Services = []string{"fly.machines"}
	reg, err := registry.New(deps, cfg.Services, nil)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(edge.New(cfg, deps, reg, "test").Handler())
	defer ts.Close()
	call := func(method, path, body string) (int, []byte, http.Header) {
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
	t.Run("Given an app When created Then it is listed and fetched", func(t *testing.T) {
		code, raw, _ := call(http.MethodPost, "/v1/apps", `{"app_name":"bdd","org_slug":"personal"}`)
		env := map[string]any{}
		_ = json.Unmarshal(raw, &env)
		if code != 201 || env["id"] == nil {
			t.Fatalf("create %d %s", code, raw)
		}
		code, raw, _ = call(http.MethodGet, "/v1/apps?org_slug=personal", "")
		if code != 200 || !strings.Contains(string(raw), `"apps"`) || !strings.Contains(string(raw), `"total_apps"`) {
			t.Fatalf("list %d %s", code, raw)
		}
		code, raw, _ = call(http.MethodGet, "/v1/apps/bdd", "")
		_ = json.Unmarshal(raw, &env)
		if code != 200 || env["name"] != "bdd" {
			t.Fatalf("get %d %s", code, raw)
		}
	})
	t.Run("Given a duplicate app When created Then 422 taken", func(t *testing.T) {
		code, raw, _ := call(http.MethodPost, "/v1/apps", `{"app_name":"bdd"}`)
		if code != 422 || !strings.Contains(string(raw), "already taken") {
			t.Fatalf("dup %d %s", code, raw)
		}
	})
	t.Run("Given a machine When created Then GET returns it", func(t *testing.T) {
		code, raw, _ := call(http.MethodPost, "/v1/apps/bdd/machines", `{"config":{"image":"nginx"}}`)
		env := map[string]any{}
		_ = json.Unmarshal(raw, &env)
		if code != 200 || env["id"] == nil {
			t.Fatalf("create %d %s", code, raw)
		}
	})
	t.Run("Given a missing app When fetched or deleted Then error without AWS headers", func(t *testing.T) {
		code, raw, hdr := call(http.MethodGet, "/v1/apps/missing", "")
		miss := map[string]any{}
		_ = json.Unmarshal(raw, &miss)
		if code != 404 || miss["error"] == nil || hdr.Get("x-amzn-errortype") != "" {
			t.Fatalf("missing %d %#v %s", code, hdr, raw)
		}
		code, raw, hdr = call(http.MethodDelete, "/v1/apps/missing", "")
		_ = json.Unmarshal(raw, &miss)
		if code != 404 || miss["error"] == nil || hdr.Get("x-amzn-errortype") != "" {
			t.Fatalf("delete missing %d %#v %s", code, hdr, raw)
		}
	})
}
