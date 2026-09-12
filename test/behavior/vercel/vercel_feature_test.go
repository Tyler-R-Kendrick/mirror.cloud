package vercel

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

	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/bundled"
)

func TestVercelProjectDeployKVBehavior(t *testing.T) {
	deps := spitest.Deps(t)
	cfg := config.Default()
	cfg.Services = []string{"vercel.api", "vercel.kv"}
	reg, err := registry.New(deps, cfg.Services, nil)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(edge.New(cfg, deps, reg, "test").Handler())
	defer ts.Close()
	call := func(method, path, body, host string) (int, map[string]any, http.Header) {
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

	t.Run("Given a project name When created Then it is listed and fetched by name", func(t *testing.T) {
		code, created, _ := call(http.MethodPost, "/v11/projects", `{"name":"bdd-app"}`, "")
		if code != 200 || created["name"] != "bdd-app" {
			t.Fatalf("create %d %#v", code, created)
		}
		code, listed, _ := call(http.MethodGet, "/v10/projects", "", "")
		// The document lists projects at /v10; the pack served /v9, a version
		// the document does not.
		if code != 200 || len(listed["projects"].([]any)) != 1 {
			t.Fatalf("list %d %#v", code, listed)
		}
		code, got, _ := call(http.MethodGet, "/v9/projects/bdd-app", "", "")
		if code != 200 || got["id"] != created["id"] {
			t.Fatalf("get %d %#v", code, got)
		}
	})
	t.Run("Given a duplicate project name When created Then conflict is returned", func(t *testing.T) {
		code, body, _ := call(http.MethodPost, "/v11/projects", `{"name":"bdd-app"}`, "")
		if code != 409 {
			t.Fatalf("dup %d %#v", code, body)
		}
	})
	t.Run("Given a project When deployed Then readyState is READY", func(t *testing.T) {
		code, dpl, _ := call(http.MethodPost, "/v13/deployments", `{"name":"bdd-app","project":"bdd-app"}`, "")
		if code != 200 || dpl["readyState"] != "READY" || dpl["url"] == nil {
			t.Fatalf("deploy %d %#v", code, dpl)
		}
	})
	t.Run("Given KV SET When GET Then the value is returned", func(t *testing.T) {
		code, set, _ := call(http.MethodPost, "/", `["SET","k","v"]`, "kv.vercel-storage.com")
		if code != 200 || set["result"] != "OK" {
			t.Fatalf("set %d %#v", code, set)
		}
		code, get, _ := call(http.MethodPost, "/", `["GET","k"]`, "kv.vercel-storage.com")
		if code != 200 || get["result"] != "v" {
			t.Fatalf("get %d %#v", code, get)
		}
		code, del, _ := call(http.MethodPost, "/", `["DEL","k"]`, "kv.vercel-storage.com")
		if code != 200 || del["result"] != float64(1) {
			t.Fatalf("del %d %#v", code, del)
		}
		code, gone, _ := call(http.MethodPost, "/", `["GET","k"]`, "kv.vercel-storage.com")
		if code != 200 || gone["result"] != nil {
			t.Fatalf("get after del %d %#v", code, gone)
		}
	})
	t.Run("Given a missing project When fetched Then not_found is returned", func(t *testing.T) {
		code, body, hdr := call(http.MethodGet, "/v9/projects/nope", "", "")
		if code != 404 || hdr.Get("x-amzn-errortype") != "" {
			t.Fatalf("missing %d %#v %#v", code, hdr, body)
		}
		errObj, _ := body["error"].(map[string]any)
		if errObj["code"] != "not_found" {
			t.Fatalf("shape %#v", body)
		}
	})
	t.Run("Given a missing project When deleted Then not_found is returned", func(t *testing.T) {
		code, body, hdr := call(http.MethodDelete, "/v9/projects/nope", "", "")
		if code != 404 || hdr.Get("x-amzn-errortype") != "" {
			t.Fatalf("delete missing %d %#v %#v", code, hdr, body)
		}
		errObj, _ := body["error"].(map[string]any)
		if errObj["code"] != "not_found" {
			t.Fatalf("delete missing shape %#v", body)
		}
	})
}
