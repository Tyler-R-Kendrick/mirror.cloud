package fly

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

// TestFlyMachinesBehavior drives the served service over its real HTTP
// surface. It boots the runtime rather than constructing an edge directly,
// because a bundle is served from the generated model and `edge.New` falls
// back to the hand-authored catalog when none is supplied.
//
// The URIs are unchanged from the pack this replaced. What changed is the
// operation names -- the document calls them apps_create and machines_show --
// and the envelope: an App and a Machine are the body now, not something
// nested under `app` or `machine`. Both are recorded as quirks in the bundle.
func TestFlyMachinesBehavior(t *testing.T) {
	cfg := config.Default()
	cfg.Services = []string{"fly.machines"}
	cfg.Seed = "fly-bdd"
	rt, err := rtpkg.Boot(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(rt.Handler())
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
		code, raw, _ := call(http.MethodPost, "/v1/apps", `{"app_name":"bdd","org_slug":"personal"}`)
		if code != 422 || !strings.Contains(string(raw), "already taken") {
			t.Fatalf("dup %d %s", code, raw)
		}
	})
	t.Run("Given a machine When created Then GET returns it and the list holds it", func(t *testing.T) {
		code, raw, _ := call(http.MethodPost, "/v1/apps/bdd/machines", `{"config":{"image":"nginx"}}`)
		env := map[string]any{}
		_ = json.Unmarshal(raw, &env)
		mid, _ := env["id"].(string)
		if code != 200 || mid == "" || env["state"] != "created" {
			t.Fatalf("create %d %s", code, raw)
		}
		code, raw, _ = call(http.MethodGet, "/v1/apps/bdd/machines/"+mid, "")
		_ = json.Unmarshal(raw, &env)
		if code != 200 || env["id"] != mid {
			t.Fatalf("get %d %s", code, raw)
		}
		// A bare JSON array, which is what the document declares the machine
		// listing to be.
		code, raw, _ = call(http.MethodGet, "/v1/apps/bdd/machines", "")
		var list []any
		if err := json.Unmarshal(raw, &list); err != nil || code != 200 || len(list) != 1 {
			t.Fatalf("list %d %s", code, raw)
		}
	})
	t.Run("Given a machine with no image When created Then 400 invalid", func(t *testing.T) {
		code, raw, _ := call(http.MethodPost, "/v1/apps/bdd/machines", `{"region":"iad"}`)
		if code != 400 || !strings.Contains(string(raw), "image is required") {
			t.Fatalf("no image %d %s", code, raw)
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
