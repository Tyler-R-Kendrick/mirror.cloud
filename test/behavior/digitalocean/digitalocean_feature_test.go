package digitalocean

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
	// for digitalocean.v2 and the edge answers from the mock tier -- which
	// looks like a working service returning synthesized data, not like a
	// failure.
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/bundled"
)

// TestDigitalOceanV2Behavior drives the served service over its real HTTP
// surface. It boots the runtime rather than constructing an edge directly,
// because a bundle is served from the generated model and `edge.New` falls
// back to the hand-authored catalog when none is supplied.
//
// The URIs are unchanged from the pack this replaced. What changed is the
// names of all eight operations -- the document spells them DropletsCreate and
// DomainsGet, not CreateDroplet and GetDomain -- and the success codes: a
// droplet create is accepted, a domain create is created. Both are recorded as
// quirks in the bundle.
func TestDigitalOceanV2Behavior(t *testing.T) {
	cfg := config.Default()
	cfg.Services = []string{"digitalocean.v2"}
	cfg.Seed = "do-bdd"
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
		req.Host = "api.digitalocean.com"
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
	t.Run("Given a droplet When created Then it is listed and fetched", func(t *testing.T) {
		code, raw, _ := call(http.MethodPost, "/v2/droplets", `{"name":"bdd","region":"nyc3"}`)
		env := map[string]any{}
		_ = json.Unmarshal(raw, &env)
		if code != 202 || env["droplet"] == nil {
			t.Fatalf("create %d %s", code, raw)
		}
		code, raw, _ = call(http.MethodGet, "/v2/droplets", "")
		if code != 200 || !strings.Contains(string(raw), `"droplets"`) {
			t.Fatalf("list %d %s", code, raw)
		}
	})
	t.Run("Given a domain When created Then GET returns it", func(t *testing.T) {
		code, raw, _ := call(http.MethodPost, "/v2/domains", `{"name":"bdd.test"}`)
		env := map[string]any{}
		_ = json.Unmarshal(raw, &env)
		if code != 201 || env["domain"] == nil {
			t.Fatalf("create %d %s", code, raw)
		}
		code, raw, _ = call(http.MethodGet, "/v2/domains/bdd.test", "")
		_ = json.Unmarshal(raw, &env)
		if code != 200 || env["domain"].(map[string]any)["name"] != "bdd.test" {
			t.Fatalf("get %d %s", code, raw)
		}
	})
	t.Run("Given a duplicate domain When created Then 409 is returned", func(t *testing.T) {
		code, raw, _ := call(http.MethodPost, "/v2/domains", `{"name":"bdd.test"}`)
		if code != 409 {
			t.Fatalf("dup %d %s", code, raw)
		}
	})
	t.Run("Given a missing domain When fetched Then id not_found without AWS headers", func(t *testing.T) {
		code, raw, hdr := call(http.MethodGet, "/v2/domains/nope.test", "")
		miss := map[string]any{}
		_ = json.Unmarshal(raw, &miss)
		if code != 404 || miss["id"] != "not_found" || hdr.Get("x-amzn-errortype") != "" {
			t.Fatalf("missing %d %#v %s", code, hdr, raw)
		}
	})
	t.Run("Given a missing droplet or domain When deleted Then 404 not 204", func(t *testing.T) {
		code, raw, hdr := call(http.MethodDelete, "/v2/droplets/missing", "")
		miss := map[string]any{}
		_ = json.Unmarshal(raw, &miss)
		if code != 404 || miss["id"] != "not_found" || hdr.Get("x-amzn-errortype") != "" {
			t.Fatalf("delete missing droplet %d %#v %s", code, hdr, raw)
		}
		code, raw, hdr = call(http.MethodDelete, "/v2/domains/missing.test", "")
		_ = json.Unmarshal(raw, &miss)
		if code != 404 || miss["id"] != "not_found" || hdr.Get("x-amzn-errortype") != "" {
			t.Fatalf("delete missing domain %d %#v %s", code, hdr, raw)
		}
	})
}
