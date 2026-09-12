package spine

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
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

func TestBootedServerDigitalOceanAPI(t *testing.T) {
	cfg := config.Default()
	cfg.Services = []string{"digitalocean.v2"}
	cfg.Seed = "do-1"
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
	code, raw, _ := do(http.MethodPost, "/v2/droplets", `{"name":"web","region":"nyc3","size":"s-1vcpu-1gb"}`)
	env := map[string]any{}
	_ = json.Unmarshal(raw, &env)
	drop, _ := env["droplet"].(map[string]any)
	// 202, not 200: the document says a droplet create is accepted rather than
	// completed, and the code is the model's now that the pack's is gone.
	if code != 202 || drop["name"] != "web" {
		t.Fatalf("create droplet %d %s", code, raw)
	}
	id := strconv.Itoa(int(drop["id"].(float64)))
	code, raw, _ = do(http.MethodGet, "/v2/droplets", "")
	if code != 200 || !strings.Contains(string(raw), `"droplets"`) || !strings.Contains(string(raw), `"total"`) {
		t.Fatalf("list droplets %d %s", code, raw)
	}
	code, raw, _ = do(http.MethodGet, "/v2/droplets/"+id, "")
	_ = json.Unmarshal(raw, &env)
	gotDrop, _ := env["droplet"].(map[string]any)
	if code != 200 || strconv.Itoa(int(gotDrop["id"].(float64))) != id {
		t.Fatalf("get droplet %d %s", code, raw)
	}
	code, raw, _ = do(http.MethodPost, "/v2/domains", `{"name":"boot.test"}`)
	_ = json.Unmarshal(raw, &env)
	dom, _ := env["domain"].(map[string]any)
	// 201: a domain create is created, where a droplet create is accepted.
	// The pack answered 200 for both.
	if code != 201 || dom["name"] != "boot.test" {
		t.Fatalf("create domain %d %s", code, raw)
	}
	code, raw, _ = do(http.MethodGet, "/v2/domains/boot.test", "")
	_ = json.Unmarshal(raw, &env)
	if code != 200 || env["domain"].(map[string]any)["name"] != "boot.test" {
		t.Fatalf("get domain %d %s", code, raw)
	}
	code, raw, _ = do(http.MethodDelete, "/v2/droplets/"+id, "")
	if code != 204 || len(raw) != 0 {
		t.Fatalf("delete droplet %d %q", code, raw)
	}
	code, raw, h := do(http.MethodGet, "/v2/droplets/"+id, "")
	miss := map[string]any{}
	_ = json.Unmarshal(raw, &miss)
	if code != 404 || miss["id"] != "not_found" || h.Get("x-amzn-errortype") != "" {
		t.Fatalf("missing droplet %d %#v %s", code, h, raw)
	}
	code, raw, h = do(http.MethodGet, "/v2/domains/missing.test", "")
	_ = json.Unmarshal(raw, &miss)
	if code != 404 || miss["id"] != "not_found" || h.Get("x-amzn-errortype") != "" {
		t.Fatalf("missing domain %d %#v %s", code, h, raw)
	}
	code, raw, h = do(http.MethodDelete, "/v2/droplets/missing", "")
	_ = json.Unmarshal(raw, &miss)
	if code != 404 || miss["id"] != "not_found" || h.Get("x-amzn-errortype") != "" {
		t.Fatalf("delete missing droplet %d %#v %s", code, h, raw)
	}
	code, raw, h = do(http.MethodDelete, "/v2/domains/missing.test", "")
	_ = json.Unmarshal(raw, &miss)
	if code != 404 || miss["id"] != "not_found" || h.Get("x-amzn-errortype") != "" {
		t.Fatalf("delete missing domain %d %#v %s", code, h, raw)
	}
}
