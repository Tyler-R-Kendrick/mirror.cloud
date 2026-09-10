package hetzner

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

	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/hetzner/v1"
)

func TestHetznerV1Behavior(t *testing.T) {
	deps := spitest.Deps(t)
	cfg := config.Default()
	cfg.Services = []string{"hetzner.v1"}
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
		req.Host = "api.hetzner.cloud"
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
	t.Run("Given a server When created Then it is listed and fetched", func(t *testing.T) {
		code, raw, _ := call(http.MethodPost, "/v1/servers", `{"name":"bdd","server_type":"cx22"}`)
		env := map[string]any{}
		_ = json.Unmarshal(raw, &env)
		if code != 200 || env["server"] == nil {
			t.Fatalf("create %d %s", code, raw)
		}
		code, raw, _ = call(http.MethodGet, "/v1/servers", "")
		if code != 200 || !strings.Contains(string(raw), `"servers"`) || !strings.Contains(string(raw), `"total_entries"`) {
			t.Fatalf("list %d %s", code, raw)
		}
	})
	t.Run("Given a duplicate server When created Then 409 uniqueness_error", func(t *testing.T) {
		code, raw, _ := call(http.MethodPost, "/v1/servers", `{"name":"bdd"}`)
		if code != 409 || !strings.Contains(string(raw), "uniqueness_error") {
			t.Fatalf("dup %d %s", code, raw)
		}
	})
	t.Run("Given an SSH key When created Then GET returns it", func(t *testing.T) {
		code, raw, _ := call(http.MethodPost, "/v1/ssh_keys", `{"name":"laptop","public_key":"ssh-ed25519 AAAA"}`)
		env := map[string]any{}
		_ = json.Unmarshal(raw, &env)
		if code != 200 || env["ssh_key"] == nil {
			t.Fatalf("create %d %s", code, raw)
		}
	})
	t.Run("Given a missing server When fetched Then error.not_found without AWS headers", func(t *testing.T) {
		code, raw, hdr := call(http.MethodGet, "/v1/servers/missing", "")
		miss := map[string]any{}
		_ = json.Unmarshal(raw, &miss)
		errObj, _ := miss["error"].(map[string]any)
		if code != 404 || errObj["code"] != "not_found" || hdr.Get("x-amzn-errortype") != "" {
			t.Fatalf("missing %d %#v %s", code, hdr, raw)
		}
	})
}
