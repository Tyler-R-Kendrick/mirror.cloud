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

	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/hetzner/v1"
)

func TestBootedServerHetznerAPI(t *testing.T) {
	cfg := config.Default()
	cfg.Services = []string{"hetzner.v1"}
	cfg.Seed = "hz-1"
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
	code, raw, _ := do(http.MethodPost, "/v1/servers", `{"name":"web","server_type":"cx22","image":"ubuntu-24.04"}`)
	env := map[string]any{}
	_ = json.Unmarshal(raw, &env)
	srv, _ := env["server"].(map[string]any)
	if code != 200 || srv["name"] != "web" {
		t.Fatalf("create server %d %s", code, raw)
	}
	id := strconv.Itoa(int(srv["id"].(float64)))
	code, raw, _ = do(http.MethodGet, "/v1/servers", "")
	if code != 200 || !strings.Contains(string(raw), `"servers"`) || !strings.Contains(string(raw), `"total_entries"`) {
		t.Fatalf("list servers %d %s", code, raw)
	}
	code, raw, _ = do(http.MethodGet, "/v1/servers/"+id, "")
	_ = json.Unmarshal(raw, &env)
	if code != 200 || env["server"] == nil {
		t.Fatalf("get server %d %s", code, raw)
	}
	code, raw, _ = do(http.MethodPost, "/v1/ssh_keys", `{"name":"laptop","public_key":"ssh-ed25519 AAAA"}`)
	_ = json.Unmarshal(raw, &env)
	key, _ := env["ssh_key"].(map[string]any)
	if code != 200 || key["name"] != "laptop" {
		t.Fatalf("create ssh %d %s", code, raw)
	}
	kid := strconv.Itoa(int(key["id"].(float64)))
	code, raw, _ = do(http.MethodGet, "/v1/ssh_keys/"+kid, "")
	_ = json.Unmarshal(raw, &env)
	if code != 200 || env["ssh_key"].(map[string]any)["name"] != "laptop" {
		t.Fatalf("get ssh %d %s", code, raw)
	}
	code, raw, h := do(http.MethodGet, "/v1/servers/missing", "")
	miss := map[string]any{}
	_ = json.Unmarshal(raw, &miss)
	errObj, _ := miss["error"].(map[string]any)
	if code != 404 || errObj["code"] != "not_found" || h.Get("x-amzn-errortype") != "" {
		t.Fatalf("missing server %d %#v %s", code, h, raw)
	}
	code, raw, h = do(http.MethodGet, "/v1/ssh_keys/missing", "")
	_ = json.Unmarshal(raw, &miss)
	errObj, _ = miss["error"].(map[string]any)
	if code != 404 || errObj["code"] != "not_found" || h.Get("x-amzn-errortype") != "" {
		t.Fatalf("missing ssh %d %#v %s", code, h, raw)
	}
}
