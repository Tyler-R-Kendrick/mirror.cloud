package hetzner

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
	// for hetzner.v1 and the edge answers from the mock tier -- which looks
	// like a working service returning synthesized data, not like a failure.
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/bundled"
)

// TestHetznerV1Behavior drives the served service over its real HTTP surface.
// It boots the runtime rather than constructing an edge directly, because a
// bundle is served from the generated model and `edge.New` falls back to the
// hand-authored catalog when none is supplied.
//
// The URIs are unchanged from the pack this replaced. What changed is the
// names of two of the four SSH-key operations -- the document spells them
// create_ssh_key, not CreateSSHKey -- and three answers: a create is 201, a
// key delete is 204, and a create carries the action hcloud clients wait on.
// All three are recorded as quirks in the bundle.
func TestHetznerV1Behavior(t *testing.T) {
	cfg := config.Default()
	cfg.Services = []string{"hetzner.v1"}
	cfg.Seed = "hetzner-bdd"
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

	t.Run("Given a server When created Then 201 with an action and it is listed", func(t *testing.T) {
		code, raw, _ := call(http.MethodPost, "/v1/servers",
			`{"name":"bdd","server_type":"cx22","image":"ubuntu-24.04"}`)
		env := map[string]any{}
		_ = json.Unmarshal(raw, &env)
		srv, _ := env["server"].(map[string]any)
		action, _ := env["action"].(map[string]any)
		if code != 201 || srv["name"] != "bdd" || action["command"] != "create_server" {
			t.Fatalf("create %d %s", code, raw)
		}
		code, raw, _ = call(http.MethodGet, "/v1/servers", "")
		_ = json.Unmarshal(raw, &env)
		list, _ := env["servers"].([]any)
		meta, _ := env["meta"].(map[string]any)
		page, _ := meta["pagination"].(map[string]any)
		if code != 200 || len(list) != 1 || page["total_entries"] != float64(1) {
			t.Fatalf("list %d %s", code, raw)
		}
	})

	t.Run("Given a request missing a required member When created Then 400 invalid_input", func(t *testing.T) {
		// The pack required only `name`. The document requires server_type and
		// image as well, and the engine checks the model before any rule runs.
		code, raw, _ := call(http.MethodPost, "/v1/servers", `{"name":"partial"}`)
		if code != 400 || !strings.Contains(string(raw), "invalid_input") {
			t.Fatalf("partial %d %s", code, raw)
		}
	})

	t.Run("Given a duplicate server name When created Then 409 uniqueness_error", func(t *testing.T) {
		code, raw, _ := call(http.MethodPost, "/v1/servers",
			`{"name":"bdd","server_type":"cx22","image":"ubuntu-24.04"}`)
		if code != 409 || !strings.Contains(string(raw), "uniqueness_error") {
			t.Fatalf("dup %d %s", code, raw)
		}
	})

	t.Run("Given an SSH key When created Then 201 and DELETE answers 204", func(t *testing.T) {
		code, raw, _ := call(http.MethodPost, "/v1/ssh_keys",
			`{"name":"laptop","public_key":"ssh-ed25519 AAAA"}`)
		env := map[string]any{}
		_ = json.Unmarshal(raw, &env)
		key, _ := env["ssh_key"].(map[string]any)
		if code != 201 || key["name"] != "laptop" {
			t.Fatalf("create %d %s", code, raw)
		}
		id, _ := key["id"].(float64)
		path := "/v1/ssh_keys/" + itoa(int64(id))
		code, raw, _ = call(http.MethodDelete, path, "")
		if code != 204 || len(raw) != 0 {
			t.Fatalf("delete %d %q", code, raw)
		}
		code, raw, _ = call(http.MethodGet, path, "")
		if code != 404 {
			t.Fatalf("get after delete %d %s", code, raw)
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

func itoa(n int64) string {
	if n == 0 {
		return "0"
	}
	var b []byte
	for n > 0 {
		b = append([]byte{byte('0' + n%10)}, b...)
		n /= 10
	}
	return string(b)
}
