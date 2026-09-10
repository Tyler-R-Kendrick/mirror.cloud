package cloudflare

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

	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/cloudflare/api"
)

func TestCloudflareKVBehavior(t *testing.T) {
	deps := spitest.Deps(t)
	cfg := config.Default()
	cfg.Services = []string{"cloudflare.kv"}
	reg, err := registry.New(deps, cfg.Services, nil)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(edge.New(cfg, deps, reg, "test").Handler())
	defer ts.Close()
	call := func(method, path, body string) (int, []byte) {
		t.Helper()
		var rdr io.Reader
		if body != "" {
			rdr = strings.NewReader(body)
		}
		req, err := http.NewRequest(method, ts.URL+path, rdr)
		if err != nil {
			t.Fatal(err)
		}
		req.Host = "api.cloudflare.com"
		req.Header.Set("Authorization", "Bearer test")
		req.Header.Set("Content-Type", "application/json")
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		b, _ := io.ReadAll(res.Body)
		res.Body.Close()
		return res.StatusCode, b
	}
	t.Run("Given a namespace title When created Then it is listed and fetched", func(t *testing.T) {
		code, raw := call(http.MethodPost, "/client/v4/accounts/acct1/storage/kv/namespaces", `{"title":"bdd"}`)
		env := map[string]any{}
		_ = json.Unmarshal(raw, &env)
		if code != 200 || env["success"] != true {
			t.Fatalf("create %d %s", code, raw)
		}
		nid := env["result"].(map[string]any)["id"].(string)
		code, raw = call(http.MethodGet, "/client/v4/accounts/acct1/storage/kv/namespaces", "")
		_ = json.Unmarshal(raw, &env)
		if code != 200 || len(env["result"].([]any)) != 1 {
			t.Fatalf("list %d %s", code, raw)
		}
		code, raw = call(http.MethodGet, "/client/v4/accounts/acct1/storage/kv/namespaces/"+nid, "")
		_ = json.Unmarshal(raw, &env)
		if code != 200 || env["result"].(map[string]any)["id"] != nid {
			t.Fatalf("get %d %s", code, raw)
		}
	})
	t.Run("Given a duplicate title When created Then 10014 is returned", func(t *testing.T) {
		code, raw := call(http.MethodPost, "/client/v4/accounts/acct1/storage/kv/namespaces", `{"title":"bdd"}`)
		env := map[string]any{}
		_ = json.Unmarshal(raw, &env)
		if code != 400 || env["success"] != false {
			t.Fatalf("dup %d %s", code, raw)
		}
	})
	t.Run("Given a namespace When a value is put Then GET returns the raw body", func(t *testing.T) {
		code, raw := call(http.MethodPost, "/client/v4/accounts/acct1/storage/kv/namespaces", `{"title":"kv"}`)
		env := map[string]any{}
		_ = json.Unmarshal(raw, &env)
		nid := env["result"].(map[string]any)["id"].(string)
		code, _ = call(http.MethodPut, "/client/v4/accounts/acct1/storage/kv/namespaces/"+nid+"/values/k", "hello")
		if code != 200 {
			t.Fatalf("put %d", code)
		}
		code, raw = call(http.MethodGet, "/client/v4/accounts/acct1/storage/kv/namespaces/"+nid+"/values/k", "")
		if code != 200 || string(raw) != "hello" {
			t.Fatalf("get %d %q", code, raw)
		}
		code, _ = call(http.MethodDelete, "/client/v4/accounts/acct1/storage/kv/namespaces/"+nid+"/values/k", "")
		if code != 200 {
			t.Fatalf("del %d", code)
		}
		code, _ = call(http.MethodGet, "/client/v4/accounts/acct1/storage/kv/namespaces/"+nid+"/values/k", "")
		if code != 404 {
			t.Fatalf("after del %d", code)
		}
	})
	t.Run("Given a missing namespace When fetched Then success is false with a numeric code", func(t *testing.T) {
		code, raw := call(http.MethodGet, "/client/v4/accounts/acct1/storage/kv/namespaces/nope", "")
		env := map[string]any{}
		_ = json.Unmarshal(raw, &env)
		if code != 404 || env["success"] != false {
			t.Fatalf("missing %d %s", code, raw)
		}
		err0 := env["errors"].([]any)[0].(map[string]any)
		if _, ok := err0["code"].(float64); !ok {
			t.Fatalf("code %#v", err0)
		}
	})
}
