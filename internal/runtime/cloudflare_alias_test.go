package runtime

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/config"

	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/bundled"
)

func TestCloudflareAliasBoot(t *testing.T) {
	cases := []struct {
		name    string
		args    []string
		profile string
	}{
		{name: "short", args: []string{"cloudflare"}},
		{name: "legacy", args: []string{"cloudflare.kv"}},
		{name: "canonical", args: []string{"cloudflare.api"}},
		{name: "profile", profile: "cloudflare-core"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := config.Default()
			cfg.Services = ExpandServices(tc.args, tc.profile, false)
			cfg.Seed = "cf-alias-" + tc.name
			rt, err := Boot(cfg)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = rt.Close() })

			enabled := rt.Reg.Enabled()
			if len(enabled) != 1 || enabled[0] != "cloudflare.api" {
				t.Fatalf("enabled=%v want [cloudflare.api]", enabled)
			}
			if _, ok := rt.Reg.Resolve("cloudflare.api"); !ok {
				t.Fatal("cloudflare.api missing")
			}

			ts := httptest.NewServer(rt.Handler())
			t.Cleanup(ts.Close)

			req, err := http.NewRequest(http.MethodPost, ts.URL+"/client/v4/accounts/acct1/storage/kv/namespaces", strings.NewReader(`{"title":"ns"}`))
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
			raw, _ := io.ReadAll(res.Body)
			res.Body.Close()
			var env map[string]any
			_ = json.Unmarshal(raw, &env)
			if res.StatusCode != 200 || env["success"] != true {
				t.Fatalf("create namespace %d %s", res.StatusCode, raw)
			}
			result, _ := env["result"].(map[string]any)
			if id, _ := result["id"].(string); id == "" {
				t.Fatalf("missing namespace id: %s", raw)
			}
		})
	}
}

func TestCloudflareCanonicalServiceID(t *testing.T) {
	for _, in := range []string{"cloudflare", "cloudflare.kv", "cloudflare.api", "Cloudflare.KV"} {
		if got := CanonicalServiceID(in); got != "cloudflare.api" {
			t.Fatalf("%q -> %q want cloudflare.api", in, got)
		}
	}
}
