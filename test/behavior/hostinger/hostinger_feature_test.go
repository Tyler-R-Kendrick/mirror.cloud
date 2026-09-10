package hostinger

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

	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/hostinger/api"
)

func TestHostingerDNSBehavior(t *testing.T) {
	deps := spitest.Deps(t)
	cfg := config.Default()
	cfg.Services = []string{"hostinger.dns"}
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
		req.Host = "api.hostinger.com"
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
	t.Run("Given a domain When created Then it is listed and fetched", func(t *testing.T) {
		code, raw := call(http.MethodPost, "/api/domains/v1/portfolio", `{"domain":"bdd.test"}`)
		dom := map[string]any{}
		_ = json.Unmarshal(raw, &dom)
		if code != 200 || dom["domain"] != "bdd.test" {
			t.Fatalf("create %d %s", code, raw)
		}
		code, raw = call(http.MethodGet, "/api/domains/v1/portfolio", "")
		var list []any
		if err := json.Unmarshal(raw, &list); err != nil || code != 200 || len(list) != 1 {
			t.Fatalf("list %d %s", code, raw)
		}
		code, raw = call(http.MethodGet, "/api/domains/v1/portfolio/bdd.test", "")
		_ = json.Unmarshal(raw, &dom)
		if code != 200 || dom["domain"] != "bdd.test" {
			t.Fatalf("get %d %s", code, raw)
		}
	})
	t.Run("Given a duplicate domain When created Then 409 is returned", func(t *testing.T) {
		code, raw := call(http.MethodPost, "/api/domains/v1/portfolio", `{"domain":"bdd.test"}`)
		if code != 409 {
			t.Fatalf("dup %d %s", code, raw)
		}
	})
	t.Run("Given a domain When DNS is updated Then GET returns the records", func(t *testing.T) {
		code, _ := call(http.MethodPut, "/api/dns/v1/zones/bdd.test", `{"overwrite":true,"zone":[{"name":"@","type":"A","ttl":300,"records":[{"content":"9.9.9.9"}]}]}`)
		if code != 200 {
			t.Fatalf("put %d", code)
		}
		code, raw := call(http.MethodGet, "/api/dns/v1/zones/bdd.test", "")
		var recs []any
		if err := json.Unmarshal(raw, &recs); err != nil || code != 200 || len(recs) != 1 {
			t.Fatalf("get %d %s", code, raw)
		}
		code, _ = call(http.MethodDelete, "/api/dns/v1/zones/bdd.test", "")
		if code != 200 {
			t.Fatalf("del %d", code)
		}
		code, raw = call(http.MethodGet, "/api/dns/v1/zones/bdd.test", "")
		_ = json.Unmarshal(raw, &recs)
		if code != 200 || len(recs) != 0 {
			t.Fatalf("after del %d %s", code, raw)
		}
	})
	t.Run("Given a missing domain When fetched Then message fault without AWS headers", func(t *testing.T) {
		code, raw := call(http.MethodGet, "/api/domains/v1/portfolio/nope.test", "")
		miss := map[string]any{}
		_ = json.Unmarshal(raw, &miss)
		if code != 404 || miss["message"] == nil {
			t.Fatalf("missing %d %s", code, raw)
		}
	})
}
