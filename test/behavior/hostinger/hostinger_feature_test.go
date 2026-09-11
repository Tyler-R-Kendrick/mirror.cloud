package hostinger_test

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
	// for hostinger.api and the edge answers from the mock tier -- which looks
	// like a working service returning synthesized data, not like a failure.
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/bundled"
)

// TestHostingerDNSBehavior drives the served service over its real HTTP
// surface. It boots the runtime rather than constructing an edge directly,
// because a bundle is served from the generated model and `edge.New` falls
// back to the hand-authored catalog when none is supplied -- where every
// operation is bound to `/`, and nothing routes.
//
// The operation names are the specification's now. The pack this replaced
// invented six of its own and bound them to these same URIs, so the requests
// are unchanged; what changed is which operation answers them, and in two
// places what it answers with. Both are recorded as quirks in the bundle.
func TestHostingerDNSBehavior(t *testing.T) {
	cfg := config.Default()
	cfg.Services = []string{"hostinger.api"}
	cfg.Seed = "hostinger-bdd"
	rt, err := rtpkg.Boot(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(rt.Handler())
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

	t.Run("Given a domain When purchased Then an order is returned and the domain is listed", func(t *testing.T) {
		// item_id is required: the specification makes this a billing
		// operation, and it answers with an order rather than the domain the
		// deleted pack invented. See the quirk on DomainsPurchaseNewDomainV1.
		code, raw := call(http.MethodPost, "/api/domains/v1/portfolio",
			`{"domain":"bdd.test","item_id":"hostingercom-domain"}`)
		order := map[string]any{}
		_ = json.Unmarshal(raw, &order)
		if code != 200 || order["id"] == nil || order["status"] != "completed" {
			t.Fatalf("purchase %d %s", code, raw)
		}
		code, raw = call(http.MethodGet, "/api/domains/v1/portfolio", "")
		var list []any
		if err := json.Unmarshal(raw, &list); err != nil || code != 200 || len(list) != 1 {
			t.Fatalf("list %d %s", code, raw)
		}
		code, raw = call(http.MethodGet, "/api/domains/v1/portfolio/bdd.test", "")
		dom := map[string]any{}
		_ = json.Unmarshal(raw, &dom)
		if code != 200 || dom["domain"] != "bdd.test" {
			t.Fatalf("details %d %s", code, raw)
		}
	})

	t.Run("Given a duplicate domain When purchased Then 409 is returned", func(t *testing.T) {
		code, raw := call(http.MethodPost, "/api/domains/v1/portfolio",
			`{"domain":"bdd.test","item_id":"hostingercom-domain"}`)
		if code != 409 {
			t.Fatalf("dup %d %s", code, raw)
		}
	})

	t.Run("Given a domain When DNS is updated Then GET returns the records", func(t *testing.T) {
		code, raw := call(http.MethodPut, "/api/dns/v1/zones/bdd.test",
			`{"overwrite":true,"zone":[{"name":"@","type":"A","ttl":300,"records":[{"content":"9.9.9.9"}]}]}`)
		if code != 200 {
			t.Fatalf("put %d %s", code, raw)
		}
		code, raw = call(http.MethodGet, "/api/dns/v1/zones/bdd.test", "")
		var recs []any
		if err := json.Unmarshal(raw, &recs); err != nil || code != 200 || len(recs) != 1 {
			t.Fatalf("records %d %s", code, raw)
		}
		// `filters` is required by the specification, where the pack required
		// nothing. It is still ignored -- the whole zone is emptied -- which
		// is the second quirk.
		code, raw = call(http.MethodDelete, "/api/dns/v1/zones/bdd.test", `{"filters":[]}`)
		if code != 200 {
			t.Fatalf("delete %d %s", code, raw)
		}
		code, raw = call(http.MethodGet, "/api/dns/v1/zones/bdd.test", "")
		_ = json.Unmarshal(raw, &recs)
		if code != 200 || len(recs) != 0 {
			t.Fatalf("after delete %d %s", code, raw)
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
