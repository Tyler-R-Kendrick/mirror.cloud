package stripe

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/config"
	rtpkg "github.com/tyler-r-kendrick/mirror.cloud/internal/runtime"

	// Links the bundles' registration in. Without it the registry has no pack
	// for the Stripe service and the edge answers from the mock tier -- which
	// looks like a working service returning synthesized data, not like a
	// failure.
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/bundled"
)

// TestStripeCheckoutPages drives the hosted checkout surface the differential
// corpus only status-checks: the corpus proves routing and status parity with
// the oracle, while this proves the pages carry the order data (the oracle's
// pixels are quirked, its data is not) and the completion wire shapes work.
func TestStripeCheckoutPages(t *testing.T) {
	cfg := config.Default()
	cfg.Services = []string{"stripe.api"}
	cfg.Seed = "stripe-pages"
	rt, err := rtpkg.Boot(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(rt.Handler())
	defer ts.Close()

	post := func(t *testing.T, path, body, contentType string) (int, []byte, http.Header) {
		t.Helper()
		req, err := http.NewRequest("POST", ts.URL+path, strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		req.Host = "api.stripe.com"
		if contentType != "" {
			req.Header.Set("Content-Type", contentType)
		}
		client := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		}}
		res, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		b, _ := io.ReadAll(res.Body)
		res.Body.Close()
		return res.StatusCode, b, res.Header
	}
	get := func(t *testing.T, path string) (int, string) {
		t.Helper()
		req, err := http.NewRequest("GET", ts.URL+path, nil)
		if err != nil {
			t.Fatal(err)
		}
		req.Host = "api.stripe.com"
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		b, _ := io.ReadAll(res.Body)
		res.Body.Close()
		return res.StatusCode, string(b)
	}
	decode := func(t *testing.T, b []byte) map[string]any {
		t.Helper()
		m := map[string]any{}
		if err := json.Unmarshal(b, &m); err != nil {
			t.Fatalf("not JSON: %s", b)
		}
		return m
	}

	_, b, _ := post(t, "/v1/products", `{"name":"Page Plan"}`, "application/json")
	prod := decode(t, b)["id"].(string)
	_, b, _ = post(t, "/v1/prices", `{"currency":"usd","product":"`+prod+`","unit_amount":2000}`, "application/json")
	price := decode(t, b)["id"].(string)

	// Form and JSON creates agree field for field, modulo the drawn id: the
	// oracle parses both, so the bundle must too.
	_, jb, _ := post(t, "/v1/customers", `{"email":"same@x.co","name":"Same"}`, "application/json")
	_, fb, _ := post(t, "/v1/customers", `email=same@x.co&name=Same`, "application/x-www-form-urlencoded")
	jm, fm := decode(t, jb), decode(t, fb)
	jm["id"], fm["id"] = nil, nil
	jm["created"], fm["created"] = nil, nil
	for _, k := range []string{"object", "email", "name", "description", "livemode"} {
		if jm[k] != fm[k] {
			t.Fatalf("json vs form diverge on %s: %v vs %v", k, jm, fm)
		}
	}
	cust := decode(t, jb)["id"].(string)
	_ = cust

	_, b, _ = post(t, "/v1/checkout/sessions",
		`{"mode":"payment","customer":"`+decode(t, jb)["id"].(string)+`","success_url":"https://ex.com/ok?sid={CHECKOUT_SESSION_ID}","cancel_url":"https://ex.com/no","line_items":[{"price":"`+price+`","quantity":2}]}`,
		"application/json")
	sess := decode(t, b)
	sid, _ := sess["id"].(string)
	if sess["url"] == nil || !strings.Contains(sess["url"].(string), "/checkout/"+sid) {
		t.Fatalf("session url %v", sess["url"])
	}

	code, page := get(t, "/checkout/"+sid)
	if code != 200 {
		t.Fatalf("page status %d", code)
	}
	for _, want := range []string{
		"Page Plan", "Qty 2", "$40.00", "$20.00 each",
		"Subtotal $40.00", "Total due $40.00 USD",
		`href="https://ex.com/no"`,
		`action="/checkout/` + sid + `/complete"`,
	} {
		if !strings.Contains(page, want) {
			t.Errorf("page missing %q:\n%s", want, page)
		}
	}

	code, _, hdr := post(t, "/checkout/"+sid+"/complete", "", "")
	if code != 302 {
		t.Fatalf("complete status %d", code)
	}
	if loc := hdr.Get("Location"); loc != "https://ex.com/ok?sid="+sid {
		t.Errorf("complete location %q", loc)
	}

	// Without a success_url the answer is the receipt card, not a redirect.
	_, b, _ = post(t, "/v1/checkout/sessions", `{"mode":"payment"}`, "application/json")
	sid2 := decode(t, b)["id"].(string)
	code, body, hdr := post(t, "/checkout/"+sid2+"/complete", "", "")
	if code != 200 || hdr.Get("Location") != "" {
		t.Fatalf("receipt status %d location %q", code, hdr.Get("Location"))
	}
	if !strings.Contains(string(body), "Payment Complete") {
		t.Errorf("receipt card:\n%s", body)
	}

	code, expired := get(t, "/checkout/"+sid)
	if code != 200 || !strings.Contains(expired, "Session Expired") || !strings.Contains(expired, "Status: complete") {
		t.Errorf("expired page %d:\n%s", code, expired)
	}
	code, missing := get(t, "/checkout/cs_nope")
	if code != 404 || !strings.Contains(missing, "Session Not Found") {
		t.Errorf("missing page %d:\n%s", code, missing)
	}
}
