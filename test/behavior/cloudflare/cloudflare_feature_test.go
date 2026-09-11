package cloudflare_test

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
	// for cloudflare.api and the edge answers from the mock tier -- which looks
	// like a working service returning synthesized data, not like a failure.
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/bundled"
)

// TestCloudflareKVBehavior drives the served service over its real HTTP
// surface. It boots the runtime rather than constructing an edge directly,
// because a bundle is served from the generated model and `edge.New` falls
// back to the hand-authored catalog when none is supplied.
//
// The operation names, the URIs and the service id are all the document's now.
// The pack registered as cloudflare.kv, invented six operation names and bound
// them to these same URIs, so the requests are unchanged; what changed is which
// operation answers them, and that the {success, errors, messages, result}
// envelope is the response shape rather than something a codec branch wrapped
// around a bare body.
func TestCloudflareKVBehavior(t *testing.T) {
	cfg := config.Default()
	cfg.Services = []string{"cloudflare.api"}
	cfg.Seed = "cloudflare-bdd"
	rt, err := rtpkg.Boot(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(rt.Handler())
	defer ts.Close()

	const base = "/client/v4/accounts/acct1/storage/kv/namespaces"
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
		req.Host = "api.cloudflare.com"
		req.Header.Set("Authorization", "Bearer test")
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		b, _ := io.ReadAll(res.Body)
		res.Body.Close()
		return res.StatusCode, b, res.Header
	}
	envelope := func(t *testing.T, raw []byte) map[string]any {
		t.Helper()
		var env map[string]any
		if err := json.Unmarshal(raw, &env); err != nil {
			t.Fatalf("not an envelope: %s", raw)
		}
		return env
	}
	// Only the first code is ever asserted; Cloudflare reports one at a time.
	code := func(t *testing.T, raw []byte) any {
		t.Helper()
		errs, _ := envelope(t, raw)["errors"].([]any)
		if len(errs) == 0 {
			t.Fatalf("no errors in %s", raw)
		}
		first, _ := errs[0].(map[string]any)
		return first["code"]
	}

	var nsID string

	t.Run("Given a title When a namespace is created Then it is returned inside the envelope", func(t *testing.T) {
		status, raw, _ := call(http.MethodPost, base, `{"title":"docs"}`)
		env := envelope(t, raw)
		result, _ := env["result"].(map[string]any)
		if status != 200 || env["success"] != true || result["title"] != "docs" {
			t.Fatalf("create %d %s", status, raw)
		}
		nsID, _ = result["id"].(string)
		if nsID == "" {
			t.Fatalf("no id in %s", raw)
		}
	})

	t.Run("Given the same title When created again Then 10014 and no second namespace", func(t *testing.T) {
		status, raw, _ := call(http.MethodPost, base, `{"title":"docs"}`)
		if status != 400 || code(t, raw) != float64(10014) {
			t.Fatalf("duplicate %d %s", status, raw)
		}
		status, raw, _ = call(http.MethodGet, base, "")
		list, _ := envelope(t, raw)["result"].([]any)
		if status != 200 || len(list) != 1 {
			t.Fatalf("list %d %s", status, raw)
		}
	})

	t.Run("Given no title When a namespace is created Then 10007", func(t *testing.T) {
		// The document marks title required, so the engine's model check
		// answers this one -- with Cloudflare's name for it, not the
		// ValidationException default.
		status, raw, _ := call(http.MethodPost, base, `{}`)
		if status != 400 || code(t, raw) != float64(10007) {
			t.Fatalf("missing title %d %s", status, raw)
		}
	})

	t.Run("Given a value When written Then it reads back as bytes with no envelope", func(t *testing.T) {
		status, raw, _ := call(http.MethodPut, base+"/"+nsID+"/values/greeting", "hello")
		if status != 200 || envelope(t, raw)["success"] != true {
			t.Fatalf("write %d %s", status, raw)
		}
		status, raw, hdr := call(http.MethodGet, base+"/"+nsID+"/values/greeting", "")
		if status != 200 || string(raw) != "hello" {
			t.Fatalf("read %d %q", status, raw)
		}
		if ct := hdr.Get("Content-Type"); ct != "application/octet-stream" {
			t.Fatalf("read content-type %q", ct)
		}
	})

	t.Run("Given a stored key When the namespace is listed Then the key is named without its value", func(t *testing.T) {
		status, raw, _ := call(http.MethodGet, base+"/"+nsID+"/keys", "")
		keys, _ := envelope(t, raw)["result"].([]any)
		if status != 200 || len(keys) != 1 {
			t.Fatalf("keys %d %s", status, raw)
		}
		first, _ := keys[0].(map[string]any)
		if first["name"] != "greeting" {
			t.Fatalf("key %s", raw)
		}
		if _, leaked := first["value"]; leaked {
			t.Fatalf("the value leaked into the key listing: %s", raw)
		}
	})

	t.Run("Given a key with a slash When percent-encoded Then it round-trips", func(t *testing.T) {
		// The document says to percent-encode a key used in a URL, and the
		// router binds exactly one path segment. An unencoded slash is two
		// segments and routes nowhere, which the deleted pack tolerated.
		status, raw, _ := call(http.MethodPut, base+"/"+nsID+"/values/a%2Fb", "nested")
		if status != 200 {
			t.Fatalf("write encoded %d %s", status, raw)
		}
		status, raw, _ = call(http.MethodGet, base+"/"+nsID+"/values/a%2Fb", "")
		if status != 200 || string(raw) != "nested" {
			t.Fatalf("read encoded %d %q", status, raw)
		}
	})

	t.Run("Given a missing key When read or deleted Then 10009", func(t *testing.T) {
		status, raw, _ := call(http.MethodGet, base+"/"+nsID+"/values/absent", "")
		if status != 404 || code(t, raw) != float64(10009) {
			t.Fatalf("read absent %d %s", status, raw)
		}
		status, raw, _ = call(http.MethodDelete, base+"/"+nsID+"/values/absent", "")
		if status != 404 || code(t, raw) != float64(10009) {
			t.Fatalf("delete absent %d %s", status, raw)
		}
	})

	t.Run("Given a missing namespace When a key is read Then the namespace fails first", func(t *testing.T) {
		// Precedence, which is the part no specification states: 10013 for the
		// namespace, never 10009 for the key inside it.
		status, raw, _ := call(http.MethodGet, base+"/absent/values/greeting", "")
		if status != 404 || code(t, raw) != float64(10013) {
			t.Fatalf("read under absent namespace %d %s", status, raw)
		}
	})

	t.Run("Given a stored key When deleted Then it is gone", func(t *testing.T) {
		status, raw, _ := call(http.MethodDelete, base+"/"+nsID+"/values/greeting", "")
		if status != 200 || envelope(t, raw)["success"] != true {
			t.Fatalf("delete %d %s", status, raw)
		}
		status, raw, _ = call(http.MethodGet, base+"/"+nsID+"/values/greeting", "")
		if status != 404 || code(t, raw) != float64(10009) {
			t.Fatalf("read after delete %d %s", status, raw)
		}
	})

	t.Run("Given a namespace When renamed Then the old title is free again", func(t *testing.T) {
		status, raw, _ := call(http.MethodPut, base+"/"+nsID, `{"title":"archive"}`)
		result, _ := envelope(t, raw)["result"].(map[string]any)
		if status != 200 || result["title"] != "archive" {
			t.Fatalf("rename %d %s", status, raw)
		}
		status, raw, _ = call(http.MethodPost, base, `{"title":"docs"}`)
		if status != 200 {
			t.Fatalf("re-create the freed title %d %s", status, raw)
		}
	})

	t.Run("Given a namespace When removed Then it is no longer found", func(t *testing.T) {
		status, raw, _ := call(http.MethodDelete, base+"/"+nsID, "")
		if status != 200 || envelope(t, raw)["success"] != true {
			t.Fatalf("remove %d %s", status, raw)
		}
		status, raw, _ = call(http.MethodGet, base+"/"+nsID, "")
		if status != 404 || code(t, raw) != float64(10013) {
			t.Fatalf("get after remove %d %s", status, raw)
		}
	})
}
