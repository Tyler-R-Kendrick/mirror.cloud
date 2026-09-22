package cloudflare_test

import (
	"context"
	"encoding/json"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/bundled"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/clock"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/config"
	rtpkg "github.com/tyler-r-kendrick/mirror.cloud/internal/runtime"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spitest"

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

	t.Run("Given binary bytes When written Then they round-trip exactly", func(t *testing.T) {
		raw := []byte{0x00, 0x01, 0xff, 0xfe, 'h', 'i', 0x00}
		req, err := http.NewRequest(http.MethodPut, ts.URL+base+"/"+nsID+"/values/bin", strings.NewReader(string(raw)))
		if err != nil {
			t.Fatal(err)
		}
		req.Host = "api.cloudflare.com"
		req.Header.Set("Authorization", "Bearer test")
		req.Header.Set("Content-Type", "application/octet-stream")
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != 200 {
			t.Fatalf("write binary %d %s", res.StatusCode, body)
		}
		status, got, _ := call(http.MethodGet, base+"/"+nsID+"/values/bin", "")
		if status != 200 || string(got) != string(raw) {
			t.Fatalf("read binary %d %q want %q", status, got, raw)
		}
	})

	t.Run("Given a multipart write When sent Then value and metadata separate", func(t *testing.T) {
		// The document takes value and metadata as parts of one multipart
		// body. Before the codec split the envelope, the whole MIME body
		// became the stored value; now the value part round-trips through
		// the raw read and the metadata part appears in the key listing.
		var buf strings.Builder
		w := multipart.NewWriter(&buf)
		if err := w.WriteField("value", "part-payload"); err != nil {
			t.Fatal(err)
		}
		if err := w.WriteField("metadata", `{"source":"bdd"}`); err != nil {
			t.Fatal(err)
		}
		w.Close()
		req, err := http.NewRequest(http.MethodPut, ts.URL+base+"/"+nsID+"/values/mp", strings.NewReader(buf.String()))
		if err != nil {
			t.Fatal(err)
		}
		req.Host = "api.cloudflare.com"
		req.Header.Set("Authorization", "Bearer test")
		req.Header.Set("Content-Type", w.FormDataContentType())
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != 200 {
			t.Fatalf("multipart write %d %s", res.StatusCode, body)
		}
		// The value is the part, never the MIME envelope: no boundary
		// string may appear in what reads back.
		status, got, _ := call(http.MethodGet, base+"/"+nsID+"/values/mp", "")
		if status != 200 || string(got) != "part-payload" {
			t.Fatalf("read after multipart %d %q", status, got)
		}
		if strings.Contains(string(got), "multipart") || strings.Contains(string(got), "boundary") {
			t.Fatalf("stored the MIME envelope: %q", got)
		}
		// The listing answers the metadata as parsed JSON beside the name.
		status, raw, _ := call(http.MethodGet, base+"/"+nsID+"/keys", "")
		if status != 200 {
			t.Fatalf("keys %d %s", status, raw)
		}
		result, _ := envelope(t, raw)["result"].([]any)
		var found bool
		for _, item := range result {
			m, _ := item.(map[string]any)
			if m["name"] != "mp" {
				continue
			}
			found = true
			md, _ := m["metadata"].(map[string]any)
			if md == nil || md["source"] != "bdd" {
				t.Fatalf("metadata not separated: %v", item)
			}
			if _, hasVal := m["value"]; hasVal {
				t.Fatalf("listing leaked the value: %v", item)
			}
		}
		if !found {
			t.Fatalf("key mp missing from %s", raw)
		}
		// The dedicated metadata read answers the same JSON: the two
		// access paths (listing projection and metadata operation) agree
		// because both read one stored entry.
		status, raw, _ = call(http.MethodGet, base+"/"+nsID+"/metadata/mp", "")
		if status != 200 {
			t.Fatalf("metadata read %d %s", status, raw)
		}
		md, _ := envelope(t, raw)["result"].(map[string]any)
		if md == nil || md["source"] != "bdd" {
			t.Fatalf("metadata read: %s", raw)
		}
	})

	t.Run("Given a malformed multipart body When written Then 400 not a stored envelope", func(t *testing.T) {
		// A content type that promises multipart and a body that is not one
		// must not fall through to "the whole body is the value": that is
		// how the MIME envelope used to get stored.
		req, err := http.NewRequest(http.MethodPut, ts.URL+base+"/"+nsID+"/values/badmp", strings.NewReader("not a multipart body at all"))
		if err != nil {
			t.Fatal(err)
		}
		req.Host = "api.cloudflare.com"
		req.Header.Set("Authorization", "Bearer test")
		req.Header.Set("Content-Type", "multipart/form-data; boundary=xyz")
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != 400 {
			t.Fatalf("malformed multipart %d %s", res.StatusCode, body)
		}
		// And nothing was stored under the promised name.
		status, _, _ := call(http.MethodGet, base+"/"+nsID+"/values/badmp", "")
		if status != 404 {
			t.Fatalf("malformed multipart stored a value: read %d", status)
		}
	})

	t.Run("Given oversized metadata When written Then 10007", func(t *testing.T) {
		var buf strings.Builder
		w := multipart.NewWriter(&buf)
		if err := w.WriteField("value", "v"); err != nil {
			t.Fatal(err)
		}
		if err := w.WriteField("metadata", strings.Repeat("x", 1025)); err != nil {
			t.Fatal(err)
		}
		w.Close()
		req, err := http.NewRequest(http.MethodPut, ts.URL+base+"/"+nsID+"/values/bigmd", strings.NewReader(buf.String()))
		if err != nil {
			t.Fatal(err)
		}
		req.Host = "api.cloudflare.com"
		req.Header.Set("Authorization", "Bearer test")
		req.Header.Set("Content-Type", w.FormDataContentType())
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != 400 || code(t, body) != float64(10007) {
			t.Fatalf("oversized metadata %d %s", res.StatusCode, body)
		}
	})

	t.Run("Given wrong-typed titles When created or renamed Then 10007", func(t *testing.T) {
		// The engine holds the model's missing-input fault until requires run,
		// so null/numeric titles reach the rule. string(null) is "null" and
		// string(42) is "42" -- without the type check these would create a
		// namespace literally named "null".
		for _, body := range []string{`{"title":null}`, `{"title":42}`, `{"title":""}`, `{"title":[]}`} {
			status, raw, _ := call(http.MethodPost, base, body)
			if status != 400 || code(t, raw) != float64(10007) {
				t.Fatalf("create %s: %d %s", body, status, raw)
			}
		}
		status, raw, _ := call(http.MethodPut, base+"/"+nsID, `{"title":null}`)
		if status != 400 || code(t, raw) != float64(10007) {
			t.Fatalf("rename null: %d %s", status, raw)
		}
	})

	t.Run("Given a TTL When it passes Then the key reads as missing", func(t *testing.T) {
		deps := spitest.Deps(t)
		clk := deps.Clock.(*clock.Controllable)
		pack, err := bundled.New("cloudflare.api", deps)
		if err != nil {
			t.Fatal(err)
		}
		ident := spi.Identity{Account: "ttl-acct", Region: "us-east-1"}
		call := func(op string, in map[string]any) (map[string]any, error) {
			t.Helper()
			resp, err := pack.Invoke(context.Background(), &spi.Request{
				ServiceID: "cloudflare.api", Operation: op, Input: in, Identity: ident,
			})
			if err != nil {
				return nil, err
			}
			return resp.Output, nil
		}
		out, err := call("WorkersKvNamespaceCreateANamespace", map[string]any{"account_id": "ttl-acct", "title": "ttl"})
		if err != nil {
			t.Fatal(err)
		}
		ns := out["result"].(map[string]any)["id"].(string)
		base := map[string]any{"account_id": "ttl-acct", "namespace_id": ns}
		withNS := func(m map[string]any) map[string]any {
			for k, v := range base {
				m[k] = v
			}
			return m
		}
		// Relative TTL: alive now, gone after the clock steps past it.
		if _, err := call("WorkersKvNamespaceWriteKeyValuePairWithMetadata", withNS(map[string]any{"key_name": "temp", "body": "x", "expiration_ttl": float64(60)})); err != nil {
			t.Fatal(err)
		}
		if _, err := call("WorkersKvNamespaceReadKeyValuePair", withNS(map[string]any{"key_name": "temp"})); err != nil {
			t.Fatalf("before expiry: %v", err)
		}
		if err := clk.Advance(61 * time.Second); err != nil {
			t.Fatal(err)
		}
		if _, err := call("WorkersKvNamespaceReadKeyValuePair", withNS(map[string]any{"key_name": "temp"})); err == nil {
			t.Fatal("read after TTL passed, want 10009")
		} else if f, ok := err.(*spi.Fault); !ok || f.Code != "10009" {
			t.Fatalf("after expiry: %v", err)
		}
		// Absolute expiration: the same boundary by timestamp.
		now := clk.Now().Unix()
		if _, err := call("WorkersKvNamespaceWriteKeyValuePairWithMetadata", withNS(map[string]any{"key_name": "abs", "body": "y", "expiration": float64(now + 60)})); err != nil {
			t.Fatal(err)
		}
		if err := clk.Advance(61 * time.Second); err != nil {
			t.Fatal(err)
		}
		if _, err := call("WorkersKvNamespaceReadKeyValuePair", withNS(map[string]any{"key_name": "abs"})); err == nil {
			t.Fatal("read after absolute expiry, want 10009")
		} else if f, ok := err.(*spi.Fault); !ok || f.Code != "10009" {
			t.Fatalf("after absolute expiry: %v", err)
		}
	})

	t.Run("Given namespace keys When the namespace is removed Then no key survives", func(t *testing.T) {
		status, raw, _ := call(http.MethodPost, base, `{"title":"doomed"}`)
		ns := envelope(t, raw)["result"].(map[string]any)["id"].(string)
		if status != 200 || ns == "" {
			t.Fatalf("create %d %s", status, raw)
		}
		status, raw, _ = call(http.MethodPut, base+"/"+ns+"/values/k", "v")
		if status != 200 {
			t.Fatalf("write %d %s", status, raw)
		}
		status, _, _ = call(http.MethodDelete, base+"/"+ns, "")
		if status != 200 {
			t.Fatalf("remove %d", status)
		}
		// Route-level: keys under the removed namespace answer 10013.
		status, raw, _ = call(http.MethodGet, base+"/"+ns+"/keys", "")
		if status != 404 || code(t, raw) != float64(10013) {
			t.Fatalf("keys after remove %d %s", status, raw)
		}
		// Store-level proof, not just route-level: the entry collection is
		// named for the namespace id, so after the cascade it must hold no
		// rows. Anything that knows the collection name -- a later writer, a
		// snapshot restore, a bug in id generation -- would otherwise find
		// the orphaned keys and resurrect them under a fresh namespace.
		col := rt.Deps.Store.Scope("000000000000", "us-east-1").Collection("cfkv:" + ns)
		kvs, _, err := col.List(context.Background(), "", "", 0)
		if err != nil {
			t.Fatal(err)
		}
		if len(kvs) != 0 {
			t.Fatalf("entry collection survived namespace removal: %d rows", len(kvs))
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

// TestCloudflareAliasBooting proves every supported spelling -- the short
// alias, the legacy `cloudflare.kv` identifier, the canonical ID, and the
// profile -- enables the one cloudflare.api bundle and routes a real request
// through it. Asserting the printed service name is not enough: the old
// alias table pointed all three at `cloudflare.kv`, an id no bundle registers,
// so the spelling resolved "successfully" and the edge answered from the mock
// tier (or not at all). The fidelity header is what distinguishes the real
// bundle's answer from a mock's.
func TestCloudflareAliasBooting(t *testing.T) {
	const base = "/client/v4/accounts/acct1/storage/kv/namespaces"
	// Each case names the CLI input and the profile flag separately, the way
	// cmd/mirror passes them to ExpandServices.
	cases := []struct {
		name    string
		args    []string
		profile string
	}{
		{name: "short alias", args: []string{"cloudflare"}},
		{name: "legacy alias", args: []string{"cloudflare.kv"}},
		{name: "canonical id", args: []string{"cloudflare.api"}},
		{name: "profile", profile: "cloudflare-core"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ids := rtpkg.ExpandServices(tc.args, tc.profile, false)
			if len(ids) != 1 || ids[0] != "cloudflare.api" {
				t.Fatalf("expansion = %v, want [cloudflare.api]", ids)
			}
			cfg := config.Default()
			cfg.Services = ids
			cfg.Seed = "cf-alias-" + tc.name
			rt, err := rtpkg.Boot(cfg)
			if err != nil {
				t.Fatal(err)
			}
			ts := httptest.NewServer(rt.Handler())
			defer ts.Close()
			req, err := http.NewRequest(http.MethodPost, ts.URL+base, strings.NewReader(`{"title":"docs"}`))
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
			// The bundle's tier answers emulate; a mock or a disabled
			// service would say mock or 501. Both are checked so a spelling
			// that "works" while silently downgrading cannot pass.
			if got := res.Header.Get("x-mirror-fidelity"); got != "emulate" {
				t.Fatalf("fidelity = %q, want emulate (%d %s)", got, res.StatusCode, raw)
			}
			if res.StatusCode != 200 {
				t.Fatalf("create %d %s", res.StatusCode, raw)
			}
		})
	}

	t.Run("Given an unselected service When called Then it is not implemented", func(t *testing.T) {
		cfg := config.Default()
		cfg.Services = []string{"cloudflare.api"}
		cfg.Seed = "cf-alias-off"
		rt, err := rtpkg.Boot(cfg)
		if err != nil {
			t.Fatal(err)
		}
		ts := httptest.NewServer(rt.Handler())
		defer ts.Close()
		req, err := http.NewRequest(http.MethodGet, ts.URL+"/", nil)
		if err != nil {
			t.Fatal(err)
		}
		req.Host = "s3.us-east-1.amazonaws.com"
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		res.Body.Close()
		if res.StatusCode != 501 {
			t.Fatalf("unselected service status %d, want 501", res.StatusCode)
		}
	})
}

// TestCloudflareStrict covers CF-STRICT: under --strict, an operation the
// bundle does not serve must fail with a distinguishable not-implemented
// answer rather than a synthesized mock success, while the operations the
// bundle does serve keep working. The generated model carries five KV
// operations (the bulk reads/writes, the deprecated bulk delete, and the
// metadata read) that no bundle rule projects; without strict they are
// answered by the deterministic mock, with strict they must refuse. The
// third leg pins the non-substitution half: a strict boot never silently
// downgrades a served operation to the mock either.
func TestCloudflareStrict(t *testing.T) {
	cfg := config.Default()
	cfg.Services = []string{"cloudflare.api"}
	cfg.Seed = "cf-strict"
	cfg.Strict = true
	rt, err := rtpkg.Boot(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(rt.Handler())
	defer ts.Close()

	call := func(method, path, body string) (int, []byte, string) {
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
		raw, _ := io.ReadAll(res.Body)
		res.Body.Close()
		return res.StatusCode, raw, res.Header.Get("x-mirror-fidelity")
	}

	t.Run("Given strict When an unserved operation is called Then 501 not a mock success", func(t *testing.T) {
		status, raw, _ := call(http.MethodPost,
			"/client/v4/accounts/acct1/storage/kv/namespaces/ns1/bulk/get", `{"keys":["a"]}`)
		if status != 501 {
			t.Fatalf("unserved bulk/get under strict: %d %s", status, raw)
		}
		if !strings.Contains(string(raw), "MirrorNotImplemented") {
			t.Fatalf("want MirrorNotImplemented, got %s", raw)
		}
		// The refusal itself carries no success flag and no synthesized
		// result: a strict failure must be distinguishable from a mock
		// success by its status and code alone, not by a header a client
		// would have to trust. (The fidelity header names the pack that
		// would have answered -- the mock pack -- which is honest naming,
		// not a mock answer: the body above proves the refusal.)
		if strings.Contains(string(raw), `"success":true`) {
			t.Fatalf("strict refusal looks like success: %s", raw)
		}
	})

	t.Run("Given strict When a served operation is called Then emulate still answers", func(t *testing.T) {
		status, raw, fidelity := call(http.MethodPost,
			"/client/v4/accounts/acct1/storage/kv/namespaces", `{"title":"strict-docs"}`)
		if status != 200 || fidelity != "emulate" {
			t.Fatalf("served create under strict: %d fidelity=%q %s", status, fidelity, raw)
		}
	})

	t.Run("Given strict When the same unserved operation is refused twice Then the answer is stable", func(t *testing.T) {
		first := func() string {
			_, raw, _ := call(http.MethodPut,
				"/client/v4/accounts/acct1/storage/kv/namespaces/ns1/bulk", `[{"key":"a","value":"b"}]`)
			return string(raw)
		}()
		if !strings.Contains(first, "MirrorNotImplemented") {
			t.Fatalf("first refused answer: %s", first)
		}
		second := func() string {
			_, raw, _ := call(http.MethodPut,
				"/client/v4/accounts/acct1/storage/kv/namespaces/ns1/bulk", `[{"key":"a","value":"b"}]`)
			return string(raw)
		}()
		if first != second {
			t.Fatalf("unstable refusal:\n1: %s\n2: %s", first, second)
		}
	})
}

// TestCloudflareAccountIsolation covers CF-ACCOUNT: identical provider-visible
// names in different accounts must not collide, leak, or share state. The
// store scopes every collection by account+region; this exercises the
// scoping through the public operations: two accounts each create a namespace
// with the same title, write the same key with different values, and must see
// only their own.
func TestCloudflareAccountIsolation(t *testing.T) {
	deps := spitest.Deps(t)
	pack, err := bundled.New("cloudflare.api", deps)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	acctA := spi.Identity{Account: "acct-A", Region: "us-east-1"}
	acctB := spi.Identity{Account: "acct-B", Region: "us-east-1"}
	invoke := func(id spi.Identity, op string, in map[string]any) (map[string]any, error) {
		t.Helper()
		resp, err := pack.Invoke(ctx, &spi.Request{
			ServiceID: "cloudflare.api", Operation: op, Input: in, Identity: id,
		})
		if err != nil {
			return nil, err
		}
		return resp.Output, nil
	}
	// Same title in both accounts: the title index is account-scoped, so
	// B's create must not see A's title as taken.
	var nsA, nsB string
	for _, tc := range []struct {
		id  *string
		who spi.Identity
	}{{&nsA, acctA}, {&nsB, acctB}} {
		out, err := invoke(tc.who, "WorkersKvNamespaceCreateANamespace",
			map[string]any{"account_id": tc.who.Account, "title": "shared-title"})
		if err != nil {
			t.Fatalf("create in %s: %v", tc.who.Account, err)
		}
		result, _ := out["result"].(map[string]any)
		*tc.id, _ = result["id"].(string)
		if *tc.id == "" {
			t.Fatalf("no id for %s: %v", tc.who.Account, out)
		}
	}
	// Same key, different values per account.
	for _, tc := range []struct {
		who   spi.Identity
		ns    string
		value string
	}{{acctA, nsA, "value-for-A"}, {acctB, nsB, "value-for-B"}} {
		_, err := invoke(tc.who, "WorkersKvNamespaceWriteKeyValuePairWithMetadata",
			map[string]any{"account_id": tc.who.Account, "namespace_id": tc.ns,
				"key_name": "k", "body": tc.value})
		if err != nil {
			t.Fatalf("write in %s: %v", tc.who.Account, err)
		}
	}
	// Each account reads back only its own value.
	for _, tc := range []struct {
		who  spi.Identity
		ns   string
		want string
	}{{acctA, nsA, "value-for-A"}, {acctB, nsB, "value-for-B"}} {
		out, err := invoke(tc.who, "WorkersKvNamespaceReadKeyValuePair",
			map[string]any{"account_id": tc.who.Account, "namespace_id": tc.ns, "key_name": "k"})
		if err != nil {
			t.Fatalf("read in %s: %v", tc.who.Account, err)
		}
		raw, _ := out["_raw"].(string)
		if raw != tc.want {
			t.Fatalf("account %s read %q, want %q", tc.who.Account, raw, tc.want)
		}
	}
	// B addressing A's namespace id answers namespace-not-found, not A's
	// data: the id resolves inside B's own scope, where it does not exist.
	if _, err := invoke(acctB, "WorkersKvNamespaceReadKeyValuePair",
		map[string]any{"account_id": "acct-B", "namespace_id": nsA, "key_name": "k"}); err == nil {
		t.Fatal("account B read account A's namespace by id")
	} else if f, ok := err.(*spi.Fault); !ok || f.Code != "10013" {
		t.Fatalf("cross-account read: %v", err)
	}
	// Titles stay independent: deleting A's namespace frees the title only
	// in A; B still holds it (10014), and A can reuse it afterwards.
	if _, err := invoke(acctA, "WorkersKvNamespaceRemoveANamespace",
		map[string]any{"account_id": "acct-A", "namespace_id": nsA}); err != nil {
		t.Fatalf("remove in acct-A: %v", err)
	}
	if _, err := invoke(acctB, "WorkersKvNamespaceCreateANamespace",
		map[string]any{"account_id": "acct-B", "title": "shared-title"}); err == nil {
		t.Fatal("second title in acct-B while acct-B still holds it")
	} else if f, ok := err.(*spi.Fault); !ok || f.Code != "10014" {
		t.Fatalf("duplicate title in acct-B: %v", err)
	}
	if _, err := invoke(acctA, "WorkersKvNamespaceCreateANamespace",
		map[string]any{"account_id": "acct-A", "title": "shared-title"}); err != nil {
		t.Fatalf("title not freed by acct-A's own delete: %v", err)
	}
}

// TestCloudflareKeyListing covers CF-KV-PAGE's native half: prefix filtering
// returns exactly the matching keys in store order, listings never carry
// values, the answered cursor is empty (the generated model declares no
// pagination trait for this operation -- stated in the bundle's quirk) with
// a truthful count, and a delete is visible to the next listing immediately.
func TestCloudflareKeyListing(t *testing.T) {
	deps := spitest.Deps(t)
	pack, err := bundled.New("cloudflare.api", deps)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	who := spi.Identity{Account: "acct-list", Region: "us-east-1"}
	invoke := func(op string, in map[string]any) (map[string]any, error) {
		t.Helper()
		resp, err := pack.Invoke(ctx, &spi.Request{
			ServiceID: "cloudflare.api", Operation: op, Input: in, Identity: who,
		})
		if err != nil {
			return nil, err
		}
		return resp.Output, nil
	}
	out, err := invoke("WorkersKvNamespaceCreateANamespace",
		map[string]any{"account_id": "acct-list", "title": "listing"})
	if err != nil {
		t.Fatal(err)
	}
	ns, _ := out["result"].(map[string]any)["id"].(string)
	withKey := func(k, v string) map[string]any {
		m := map[string]any{"account_id": "acct-list", "namespace_id": ns,
			"key_name": k, "body": v}
		return m
	}
	listOut := func(prefix string) map[string]any {
		t.Helper()
		in := map[string]any{"account_id": "acct-list", "namespace_id": ns}
		if prefix != "" {
			in["prefix"] = prefix
		}
		out, err := invoke("WorkersKvNamespaceListANamespace'SKeys", in)
		if err != nil {
			t.Fatalf("list %q: %v", prefix, err)
		}
		return out
	}
	list := func(prefix string) []any {
		result, _ := listOut(prefix)["result"].([]any)
		return result
	}
	names := func(items []any) []string {
		var got []string
		for _, it := range items {
			m, _ := it.(map[string]any)
			name, _ := m["name"].(string)
			got = append(got, name)
			if _, leaked := m["value"]; leaked {
				t.Fatalf("listing leaked value for %q: %v", name, m)
			}
		}
		return got
	}
	// Interleaved prefixes; the store returns keys in order, so a/...
	// comes back as exactly the three, ordered.
	for _, k := range []string{"a/1", "b/1", "a/2", "c/1", "a/3"} {
		if _, err := invoke("WorkersKvNamespaceWriteKeyValuePairWithMetadata", withKey(k, "value-of-"+k)); err != nil {
			t.Fatalf("write %s: %v", k, err)
		}
	}
	if all := names(list("")); len(all) != 5 {
		t.Fatalf("unfiltered list: %v", all)
	}
	if prefA := names(list("a/")); len(prefA) != 3 || strings.Join(prefA, ",") != "a/1,a/2,a/3" {
		t.Fatalf("prefix a/: %v", prefA)
	}
	if prefB := names(list("b/")); len(prefB) != 1 || prefB[0] != "b/1" {
		t.Fatalf("prefix b/: %v", prefB)
	}
	// A prefix matching nothing is an empty page, not an error and not
	// every key.
	if got := names(list("zzz/")); len(got) != 0 {
		t.Fatalf("prefix zzz/: %v", got)
	}
	// result_info answers a truthful count for this page and an empty
	// cursor meaning "no more pages" (no pagination trait to continue from).
	info, _ := listOut("a/")["result_info"].(map[string]any)
	if info == nil {
		t.Fatal("no result_info")
	}
	if cursor, _ := info["cursor"].(string); cursor != "" {
		t.Fatalf("cursor %q, want empty (no pagination trait)", cursor)
	}
	if count, _ := info["count"].(float64); int(count) != 3 {
		t.Fatalf("count %v, want 3", info["count"])
	}
	// Mutation between listings is visible immediately: no snapshot
	// isolation is claimed, and none may appear to work.
	if _, err := invoke("WorkersKvNamespaceDeleteKeyValuePair", withKey("a/2", "")); err != nil {
		t.Fatalf("delete a/2: %v", err)
	}
	after := names(list("a/"))
	if len(after) != 2 || strings.Join(after, ",") != "a/1,a/3" {
		t.Fatalf("prefix a/ after delete: %v", after)
	}
	// Expiration between listings too: a key written with a 60s TTL and
	// then expired vanishes from the listing under the controlled clock.
	clk := deps.Clock.(*clock.Controllable)
	exp := withKey("a/4", "soon-gone")
	exp["expiration_ttl"] = float64(60)
	if _, err := invoke("WorkersKvNamespaceWriteKeyValuePairWithMetadata", exp); err != nil {
		t.Fatalf("write a/4: %v", err)
	}
	if prefA := names(list("a/")); len(prefA) != 3 {
		t.Fatalf("prefix a/ with live a/4: %v", prefA)
	}
	if err := clk.Advance(61 * time.Second); err != nil {
		t.Fatal(err)
	}
	prefA := names(list("a/"))
	if len(prefA) != 2 || strings.Join(prefA, ",") != "a/1,a/3" {
		t.Fatalf("prefix a/ after a/4 expired: %v", prefA)
	}
}
