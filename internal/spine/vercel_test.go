package spine

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/config"
	rtpkg "github.com/tyler-r-kendrick/mirror.cloud/internal/runtime"
)

func TestBootedServerVercelAPI(t *testing.T) {
	cfg := config.Default()
	// Two services now, where one registration used to carry both products.
	cfg.Services = []string{"vercel.api", "vercel.kv"}
	cfg.Seed = "vercel-1"
	rt, err := rtpkg.Boot(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(rt.Handler())
	defer ts.Close()
	do := func(method, path, body, host string) (int, map[string]any, http.Header) {
		t.Helper()
		var rdr io.Reader
		if body != "" {
			rdr = strings.NewReader(body)
		}
		req, err := http.NewRequest(method, ts.URL+path, rdr)
		if err != nil {
			t.Fatal(err)
		}
		if host == "" {
			host = "api.vercel.com"
		}
		req.Host = host
		req.Header.Set("Authorization", "Bearer test")
		req.Header.Set("Content-Type", "application/json")
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		b, _ := io.ReadAll(res.Body)
		res.Body.Close()
		m := map[string]any{}
		_ = json.Unmarshal(b, &m)
		return res.StatusCode, m, res.Header
	}
	// The identity sits under `user`, which is the one member the document
	// declares for this response; the pack answered its four members at the
	// top level.
	code, body, _ := do(http.MethodGet, "/v2/user", "", "")
	user, _ := body["user"].(map[string]any)
	if code != 200 || user["username"] != "test" {
		t.Fatalf("user %d %#v", code, body)
	}
	code, prj, _ := do(http.MethodPost, "/v11/projects", `{"name":"app"}`, "")
	if code != 200 || prj["name"] != "app" {
		t.Fatalf("create %d %#v", code, prj)
	}
	code, got, _ := do(http.MethodGet, "/v9/projects/app", "", "")
	if code != 200 || got["id"] != prj["id"] {
		t.Fatalf("get %d %#v", code, got)
	}
	code, dpl, _ := do(http.MethodPost, "/v13/deployments", `{"name":"app","project":"app"}`, "")
	if code != 200 || dpl["readyState"] != "READY" {
		t.Fatalf("deploy %d %#v", code, dpl)
	}
	// Versioning is restored. The pack's route table stripped the leading
	// version segment before matching, so every version of a path answered;
	// each operation now binds to the one version its document declares.
	// Listing projects is /v10, and /v9/projects is a project named
	// "projects"... no: /v9/projects/{idOrName} needs a label, so a bare
	// /v9/projects matches nothing this service serves.
	code, listed, _ := do(http.MethodGet, "/v10/projects", "", "")
	if code != 200 {
		t.Fatalf("list at the declared version %d %#v", code, listed)
	}
	if code, _, _ := do(http.MethodGet, "/v99/projects", "", ""); code == 200 {
		t.Fatal("an undeclared version still answered; the pack's version-stripping is back")
	}
	code, kv, _ := do(http.MethodPost, "/", `["SET","k","v"]`, "id.kv.vercel-storage.com")
	if code != 200 || kv["result"] != "OK" {
		t.Fatalf("kv set %d %#v", code, kv)
	}
	code, kv, _ = do(http.MethodPost, "/", `["GET","k"]`, "id.kv.vercel-storage.com")
	if code != 200 || kv["result"] != "v" {
		t.Fatalf("kv get %d %#v", code, kv)
	}
	code, kv, _ = do(http.MethodPost, "/", `["DEL","k"]`, "id.kv.vercel-storage.com")
	if code != 200 || kv["result"] != float64(1) {
		t.Fatalf("kv del %d %#v", code, kv)
	}
	code, kv, _ = do(http.MethodPost, "/", `["GET","k"]`, "id.kv.vercel-storage.com")
	if code != 200 || kv["result"] != nil {
		t.Fatalf("kv get after del %d %#v", code, kv)
	}
	// An unimplemented verb is 501 with the header that says so, not a 400:
	// INCR is a real command this emulator does not serve, and a client can
	// tell that from a malformed request without reading the message. The
	// error is a plain string, which is what the KV document declares and what
	// Upstash answers -- not the REST API's {error: {code, message}}.
	code, kv, h := do(http.MethodPost, "/", `["INCR","k"]`, "id.kv.vercel-storage.com")
	if code != 501 || h.Get("x-mirror-not-implemented") == "" {
		t.Fatalf("kv unsupported verb %d %#v %v", code, kv, h)
	}
	if _, isString := kv["error"].(string); !isString {
		t.Fatalf("kv error envelope %#v", kv)
	}
	code, missing, h := do(http.MethodGet, "/v9/projects/missing", "", "")
	if code != 404 || h.Get("x-amzn-errortype") != "" {
		t.Fatalf("missing %d %#v %s", code, h, missing)
	}
	errObj, _ := missing["error"].(map[string]any)
	if errObj["code"] != "not_found" {
		t.Fatalf("error shape %#v", missing)
	}
	code, missing, h = do(http.MethodDelete, "/v9/projects/missing", "", "")
	if code != 404 || h.Get("x-amzn-errortype") != "" {
		t.Fatalf("delete missing %d %#v %s", code, h, missing)
	}
	errObj, _ = missing["error"].(map[string]any)
	if errObj["code"] != "not_found" {
		t.Fatalf("delete missing shape %#v", missing)
	}
}
