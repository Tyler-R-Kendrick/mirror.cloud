package vercel

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/config"
	rtpkg "github.com/tyler-r-kendrick/mirror.cloud/internal/runtime"

	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/bundled"
)

// TestVercelAuthSupplementBehavior drives the routes the vendored document
// never declared and the vendor's own emulator serves: teams create, members,
// user patch, the registration probe, the v6 deployment listing, get-one env,
// the API-key lifecycle, and the local OAuth flow end to end. The authored
// supplement is specs/vercel/api-extra.json; expectations follow
// vercel-labs/emulate's user.go/api-keys.go/oauth.go.
func TestVercelAuthSupplementBehavior(t *testing.T) {
	cfg := config.Default()
	cfg.Services = []string{"vercel.api"}
	cfg.Seed = "vercel-auth-bdd"
	rt, err := rtpkg.Boot(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(rt.Handler())
	defer ts.Close()

	call := func(method, path, body, contentType string) (int, []byte, http.Header) {
		t.Helper()
		var rdr io.Reader
		if body != "" {
			rdr = strings.NewReader(body)
		}
		req, err := http.NewRequest(method, ts.URL+path, rdr)
		if err != nil {
			t.Fatal(err)
		}
		req.Host = "api.vercel.com"
		req.Header.Set("Authorization", "Bearer test")
		if contentType != "" {
			req.Header.Set("Content-Type", contentType)
		}
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		b, _ := io.ReadAll(res.Body)
		res.Body.Close()
		return res.StatusCode, b, res.Header
	}

	t.Run("Given a team When created Then listed patched and its members read", func(t *testing.T) {
		code, raw, _ := call(http.MethodPost, "/v2/teams", `{"slug":"acme"}`, "application/json")
		if code != 200 || !strings.Contains(string(raw), `"slug":"acme"`) || !strings.Contains(string(raw), `"name":"acme"`) {
			t.Fatalf("create team %d %s", code, raw)
		}
		code, raw, _ = call(http.MethodPost, "/v2/teams", `{"slug":"acme"}`, "application/json")
		if code != 409 || !strings.Contains(string(raw), "team_slug_already_exists") {
			t.Fatalf("dup team %d %s", code, raw)
		}
		code, raw, _ = call(http.MethodGet, "/v2/teams", "", "")
		if code != 200 || !strings.Contains(string(raw), `"acme"`) {
			t.Fatalf("list teams %d %s", code, raw)
		}
		code, raw, _ = call(http.MethodGet, "/v2/teams/acme/members", "", "")
		if code != 200 || !strings.Contains(string(raw), `"username":"test"`) || !strings.Contains(string(raw), `"role":"OWNER"`) {
			t.Fatalf("members by slug %d %s", code, raw)
		}
		code, raw, _ = call(http.MethodPatch, "/v2/teams/acme", `{"name":"Acme Inc"}`, "application/json")
		if code != 200 || !strings.Contains(string(raw), `"name":"Acme Inc"`) {
			t.Fatalf("patch team by slug %d %s", code, raw)
		}
	})

	t.Run("Given no token When a supplemented route is called Then 401", func(t *testing.T) {
		req, _ := http.NewRequest(http.MethodGet, ts.URL+"/v1/api-keys", nil)
		req.Host = "api.vercel.com"
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		raw, _ := io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != 401 || !strings.Contains(string(raw), "not_authenticated") {
			t.Fatalf("unauthenticated %d %s", res.StatusCode, raw)
		}
	})

	t.Run("Given an API key When listed and deleted Then the lifecycle answers", func(t *testing.T) {
		code, raw, _ := call(http.MethodPost, "/v1/api-keys", `{"name":"ci"}`, "application/json")
		if code != 200 || !strings.Contains(string(raw), `"apiKeyString":"vercel_api_`) || !strings.Contains(string(raw), `"name":"ci"`) {
			t.Fatalf("create key %d %s", code, raw)
		}
		code, raw, _ = call(http.MethodGet, "/v1/api-keys", "", "")
		if code != 200 || !strings.Contains(string(raw), `"ci"`) {
			t.Fatalf("list keys %d %s", code, raw)
		}
		code, raw, _ = call(http.MethodDelete, "/v1/api-keys/ak_ghost", "", "")
		if code != 404 || !strings.Contains(string(raw), "not_found") {
			t.Fatalf("delete ghost %d %s", code, raw)
		}
	})

	t.Run("Given a patched user When read Then the patch shows", func(t *testing.T) {
		code, raw, _ := call(http.MethodPatch, "/v2/user", `{"name":"Pat"}`, "application/json")
		if code != 200 || !strings.Contains(string(raw), `"name":"Pat"`) {
			t.Fatalf("patch user %d %s", code, raw)
		}
		code, raw, _ = call(http.MethodGet, "/v2/user", "", "")
		if code != 200 || !strings.Contains(string(raw), `"name":"Pat"`) {
			t.Fatalf("get after patch %d %s", code, raw)
		}
	})

	t.Run("Given the probes When registration and v6 list are read Then they answer", func(t *testing.T) {
		code, raw, _ := call(http.MethodGet, "/registration", "", "")
		if code != 200 || !strings.Contains(string(raw), `"registration":false`) {
			t.Fatalf("registration %d %s", code, raw)
		}
		code, raw, _ = call(http.MethodGet, "/v6/deployments", "", "")
		if code != 200 || !strings.Contains(string(raw), `"deployments"`) {
			t.Fatalf("v6 deployments %d %s", code, raw)
		}
	})

	t.Run("Given the OAuth flow When driven end to end Then a token answers userinfo", func(t *testing.T) {
		code, raw, _ := call(http.MethodGet, "/oauth/authorize?client_id=client_x&redirect_uri=http://localhost:3000/cb&state=s9", "", "")
		if code != 200 || !strings.Contains(string(raw), "Continue as test") {
			t.Fatalf("authorize %d %s", code, raw)
		}
		// The callback is a form post; the answer is the redirect carrying the code.
		form := "username=test&redirect_uri=http%3A%2F%2Flocalhost%3A3000%2Fcb&state=s9&client_id=client_x"
		req, _ := http.NewRequest(http.MethodPost, ts.URL+"/oauth/authorize/callback", strings.NewReader(form))
		req.Host = "api.vercel.com"
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		noRedirect := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
		res, err := noRedirect.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		res.Body.Close()
		if res.StatusCode != 302 {
			t.Fatalf("callback %d", res.StatusCode)
		}
		loc := res.Header.Get("Location")
		if !strings.Contains(loc, "code=") || !strings.Contains(loc, "state=s9") {
			t.Fatalf("callback location %q", loc)
		}
		authCode := strings.Split(strings.Split(loc, "code=")[1], "&")[0]

		// The token exchange is a form post too; the envelope for its faults
		// is RFC 6749's, not Vercel's.
		code2, raw, _ := call(http.MethodPost, "/login/oauth/token", "code=deadbeef", "application/x-www-form-urlencoded")
		if code2 != 400 || !strings.Contains(string(raw), `"error":"invalid_grant"`) {
			t.Fatalf("token bad code %d %s", code2, raw)
		}
		code2, raw, _ = call(http.MethodPost, "/login/oauth/token", "code="+authCode+"&redirect_uri=http%3A%2F%2Flocalhost%3A3000%2Fcb&client_id=client_x", "application/x-www-form-urlencoded")
		if code2 != 200 || !strings.Contains(string(raw), `"access_token":"vercel_`) || !strings.Contains(string(raw), `"token_type":"Bearer"`) {
			t.Fatalf("token %d %s", code2, raw)
		}
		tok := strings.Split(strings.Split(string(raw), `"access_token":"`)[1], `"`)[0]

		// Codes are single-use.
		code2, raw, _ = call(http.MethodPost, "/login/oauth/token", "code="+authCode, "application/x-www-form-urlencoded")
		if code2 != 400 {
			t.Fatalf("replayed code %d %s", code2, raw)
		}

		code2, raw, _ = call(http.MethodGet, "/login/oauth/userinfo", "", "")
		_ = raw
		if code2 != 401 {
			t.Fatalf("userinfo without a token should be 401, got %d", code2)
		}
		req, _ = http.NewRequest(http.MethodGet, ts.URL+"/login/oauth/userinfo", nil)
		req.Host = "api.vercel.com"
		req.Header.Set("Authorization", "Bearer "+tok)
		res, err = http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		raw, _ = io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != 200 || !strings.Contains(string(raw), `"preferred_username":"test"`) {
			t.Fatalf("userinfo %d %s", res.StatusCode, raw)
		}
	})
}
