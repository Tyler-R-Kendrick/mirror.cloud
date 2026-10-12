package vercel

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/config"
)

// The emulator's covenant is that any credentials work, none included, unless it
// is started with MIRROR_VERCEL_STRICT_AUTH, which refuses the way Vercel does
// (403 forbidden, with invalidToken for a token that is not one). Six routes the
// vendor's own emulator serves (CreateTeam, ListTeamMembers, UpdateAuthUser and the
// three API-key routes) predated that and answered their own 401 to a request with
// no Authorization at all, which no Vercel client expects and which strict mode
// does not use. These tests hold every one of them to the covenant, with every
// kind of credential, in both modes.

type covenantRoute struct {
	name   string
	method string
	// build answers the path and body for one call; n makes each call's names unique.
	build func(t *testing.T, v *vx, n int) (path string, body any)
	want  int
}

func covenantRoutes() []covenantRoute {
	return []covenantRoute{
		{name: "CreateTeam", method: http.MethodPost, want: 200,
			build: func(t *testing.T, v *vx, n int) (string, any) {
				return "/v2/teams", map[string]any{"slug": fmt.Sprintf("covenant-team-%d", n)}
			}},
		{name: "ListTeamMembers", method: http.MethodGet, want: 200,
			build: func(t *testing.T, v *vx, n int) (string, any) {
				slug := fmt.Sprintf("covenant-members-%d", n)
				if r := v.do(http.MethodPost, "/v2/teams", map[string]any{"slug": slug}); r.status != 200 {
					t.Fatalf("set up a team = %d %s", r.status, r.raw)
				}
				return "/v2/teams/" + slug + "/members", nil
			}},
		{name: "UpdateAuthUser", method: http.MethodPatch, want: 200,
			build: func(t *testing.T, v *vx, n int) (string, any) {
				return "/v2/user", map[string]any{"name": fmt.Sprintf("Covenant %d", n)}
			}},
		{name: "CreateApiKey", method: http.MethodPost, want: 200,
			build: func(t *testing.T, v *vx, n int) (string, any) {
				return "/v1/api-keys", map[string]any{"name": fmt.Sprintf("covenant-%d", n)}
			}},
		{name: "ListApiKeys", method: http.MethodGet, want: 200,
			build: func(t *testing.T, v *vx, n int) (string, any) { return "/v1/api-keys", nil }},
		{name: "DeleteApiKey", method: http.MethodDelete, want: 200,
			build: func(t *testing.T, v *vx, n int) (string, any) {
				made := v.do(http.MethodPost, "/v1/api-keys", map[string]any{"name": fmt.Sprintf("to-delete-%d", n)})
				key, _ := made.body["apiKey"].(map[string]any)
				if made.status != 200 || str(key, "id") == "" {
					t.Fatalf("set up a key = %d %s", made.status, made.raw)
				}
				return "/v1/api-keys/" + str(key, "id"), nil
			}},
	}
}

type covenantCredential struct {
	name   string
	header string // "" removes the Authorization header
	// strict is the status strict mode answers; 0 is the route's own.
	strict      int
	invalidFlag bool
}

var covenantCredentials = []covenantCredential{
	{name: "no Authorization header", header: "", strict: 403},
	{name: "an arbitrary bearer token", header: "Bearer anything"},
	{name: "a bearer token that starts with invalid", header: "Bearer invalid-token", strict: 403, invalidFlag: true},
	{name: "basic credentials", header: "Basic dGVzdDp0ZXN0", strict: 403},
}

func TestVercelOracleRoutesFollowTheCredentialCovenant(t *testing.T) {
	strict := withConfig(func(c *config.Config) { c.VercelStrictAuth = true })
	for _, mode := range []struct {
		name string
		opts []func(*bootOptions)
	}{{"default mode", nil}, {"strict mode", []func(*bootOptions){strict}}} {
		mode := mode
		isStrict := len(mode.opts) > 0
		v := newVXWith(t, "vercel-covenant", []string{"vercel.api"}, mode.opts...)
		n := 0
		for _, route := range covenantRoutes() {
			for _, cred := range covenantCredentials {
				route, cred := route, cred
				t.Run(fmt.Sprintf("Given %s When %s is called with %s Then %s", mode.name, route.name, cred.name, expectation(isStrict, cred, route)), func(t *testing.T) {
					n++
					path, body := route.build(t, v, n)
					r := v.do(route.method, path, body, "Authorization", cred.header)
					if r.status == 401 {
						t.Fatalf("%s %s answered 401 %s: the covenant is that any credentials work (strict mode refuses with 403)", route.method, path, r.raw)
					}
					want := route.want
					if isStrict && cred.strict != 0 {
						want = cred.strict
					}
					if r.status != want {
						t.Fatalf("%s %s = %d %s, want %d", route.method, path, r.status, r.raw, want)
					}
					if isStrict && cred.strict == 403 {
						e := errorOf(r)
						if str(e, "code") != "forbidden" {
							t.Errorf("strict refusal is not Vercel's forbidden envelope: %s", r.raw)
						}
						if _, flagged := e["invalidToken"]; flagged != cred.invalidFlag {
							t.Errorf("invalidToken present = %v, want %v: %s", flagged, cred.invalidFlag, r.raw)
						}
					}
				})
			}
		}
	}
}

func expectation(strict bool, cred covenantCredential, route covenantRoute) string {
	if strict && cred.strict != 0 {
		return fmt.Sprintf("%d forbidden", cred.strict)
	}
	return fmt.Sprintf("it is served (%d)", route.want)
}

// TestVercelCreateTeamWithNoSlugIsABadRequest holds the answer the missing 401 used
// to hide: a body with no slug is the 400 the rule says, with or without a
// credential, and not an evaluation error.
func TestVercelCreateTeamWithNoSlugIsABadRequest(t *testing.T) {
	v := newVX(t, "vercel-covenant-slug")
	for _, auth := range []string{"", "Bearer anything"} {
		for _, body := range []any{map[string]any{}, map[string]any{"slug": ""}, map[string]any{"slug": "   "}, map[string]any{"slug": nil}} {
			r := v.do(http.MethodPost, "/v2/teams", body, "Authorization", auth)
			if r.status != 400 || str(errorOf(r), "code") != "bad_request" {
				t.Errorf("POST /v2/teams %v with %q = %d %s, want 400 bad_request", body, auth, r.status, r.raw)
			}
		}
	}
}
