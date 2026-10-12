package vercel

import (
	"net/http"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/config"
)

// errorOf reads the object under a reply's "error" member.
func errorOf(r reply) map[string]any {
	e, _ := r.body["error"].(map[string]any)
	return e
}

// TestVercelCredentialsAreAcceptedUnlessStrictIsAsked pins the covenant's side
// of authentication first, then the opt-in that lets a client see Vercel's
// refusals.
func TestVercelCredentialsAreAcceptedUnlessStrictIsAsked(t *testing.T) {
	t.Run("Given the default configuration When no token is sent Then the request is served", func(t *testing.T) {
		v := newVX(t, "vercel-open")
		if r := v.do(http.MethodGet, "/v2/user", nil, "Authorization", ""); r.status != 200 {
			t.Fatalf("no credential = %d %s, want 200: any credentials work, none included", r.status, r.raw)
		}
	})

	strict := withConfig(func(c *config.Config) { c.VercelStrictAuth = true })
	v := newVXWith(t, "vercel-strict", []string{"vercel.api"}, strict)

	t.Run("Given strict auth When no token is sent Then Vercel's 403 forbidden answers", func(t *testing.T) {
		r := v.do(http.MethodGet, "/v2/user", nil, "Authorization", "")
		e := errorOf(r)
		if r.status != 403 || str(e, "code") != "forbidden" || str(e, "message") != "Not authenticated" {
			t.Fatalf("no token = %d %s", r.status, r.raw)
		}
		if _, flagged := e["invalidToken"]; flagged {
			t.Fatalf("a missing token is not an invalid one: %s", r.raw)
		}
	})
	t.Run("Given strict auth When the scheme is not Bearer Then it is refused as missing", func(t *testing.T) {
		if r := v.do(http.MethodGet, "/v2/user", nil, "Authorization", "Basic dGVzdDp0ZXN0"); r.status != 403 {
			t.Fatalf("basic credentials = %d %s", r.status, r.raw)
		}
	})
	t.Run("Given strict auth When the token is invalid Then the 403 says so", func(t *testing.T) {
		r := v.do(http.MethodGet, "/v2/user", nil, "Authorization", "Bearer invalid-token")
		e := errorOf(r)
		if r.status != 403 || str(e, "code") != "forbidden" || e["invalidToken"] != true {
			t.Fatalf("invalid token = %d %s", r.status, r.raw)
		}
	})
	t.Run("Given strict auth When any other token is sent Then the request is served", func(t *testing.T) {
		if r := v.do(http.MethodGet, "/v2/user", nil, "Authorization", "Bearer anything"); r.status != 200 {
			t.Fatalf("a token = %d %s", r.status, r.raw)
		}
	})
}

// TestVercelControlRoutesExistOnlyWhenEnabled holds the gate the task asks for:
// a route that breaks the emulator on purpose is not there to be called by
// accident.
func TestVercelControlRoutesExistOnlyWhenEnabled(t *testing.T) {
	t.Run("Given the default configuration When a control route is called Then there is none", func(t *testing.T) {
		v := newVX(t, "vercel-faults-off")
		for _, path := range []string{"/_mirror/vercel/rate-limit", "/_mirror/vercel/deployments/fail-next", "/_mirror/vercel/deployments/dpl_x/state"} {
			if r := v.do(http.MethodPost, path, map[string]any{"requests": 1}); r.status != 404 {
				t.Errorf("POST %s = %d %s, want 404 while faults are not enabled", path, r.status, r.raw)
			}
		}
	})

	faults := withConfig(func(c *config.Config) { c.VercelFaults = true })
	v := newVXWith(t, "vercel-faults-on", []string{"vercel.api"}, faults)

	t.Run("Given faults are enabled When the limit is armed Then that many requests answer 429", func(t *testing.T) {
		if r := v.do(http.MethodPost, "/_mirror/vercel/rate-limit", map[string]any{"requests": 2, "retryAfter": 7}, "Authorization", ""); r.status != 200 {
			t.Fatalf("arm = %d %s: the control route takes no credential", r.status, r.raw)
		}
		for i := 0; i < 2; i++ {
			r := v.do(http.MethodGet, "/v2/user", nil)
			e := errorOf(r)
			if r.status != 429 || str(e, "code") != "rate_limited" {
				t.Fatalf("request %d = %d %s, want the rate-limit envelope", i, r.status, r.raw)
			}
			limit, _ := e["limit"].(map[string]any)
			if limit["remaining"] != float64(0) || limit["total"] != float64(2) || limit["resetMs"] == nil {
				t.Errorf("limit = %v", limit)
			}
			if r.header.Get("Retry-After") != "7" || r.header.Get("X-RateLimit-Remaining") != "0" || r.header.Get("X-RateLimit-Reset") == "" {
				t.Errorf("headers = %v", r.header)
			}
		}
		if r := v.do(http.MethodGet, "/v2/user", nil); r.status != 200 {
			t.Fatalf("after the limit = %d %s", r.status, r.raw)
		}
	})
	t.Run("Given an armed limit When it is disarmed Then requests are served", func(t *testing.T) {
		v.do(http.MethodPost, "/_mirror/vercel/rate-limit", map[string]any{"requests": 5})
		if r := v.do(http.MethodDelete, "/_mirror/vercel/rate-limit", nil); r.status != 204 {
			t.Fatalf("disarm = %d %s", r.status, r.raw)
		}
		if r := v.do(http.MethodGet, "/v2/user", nil); r.status != 200 {
			t.Fatalf("after disarm = %d %s", r.status, r.raw)
		}
	})
	t.Run("Given a bad arm request When it is sent Then it is a 400", func(t *testing.T) {
		if r := v.do(http.MethodPost, "/_mirror/vercel/rate-limit", map[string]any{"requests": 0}); r.status != 400 {
			t.Fatalf("zero requests = %d %s", r.status, r.raw)
		}
	})
}

// TestVercelTeamScopeIsolatesWhatATeamOwns holds the teamId rule: the same
// request, with and without a team, acts on different accounts.
func TestVercelTeamScopeIsolatesWhatATeamOwns(t *testing.T) {
	v := newVX(t, "vercel-scope")
	create := func(team, name string) reply {
		path := "/v11/projects"
		if team != "" {
			path += "?teamId=" + team
		}
		return v.do(http.MethodPost, path, map[string]any{"name": name})
	}

	mine := create("", "shared-name")
	a := create("team_a", "shared-name")
	if mine.status != 200 || a.status != 200 {
		t.Fatalf("one name in two accounts: personal %d %s, team %d %s", mine.status, mine.raw, a.status, a.raw)
	}
	if dup := create("team_a", "shared-name"); dup.status != 409 {
		t.Fatalf("a name twice in one team = %d %s, want 409", dup.status, dup.raw)
	}
	if str(mine.body, "id") == str(a.body, "id") {
		t.Fatalf("two projects share an id: %s", a.raw)
	}
	if str(a.body, "accountId") != "team_a" {
		t.Errorf("a team's project is owned by %q, want the team", str(a.body, "accountId"))
	}

	list := func(team string) int {
		path := "/v10/projects"
		if team != "" {
			path += "?teamId=" + team
		}
		r := v.do(http.MethodGet, path, nil)
		projects, _ := r.body["projects"].([]any)
		return len(projects)
	}
	if list("") != 1 || list("team_a") != 1 || list("team_b") != 0 {
		t.Fatalf("listings: personal %d, team_a %d, team_b %d; want 1, 1, 0", list(""), list("team_a"), list("team_b"))
	}
	if r := v.do(http.MethodGet, "/v9/projects/shared-name?teamId=team_b", nil); r.status != 404 {
		t.Fatalf("another team's lookup = %d %s, want 404", r.status, r.raw)
	}
	if r := v.do(http.MethodGet, "/v9/projects/"+str(a.body, "id"), nil); r.status != 404 {
		t.Fatalf("a team's project by id without the team = %d %s, want 404", r.status, r.raw)
	}
	if r := v.do(http.MethodGet, "/v9/projects/shared-name?slug=nobody", nil); r.status != 403 {
		t.Fatalf("an unknown slug = %d %s, want 403", r.status, r.raw)
	}
}
