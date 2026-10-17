package vercel

import (
	"net/http"
	"testing"
	"time"
)

// An alias names a hostname, and a hostname is the account's to name only if it
// holds the domain: its own, the apex it sits under, or a name one of its
// projects holds. The document says what happens otherwise ("404: The domain
// used for the alias was not found") and not under which code; the code is
// authored, `domain_not_found`, the one the CLI itself lists for a domain it
// cannot use (nextCommandsForDomainsAddFailure). A *.vercel.app name needs no
// domain: every project has one, and `vercel alias set` skips the domain lookup
// for it. Before this, an alias to any hostname at all was accepted.

func aliasAt(v *vx, address, deployment, host string, query ...string) reply {
	path := address
	switch address {
	case "v2":
		path = "/v2/deployments/" + deployment + "/aliases"
	case "v3":
		path = "/v3/deployments/" + deployment + "/aliases"
	case "now":
		path = "/now/deployments/" + deployment + "/aliases"
	}
	if len(query) > 0 {
		path += "?" + query[0]
	}
	return v.do(http.MethodPost, path, map[string]any{"alias": host})
}

func TestVercelAliasNeedsADomainTheAccountHolds(t *testing.T) {
	v := newVX(t, "vercel-aliasownership")
	v.do(http.MethodPost, "/v11/projects", map[string]any{"name": "site"})
	dep := deploy(t, v, "site", true)
	refused := func(t *testing.T, r reply, host string) {
		t.Helper()
		e := errorOf(r)
		if r.status != 404 || str(e, "code") != "domain_not_found" || str(e, "message") == "" {
			t.Fatalf("alias %s = %d %s, want 404 domain_not_found in the error envelope", host, r.status, r.raw)
		}
	}

	t.Run("Given a hostname the account does not hold When aliased at any address Then 404 domain_not_found and nothing is recorded", func(t *testing.T) {
		for _, address := range []string{"v2", "v3", "now"} {
			refused(t, aliasAt(v, address, dep, "www.unowned.dev"), "www.unowned.dev")
		}
		if g := v.do(http.MethodGet, "/v4/aliases/www.unowned.dev", nil); g.status != 404 {
			t.Errorf("the refused alias was recorded: %d %s", g.status, g.raw)
		}
		// Every deployment is an alias of its own URL; a refused one is not among them.
		l := v.do(http.MethodGet, "/v2/deployments/"+dep+"/aliases", nil)
		aliases, _ := l.body["aliases"].([]any)
		for _, a := range aliases {
			if str(a.(map[string]any), "alias") == "www.unowned.dev" {
				t.Errorf("the deployment names a refused alias: %s", l.raw)
			}
		}
	})

	t.Run("Given a *.vercel.app name When aliased Then it is accepted without any domain", func(t *testing.T) {
		if r := aliasAt(v, "v2", dep, "site-preview.vercel.app"); r.status != 200 {
			t.Fatalf("vercel.app alias = %d %s", r.status, r.raw)
		}
	})

	t.Run("Given the account holds a domain When aliased at it, under it, or below it Then accepted; another apex is not", func(t *testing.T) {
		if r := v.do(http.MethodPost, "/v7/domains", map[string]any{"name": "example.dev"}); r.status != 200 {
			t.Fatalf("add domain = %d %s", r.status, r.raw)
		}
		for _, host := range []string{"example.dev", "www.example.dev", "a.b.example.dev"} {
			if r := aliasAt(v, "v2", dep, host); r.status != 200 {
				t.Errorf("alias %s under a held domain = %d %s", host, r.status, r.raw)
			}
		}
		for _, host := range []string{"notexample.dev", "example.dev.evil.test", "www.example.org"} {
			refused(t, aliasAt(v, "v2", dep, host), host)
		}
	})

	t.Run("Given a project holds a hostname When aliased at that hostname Then accepted; its siblings are not", func(t *testing.T) {
		pid := str(v.do(http.MethodGet, "/v9/projects/site", nil).body, "id")
		if r := v.do(http.MethodPost, "/v10/projects/"+pid+"/domains", map[string]any{"name": "shop.owned.dev"}); r.status != 200 {
			t.Fatalf("add project domain = %d %s", r.status, r.raw)
		}
		if r := aliasAt(v, "v2", dep, "shop.owned.dev"); r.status != 200 {
			t.Errorf("alias at a project's own hostname = %d %s", r.status, r.raw)
		}
		refused(t, aliasAt(v, "v2", dep, "other.owned.dev"), "other.owned.dev")
	})

	t.Run("Given a project holds a hostname it has not verified When aliased at it Then accepted: holding is a record, not a proof of ownership", func(t *testing.T) {
		pid := str(v.do(http.MethodGet, "/v9/projects/site", nil).body, "id")
		added := v.do(http.MethodPost, "/v10/projects/"+pid+"/domains", map[string]any{"name": "www.google.com"})
		if added.status != 200 || added.body["verified"] != false {
			t.Fatalf("add project domain = %d %s, want an unverified one", added.status, added.raw)
		}
		if r := aliasAt(v, "v2", dep, "www.google.com"); r.status != 200 {
			t.Errorf("alias at an unverified project domain = %d %s: docs/VERCEL.md says it is accepted", r.status, r.raw)
		}
		// What the project holds is that name, not what lies beneath it.
		refused(t, aliasAt(v, "v2", dep, "mail.www.google.com"), "mail.www.google.com")
		refused(t, aliasAt(v, "v2", dep, "www.example.com"), "www.example.com")
	})

	t.Run("Given the domain is deleted When a new host under it is aliased Then it is refused again", func(t *testing.T) {
		if r := v.do(http.MethodDelete, "/v6/domains/example.dev", nil); r.status != 200 {
			t.Fatalf("delete domain = %d %s", r.status, r.raw)
		}
		refused(t, aliasAt(v, "v2", dep, "late.example.dev"), "late.example.dev")
	})

	t.Run("Given another team When it aliases a host under this account's domain Then it is refused", func(t *testing.T) {
		if r := v.do(http.MethodPost, "/v7/domains", map[string]any{"name": "mine.dev"}); r.status != 200 {
			t.Fatalf("add domain = %d %s", r.status, r.raw)
		}
		team := str(v.do(http.MethodPost, "/v2/teams", map[string]any{"slug": "rivals"}).body, "id")
		q := "teamId=" + team
		made := v.do(http.MethodPost, "/v13/deployments?"+q, map[string]any{"name": "rival-site", "files": []map[string]any{{"file": "index.html", "data": "hi"}}})
		if made.status != 200 {
			t.Fatalf("team deploy = %d %s", made.status, made.raw)
		}
		v.advance(3 * time.Second)
		refused(t, aliasAt(v, "v2", str(made.body, "id"), "www.mine.dev", q), "www.mine.dev")
		if r := aliasAt(v, "v2", dep, "www.mine.dev"); r.status != 200 {
			t.Errorf("the account that holds the domain = %d %s", r.status, r.raw)
		}
	})
}
