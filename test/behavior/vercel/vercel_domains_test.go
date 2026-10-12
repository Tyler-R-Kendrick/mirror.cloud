package vercel

import (
	"net/http"
	"reflect"
	"strings"
	"testing"
)

// TestVercelProjectDomainsLifecycle holds what a client relies on when it puts
// a hostname on a project: the challenge it is told to publish, that a hostname
// is on one project at a time, and what verify and remove do.
func TestVercelProjectDomainsLifecycle(t *testing.T) {
	v := newVX(t, "vercel-domains")
	a := envProject(t, v, "site-a")
	b := envProject(t, v, "site-b")

	add := v.do(http.MethodPost, "/v10/projects/"+a+"/domains", map[string]any{"name": "WWW.Example.com"})
	if add.status != 200 {
		t.Fatalf("add = %d %s", add.status, add.raw)
	}
	if str(add.body, "name") != "www.example.com" || str(add.body, "apexName") != "example.com" || str(add.body, "projectId") != a {
		t.Fatalf("a hostname is lower case and has its apex: %s", add.raw)
	}
	if add.body["verified"] != false {
		t.Fatalf("a new domain is unverified: %s", add.raw)
	}
	chal := add.body["verification"].([]any)
	if len(chal) != 1 {
		t.Fatalf("one challenge: %s", add.raw)
	}
	c := chal[0].(map[string]any)
	if str(c, "type") != "TXT" || str(c, "domain") != "_vercel.example.com" ||
		!strings.HasPrefix(str(c, "value"), "vc-domain-verify=www.example.com,") ||
		str(c, "reason") != "Add the TXT record above to verify domain ownership" {
		t.Fatalf("challenge = %v", c)
	}

	t.Run("a hostname is on one project", func(t *testing.T) {
		for name, path := range map[string]string{"the same project": a, "another project": b} {
			r := v.do(http.MethodPost, "/v10/projects/"+path+"/domains", map[string]any{"name": "www.example.com"})
			if r.status != 409 || str(r.body["error"].(map[string]any), "code") != "domain_already_in_use" {
				t.Errorf("%s = %d %s", name, r.status, r.raw)
			}
		}
	})

	t.Run("a name that is not a hostname", func(t *testing.T) {
		for _, bad := range []string{"nodots", "-bad.example.com", "has space.com", "a..b.com", "https://example.com"} {
			r := v.do(http.MethodPost, "/v10/projects/"+a+"/domains", map[string]any{"name": bad})
			if r.status != 400 || str(r.body["error"].(map[string]any), "code") != "invalid_domain" {
				t.Errorf("%q = %d %s", bad, r.status, r.raw)
			}
		}
	})

	t.Run("a hostname under vercel.app is verified", func(t *testing.T) {
		r := v.do(http.MethodPost, "/v10/projects/"+a+"/domains", map[string]any{"name": "site-a.vercel.app"})
		if r.status != 200 || r.body["verified"] != true || len(r.body["verification"].([]any)) != 0 || str(r.body, "apexName") != "vercel.app" {
			t.Fatalf("vercel.app = %d %s", r.status, r.raw)
		}
	})

	t.Run("every spelling reads the one store", func(t *testing.T) {
		for _, p := range []string{
			"/v9/projects/" + a + "/domains/www.example.com",
			"/v10/projects/site-a/domains/WWW.example.com",
			"/v8/projects/" + a + "/domains/www.example.com",
		} {
			if r := v.do(http.MethodGet, p, nil); r.status != 200 || str(r.body, "name") != "www.example.com" {
				t.Errorf("get %s = %d %s", p, r.status, r.raw)
			}
		}
		if r := v.do(http.MethodGet, "/v9/projects/"+a+"/domains/nope.example.com", nil); r.status != 404 {
			t.Errorf("unknown = %d", r.status)
		}
		if r := v.do(http.MethodGet, "/v9/projects/prj_nope/domains", nil); r.status != 404 {
			t.Errorf("unknown project = %d", r.status)
		}
	})

	t.Run("listing is newest first and pages", func(t *testing.T) {
		names := func(path string) []string {
			r := v.do(http.MethodGet, path, nil)
			if r.status != 200 {
				t.Fatalf("list %s = %d %s", path, r.status, r.raw)
			}
			var out []string
			for _, d := range r.body["domains"].([]any) {
				out = append(out, str(d.(map[string]any), "name"))
			}
			return out
		}
		if got := names("/v9/projects/" + a + "/domains"); !reflect.DeepEqual(got, []string{"site-a.vercel.app", "www.example.com"}) {
			t.Errorf("default order = %v", got)
		}
		if got := names("/v10/projects/" + a + "/domains?order=ASC"); !reflect.DeepEqual(got, []string{"www.example.com", "site-a.vercel.app"}) {
			t.Errorf("ascending = %v", got)
		}
		page := v.do(http.MethodGet, "/v9/projects/"+a+"/domains?limit=1", nil)
		if p := page.body["pagination"].(map[string]any); p["count"] != float64(1) || p["next"] == nil {
			t.Errorf("a page of one of two has a next cursor: %s", page.raw)
		}
		if got := names("/v9/projects/" + a + "/domains?verified=true"); !reflect.DeepEqual(got, []string{"site-a.vercel.app"}) {
			t.Errorf("verified filter = %v", got)
		}
	})

	t.Run("an update sets a redirect and a branch, never both", func(t *testing.T) {
		u := v.do(http.MethodPatch, "/v9/projects/"+a+"/domains/www.example.com", map[string]any{"redirect": "example.com", "redirectStatusCode": 308})
		if u.status != 200 || str(u.body, "redirect") != "example.com" || u.body["redirectStatusCode"] != float64(308) {
			t.Fatalf("redirect = %d %s", u.status, u.raw)
		}
		for name, body := range map[string]map[string]any{
			"a branch beside the redirect": {"gitBranch": "main"},
			"a redirect to itself":         {"redirect": "www.example.com"},
			"a redirect that is no host":   {"redirect": "not a host"},
		} {
			if r := v.do(http.MethodPatch, "/v9/projects/"+a+"/domains/www.example.com", body); r.status != 400 {
				t.Errorf("%s = %d %s", name, r.status, r.raw)
			}
		}
		clear := v.do(http.MethodPatch, "/v10/projects/"+a+"/domains/www.example.com", map[string]any{"redirect": nil, "redirectStatusCode": nil, "gitBranch": "staging"})
		if clear.status != 200 || clear.body["redirect"] != nil || str(clear.body, "gitBranch") != "staging" {
			t.Fatalf("a null clears: %d %s", clear.status, clear.raw)
		}
		if got := v.do(http.MethodGet, "/v9/projects/"+a+"/domains?gitBranch=staging", nil); len(got.body["domains"].([]any)) != 1 {
			t.Errorf("branch filter = %s", got.raw)
		}
	})

	t.Run("verify takes the challenge as met", func(t *testing.T) {
		r := v.do(http.MethodPost, "/v9/projects/"+a+"/domains/www.example.com/verify", nil)
		if r.status != 200 || r.body["verified"] != true {
			t.Fatalf("verify = %d %s", r.status, r.raw)
		}
		g := v.do(http.MethodGet, "/v9/projects/"+a+"/domains/www.example.com", nil)
		if g.body["verified"] != true || len(g.body["verification"].([]any)) != 0 {
			t.Fatalf("after verify = %s", g.raw)
		}
		if again := v.do(http.MethodPost, "/v10/projects/"+a+"/domains/www.example.com/verify", nil); again.status != 200 {
			t.Fatalf("verify again = %d", again.status)
		}
		if nope := v.do(http.MethodPost, "/v9/projects/"+a+"/domains/nope.example.com/verify", nil); nope.status != 404 {
			t.Fatalf("verify unknown = %d", nope.status)
		}
	})

	t.Run("remove frees the hostname", func(t *testing.T) {
		if r := v.do(http.MethodDelete, "/v9/projects/"+a+"/domains/www.example.com", nil); r.status != 200 {
			t.Fatalf("remove = %d %s", r.status, r.raw)
		}
		if r := v.do(http.MethodDelete, "/v9/projects/"+a+"/domains/www.example.com", nil); r.status != 404 {
			t.Fatalf("remove again = %d", r.status)
		}
		if r := v.do(http.MethodPost, "/v10/projects/"+b+"/domains", map[string]any{"name": "www.example.com"}); r.status != 200 {
			t.Fatalf("another project takes it: %d %s", r.status, r.raw)
		}
	})

	t.Run("deleting a project frees its hostnames", func(t *testing.T) {
		if r := v.do(http.MethodDelete, "/v9/projects/"+b, nil); r.status != 204 {
			t.Fatalf("delete project = %d", r.status)
		}
		c := envProject(t, v, "site-c")
		if r := v.do(http.MethodPost, "/v10/projects/"+c+"/domains", map[string]any{"name": "www.example.com"}); r.status != 200 {
			t.Fatalf("the hostname is free: %d %s", r.status, r.raw)
		}
	})
}

// TestVercelAccountDomains holds the account's own domains: what adding one
// answers, the listing, and that the TXT record the account asks for is the one
// a project's domain asks for.
func TestVercelAccountDomains(t *testing.T) {
	v := newVX(t, "vercel-account-domains")

	add := v.do(http.MethodPost, "/v7/domains", map[string]any{"name": "Example.com"})
	if add.status != 200 {
		t.Fatalf("add = %d %s", add.status, add.raw)
	}
	d := add.body["domain"].(map[string]any)
	if str(d, "name") != "example.com" || !strings.HasPrefix(str(d, "id"), "dom_") || d["verified"] != false ||
		!reflect.DeepEqual(d["intendedNameservers"], []any{"ns1.vercel-dns.com", "ns2.vercel-dns.com"}) ||
		str(d, "serviceType") != "external" || d["teamId"] != nil {
		t.Fatalf("domain = %v", d)
	}
	if creator := d["creator"].(map[string]any); str(creator, "id") != str(d, "userId") {
		t.Fatalf("creator = %v", creator)
	}
	if dup := v.do(http.MethodPost, "/v7/domains", map[string]any{"name": "example.com"}); dup.status != 409 {
		t.Fatalf("duplicate = %d %s", dup.status, dup.raw)
	}
	if bad := v.do(http.MethodPost, "/v7/domains", map[string]any{"name": "nodots"}); bad.status != 400 {
		t.Fatalf("invalid = %d %s", bad.status, bad.raw)
	}
	v.do(http.MethodPost, "/v7/domains", map[string]any{"name": "second.dev"})

	t.Run("listing is newest first at every version", func(t *testing.T) {
		for _, p := range []string{"/v5/domains", "/v6/domains", "/v4/domains"} {
			r := v.do(http.MethodGet, p, nil)
			ds, _ := r.body["domains"].([]any)
			if r.status != 200 || len(ds) != 2 || str(ds[0].(map[string]any), "name") != "second.dev" {
				t.Errorf("%s = %d %s", p, r.status, r.raw)
			}
		}
		page := v.do(http.MethodGet, "/v5/domains?limit=1", nil)
		if p := page.body["pagination"].(map[string]any); p["next"] == nil {
			t.Errorf("a page of one of two has a next cursor: %s", page.raw)
		}
	})

	t.Run("one domain", func(t *testing.T) {
		for _, p := range []string{"/v5/domains/example.com", "/v6/domains/Example.com", "/v4/domains/example.com"} {
			r := v.do(http.MethodGet, p, nil)
			if r.status != 200 || str(r.body["domain"].(map[string]any), "name") != "example.com" || r.body["domain"].(map[string]any)["suffix"] != true {
				t.Errorf("%s = %d %s", p, r.status, r.raw)
			}
		}
		if r := v.do(http.MethodGet, "/v5/domains/nope.dev", nil); r.status != 404 {
			t.Errorf("unknown = %d", r.status)
		}
	})

	t.Run("patch sets what it names", func(t *testing.T) {
		r := v.do(http.MethodPatch, "/v3/domains/example.com", map[string]any{"renew": true, "customNameservers": []string{"ns1.example.net"}})
		if r.status != 200 || r.body["renew"] != true || r.body["echMode"] != "auto" ||
			!reflect.DeepEqual(r.body["customNameservers"], []any{"ns1.example.net"}) {
			t.Fatalf("patch = %d %s", r.status, r.raw)
		}
		if bad := v.do(http.MethodPatch, "/v3/domains/example.com", map[string]any{"op": "move-out", "destination": "x"}); bad.status != 400 {
			t.Fatalf("move-out = %d", bad.status)
		}
		if nope := v.do(http.MethodPatch, "/v3/domains/nope.dev", map[string]any{"renew": true}); nope.status != 404 {
			t.Fatalf("unknown = %d", nope.status)
		}
	})

	t.Run("the verification record is the project's challenge", func(t *testing.T) {
		rec := v.do(http.MethodGet, "/v9/domains/example.com/verification", nil)
		if rec.status != 200 || str(rec.body, "verificationDomain") != "_vercel.example.com" {
			t.Fatalf("record = %d %s", rec.status, rec.raw)
		}
		p := envProject(t, v, "site")
		added := v.do(http.MethodPost, "/v10/projects/"+p+"/domains", map[string]any{"name": "app.example.com"})
		// The record is for the hostname asked about; the project's challenge is for its own hostname.
		appRec := v.do(http.MethodGet, "/v9/domains/app.example.com/verification", nil)
		chal := added.body["verification"].([]any)[0].(map[string]any)
		if str(appRec.body, "txtRecord") != str(chal, "value") {
			t.Fatalf("the account's record %q is not the project's challenge %q", str(appRec.body, "txtRecord"), str(chal, "value"))
		}
		if nope := v.do(http.MethodGet, "/v9/domains/unheld.dev/verification", nil); nope.status != 404 {
			t.Fatalf("a domain nothing holds = %d", nope.status)
		}
	})

	t.Run("proving a hostname proves the apex", func(t *testing.T) {
		before := v.do(http.MethodGet, "/v5/domains/example.com", nil).body["domain"].(map[string]any)
		if before["verified"] != false {
			t.Fatalf("unverified at first: %v", before)
		}
		p := envProject(t, v, "site-two")
		if r := v.do(http.MethodPost, "/v10/projects/"+p+"/domains", map[string]any{"name": "www.example.com"}); r.body["verified"] != false {
			t.Fatalf("an unproven apex does not verify its hosts: %s", r.raw)
		}
		v.do(http.MethodPost, "/v9/projects/"+p+"/domains/www.example.com/verify", nil)
		after := v.do(http.MethodGet, "/v5/domains/example.com", nil).body["domain"].(map[string]any)
		if after["verified"] != true {
			t.Fatalf("verified once a host under it is: %v", after)
		}
		q := envProject(t, v, "site-three")
		if r := v.do(http.MethodPost, "/v10/projects/"+q+"/domains", map[string]any{"name": "shop.example.com"}); r.body["verified"] != true {
			t.Fatalf("a host under a proven apex is verified from the start: %s", r.raw)
		}
	})

	t.Run("config says what to publish and whether it is done", func(t *testing.T) {
		p := envProject(t, v, "site-four")
		v.do(http.MethodPost, "/v10/projects/"+p+"/domains", map[string]any{"name": "docs.example.org"})
		cfg := v.do(http.MethodGet, "/v6/domains/docs.example.org/config", nil)
		if cfg.status != 200 || cfg.body["misconfigured"] != true || cfg.body["configuredBy"] != nil {
			t.Fatalf("config = %d %s", cfg.status, cfg.raw)
		}
		if !reflect.DeepEqual(cfg.body["recommendedCNAME"], []any{map[string]any{"rank": float64(1), "value": "cname.vercel-dns.com"}}) ||
			!reflect.DeepEqual(cfg.body["recommendedIPv4"], []any{map[string]any{"rank": float64(1), "value": []any{"76.76.21.21"}}}) {
			t.Fatalf("recommendations = %s", cfg.raw)
		}
		v.do(http.MethodPost, "/v9/projects/"+p+"/domains/docs.example.org/verify", nil)
		cfg = v.do(http.MethodGet, "/v6/domains/docs.example.org/config", nil)
		if cfg.body["misconfigured"] != false || cfg.body["configuredBy"] != "CNAME" {
			t.Fatalf("configured = %s", cfg.raw)
		}
		apex := v.do(http.MethodGet, "/v6/domains/example.org/config", nil)
		if apex.body["misconfigured"] != true {
			t.Fatalf("a name no project holds is not configured: %s", apex.raw)
		}
	})

	t.Run("delete answers the id and takes the domain", func(t *testing.T) {
		id := str(v.do(http.MethodGet, "/v5/domains/second.dev", nil).body["domain"].(map[string]any), "id")
		for i, p := range []string{"/v6/domains/second.dev", "/v6/domains/second.dev"} {
			r := v.do(http.MethodDelete, p, nil)
			if i == 0 && (r.status != 200 || str(r.body, "uid") != id) {
				t.Fatalf("delete = %d %s", r.status, r.raw)
			}
			if i == 1 && r.status != 404 {
				t.Fatalf("delete again = %d", r.status)
			}
		}
		if v.do(http.MethodGet, "/v5/domains/second.dev", nil).status != 404 {
			t.Fatal("still there")
		}
	})
}

// TestVercelDomainsAreScopedToTheTeam holds that a team's domains are its own:
// the same hostname can be held by a team and by the personal account.
func TestVercelDomainsAreScopedToTheTeam(t *testing.T) {
	v := newVX(t, "vercel-team-domains")
	team := str(v.do(http.MethodPost, "/v1/teams", map[string]any{"slug": "acme"}).body, "id")
	if r := v.do(http.MethodPost, "/v7/domains?teamId="+team, map[string]any{"name": "acme.dev"}); r.status != 200 ||
		str(r.body["domain"].(map[string]any), "teamId") != team {
		t.Fatalf("team add = %d %s", r.status, r.raw)
	}
	if r := v.do(http.MethodGet, "/v5/domains", nil); len(r.body["domains"].([]any)) != 0 {
		t.Fatalf("the personal account sees the team's: %s", r.raw)
	}
	if r := v.do(http.MethodPost, "/v7/domains", map[string]any{"name": "acme.dev"}); r.status != 200 {
		t.Fatalf("the personal account may hold the same name: %d %s", r.status, r.raw)
	}
}
