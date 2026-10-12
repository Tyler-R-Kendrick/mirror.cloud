package vercel

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"
)

// `vercel domains add <domain> <project>` does not use the document's
// AddProjectDomain. vercel 63.1.2 posts the older
//
//	POST /projects/{project}/alias   {"target": "PRODUCTION", "domain": "<host>"}
//
// reads the host back out of an array of alias targets, and keys on the code
// ALIAS_DOMAIN_EXIST (with the project that holds the host) to say "already
// assigned" or, under --force, to remove it there and add it here. `domains
// inspect` and `domains rm` list what a domain's projects hold with the
// document's GET /v1/domains/{domain}/project-domains, and `domains rm` deletes
// at DELETE /v3/domains/{domain}. The tests hold the answers to what the CLI
// does with them.

func newProjectNamed(t *testing.T, v *vx, name string) string {
	t.Helper()
	r := v.do(http.MethodPost, "/v11/projects", map[string]any{"name": name})
	if r.status != 200 {
		t.Fatalf("create project %s = %d %s", name, r.status, r.raw)
	}
	return str(r.body, "id")
}

// addAlias posts what the CLI posts and returns the answer.
func addAlias(v *vx, project, host string) reply {
	return v.do(http.MethodPost, "/projects/"+project+"/alias", map[string]any{"target": "PRODUCTION", "domain": host})
}

func TestVercelProjectAliasAddsADomainTheWayTheCLIDoes(t *testing.T) {
	v := newVX(t, "vercel-projectalias")
	pid := newProjectNamed(t, v, "alias-app")

	t.Run("Given a custom domain When it is added Then the answer is an array holding that host", func(t *testing.T) {
		r := addAlias(v, "alias-app", "www.example.dev")
		if r.status != 200 {
			t.Fatalf("add = %d %s", r.status, r.raw)
		}
		var targets []map[string]any
		if err := unmarshalJSON(r.raw, &targets); err != nil {
			t.Fatalf("the answer is not an array: %s", r.raw)
		}
		found := false
		for _, a := range targets {
			if str(a, "domain") == "www.example.dev" {
				found = true
			}
		}
		if !found {
			t.Fatalf("no target for www.example.dev in %s: the CLI reports 'did not return a matching alias' without one", r.raw)
		}
	})

	t.Run("Given that domain When the project is read Then it is a domain of the project, unverified", func(t *testing.T) {
		r := v.do(http.MethodGet, "/v9/projects/"+pid+"/domains/www.example.dev", nil)
		if r.status != 200 || r.body["projectId"] != pid || r.body["verified"] != false {
			t.Fatalf("project domain = %d %s", r.status, r.raw)
		}
	})

	t.Run("Given a vercel.app host When it is added Then it is verified from the start", func(t *testing.T) {
		r := addAlias(v, pid, "alias-app-two.vercel.app")
		if r.status != 200 {
			t.Fatalf("add = %d %s", r.status, r.raw)
		}
		got := v.do(http.MethodGet, "/v9/projects/"+pid+"/domains/alias-app-two.vercel.app", nil)
		if got.body["verified"] != true {
			t.Fatalf("a vercel.app domain is verified: %s", got.raw)
		}
	})

	t.Run("Given a host that is not a domain When it is added Then 400 invalid_domain", func(t *testing.T) {
		r := addAlias(v, pid, "nodots")
		if r.status != 400 || str(errorOf(r), "code") != "invalid_domain" {
			t.Fatalf("add nodots = %d %s", r.status, r.raw)
		}
	})

	t.Run("Given an unknown project When a domain is added Then 404 in the envelope", func(t *testing.T) {
		r := addAlias(v, "prj_nope", "other.example.dev")
		if r.status != 404 || str(errorOf(r), "message") == "" {
			t.Fatalf("add to a missing project = %d %s", r.status, r.raw)
		}
	})
}

func TestVercelProjectAliasRefusesADomainAnotherProjectHolds(t *testing.T) {
	v := newVX(t, "vercel-projectalias-conflict")
	first := newProjectNamed(t, v, "first-app")
	second := newProjectNamed(t, v, "second-app")
	if r := addAlias(v, first, "shop.example.dev"); r.status != 200 {
		t.Fatalf("add = %d %s", r.status, r.raw)
	}

	t.Run("Given another project holds it When it is added Then 409 ALIAS_DOMAIN_EXIST names the holder", func(t *testing.T) {
		r := addAlias(v, second, "shop.example.dev")
		e := errorOf(r)
		if r.status != 409 || str(e, "code") != "ALIAS_DOMAIN_EXIST" {
			t.Fatalf("add = %d %s", r.status, r.raw)
		}
		holder, _ := e["project"].(map[string]any)
		if str(holder, "id") != first {
			t.Fatalf("the error names project %v, the holder is %s: %s", holder, first, r.raw)
		}
	})

	t.Run("Given the same project holds it When it is added again Then the same code names that project", func(t *testing.T) {
		r := addAlias(v, first, "shop.example.dev")
		holder, _ := errorOf(r)["project"].(map[string]any)
		if r.status != 409 || str(errorOf(r), "code") != "ALIAS_DOMAIN_EXIST" || str(holder, "id") != first {
			t.Fatalf("add again = %d %s", r.status, r.raw)
		}
	})

	t.Run("Given the holder removes it When the other project adds it Then it goes through (the CLI's --force)", func(t *testing.T) {
		rm := v.do(http.MethodDelete, "/projects/"+first+"/alias?domain=shop.example.dev", nil)
		if rm.status != 200 {
			t.Fatalf("remove = %d %s", rm.status, rm.raw)
		}
		if gone := v.do(http.MethodGet, "/v9/projects/"+first+"/domains/shop.example.dev", nil); gone.status != 404 {
			t.Fatalf("the holder still has it: %d %s", gone.status, gone.raw)
		}
		if r := addAlias(v, second, "shop.example.dev"); r.status != 200 {
			t.Fatalf("add after remove = %d %s", r.status, r.raw)
		}
		if again := v.do(http.MethodDelete, "/projects/"+first+"/alias?domain=shop.example.dev", nil); again.status != 404 {
			t.Fatalf("removing what the project no longer holds = %d %s, want 404", again.status, again.raw)
		}
	})
}

func TestVercelListsTheProjectDomainsOfAnAccountDomain(t *testing.T) {
	v := newVX(t, "vercel-domainprojects")
	a := newProjectNamed(t, v, "dp-a")
	b := newProjectNamed(t, v, "dp-b")
	if r := v.do(http.MethodPost, "/v7/domains", map[string]any{"name": "example.dev", "zone": true}); r.status != 200 {
		t.Fatalf("add account domain = %d %s", r.status, r.raw)
	}
	addAlias(v, a, "www.example.dev")
	v.do(http.MethodPost, "/v10/projects/"+b+"/domains", map[string]any{"name": "api.example.dev"})
	addAlias(v, b, "elsewhere.test")

	names := func(path string) map[string]string {
		t.Helper()
		r := v.do(http.MethodGet, path, nil)
		if r.status != 200 {
			t.Fatalf("GET %s = %d %s", path, r.status, r.raw)
		}
		list, _ := r.body["projectDomains"].([]any)
		out := map[string]string{}
		for _, item := range list {
			m := item.(map[string]any)
			out[str(m, "name")] = str(m, "projectId")
		}
		return out
	}

	t.Run("Given an account domain When its project domains are listed Then every project's host under it shows with its project", func(t *testing.T) {
		got := names("/v1/domains/example.dev/project-domains?limit=100")
		if len(got) != 2 || got["www.example.dev"] != a || got["api.example.dev"] != b {
			t.Fatalf("project domains = %v, want www -> %s and api -> %s", got, a, b)
		}
	})

	t.Run("Given a domain the account does not hold When listed Then 404", func(t *testing.T) {
		if r := v.do(http.MethodGet, "/v1/domains/elsewhere.test/project-domains", nil); r.status != 404 {
			t.Fatalf("a domain the account lacks = %d %s", r.status, r.raw)
		}
	})

	t.Run("Given a redirect set on a project domain When listed Then the list shows it", func(t *testing.T) {
		if r := v.do(http.MethodPatch, "/v9/projects/"+a+"/domains/www.example.dev", map[string]any{"redirect": "api.example.dev", "redirectStatusCode": 308}); r.status != 200 {
			t.Fatalf("patch = %d %s", r.status, r.raw)
		}
		r := v.do(http.MethodGet, "/v1/domains/example.dev/project-domains", nil)
		for _, item := range r.body["projectDomains"].([]any) {
			m := item.(map[string]any)
			if str(m, "name") == "www.example.dev" && (str(m, "redirect") != "api.example.dev" || m["redirectStatusCode"] != float64(308)) {
				t.Fatalf("www.example.dev lost its redirect in the list: %v", m)
			}
		}
	})

	t.Run("Given a domain removed from its project When listed Then it is gone, and so is a deleted project's", func(t *testing.T) {
		v.do(http.MethodDelete, "/projects/"+a+"/alias?domain=www.example.dev", nil)
		if got := names("/v1/domains/example.dev/project-domains"); len(got) != 1 || got["api.example.dev"] != b {
			t.Fatalf("after remove = %v", got)
		}
		v.do(http.MethodDelete, "/v9/projects/"+b, nil)
		if got := names("/v1/domains/example.dev/project-domains"); len(got) != 0 {
			t.Fatalf("after the project went = %v", got)
		}
	})
}

func TestVercelDeletesAnAccountDomainAtTheOlderAddress(t *testing.T) {
	v := newVX(t, "vercel-domainrm")
	if r := v.do(http.MethodPost, "/v7/domains", map[string]any{"name": "old.example.dev", "zone": true}); r.status != 200 {
		t.Fatalf("add = %d %s", r.status, r.raw)
	}
	t.Run("Given the CLI's address When the domain is deleted Then it is gone", func(t *testing.T) {
		r := v.do(http.MethodDelete, "/v3/domains/old.example.dev", nil)
		if r.status != 200 {
			t.Fatalf("DELETE /v3/domains = %d %s", r.status, r.raw)
		}
		if got := v.do(http.MethodGet, "/v5/domains/old.example.dev", nil); got.status != 404 {
			t.Fatalf("still there: %d %s", got.status, got.raw)
		}
	})
	t.Run("Given an unknown domain When deleted Then 404 not_found, the code the CLI keys on", func(t *testing.T) {
		r := v.do(http.MethodDelete, "/v3/domains/old.example.dev", nil)
		if r.status != 404 || !strings.EqualFold(str(errorOf(r), "code"), "not_found") {
			t.Fatalf("second delete = %d %s", r.status, r.raw)
		}
	})
}

// TestVercelProjectAliasEchoesTheSpellingTheCLIPosted holds the request the real
// CLI sent (the recorded `domains add <domain> <project>`) with the host spelled as
// a person may type it. The CLI finds its host in the answer by comparing
// `alias.domain === the domain it posted`, strictly, so an answer that lower-cased
// the host made `vercel domains add WWW2.Example.Dev app` fail with "did not return
// a matching alias" after the host had been recorded. Hosts are case-insensitive:
// the emulator stores and lists the lower-case name, a second add in another case
// finds the first, and only the entry the add answers keeps the spelling posted.
// What Vercel itself echoes is not known.
func TestVercelProjectAliasEchoesTheSpellingTheCLIPosted(t *testing.T) {
	v := newVX(t, "vercel-projectalias-case")
	recorded := recordedStep(t, "vercel-cli-63.1.2", "domains", "domains add <domain> <project>")
	var posted map[string]any
	if err := json.Unmarshal([]byte(recorded.Request.Body), &posted); err != nil || posted["domain"] == nil {
		t.Fatalf("the recorded body %q is not the CLI's alias request: %v", recorded.Request.Body, err)
	}
	pid := newProjectNamed(t, v, "cli-domains")
	other := newProjectNamed(t, v, "cli-domains-other")
	add := func(project, host string) reply {
		body := map[string]any{}
		for k, val := range posted {
			body[k] = val
		}
		body["domain"] = host
		return v.do(recorded.Request.Method, strings.Replace(recorded.Request.Path, "cli-domains", project, 1), body)
	}

	r := add(pid, "WWW2.Example.Dev")
	var targets []map[string]any
	if err := unmarshalJSON(r.raw, &targets); err != nil || r.status != 200 {
		t.Fatalf("add = %d %s", r.status, r.raw)
	}
	matching := 0
	for _, a := range targets {
		if str(a, "domain") == "WWW2.Example.Dev" {
			matching++
		}
	}
	if matching != 1 {
		t.Fatalf("no entry whose domain is exactly the spelling posted, which the CLI compares with ===: %s", r.raw)
	}

	t.Run("Given the host was added in mixed case Then the project lists it in lower case", func(t *testing.T) {
		got := v.do(http.MethodGet, "/v9/projects/"+pid+"/domains/WWW2.Example.Dev", nil)
		if got.status != 200 || got.body["name"] != "www2.example.dev" {
			t.Fatalf("the CLI's follow-up for the host = %d %s", got.status, got.raw)
		}
	})
	t.Run("Given the host is added again in another case Then it is the same host, held by that project", func(t *testing.T) {
		again := add(other, "www2.EXAMPLE.dev")
		holder, _ := errorOf(again)["project"].(map[string]any)
		if again.status != 409 || str(errorOf(again), "code") != "ALIAS_DOMAIN_EXIST" || str(holder, "id") != pid {
			t.Fatalf("add again in another case = %d %s", again.status, again.raw)
		}
	})
	t.Run("Given the spelling already lower case Then it is echoed as it is", func(t *testing.T) {
		r := add(pid, "plain.example.dev")
		var plain []map[string]any
		if err := unmarshalJSON(r.raw, &plain); err != nil || len(plain) != 1 || str(plain[0], "domain") != "plain.example.dev" {
			t.Fatalf("add = %d %s", r.status, r.raw)
		}
	})
}
