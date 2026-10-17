package vercel

import (
	"net/http"
	"testing"
)

// A host is named by `domain` on the older address the CLI posts to
// (POST /projects/{project}/alias) and by `name` on the document's own
// (POST /v10/projects/{project}/domains). One precedence reads it everywhere it is
// read: the spelling the add echoes, the host that is stored, the apex that is
// looked up, and the key the ownership of the host is held under: `domain`, and
// `name` where there is none. A body that names two different hosts is a 400: the
// project it would put under one key and the record it would store under the other
// are not the same host.

func aliasBody(v *vx, project string, body map[string]any) reply {
	b := map[string]any{"target": "PRODUCTION"}
	for k, val := range body {
		b[k] = val
	}
	return v.do(http.MethodPost, "/projects/"+project+"/alias", b)
}

// echoed is the domain of the alias entry the add answers, "" when it is none.
func echoed(t *testing.T, r reply) string {
	t.Helper()
	var targets []map[string]any
	if err := unmarshalJSON(r.raw, &targets); err != nil || len(targets) != 1 {
		t.Fatalf("the add answered %d %s, want an array of one alias target", r.status, r.raw)
	}
	return str(targets[0], "domain")
}

func TestVercelProjectAliasReadsTheHostWithOnePrecedence(t *testing.T) {
	v := newVX(t, "vercel-projectalias-names")
	a := newProjectNamed(t, v, "names-a")
	b := newProjectNamed(t, v, "names-b")
	holds := func(project, host string) bool {
		t.Helper()
		return v.do(http.MethodGet, "/v9/projects/"+project+"/domains/"+host, nil).status == 200
	}

	t.Run("Given a body with only name When posted Then it is added, and echoed in the spelling posted", func(t *testing.T) {
		r := aliasBody(v, a, map[string]any{"name": "NameOnly.Mixed.Dev"})
		if r.status != 200 || echoed(t, r) != "NameOnly.Mixed.Dev" {
			t.Fatalf("add = %d %s", r.status, r.raw)
		}
		if !holds(a, "nameonly.mixed.dev") {
			t.Errorf("the host was not stored under its lower-case name")
		}
		if again := aliasBody(v, b, map[string]any{"name": "NAMEONLY.mixed.dev"}); again.status != 409 || str(errorOf(again), "code") != "ALIAS_DOMAIN_EXIST" {
			t.Errorf("another project adding it in another case = %d %s, want 409 ALIAS_DOMAIN_EXIST", again.status, again.raw)
		}
	})

	t.Run("Given a body with only domain When posted Then it is echoed in the spelling posted", func(t *testing.T) {
		r := aliasBody(v, a, map[string]any{"domain": "DomainOnly.Mixed.Dev"})
		if r.status != 200 || echoed(t, r) != "DomainOnly.Mixed.Dev" {
			t.Fatalf("add = %d %s", r.status, r.raw)
		}
	})

	t.Run("Given name and domain that are one host in two spellings When posted Then it is added once, echoed as domain was spelled", func(t *testing.T) {
		r := aliasBody(v, a, map[string]any{"name": "Both.Mixed.Dev", "domain": "BOTH.mixed.dev"})
		if r.status != 200 || echoed(t, r) != "BOTH.mixed.dev" {
			t.Fatalf("add = %d %s", r.status, r.raw)
		}
		if !holds(a, "both.mixed.dev") {
			t.Errorf("the host was not stored under its lower-case name")
		}
	})

	t.Run("Given name and domain that are two hosts When posted Then 400, nothing is stored and no project can be locked out of either", func(t *testing.T) {
		r := aliasBody(v, b, map[string]any{"name": "nm.mixed.dev", "domain": "DM.mixed.dev"})
		if r.status != 400 || str(errorOf(r), "code") != "bad_request" {
			t.Fatalf("add = %d %s, want 400 bad_request", r.status, r.raw)
		}
		if holds(b, "nm.mixed.dev") || holds(b, "dm.mixed.dev") {
			t.Errorf("the refused add stored a host")
		}
		if taken := aliasBody(v, a, map[string]any{"domain": "nm.mixed.dev"}); taken.status != 200 || echoed(t, taken) != "nm.mixed.dev" {
			t.Errorf("project a adding nm.mixed.dev = %d %s, want 200: b holds nothing", taken.status, taken.raw)
		}
		if holds(b, "nm.mixed.dev") {
			t.Errorf("project b lists nm.mixed.dev, which a holds")
		}
	})

	t.Run("Given the document's address and a body naming two hosts When posted Then 400 as well", func(t *testing.T) {
		r := v.do(http.MethodPost, "/v10/projects/"+b+"/domains", map[string]any{"name": "one.mixed.dev", "domain": "two.mixed.dev"})
		if r.status != 400 || str(errorOf(r), "code") != "bad_request" {
			t.Fatalf("add = %d %s, want 400 bad_request", r.status, r.raw)
		}
		if ok := v.do(http.MethodPost, "/v10/projects/"+b+"/domains", map[string]any{"name": "Plain.Mixed.Dev"}); ok.status != 200 || str(ok.body, "name") != "plain.mixed.dev" {
			t.Errorf("the document's own add = %d %s, want the lower-case name", ok.status, ok.raw)
		}
	})
}
