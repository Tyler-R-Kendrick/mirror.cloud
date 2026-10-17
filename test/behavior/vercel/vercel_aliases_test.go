package vercel

import (
	"net/http"
	"reflect"
	"testing"
	"time"
)

// deploy makes a deployment of a project and answers its id. It is READY when
// ready is true: the build's two seconds are spent.
func deploy(t *testing.T, v *vx, project string, ready bool) string {
	t.Helper()
	r := v.do(http.MethodPost, "/v13/deployments", map[string]any{"name": project, "project": project, "files": []map[string]any{{"file": "index.html", "data": "hi"}}})
	if r.status != 200 {
		t.Fatalf("deploy = %d %s", r.status, r.raw)
	}
	if ready {
		v.advance(3 * time.Second)
	}
	return str(r.body, "id")
}

// TestVercelAliasesFollowTheDeployment holds the alias life a deploy pipeline
// drives: a hostname can name only a deployment that has finished building,
// naming one moves it from the deployment that had it, and the deployments say
// which hostnames they answer for.
func TestVercelAliasesFollowTheDeployment(t *testing.T) {
	v := newVX(t, "vercel-aliases")
	v.do(http.MethodPost, "/v11/projects", map[string]any{"name": "site"})
	// A hostname is the account's to alias only once it holds the domain.
	if r := v.do(http.MethodPost, "/v7/domains", map[string]any{"name": "example.com"}); r.status != 200 {
		t.Fatalf("hold the domain = %d %s", r.status, r.raw)
	}

	building := deploy(t, v, "site", false)
	if r := v.do(http.MethodPost, "/v2/deployments/"+building+"/aliases", map[string]any{"alias": "www.example.com"}); r.status != 400 {
		t.Fatalf("a deployment still building = %d %s", r.status, r.raw)
	}
	v.advance(3 * time.Second)

	first := deploy(t, v, "site", true)
	r := v.do(http.MethodPost, "/v2/deployments/"+first+"/aliases", map[string]any{"alias": "WWW.Example.com"})
	if r.status != 200 || str(r.body, "alias") != "www.example.com" || r.body["oldDeploymentId"] != nil || str(r.body, "created") == "" {
		t.Fatalf("assign = %d %s", r.status, r.raw)
	}
	uid := str(r.body, "uid")

	t.Run("the deployment names it", func(t *testing.T) {
		g := v.do(http.MethodGet, "/v13/deployments/"+first, nil)
		alias, _ := g.body["alias"].([]any)
		if !reflect.DeepEqual(alias, []any{"www.example.com"}) || g.body["aliasAssigned"] != true {
			t.Fatalf("alias = %v assigned = %v", g.body["alias"], g.body["aliasAssigned"])
		}
	})

	t.Run("naming the same deployment twice is a conflict", func(t *testing.T) {
		if r := v.do(http.MethodPost, "/v2/deployments/"+first+"/aliases", map[string]any{"alias": "www.example.com"}); r.status != 409 {
			t.Fatalf("twice = %d %s", r.status, r.raw)
		}
	})

	t.Run("a bad hostname, a missing deployment", func(t *testing.T) {
		if r := v.do(http.MethodPost, "/v2/deployments/"+first+"/aliases", map[string]any{"alias": "no dots"}); r.status != 400 {
			t.Errorf("bad = %d %s", r.status, r.raw)
		}
		if r := v.do(http.MethodPost, "/v2/deployments/"+first+"/aliases", map[string]any{}); r.status != 400 {
			t.Errorf("none = %d %s", r.status, r.raw)
		}
		if r := v.do(http.MethodPost, "/v2/deployments/dpl_nope/aliases", map[string]any{"alias": "x.example.com"}); r.status != 404 {
			t.Errorf("missing deployment = %d %s", r.status, r.raw)
		}
	})

	second := deploy(t, v, "site", true)
	t.Run("assigning a hostname moves it", func(t *testing.T) {
		m := v.do(http.MethodPost, "/v2/deployments/"+second+"/aliases", map[string]any{"alias": "www.example.com"})
		if m.status != 200 || str(m.body, "oldDeploymentId") != first || str(m.body, "uid") != uid {
			t.Fatalf("move = %d %s (the alias keeps its uid %s)", m.status, m.raw, uid)
		}
		if alias, _ := v.do(http.MethodGet, "/v13/deployments/"+first, nil).body["alias"].([]any); len(alias) != 0 {
			t.Errorf("the deployment it left still names it: %v", alias)
		}
		if alias, _ := v.do(http.MethodGet, "/v13/deployments/"+second, nil).body["alias"].([]any); !reflect.DeepEqual(alias, []any{"www.example.com"}) {
			t.Errorf("the deployment it reached does not: %v", alias)
		}
		g := v.do(http.MethodGet, "/v4/aliases/www.example.com", nil)
		if g.status != 200 || str(g.body, "deploymentId") != second {
			t.Errorf("get = %d %s", g.status, g.raw)
		}
	})

	t.Run("listing at every version, newest first", func(t *testing.T) {
		v.do(http.MethodPost, "/v2/deployments/"+second+"/aliases", map[string]any{"alias": "app.example.com"})
		for _, p := range []string{"/v4/aliases", "/v3/aliases", "/v2/aliases"} {
			r := v.do(http.MethodGet, p, nil)
			list, _ := r.body["aliases"].([]any)
			if r.status != 200 || len(list) < 3 {
				t.Fatalf("%s = %d %s", p, r.status, r.raw)
			}
			if str(list[0].(map[string]any), "alias") != "app.example.com" {
				t.Errorf("%s newest first: %s", p, r.raw)
			}
		}
		by := v.do(http.MethodGet, "/v4/aliases?domain=example.com", nil).body["aliases"].([]any)
		if len(by) != 2 {
			t.Errorf("by domain = %d, want the two assigned", len(by))
		}
		page := v.do(http.MethodGet, "/v4/aliases?limit=1", nil)
		if page.body["pagination"].(map[string]any)["next"] == nil {
			t.Errorf("a page of one has a next cursor: %s", page.raw)
		}
		dep := v.do(http.MethodGet, "/v2/deployments/"+second+"/aliases", nil)
		if got := len(dep.body["aliases"].([]any)); got != 3 {
			t.Errorf("a deployment's aliases (its URL and two assigned) = %d: %s", got, dep.raw)
		}
		if r := v.do(http.MethodGet, "/v3/deployments/"+second+"/aliases", nil); r.status != 200 {
			t.Errorf("v3 = %d", r.status)
		}
	})

	t.Run("delete takes the alias and the deployment forgets it", func(t *testing.T) {
		d := v.do(http.MethodDelete, "/v2/aliases/"+uid, nil)
		if d.status != 200 || str(d.body, "status") != "SUCCESS" {
			t.Fatalf("delete = %d %s", d.status, d.raw)
		}
		if g := v.do(http.MethodGet, "/v4/aliases/www.example.com", nil); g.status != 404 {
			t.Errorf("get after = %d", g.status)
		}
		if alias, _ := v.do(http.MethodGet, "/v13/deployments/"+second, nil).body["alias"].([]any); !reflect.DeepEqual(alias, []any{"app.example.com"}) {
			t.Errorf("the deployment still names it: %v", alias)
		}
		if d := v.do(http.MethodDelete, "/v2/aliases/"+uid, nil); d.status != 404 {
			t.Errorf("delete again = %d", d.status)
		}
		// Free to name again.
		if r := v.do(http.MethodPost, "/v2/deployments/"+first+"/aliases", map[string]any{"alias": "www.example.com"}); r.status != 200 || r.body["oldDeploymentId"] != nil {
			t.Errorf("reassign = %d %s", r.status, r.raw)
		}
	})

	t.Run("deleting a deployment takes its aliases", func(t *testing.T) {
		if d := v.do(http.MethodDelete, "/v13/deployments/"+second, nil); d.status != 200 {
			t.Fatalf("delete deployment = %d %s", d.status, d.raw)
		}
		if g := v.do(http.MethodGet, "/v4/aliases/app.example.com", nil); g.status != 404 {
			t.Errorf("alias of a deleted deployment = %d", g.status)
		}
	})
}
