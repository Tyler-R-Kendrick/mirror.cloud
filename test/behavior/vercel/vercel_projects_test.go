package vercel

import (
	"fmt"
	"net/http"
	"reflect"
	"testing"
	"time"
)

// TestVercelTeamsScopeByIdAndBySlug holds the rule the CLI works by: a team
// is acted for through ?teamId= or ?slug=, they name the same account, and
// renaming a team's slug moves where ?slug= leads.
func TestVercelTeamsScopeByIdAndBySlug(t *testing.T) {
	v := newVX(t, "vercel-teams")
	team := v.do(http.MethodPost, "/v1/teams", map[string]any{"slug": "acme"})
	id := str(team.body, "id")
	if team.status != 200 || id == "" || str(team.body, "slug") != "acme" {
		t.Fatalf("create team = %d %s", team.status, team.raw)
	}

	made := v.do(http.MethodPost, "/v11/projects?slug=acme", map[string]any{"name": "site"})
	if made.status != 200 || str(made.body, "accountId") != id {
		t.Fatalf("a project made through the slug belongs to the team: %d %s", made.status, made.raw)
	}
	if r := v.do(http.MethodGet, "/v9/projects/site?teamId="+id, nil); r.status != 200 || str(r.body, "id") != str(made.body, "id") {
		t.Fatalf("the same project through the id = %d %s", r.status, r.raw)
	}
	if r := v.do(http.MethodGet, "/v9/projects/site", nil); r.status != 404 {
		t.Fatalf("the project is not in the personal account: %d %s", r.status, r.raw)
	}

	if r := v.do(http.MethodPatch, "/v2/teams/"+id, map[string]any{"slug": "acme-inc"}); r.status != 200 {
		t.Fatalf("rename = %d %s", r.status, r.raw)
	}
	if r := v.do(http.MethodGet, "/v9/projects/site?slug=acme", nil); r.status != 403 {
		t.Fatalf("the old slug = %d %s, want 403", r.status, r.raw)
	}
	if r := v.do(http.MethodGet, "/v9/projects/site?slug=acme-inc", nil); r.status != 200 {
		t.Fatalf("the new slug = %d %s", r.status, r.raw)
	}
	if r := v.do(http.MethodPost, "/v1/teams", map[string]any{"slug": "acme"}); r.status != 200 {
		t.Fatalf("the old slug is free to take again: %d %s", r.status, r.raw)
	}
	if r := v.do(http.MethodPost, "/v1/teams", map[string]any{"slug": "acme-inc"}); r.status != 409 {
		t.Fatalf("a slug in use = %d %s, want 409", r.status, r.raw)
	}
}

// TestVercelProjectNamesAndSettings holds what a client relies on when it
// links a directory: names are unique and move with a rename, the settings a
// new project has are the ones the document names, and an update keeps what it
// did not mention.
func TestVercelProjectNamesAndSettings(t *testing.T) {
	v := newVX(t, "vercel-projects")
	app := v.do(http.MethodPost, "/v11/projects", map[string]any{"name": "app", "framework": "vite", "rootDirectory": "web"})
	if app.status != 200 {
		t.Fatalf("create = %d %s", app.status, app.raw)
	}
	for member, want := range map[string]any{
		"nodeVersion": "22.x", "directoryListing": false, "autoExposeSystemEnvs": true,
		"gitForkProtection": true, "framework": "vite", "rootDirectory": "web", "buildCommand": nil,
	} {
		if got := app.body[member]; !reflect.DeepEqual(got, want) {
			t.Errorf("new project %s = %v, want %v", member, got, want)
		}
	}

	t.Run("a rename frees the old name and takes the new one", func(t *testing.T) {
		if r := v.do(http.MethodPatch, "/v9/projects/app", map[string]any{"name": "app2"}); r.status != 200 || str(r.body, "name") != "app2" {
			t.Fatalf("rename = %d %s", r.status, r.raw)
		}
		if r := v.do(http.MethodGet, "/v9/projects/app", nil); r.status != 404 {
			t.Fatalf("the old name = %d", r.status)
		}
		if r := v.do(http.MethodPost, "/v11/projects", map[string]any{"name": "app"}); r.status != 200 {
			t.Fatalf("the old name is free: %d %s", r.status, r.raw)
		}
		if r := v.do(http.MethodPost, "/v11/projects", map[string]any{"name": "app2"}); r.status != 409 {
			t.Fatalf("the new name is taken: %d %s", r.status, r.raw)
		}
	})
	t.Run("an update keeps what it does not mention", func(t *testing.T) {
		r := v.do(http.MethodPatch, "/v9/projects/app2", map[string]any{"buildCommand": "make", "nodeVersion": "20.x"})
		if r.status != 200 || str(r.body, "buildCommand") != "make" || str(r.body, "nodeVersion") != "20.x" {
			t.Fatalf("update = %d %s", r.status, r.raw)
		}
		if str(r.body, "framework") != "vite" || str(r.body, "rootDirectory") != "web" {
			t.Errorf("an update lost what it did not mention: %s", r.raw)
		}
		cleared := v.do(http.MethodPatch, "/v9/projects/app2", map[string]any{"buildCommand": nil})
		if cleared.body["buildCommand"] != nil {
			t.Errorf("null clears a setting: %s", cleared.raw)
		}
	})
	t.Run("a project is listed newest first and paged by its cursor", func(t *testing.T) {
		for _, name := range []string{"p1", "p2", "p3"} {
			v.advance(time.Millisecond)
			v.do(http.MethodPost, "/v11/projects", map[string]any{"name": name})
		}
		names := func(r reply) (out []string) {
			list, _ := r.body["projects"].([]any)
			for _, p := range list {
				out = append(out, str(p.(map[string]any), "name"))
			}
			return
		}
		page := v.do(http.MethodGet, "/v10/projects?limit=2", nil)
		if got := names(page); !reflect.DeepEqual(got, []string{"p3", "p2"}) {
			t.Fatalf("first page = %v", got)
		}
		next := page.body["pagination"].(map[string]any)["next"]
		if next == nil {
			t.Fatalf("no cursor: %s", page.raw)
		}
		second := v.do(http.MethodGet, fmt.Sprintf("/v10/projects?limit=2&from=%d", int64(next.(float64))), nil)
		if got := names(second); len(got) == 0 || got[0] == "p3" || got[0] == "p2" {
			t.Fatalf("the cursor did not continue: %v", got)
		}
		if got := names(v.do(http.MethodGet, "/v10/projects?search=p3", nil)); !reflect.DeepEqual(got, []string{"p3"}) {
			t.Fatalf("search = %v", got)
		}
	})
}

// TestVercelDeletingAProjectTakesWhatHangsOffIt holds the cascade.
func TestVercelDeletingAProjectTakesWhatHangsOffIt(t *testing.T) {
	v := newVX(t, "vercel-cascade")
	dpl := v.do(http.MethodPost, "/v13/deployments", map[string]any{"name": "doomed"})
	id, host := str(dpl.body, "id"), str(dpl.body, "url")
	if v.do(http.MethodGet, "/v13/deployments/"+host, nil).status != 200 {
		t.Fatal("the deployment is not there to begin with")
	}
	if r := v.do(http.MethodDelete, "/v9/projects/doomed", nil); r.status != 204 || len(r.raw) != 0 {
		t.Fatalf("delete = %d %s", r.status, r.raw)
	}
	for _, path := range []string{"/v13/deployments/" + id, "/v13/deployments/" + host, "/v2/deployments/" + id + "/aliases"} {
		if r := v.do(http.MethodGet, path, nil); r.status != 404 {
			t.Errorf("GET %s after the project went = %d %s", path, r.status, r.raw)
		}
	}
	if r := v.do(http.MethodGet, "/v7/deployments", nil); len(r.body["deployments"].([]any)) != 0 {
		t.Errorf("the listing still carries the deployment: %s", r.raw)
	}
	if r := v.do(http.MethodPost, "/v11/projects", map[string]any{"name": "doomed"}); r.status != 200 {
		t.Errorf("the name is free again: %d %s", r.status, r.raw)
	}
}
