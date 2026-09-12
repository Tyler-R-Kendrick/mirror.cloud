package bundled_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/bundled"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spitest"
)

// TestVercelBundleBehaves covers the twelve operations no pack ever served.
// The equivalence recording gates the fourteen the pack did serve against the
// pack's own answers; for these there is nothing to replay against, so the
// gate is what the rules mean: each happy path, and each 404/400 edge the
// document's required labels and the bundle's own rules produce.
//
// It also pins the two update traps the resources are shaped around: a patch
// must not re-prefix the resolved key (an edit that moves the env row to
// env_env_... orphans it), and a member an update omits must keep its value
// rather than reset to the resource record's default.
func TestVercelBundleBehaves(t *testing.T) {
	pack, err := bundled.New("vercel.api", spitest.Deps(t))
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	id := spi.Identity{Account: "000000000000", Region: "us-east-1"}
	call := func(op string, in map[string]any) (*spi.Response, error) {
		return pack.Invoke(ctx, &spi.Request{ServiceID: "vercel.api", Operation: op, Identity: id, Input: in})
	}
	ok := func(t *testing.T, op string, in map[string]any) map[string]any {
		t.Helper()
		res, err := call(op, in)
		if err != nil {
			t.Fatalf("%s: %v", op, err)
		}
		return res.Output
	}
	fault := func(t *testing.T, err error, code string, status int) {
		t.Helper()
		f, isFault := err.(*spi.Fault)
		if !isFault {
			t.Fatalf("want %s, got %v", code, err)
		}
		if f.Code != code || f.HTTPStatus != status {
			t.Fatalf("want %s/%d, got %s/%d", code, status, f.Code, f.HTTPStatus)
		}
	}

	made := ok(t, "CreateProject", map[string]any{"name": "app", "framework": "nextjs"})
	pid, _ := made["id"].(string)
	if pid == "" {
		t.Fatalf("create answered %v", made)
	}
	ok(t, "CreateProject", map[string]any{"name": "other"})

	// UpdateProject: framework only, then a rename, then the edges.
	updated := ok(t, "UpdateProject", map[string]any{"idOrName": "app", "framework": "astro"})
	if updated["framework"] != "astro" || updated["name"] != "app" || updated["id"] != pid {
		t.Fatalf("update answered %v", updated)
	}
	renamed := ok(t, "UpdateProject", map[string]any{"idOrName": pid, "name": "app2"})
	if renamed["name"] != "app2" || renamed["framework"] != "astro" {
		t.Fatalf("rename answered %v", renamed)
	}
	if got := ok(t, "GetProject", map[string]any{"idOrName": "app2"}); got["id"] != pid {
		t.Fatalf("get by new name answered %v", got)
	}
	if _, err := call("GetProject", map[string]any{"idOrName": "app"}); err == nil {
		t.Fatal("the old name still resolves after a rename")
	} else {
		fault(t, err, "not_found", 404)
	}
	if _, err := call("UpdateProject", map[string]any{"idOrName": "prj_nope", "name": "x"}); err == nil {
		t.Fatal("a missing project was updated")
	} else {
		fault(t, err, "not_found", 404)
	}
	if _, err := call("UpdateProject", map[string]any{"idOrName": "app2", "name": "other"}); err == nil {
		t.Fatal("a rename onto a taken name was accepted")
	} else {
		fault(t, err, "conflict", 409)
	}
	if _, err := call("UpdateProject", map[string]any{"idOrName": "app2", "name": ""}); err == nil {
		t.Fatal("an empty rename was accepted")
	} else {
		fault(t, err, "bad_request", 400)
	}

	// EditProjectEnv: the edit keeps what it does not name, and the row does
	// not move.
	env := ok(t, "CreateProjectEnv", map[string]any{"idOrName": "app2", "key": "TOKEN", "value": "s3cret"})
	created, _ := env["created"].(map[string]any)
	envID, _ := created["id"].(string)
	if envID == "" {
		t.Fatalf("env create answered %v", env)
	}
	edited := ok(t, "EditProjectEnv", map[string]any{"idOrName": "app2", "id": envID, "value": "rotated"})
	if edited["value"] != "rotated" || edited["key"] != "TOKEN" || edited["type"] != "encrypted" || edited["id"] != envID {
		t.Fatalf("edit answered %v", edited)
	}
	listed := ok(t, "FilterProjectEnvs", map[string]any{"idOrName": "app2"})
	envs, _ := listed["envs"].([]any)
	if len(envs) != 1 {
		t.Fatalf("an edit moved the row: %v", listed)
	}
	if item, _ := envs[0].(map[string]any); item["value"] != "rotated" || item["id"] != envID {
		t.Fatalf("listed after edit %v", item)
	}
	if _, err := call("EditProjectEnv", map[string]any{"idOrName": "app2", "id": "env_nope", "value": "x"}); err == nil {
		t.Fatal("a missing variable was edited")
	} else {
		fault(t, err, "not_found", 404)
	}
	if _, err := call("EditProjectEnv", map[string]any{"idOrName": "prj_nope", "id": envID, "value": "x"}); err == nil {
		t.Fatal("a variable under a missing project was edited")
	} else {
		fault(t, err, "not_found", 404)
	}

	// Domains: get, update, verify, remove.
	ok(t, "AddProjectDomain", map[string]any{"idOrName": "app2", "name": "app.example.com"})
	dom := ok(t, "GetProjectDomain", map[string]any{"idOrName": "app2", "domain": "app.example.com"})
	if dom["name"] != "app.example.com" || dom["verified"] != true || dom["projectId"] != pid {
		t.Fatalf("get domain answered %v", dom)
	}
	if _, err := call("GetProjectDomain", map[string]any{"idOrName": "app2", "domain": "nope.example.com"}); err == nil {
		t.Fatal("a missing domain was found")
	} else {
		fault(t, err, "not_found", 404)
	}
	udom := ok(t, "UpdateProjectDomain", map[string]any{
		"idOrName": "app2", "domain": "app.example.com",
		"redirect": "example.com", "redirectStatusCode": 307,
	})
	if udom["redirect"] != "example.com" || fmt.Sprint(udom["redirectStatusCode"]) != "307" {
		t.Fatalf("update domain answered %v", udom)
	}
	vdom := ok(t, "VerifyProjectDomain", map[string]any{"idOrName": "app2", "domain": "app.example.com"})
	if vdom["verified"] != true || vdom["redirect"] != "example.com" {
		t.Fatalf("verify answered %v", vdom)
	}
	if out := ok(t, "RemoveProjectDomain", map[string]any{"idOrName": "app2", "domain": "app.example.com"}); len(out) != 0 {
		t.Fatalf("remove domain answered %v", out)
	}
	if _, err := call("RemoveProjectDomain", map[string]any{"idOrName": "app2", "domain": "app.example.com"}); err == nil {
		t.Fatal("removing twice succeeded")
	} else {
		fault(t, err, "not_found", 404)
	}

	// Custom environments: addressed by slug or by id, as the label says.
	cenv := ok(t, "CreateCustomEnvironment", map[string]any{
		"idOrName": "app2", "slug": "staging", "description": "Staging",
	})
	cenvID, _ := cenv["id"].(string)
	if cenvID == "" || cenv["slug"] != "staging" || cenv["type"] != "preview" {
		t.Fatalf("custom env create answered %v", cenv)
	}
	if _, err := call("CreateCustomEnvironment", map[string]any{"idOrName": "app2", "slug": "staging"}); err == nil {
		t.Fatal("a duplicate slug was accepted")
	} else {
		fault(t, err, "conflict", 409)
	}
	if _, err := call("CreateCustomEnvironment", map[string]any{"idOrName": "app2"}); err == nil {
		t.Fatal("a slugless create was accepted")
	} else {
		fault(t, err, "bad_request", 400)
	}
	if _, err := call("CreateCustomEnvironment", map[string]any{"idOrName": "prj_nope", "slug": "x"}); err == nil {
		t.Fatal("a custom environment under a missing project was created")
	} else {
		fault(t, err, "not_found", 404)
	}
	clist := ok(t, "GetProjectsByIdOrNameCustomEnvironments", map[string]any{"idOrName": "app2"})
	cenvs, _ := clist["environments"].([]any)
	if len(cenvs) != 1 {
		t.Fatalf("custom env listing answered %v", clist)
	}
	if limit, _ := clist["accountLimit"].(map[string]any); limit["total"] == nil {
		t.Fatalf("accountLimit answered %v", clist)
	}
	bySlug := ok(t, "GetCustomEnvironment", map[string]any{"idOrName": "app2", "environmentSlugOrId": "staging"})
	byID := ok(t, "GetCustomEnvironment", map[string]any{"idOrName": "app2", "environmentSlugOrId": cenvID})
	if bySlug["id"] != cenvID || byID["slug"] != "staging" {
		t.Fatalf("addressed by slug %v, by id %v", bySlug, byID)
	}
	if _, err := call("GetCustomEnvironment", map[string]any{"idOrName": "app2", "environmentSlugOrId": "nope"}); err == nil {
		t.Fatal("a missing custom environment was found")
	} else {
		fault(t, err, "not_found", 404)
	}
	uenv := ok(t, "UpdateCustomEnvironment", map[string]any{
		"idOrName": "app2", "environmentSlugOrId": "staging", "description": "Staging v2",
	})
	if uenv["description"] != "Staging v2" || uenv["slug"] != "staging" || uenv["id"] != cenvID {
		t.Fatalf("custom env update answered %v", uenv)
	}
	renv := ok(t, "RemoveCustomEnvironment", map[string]any{"idOrName": "app2", "environmentSlugOrId": "staging"})
	if renv["id"] != cenvID {
		t.Fatalf("custom env remove answered %v", renv)
	}
	if _, err := call("RemoveCustomEnvironment", map[string]any{"idOrName": "app2", "environmentSlugOrId": "staging"}); err == nil {
		t.Fatal("removing twice succeeded")
	} else {
		fault(t, err, "not_found", 404)
	}

	// Promote: 201 against a known pair, 404 for either side unknown.
	dpl := ok(t, "CreateDeployment", map[string]any{"name": "app2", "project": "app2"})
	dplID, _ := dpl["id"].(string)
	if out := ok(t, "RequestPromote", map[string]any{"projectId": pid, "deploymentId": dplID}); len(out) != 0 {
		t.Fatalf("promote answered %v", out)
	}
	if _, err := call("RequestPromote", map[string]any{"projectId": pid, "deploymentId": "dpl_nope"}); err == nil {
		t.Fatal("a missing deployment was promoted")
	} else {
		fault(t, err, "not_found", 404)
	}
	if _, err := call("RequestPromote", map[string]any{"projectId": "prj_nope", "deploymentId": dplID}); err == nil {
		t.Fatal("a deployment under a missing project was promoted")
	} else {
		fault(t, err, "not_found", 404)
	}
}
