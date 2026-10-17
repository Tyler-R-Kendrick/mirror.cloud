package vercel

import (
	"net/http"
	"strings"
	"testing"
)

// `vercel env pull`, `vercel pull` (and `vercel link`, which refreshes
// VERCEL_OIDC_TOKEN the same way) read a project's variables for one target
// from GET /v3/env/pull/{project}/{target}[/{gitBranch}]. The vendored document
// does not list the route; what the CLI sends and reads is in specs/vercel and
// docs/VERCEL.md. These tests hold the answer to what the CLI does with it:
// `{"env": {KEY: value}}`, a value for every key it should write, and an empty
// value (never the secret) for a sensitive one, which the CLI then marks as
// unavailable.

// pullEnv sends the pull the CLI sends and returns the `env` object.
func pullEnv(t *testing.T, v *vx, path string) map[string]any {
	t.Helper()
	r := v.do(http.MethodGet, path, nil)
	if r.status != 200 {
		t.Fatalf("GET %s = %d %s", path, r.status, r.raw)
	}
	env, ok := r.body["env"].(map[string]any)
	if !ok {
		t.Fatalf("GET %s has no env object: %s", path, r.raw)
	}
	return env
}

// pullFixture is a project holding one variable of every kind the pull treats
// differently.
func pullFixture(t *testing.T, v *vx) string {
	t.Helper()
	pid := str(v.do(http.MethodPost, "/v11/projects", map[string]any{"name": "pull-me"}).body, "id")
	add := func(body map[string]any) {
		t.Helper()
		if r := v.do(http.MethodPost, "/v10/projects/"+pid+"/env", body); r.status != 201 {
			t.Fatalf("create %v = %d %s", body["key"], r.status, r.raw)
		}
	}
	all := []string{"production", "preview", "development"}
	add(map[string]any{"key": "PLAIN", "value": "plain-value", "type": "plain", "target": all})
	add(map[string]any{"key": "ENCRYPTED", "value": "enc-value", "type": "encrypted", "target": all})
	add(map[string]any{"key": "SECRETISH", "value": "secret-value", "type": "secret", "target": []string{"production"}})
	add(map[string]any{"key": "SYSTEMISH", "value": "system-value", "type": "system", "target": []string{"production"}})
	add(map[string]any{"key": "SENSITIVE", "value": "vtok_dummy_marker_0123456789abcdef", "type": "sensitive", "target": []string{"production", "preview"}})
	add(map[string]any{"key": "PROD_ONLY", "value": "p", "type": "plain", "target": []string{"production"}})
	add(map[string]any{"key": "DEV_ONLY", "value": "d", "type": "plain", "target": []string{"development"}})
	add(map[string]any{"key": "PREVIEW_ONLY", "value": "pv", "type": "plain", "target": []string{"preview"}})
	add(map[string]any{"key": "BRANCHED", "value": "on-feature", "type": "plain", "target": []string{"preview"}, "gitBranch": "feature"})
	add(map[string]any{"key": "OVERRIDDEN", "value": "generic", "type": "plain", "target": []string{"preview"}})
	add(map[string]any{"key": "OVERRIDDEN", "value": "from-feature", "type": "plain", "target": []string{"preview"}, "gitBranch": "feature"})
	return pid
}

func TestVercelEnvPullAnswersWhatTheCLIReads(t *testing.T) {
	v := newVX(t, "vercel-envpull")
	pid := pullFixture(t, v)

	t.Run("Given a development pull When read Then it holds the development variables with their values", func(t *testing.T) {
		env := pullEnv(t, v, "/v3/env/pull/"+pid+"/development?source=vercel-cli%3Aenv%3Apull")
		want := map[string]string{"PLAIN": "plain-value", "ENCRYPTED": "enc-value", "DEV_ONLY": "d"}
		if len(env) != len(want) {
			t.Fatalf("development env = %v, want exactly %v", env, want)
		}
		for k, val := range want {
			if env[k] != val {
				t.Errorf("%s = %v, want %q", k, env[k], val)
			}
		}
	})

	t.Run("Given a production pull When read Then every decryptable type shows its value and a sensitive one shows none", func(t *testing.T) {
		r := v.do(http.MethodGet, "/v3/env/pull/"+pid+"/production", nil)
		env, _ := r.body["env"].(map[string]any)
		for k, val := range map[string]string{"PLAIN": "plain-value", "ENCRYPTED": "enc-value", "SECRETISH": "secret-value", "SYSTEMISH": "system-value", "PROD_ONLY": "p"} {
			if env[k] != val {
				t.Errorf("%s = %v, want %q", k, env[k], val)
			}
		}
		// The key is there, so the CLI can tell the person which values it could not write.
		if val, there := env["SENSITIVE"]; !there || val != "" {
			t.Errorf("SENSITIVE = %v (present: %v), want the key with an empty value", val, there)
		}
		if strings.Contains(string(r.raw), "vtok_dummy_marker") {
			t.Fatalf("a sensitive value came back: %s", r.raw)
		}
		for _, k := range []string{"DEV_ONLY", "PREVIEW_ONLY", "BRANCHED"} {
			if _, there := env[k]; there {
				t.Errorf("%s is not a production variable and was pulled: %v", k, env)
			}
		}
	})

	t.Run("Given a preview pull When no branch is named Then branch variables are left out", func(t *testing.T) {
		env := pullEnv(t, v, "/v3/env/pull/"+pid+"/preview")
		if _, there := env["BRANCHED"]; there {
			t.Errorf("BRANCHED belongs to branch feature and came with no branch: %v", env)
		}
		if env["OVERRIDDEN"] != "generic" || env["PREVIEW_ONLY"] != "pv" || env["PLAIN"] != "plain-value" {
			t.Errorf("preview env = %v", env)
		}
	})

	t.Run("Given a preview pull for a branch When read Then the branch's variables are added and override the generic ones", func(t *testing.T) {
		env := pullEnv(t, v, "/v3/env/pull/"+pid+"/preview/feature")
		if env["BRANCHED"] != "on-feature" || env["OVERRIDDEN"] != "from-feature" || env["PREVIEW_ONLY"] != "pv" {
			t.Errorf("preview/feature env = %v", env)
		}
		other := pullEnv(t, v, "/v3/env/pull/"+pid+"/preview/other-branch")
		if _, there := other["BRANCHED"]; there || other["OVERRIDDEN"] != "generic" {
			t.Errorf("preview/other-branch env = %v", other)
		}
	})

	t.Run("Given a pull with no target When read Then it is the development environment", func(t *testing.T) {
		env := pullEnv(t, v, "/v3/env/pull/"+pid)
		if env["DEV_ONLY"] != "d" || len(env) != 3 {
			t.Errorf("env with no target = %v", env)
		}
	})

	t.Run("Given the project's name When pulled Then it is the same project", func(t *testing.T) {
		env := pullEnv(t, v, "/v3/env/pull/pull-me/development")
		if env["DEV_ONLY"] != "d" {
			t.Errorf("env by name = %v", env)
		}
	})
}

func TestVercelEnvPullRefusals(t *testing.T) {
	v := newVX(t, "vercel-envpull-refusals")
	pullFixture(t, v)

	t.Run("Given an unknown project When pulled Then 404 in the error envelope", func(t *testing.T) {
		for _, path := range []string{"/v3/env/pull/prj_nope", "/v3/env/pull/prj_nope/development", "/v3/env/pull/prj_nope/preview/feature"} {
			r := v.do(http.MethodGet, path, nil)
			e := errorOf(r)
			if r.status != 404 || str(e, "code") == "" || str(e, "message") == "" {
				t.Errorf("GET %s = %d %s, want a 404 in {error:{code,message}}", path, r.status, r.raw)
			}
		}
	})

	t.Run("Given a target that is no environment When pulled Then 400 rather than an empty file", func(t *testing.T) {
		r := v.do(http.MethodGet, "/v3/env/pull/pull-me/prod", nil)
		if r.status != 400 || str(errorOf(r), "code") != "bad_request" {
			t.Errorf("GET .../prod = %d %s", r.status, r.raw)
		}
	})

	t.Run("Given another team When it pulls the project Then the project is not there", func(t *testing.T) {
		made := v.do(http.MethodPost, "/v2/teams", map[string]any{"slug": "other-team"})
		if made.status != 200 {
			t.Fatalf("create team = %d %s", made.status, made.raw)
		}
		r := v.do(http.MethodGet, "/v3/env/pull/pull-me/development?teamId="+str(made.body, "id"), nil)
		if r.status != 404 {
			t.Errorf("a team pulled the personal account's project: %d %s", r.status, r.raw)
		}
	})
}

func TestVercelEnvPullReadsCustomEnvironments(t *testing.T) {
	v := newVX(t, "vercel-envpull-custom")
	pid := str(v.do(http.MethodPost, "/v11/projects", map[string]any{"name": "pull-custom"}).body, "id")
	ce := v.do(http.MethodPost, "/v9/projects/"+pid+"/custom-environments", map[string]any{"slug": "staging"})
	if ce.status != 200 && ce.status != 201 {
		t.Fatalf("create custom environment = %d %s", ce.status, ce.raw)
	}
	cid := str(ce.body, "id")
	for _, body := range []map[string]any{
		{"key": "ONLY_STAGING", "value": "s", "type": "plain", "customEnvironmentIds": []string{cid}},
		{"key": "EVERYWHERE", "value": "e", "type": "plain", "target": []string{"production"}},
	} {
		if r := v.do(http.MethodPost, "/v10/projects/"+pid+"/env", body); r.status != 201 {
			t.Fatalf("create %v = %d %s", body["key"], r.status, r.raw)
		}
	}
	for _, target := range []string{"staging", cid} {
		env := pullEnv(t, v, "/v3/env/pull/"+pid+"/"+target)
		if len(env) != 1 || env["ONLY_STAGING"] != "s" {
			t.Errorf("pull of custom environment %q = %v, want only ONLY_STAGING", target, env)
		}
	}
	if env := pullEnv(t, v, "/v3/env/pull/"+pid+"/production"); env["ONLY_STAGING"] != nil || env["EVERYWHERE"] != "e" {
		t.Errorf("production env = %v", env)
	}
}
