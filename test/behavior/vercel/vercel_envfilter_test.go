package vercel

import (
	"net/http"
	"strings"
	"testing"
)

// `vercel env pull` reads the values from GET /v3/env/pull and, for every key that
// came back empty, asks GET /v10/projects/{id}/env?target=<environment> whether
// that variable is a sensitive one it could not read. It writes a placeholder only
// if the answer says so. An answer that ignored `target` would list a sensitive
// variable of production beside a legitimately empty one of development, and the
// CLI would tell the person the development value was unavailable.

// recordedSensitiveLookup is the request the real CLI sent, read from the
// recorded journey, with the project it was recorded against replaced by pid.
func recordedSensitiveLookup(t *testing.T, pid string) string {
	t.Helper()
	step := recordedStep(t, "vercel-cli-63.1.2", "env-pull", "env pull asks which empty values are sensitive")
	return strings.ReplaceAll(step.Request.Path, "{{projectId}}", pid)
}

func envKeys(t *testing.T, r reply) map[string]string {
	t.Helper()
	if r.status != 200 {
		t.Fatalf("list = %d %s", r.status, r.raw)
	}
	out := map[string]string{}
	list, _ := r.body["envs"].([]any)
	for _, e := range list {
		m := e.(map[string]any)
		out[str(m, "key")+"/"+str(m, "type")] = str(m, "value")
	}
	return out
}

func TestVercelProjectEnvsAreListedForTheTargetTheCLIAsksAbout(t *testing.T) {
	v := newVX(t, "vercel-envfilter")
	pid := str(v.do(http.MethodPost, "/v11/projects", map[string]any{"name": "shared-key"}).body, "id")
	add := func(body map[string]any) {
		t.Helper()
		if r := v.do(http.MethodPost, "/v10/projects/"+pid+"/env", body); r.status != 201 {
			t.Fatalf("create %v = %d %s", body["key"], r.status, r.raw)
		}
	}
	// One key, a legitimately empty plain value for development and a sensitive
	// one for production.
	add(map[string]any{"key": "SHARED", "value": "", "type": "plain", "target": []string{"development"}})
	add(map[string]any{"key": "SHARED", "value": "vtok_dummy_marker_0123456789abcdef", "type": "sensitive", "target": []string{"production"}})
	add(map[string]any{"key": "EVERYWHERE", "value": "x", "type": "plain", "target": []string{"production", "preview", "development"}})
	add(map[string]any{"key": "FEATURE", "value": "on", "type": "plain", "target": []string{"preview"}, "gitBranch": "feature"})
	add(map[string]any{"key": "PREVIEW", "value": "p", "type": "plain", "target": []string{"preview"}})
	ce := v.do(http.MethodPost, "/v9/projects/"+pid+"/custom-environments", map[string]any{"slug": "staging"})
	if ce.status != 201 {
		t.Fatalf("custom environment = %d %s", ce.status, ce.raw)
	}
	cid := str(ce.body, "id")
	add(map[string]any{"key": "STAGING_ONLY", "value": "s", "type": "plain", "customEnvironmentIds": []string{cid}})
	// One that lists a built-in target and the custom environment both.
	add(map[string]any{"key": "BOTH", "value": "b", "type": "plain", "target": []string{"production"}, "customEnvironmentIds": []string{cid}})
	list := func(query string) map[string]string {
		t.Helper()
		return envKeys(t, v.do(http.MethodGet, "/v10/projects/"+pid+"/env?"+query, nil))
	}
	same := func(t *testing.T, what string, got map[string]string, want ...string) {
		t.Helper()
		if len(got) != len(want) {
			t.Fatalf("%s lists %v, want exactly %v", what, got, want)
		}
		for _, k := range want {
			if _, ok := got[k]; !ok {
				t.Errorf("%s lists %v, want %s", what, got, k)
			}
		}
	}

	prod := recordedSensitiveLookup(t, pid)
	if !strings.Contains(prod, "target=production") {
		t.Fatalf("the recorded lookup is %q, which no longer names a target", prod)
	}
	t.Run("Given the CLI's own lookup for production Then the sensitive variable is there and the development one is not", func(t *testing.T) {
		got := envKeys(t, v.do(http.MethodGet, prod, nil))
		same(t, prod, got, "SHARED/sensitive", "EVERYWHERE/plain", "BOTH/plain")
		if got["SHARED/sensitive"] != "" {
			t.Errorf("a sensitive variable lists its value: %q", got["SHARED/sensitive"])
		}
	})
	t.Run("Given the same lookup for development Then SHARED is the plain empty one the CLI must write as empty", func(t *testing.T) {
		dev := strings.Replace(prod, "target=production", "target=development", 1)
		same(t, dev, envKeys(t, v.do(http.MethodGet, dev, nil)), "SHARED/plain", "EVERYWHERE/plain")
	})
	t.Run("Given a preview lookup Then it holds the preview variables, and a branch narrows it to that branch's and the unbranched", func(t *testing.T) {
		same(t, "preview", list("target=preview"), "EVERYWHERE/plain", "PREVIEW/plain", "FEATURE/plain")
		same(t, "preview on feature", list("target=preview&gitBranch=feature"), "EVERYWHERE/plain", "PREVIEW/plain", "FEATURE/plain")
		same(t, "preview on another branch", list("target=preview&gitBranch=other"), "EVERYWHERE/plain", "PREVIEW/plain")
	})
	t.Run("Given a custom environment, by slug, by id or as the target Then it holds the variables that list it", func(t *testing.T) {
		same(t, "customEnvironmentSlug", list("customEnvironmentSlug=staging"), "STAGING_ONLY/plain", "BOTH/plain")
		same(t, "customEnvironmentId", list("customEnvironmentId="+cid), "STAGING_ONLY/plain", "BOTH/plain")
		same(t, "target=<slug>", list("target=staging"), "STAGING_ONLY/plain", "BOTH/plain")
	})
	t.Run("Given a built-in target and a custom environment together Then both narrow the listing", func(t *testing.T) {
		same(t, "production and staging, by slug", list("target=production&customEnvironmentSlug=staging"), "BOTH/plain")
		same(t, "production and staging, by id", list("target=production&customEnvironmentId="+cid), "BOTH/plain")
		same(t, "staging as target and by slug", list("target=staging&customEnvironmentSlug=staging"), "STAGING_ONLY/plain", "BOTH/plain")
		same(t, "development and staging", list("target=development&customEnvironmentSlug=staging"))
		same(t, "production and a slug the project lacks", list("target=production&customEnvironmentSlug=nowhere"))
	})
	t.Run("Given no target Then every variable is listed, as it always was", func(t *testing.T) {
		same(t, "no target", list("source=vercel-cli%3Aenv%3Apull"),
			"SHARED/plain", "SHARED/sensitive", "EVERYWHERE/plain", "FEATURE/plain", "PREVIEW/plain", "STAGING_ONLY/plain", "BOTH/plain")
	})
	t.Run("Given a target that is neither one of the three nor a custom environment Then nothing matches", func(t *testing.T) {
		same(t, "target=prod", list("target=prod"))
	})
}
