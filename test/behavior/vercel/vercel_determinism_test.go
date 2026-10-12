package vercel

import (
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/config"
)

// transcript drives the deploy path and answers every body it saw, in order.
// Nothing in it is random: the ids come from the seed, the instants from the
// controllable clock, so two runs of one seed must produce one transcript.
func transcript(t *testing.T, v *vx) []string {
	t.Helper()
	var out []string
	note := func(r reply) reply { out = append(out, string(r.raw)); return r }
	pid := str(note(v.do(http.MethodPost, "/v11/projects", map[string]any{"name": "det-app"})).body, "id")
	note(v.do(http.MethodPost, "/v10/projects/"+pid+"/env", map[string]any{"key": "A", "value": "1", "type": "encrypted", "target": []string{"production"}}))
	content := "<h1>det</h1>"
	note(v.do(http.MethodPost, "/v2/files", []byte(content), "x-vercel-digest", sha1Of(content)))
	dep := note(v.do(http.MethodPost, "/v13/deployments", map[string]any{"name": "det-app", "project": "det-app", "target": "production", "files": []map[string]any{{"file": "index.html", "sha": sha1Of(content), "size": len(content)}}}))
	id := str(dep.body, "id")
	v.advance(3 * time.Second)
	note(v.do(http.MethodGet, "/v13/deployments/"+id, nil))
	note(v.do(http.MethodPost, "/v2/deployments/"+id+"/aliases", map[string]any{"alias": "det.example.com"}))
	note(v.do(http.MethodPost, "/v10/projects/"+pid+"/domains", map[string]any{"name": "det.example.com"}))
	note(v.do(http.MethodPost, "/v1/edge-config", map[string]any{"slug": "det_cfg", "items": map[string]any{"k": "v"}}))
	note(v.do(http.MethodGet, "/v10/projects", nil))
	return out
}

// TestVercelIsDeterministic holds what a test suite built on the emulator
// relies on: the same seed gives the same ids, urls and instants on every run,
// and another seed gives others.
func TestVercelIsDeterministic(t *testing.T) {
	a := transcript(t, newVX(t, "det-seed"))
	b := transcript(t, newVX(t, "det-seed"))
	if len(a) != len(b) {
		t.Fatalf("lengths differ: %d vs %d", len(a), len(b))
	}
	for i := range a {
		if a[i] != b[i] {
			t.Errorf("step %d differs between two runs of one seed:\n  %s\n  %s", i, a[i], b[i])
		}
	}
	c := transcript(t, newVX(t, "another-seed"))
	if strings.Join(a, "") == strings.Join(c, "") {
		t.Error("another seed produced the same transcript")
	}
}

// TestVercelStateSurvivesARestart holds persistence through the snapshot every
// service uses: what was made is there after a restart, and what is made next
// does not take an identifier the last run handed out.
func TestVercelStateSurvivesARestart(t *testing.T) {
	dir := t.TempDir()
	persist := withConfig(func(c *config.Config) { c.PersistDir = dir })

	first := newVXWith(t, "persist-vercel", nil, persist)
	made := first.do(http.MethodPost, "/v11/projects", map[string]any{"name": "kept"})
	pid := str(made.body, "id")
	first.do(http.MethodPost, "/v10/projects/"+pid+"/env", map[string]any{"key": "KEPT", "value": "yes", "type": "plain", "target": []string{"production"}})
	dep := first.do(http.MethodPost, "/v13/deployments", map[string]any{"name": "kept", "project": "kept"})
	first.do(http.MethodPost, "/v1/edge-config", map[string]any{"slug": "kept_cfg", "items": map[string]any{"k": "v"}})
	if err := first.rt.SavePersist(); err != nil {
		t.Fatal(err)
	}
	if err := first.rt.Close(); err != nil {
		t.Fatal(err)
	}

	second := newVXWith(t, "persist-vercel", nil, persist)
	if r := second.do(http.MethodGet, "/v9/projects/kept", nil); r.status != 200 || str(r.body, "id") != pid {
		t.Fatalf("the project after a restart = %d %s", r.status, r.raw)
	}
	envs := second.do(http.MethodGet, "/v10/projects/"+pid+"/env", nil)
	if list, _ := envs.body["envs"].([]any); len(list) != 1 {
		t.Fatalf("the variables after a restart = %s", envs.raw)
	}
	if r := second.do(http.MethodGet, "/v13/deployments/"+str(dep.body, "id"), nil); r.status != 200 {
		t.Fatalf("the deployment after a restart = %d", r.status)
	}
	if list := jsonList(t, second.do(http.MethodGet, "/v1/edge-config", nil)); len(list) != 1 {
		t.Fatalf("the edge config after a restart = %v", list)
	}

	// What is made next takes new identifiers: not the project's, not the
	// deployment's, none the first run drew.
	next := second.do(http.MethodPost, "/v11/projects", map[string]any{"name": "after"})
	if str(next.body, "id") == pid {
		t.Fatalf("the project made after a restart took the id of the one before it: %s", next.raw)
	}
	nextDep := second.do(http.MethodPost, "/v13/deployments", map[string]any{"name": "after", "project": "after"})
	if str(nextDep.body, "id") == str(dep.body, "id") || str(nextDep.body, "url") == str(dep.body, "url") {
		t.Fatalf("the deployment made after a restart reused an identity: %s", nextDep.raw)
	}
	if r := second.do(http.MethodGet, "/v9/projects/kept", nil); str(r.body, "id") != pid {
		t.Fatalf("the first project was overwritten: %s", r.raw)
	}
}
