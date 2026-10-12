package vercel

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"reflect"
	"strings"
	"testing"
)

// envProject makes a project and answers its id.
func envProject(t *testing.T, v *vx, name string) string {
	t.Helper()
	r := v.do(http.MethodPost, "/v11/projects", map[string]any{"name": name})
	if r.status != 200 {
		t.Fatalf("create project = %d %s", r.status, r.raw)
	}
	return str(r.body, "id")
}

// storeText is everything the emulator holds, as text: the snapshot's JSON with
// every string leaf both as written and base64-decoded, because the store keeps
// a record as bytes and the snapshot writes bytes as base64. A search of it
// answers "is this value anywhere at rest", whichever layer holds it.
func storeText(t *testing.T, v *vx) string {
	t.Helper()
	var buf bytes.Buffer
	if err := v.rt.Deps.Store.Snapshot(context.Background(), &buf); err != nil {
		t.Fatal(err)
	}
	tr := tar.NewReader(&buf)
	var out strings.Builder
	var walk func(any)
	walk = func(n any) {
		switch x := n.(type) {
		case map[string]any:
			for k, e := range x {
				out.WriteString(k + "\n")
				walk(e)
			}
		case []any:
			for _, e := range x {
				walk(e)
			}
		case string:
			out.WriteString(x + "\n")
			if raw, err := base64.StdEncoding.DecodeString(x); err == nil {
				out.Write(raw)
				out.WriteString("\n")
			}
		}
	}
	for {
		h, err := tr.Next()
		if err != nil {
			break
		}
		if h.Name != "store.json" {
			continue
		}
		body, _ := io.ReadAll(tr)
		var doc any
		if err := json.Unmarshal(body, &doc); err != nil {
			t.Fatal(err)
		}
		walk(doc)
	}
	return out.String()
}

// envList reads a project's variables as a client would and answers them keyed
// by variable key (the tests below never repeat a key within a listing unless
// they mean to; they use envListAll when they do).
func envList(t *testing.T, v *vx, path string) []map[string]any {
	t.Helper()
	r := v.do(http.MethodGet, path, nil)
	if r.status != 200 {
		t.Fatalf("list %s = %d %s", path, r.status, r.raw)
	}
	var out []map[string]any
	for _, e := range r.body["envs"].([]any) {
		out = append(out, e.(map[string]any))
	}
	return out
}

// TestVercelEnvTypesAndWhatEachShows holds the one rule a client would get
// wrong: what the API shows of a value depends on the variable's type and on
// how it was asked for.
func TestVercelEnvTypesAndWhatEachShows(t *testing.T) {
	v := newVX(t, "vercel-env-types")
	pid := envProject(t, v, "site")
	base := "/v10/projects/" + pid + "/env"

	for _, c := range []map[string]any{
		{"key": "PLAIN", "value": "p-value", "type": "plain", "target": []string{"production", "preview", "development"}},
		{"key": "SECRET", "value": "s-value", "type": "secret", "target": []string{"production"}},
		{"key": "ENC", "value": "e-value", "type": "encrypted", "target": []string{"production"}},
		{"key": "SENS", "value": "x-value", "type": "sensitive", "target": []string{"production"}},
	} {
		r := v.do(http.MethodPost, base, c)
		if r.status != 201 {
			t.Fatalf("create %v = %d %s", c["key"], r.status, r.raw)
		}
		created := r.body["created"].(map[string]any)
		if str(created, "key") != c["key"] || !strings.HasPrefix(str(created, "id"), "env_") {
			t.Fatalf("created = %v", created)
		}
		if failed, _ := r.body["failed"].([]any); len(failed) != 0 {
			t.Fatalf("failed = %v", failed)
		}
	}

	byKey := func(rows []map[string]any) map[string]map[string]any {
		m := map[string]map[string]any{}
		for _, e := range rows {
			m[str(e, "key")] = e
		}
		return m
	}
	shown := byKey(envList(t, v, base))
	if got := str(shown["PLAIN"], "value"); got != "p-value" {
		t.Errorf("plain shows %q", got)
	}
	if got := str(shown["SECRET"], "value"); got != "s-value" {
		t.Errorf("secret shows %q", got)
	}
	if got := str(shown["SENS"], "value"); got != "" {
		t.Errorf("a sensitive value is never shown, listing shows %q", got)
	}
	enc := str(shown["ENC"], "value")
	if enc == "" || enc == "e-value" {
		t.Errorf("an encrypted value listed is opaque, shows %q", enc)
	}
	if _, has := shown["ENC"]["decrypted"]; has {
		t.Errorf("an opaque value is not marked decrypted: %v", shown["ENC"])
	}

	t.Run("decrypt=true shows an encrypted value and marks it", func(t *testing.T) {
		d := byKey(envList(t, v, base+"?decrypt=true"))
		if str(d["ENC"], "value") != "e-value" || d["ENC"]["decrypted"] != true {
			t.Errorf("decrypted = %v", d["ENC"])
		}
		if str(d["SENS"], "value") != "" {
			t.Errorf("decrypt never shows a sensitive value: %v", d["SENS"])
		}
	})

	t.Run("addressing one variable by id shows an encrypted value", func(t *testing.T) {
		r := v.do(http.MethodGet, "/v1/projects/"+pid+"/env/"+str(shown["ENC"], "id"), nil)
		if r.status != 200 || str(r.body, "value") != "e-value" {
			t.Fatalf("get = %d %s", r.status, r.raw)
		}
		s := v.do(http.MethodGet, "/v1/projects/"+pid+"/env/"+str(shown["SENS"], "id"), nil)
		if s.status != 200 || str(s.body, "value") != "" || str(s.body, "type") != "sensitive" {
			t.Fatalf("get sensitive = %d %s", s.status, s.raw)
		}
	})

	t.Run("a sensitive value is not stored", func(t *testing.T) {
		if strings.Contains(string(v.do(http.MethodGet, base+"?decrypt=true", nil).raw), "x-value") {
			t.Fatal("the sensitive value came back")
		}
		snap := storeText(t, v)
		if strings.Contains(snap, "x-value") {
			t.Fatal("the sensitive value is in the store")
		}
		if !strings.Contains(snap, "e-value") {
			t.Fatal("an encrypted value is kept, so the control above proves the search")
		}
	})
}

// TestVercelEnvDuplicatesAndUpsert holds the 409 a client reads to ask whether
// to overwrite, what counts as a duplicate, and what ?upsert=true does.
func TestVercelEnvDuplicatesAndUpsert(t *testing.T) {
	v := newVX(t, "vercel-env-dup")
	pid := envProject(t, v, "site")
	base := "/v10/projects/" + pid + "/env"
	first := v.do(http.MethodPost, base, map[string]any{"key": "K", "value": "1", "type": "plain", "target": []string{"production"}})
	if first.status != 201 {
		t.Fatalf("create = %d %s", first.status, first.raw)
	}
	id := str(first.body["created"].(map[string]any), "id")

	t.Run("the same key and target conflicts", func(t *testing.T) {
		r := v.do(http.MethodPost, base, map[string]any{"key": "K", "value": "2", "type": "plain", "target": []string{"production", "preview"}})
		if r.status != 409 {
			t.Fatalf("duplicate = %d %s", r.status, r.raw)
		}
		e := r.body["error"].(map[string]any)
		if str(e, "code") != "ENV_CONFLICT" || str(e, "envVarKey") != "K" || !strings.Contains(str(e, "message"), "already exists") {
			t.Fatalf("envelope = %v", e)
		}
		if !reflect.DeepEqual(e["target"], []any{"production", "preview"}) {
			t.Fatalf("target = %v", e["target"])
		}
	})

	t.Run("the same key on another target is another variable", func(t *testing.T) {
		if r := v.do(http.MethodPost, base, map[string]any{"key": "K", "value": "3", "type": "plain", "target": []string{"development"}}); r.status != 201 {
			t.Fatalf("other target = %d %s", r.status, r.raw)
		}
	})

	t.Run("the same key for a branch is another variable", func(t *testing.T) {
		if r := v.do(http.MethodPost, base, map[string]any{"key": "K", "value": "b", "type": "plain", "target": []string{"preview"}, "gitBranch": "feature"}); r.status != 201 {
			t.Fatalf("branch = %d %s", r.status, r.raw)
		}
		if r := v.do(http.MethodPost, base, map[string]any{"key": "K", "value": "b", "type": "plain", "target": []string{"preview"}, "gitBranch": "feature"}); r.status != 409 {
			t.Fatalf("branch duplicate = %d %s", r.status, r.raw)
		}
	})

	t.Run("upsert updates the variable it would conflict with", func(t *testing.T) {
		r := v.do(http.MethodPost, base+"?upsert=true", map[string]any{"key": "K", "value": "updated", "type": "plain", "target": []string{"production"}})
		if r.status != 201 {
			t.Fatalf("upsert = %d %s", r.status, r.raw)
		}
		if got := r.body["created"].(map[string]any); str(got, "id") != id || str(got, "value") != "updated" {
			t.Fatalf("upsert answered %v, want variable %s updated", got, id)
		}
		if g := v.do(http.MethodGet, "/v1/projects/"+pid+"/env/"+id, nil); str(g.body, "value") != "updated" {
			t.Fatalf("stored = %s", g.raw)
		}
	})

	t.Run("a batch reports what did not go through", func(t *testing.T) {
		r := v.do(http.MethodPost, base, []map[string]any{
			{"key": "NEW", "value": "n", "type": "plain", "target": []string{"production"}},
			{"key": "K", "value": "dup", "type": "plain", "target": []string{"production"}},
			{"key": "NEW", "value": "again", "type": "plain", "target": []string{"production"}},
		})
		if r.status != 201 {
			t.Fatalf("batch = %d %s", r.status, r.raw)
		}
		created, _ := r.body["created"].([]any)
		failed, _ := r.body["failed"].([]any)
		if len(created) != 1 || len(failed) != 2 {
			t.Fatalf("created %d failed %d: %s", len(created), len(failed), r.raw)
		}
		f := failed[0].(map[string]any)["error"].(map[string]any)
		if str(f, "code") != "ENV_CONFLICT" {
			t.Fatalf("failure = %v", f)
		}
	})
}

// TestVercelEnvDefaultsAndRefusals holds what a variable is when a request
// leaves things out, and the requests that are refused.
func TestVercelEnvDefaultsAndRefusals(t *testing.T) {
	v := newVX(t, "vercel-env-defaults")
	pid := envProject(t, v, "site")
	base := "/v10/projects/" + pid + "/env"

	r := v.do(http.MethodPost, base, map[string]any{"key": "BARE", "value": "v"})
	if r.status != 201 {
		t.Fatalf("bare create = %d %s", r.status, r.raw)
	}
	c := r.body["created"].(map[string]any)
	if str(c, "type") != "encrypted" || !reflect.DeepEqual(c["target"], []any{"production", "preview", "development"}) {
		t.Fatalf("defaults = %v", c)
	}

	for name, c := range map[string]struct {
		body map[string]any
		want int
	}{
		"no key":               {map[string]any{"value": "v"}, 400},
		"an unknown type":      {map[string]any{"key": "A", "value": "v", "type": "wishful"}, 400},
		"an unknown target":    {map[string]any{"key": "A", "value": "v", "target": []string{"staging"}}, 400},
		"a branch off preview": {map[string]any{"key": "A", "value": "v", "target": []string{"production"}, "gitBranch": "x"}, 400},
		"sensitive for dev":    {map[string]any{"key": "A", "value": "v", "type": "sensitive", "target": []string{"development"}}, 400},
	} {
		if got := v.do(http.MethodPost, base, c.body); got.status != c.want {
			t.Errorf("%s = %d %s, want %d", name, got.status, got.raw, c.want)
		}
	}
	if got := v.do(http.MethodPost, "/v10/projects/prj_nope/env", map[string]any{"key": "A", "value": "v"}); got.status != 404 {
		t.Errorf("unknown project = %d", got.status)
	}
	if got := v.do(http.MethodGet, "/v10/projects/prj_nope/env", nil); got.status != 404 {
		t.Errorf("list in an unknown project = %d", got.status)
	}
}

// TestVercelEnvEditAndRemove holds editing a variable, removing one, removing
// several, and that every version spelling reaches the same store.
func TestVercelEnvEditAndRemove(t *testing.T) {
	v := newVX(t, "vercel-env-edit")
	pid := envProject(t, v, "site")

	// Created through /v9, read through /v10, /v8 and by project name: one store.
	r := v.do(http.MethodPost, "/v9/projects/"+pid+"/env", map[string]any{"key": "A", "value": "1", "type": "plain", "target": []string{"production"}})
	if r.status != 201 {
		t.Fatalf("v9 create = %d %s", r.status, r.raw)
	}
	a := str(r.body["created"].(map[string]any), "id")
	b := str(v.do(http.MethodPost, "/v8/projects/site/env", map[string]any{"key": "B", "value": "2", "type": "plain", "target": []string{"production"}}).body["created"].(map[string]any), "id")
	if b == "" {
		t.Fatal("v8 create by project name made nothing")
	}
	for _, p := range []string{"/v10/projects/" + pid + "/env", "/v9/projects/site/env", "/v8/projects/" + pid + "/env"} {
		if rows := envList(t, v, p); len(rows) != 2 {
			t.Errorf("%s lists %d variables, want 2", p, len(rows))
		}
	}
	for _, p := range []string{"/v1/projects/" + pid + "/env/" + a, "/v10/projects/" + pid + "/env/" + a, "/v9/projects/site/env/" + a, "/v8/projects/" + pid + "/env/" + a} {
		if g := v.do(http.MethodGet, p, nil); g.status != 200 || str(g.body, "key") != "A" {
			t.Errorf("get %s = %d %s", p, g.status, g.raw)
		}
	}

	t.Run("an edit changes what it names and keeps the rest", func(t *testing.T) {
		e := v.do(http.MethodPatch, "/v9/projects/"+pid+"/env/"+a, map[string]any{"value": "one", "comment": "first"})
		if e.status != 200 || str(e.body, "value") != "one" || str(e.body, "comment") != "first" || str(e.body, "key") != "A" {
			t.Fatalf("edit = %d %s", e.status, e.raw)
		}
		if !reflect.DeepEqual(e.body["target"], []any{"production"}) {
			t.Fatalf("an edit that names no target keeps it: %v", e.body["target"])
		}
		if e2 := v.do(http.MethodPatch, "/v10/projects/"+pid+"/env/"+a, map[string]any{"target": []string{"production", "preview"}}); e2.status != 200 || str(e2.body, "value") != "one" {
			t.Fatalf("edit at v10 = %d %s", e2.status, e2.raw)
		}
	})

	t.Run("an edit into a collision is refused", func(t *testing.T) {
		e := v.do(http.MethodPatch, "/v9/projects/"+pid+"/env/"+b, map[string]any{"key": "A"})
		if e.status != 409 || str(e.body["error"].(map[string]any), "code") != "ENV_CONFLICT" {
			t.Fatalf("edit into a clash = %d %s", e.status, e.raw)
		}
	})

	t.Run("editing a variable that is not there", func(t *testing.T) {
		if e := v.do(http.MethodPatch, "/v9/projects/"+pid+"/env/env_nope", map[string]any{"value": "x"}); e.status != 404 {
			t.Fatalf("edit = %d %s", e.status, e.raw)
		}
	})

	t.Run("remove answers the variable and takes it", func(t *testing.T) {
		d := v.do(http.MethodDelete, "/v9/projects/"+pid+"/env/"+b, nil)
		if d.status != 200 || str(d.body, "id") != b || str(d.body, "key") != "B" {
			t.Fatalf("remove = %d %s", d.status, d.raw)
		}
		if g := v.do(http.MethodGet, "/v1/projects/"+pid+"/env/"+b, nil); g.status != 404 {
			t.Fatalf("after remove = %d", g.status)
		}
		if d2 := v.do(http.MethodDelete, "/v9/projects/"+pid+"/env/"+b, nil); d2.status != 404 {
			t.Fatalf("remove again = %d", d2.status)
		}
	})

	t.Run("batch remove takes the ids it names", func(t *testing.T) {
		c := str(v.do(http.MethodPost, "/v10/projects/"+pid+"/env", map[string]any{"key": "C", "value": "3", "type": "plain", "target": []string{"production"}}).body["created"].(map[string]any), "id")
		d := v.do(http.MethodDelete, "/v1/projects/"+pid+"/env", map[string]any{"ids": []string{a, c}})
		if d.status != 200 || d.body["deleted"] != float64(2) {
			t.Fatalf("batch remove = %d %s", d.status, d.raw)
		}
		if rows := envList(t, v, "/v10/projects/"+pid+"/env"); len(rows) != 0 {
			t.Fatalf("left %v", rows)
		}
		if bad := v.do(http.MethodDelete, "/v1/projects/"+pid+"/env", map[string]any{"ids": []string{}}); bad.status != 400 {
			t.Fatalf("empty ids = %d %s", bad.status, bad.raw)
		}
	})
}

// TestVercelEnvListingFiltersByBranchAndDeleteWithProject holds the git-branch
// filter and that a project takes its variables with it.
func TestVercelEnvListingFiltersByBranchAndDeleteWithProject(t *testing.T) {
	v := newVX(t, "vercel-env-branch")
	pid := envProject(t, v, "site")
	base := "/v10/projects/" + pid + "/env"
	for _, c := range []map[string]any{
		{"key": "ALL", "value": "1", "type": "plain", "target": []string{"production", "preview"}},
		{"key": "ONLY", "value": "2", "type": "plain", "target": []string{"preview"}, "gitBranch": "feature"},
		{"key": "OTHER", "value": "3", "type": "plain", "target": []string{"preview"}, "gitBranch": "elsewhere"},
	} {
		if r := v.do(http.MethodPost, base, c); r.status != 201 {
			t.Fatalf("create = %d %s", r.status, r.raw)
		}
	}
	keys := func(rows []map[string]any) []string {
		var k []string
		for _, e := range rows {
			k = append(k, str(e, "key"))
		}
		return k
	}
	if got := keys(envList(t, v, base)); len(got) != 3 || got[0] != "ALL" {
		t.Errorf("listing is in creation order: %v", got)
	}
	if got := keys(envList(t, v, base+"?gitBranch=feature")); !reflect.DeepEqual(got, []string{"ALL", "ONLY"}) {
		t.Errorf("branch listing = %v", got)
	}
	if d := v.do(http.MethodDelete, "/v9/projects/"+pid, nil); d.status != 204 {
		t.Fatalf("delete project = %d", d.status)
	}
	pid2 := envProject(t, v, "site")
	if pid2 == pid {
		t.Fatal("a new project got the old id")
	}
	if got := envList(t, v, "/v10/projects/"+pid2+"/env"); len(got) != 0 {
		t.Errorf("a new project inherits %v", got)
	}
}
