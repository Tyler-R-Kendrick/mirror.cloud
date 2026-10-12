package vercel

import (
	"net/http"
	"reflect"
	"strings"
	"testing"
)

// edgeConfig makes a config at the path given and answers its id.
func edgeConfig(t *testing.T, v *vx, path string, body map[string]any) string {
	t.Helper()
	r := v.do(http.MethodPost, path, body)
	if r.status != 201 {
		t.Fatalf("create %s = %d %s", path, r.status, r.raw)
	}
	return str(r.body, "id")
}

func jsonList(t *testing.T, r reply) []map[string]any {
	t.Helper()
	var out []map[string]any
	if err := unmarshalJSON(r.raw, &out); err != nil {
		t.Fatalf("not a JSON array: %s", r.raw)
	}
	return out
}

// TestVercelEdgeConfigLifecycle holds the control plane a client drives: a
// config is made at either spelling of the route, listed as an array, renamed
// and deleted, and what is in it goes with it.
func TestVercelEdgeConfigLifecycle(t *testing.T) {
	v := newVX(t, "vercel-edge-config")

	made := v.do(http.MethodPost, "/v1/edge-config", map[string]any{"slug": "flags", "items": map[string]any{"greeting": "hello", "limits": map[string]any{"max": 3}}})
	if made.status != 201 || !strings.HasPrefix(str(made.body, "id"), "ecfg_") || str(made.body, "slug") != "flags" ||
		made.body["itemCount"] != float64(2) || made.body["sizeInBytes"].(float64) <= 0 || str(made.body, "digest") == "" {
		t.Fatalf("create = %d %s", made.status, made.raw)
	}
	id := str(made.body, "id")
	second := edgeConfig(t, v, "/v1/global-config", map[string]any{"slug": "second"})

	t.Run("a slug starts with a letter", func(t *testing.T) {
		for _, bad := range []string{"", "1abc", "has space", "a/b"} {
			if r := v.do(http.MethodPost, "/v1/edge-config", map[string]any{"slug": bad}); r.status != 400 {
				t.Errorf("slug %q = %d %s", bad, r.status, r.raw)
			}
		}
		if r := v.do(http.MethodPost, "/v1/edge-config", map[string]any{"slug": "ok", "items": map[string]any{"bad key": 1}}); r.status != 400 {
			t.Errorf("a bad item key = %d %s", r.status, r.raw)
		}
	})

	t.Run("listing is an array at both spellings, newest first", func(t *testing.T) {
		for _, p := range []string{"/v1/edge-config", "/v1/global-config"} {
			list := jsonList(t, v.do(http.MethodGet, p, nil))
			if len(list) != 2 || str(list[0], "id") != second || str(list[1], "id") != id {
				t.Errorf("%s = %v", p, list)
			}
		}
	})

	t.Run("get, rename, delete", func(t *testing.T) {
		for _, p := range []string{"/v1/edge-config/" + id, "/v1/global-config/" + id} {
			if r := v.do(http.MethodGet, p, nil); r.status != 200 || str(r.body, "slug") != "flags" {
				t.Errorf("get %s = %d %s", p, r.status, r.raw)
			}
		}
		if r := v.do(http.MethodGet, "/v1/edge-config/ecfg_nope", nil); r.status != 404 {
			t.Errorf("unknown = %d", r.status)
		}
		u := v.do(http.MethodPut, "/v1/edge-config/"+id, map[string]any{"slug": "renamed"})
		if u.status != 200 || str(u.body, "slug") != "renamed" || u.body["itemCount"] != float64(2) {
			t.Fatalf("rename = %d %s", u.status, u.raw)
		}
		if bad := v.do(http.MethodPut, "/v1/edge-config/"+id, map[string]any{"slug": "9"}); bad.status != 400 {
			t.Errorf("bad rename = %d", bad.status)
		}
		d := v.do(http.MethodDelete, "/v1/edge-config/"+id, nil)
		if d.status != 204 || len(d.raw) != 0 {
			t.Fatalf("delete = %d %q", d.status, d.raw)
		}
		if g := v.do(http.MethodGet, "/v1/edge-config/"+id, nil); g.status != 404 {
			t.Errorf("after delete = %d", g.status)
		}
		if d2 := v.do(http.MethodDelete, "/v1/edge-config/"+id, nil); d2.status != 404 {
			t.Errorf("delete again = %d", d2.status)
		}
		if g := v.do(http.MethodGet, "/v1/edge-config/"+id+"/items", nil); g.status != 404 {
			t.Errorf("items of a deleted config = %d", g.status)
		}
	})
}

// TestVercelEdgeConfigItems holds the item rules: a change is all or nothing,
// each operation means what it says, and the config's digest and counts follow
// its items.
func TestVercelEdgeConfigItems(t *testing.T) {
	v := newVX(t, "vercel-edge-config-items")
	id := edgeConfig(t, v, "/v1/edge-config", map[string]any{"slug": "cfg", "items": map[string]any{"a": "one"}})
	base := "/v1/edge-config/" + id
	digest := func() string { return str(v.do(http.MethodGet, base, nil).body, "digest") }
	d0 := digest()

	patch := func(items ...map[string]any) reply {
		return v.do(http.MethodPatch, base+"/items", map[string]any{"items": items})
	}

	r := patch(
		map[string]any{"operation": "create", "key": "b", "value": map[string]any{"nested": []any{1, 2, 3}}, "description": "an object"},
		map[string]any{"operation": "upsert", "key": "c", "value": true},
		map[string]any{"operation": "upsert", "key": "a", "value": "uno"},
		map[string]any{"operation": "create", "key": "n", "value": nil},
	)
	if r.status != 200 || str(r.body, "status") != "ok" {
		t.Fatalf("patch = %d %s", r.status, r.raw)
	}
	if d1 := digest(); d1 == d0 {
		t.Error("a change moves the digest")
	}
	cfg := v.do(http.MethodGet, base, nil).body
	if cfg["itemCount"] != float64(4) {
		t.Errorf("itemCount = %v, want 4", cfg["itemCount"])
	}

	t.Run("items read back as they were set", func(t *testing.T) {
		list := jsonList(t, v.do(http.MethodGet, base+"/items", nil))
		keys := []string{}
		for _, i := range list {
			keys = append(keys, str(i, "key"))
		}
		if !reflect.DeepEqual(keys, []string{"a", "b", "c", "n"}) {
			t.Fatalf("items in creation order = %v", keys)
		}
		if list[0]["value"] != "uno" || list[2]["value"] != true || list[3]["value"] != nil {
			t.Errorf("values = %v", list)
		}
		if _, has := list[0]["description"]; has {
			t.Errorf("no description means no member: %v", list[0])
		}
		one := v.do(http.MethodGet, base+"/item/b", nil)
		if one.status != 200 || str(one.body, "description") != "an object" || str(one.body, "edgeConfigId") != id ||
			!reflect.DeepEqual(one.body["value"], map[string]any{"nested": []any{float64(1), float64(2), float64(3)}}) {
			t.Fatalf("item = %d %s", one.status, one.raw)
		}
		if g := v.do(http.MethodGet, "/v1/global-config/"+id+"/item/nope", nil); g.status != 404 {
			t.Errorf("unknown item = %d", g.status)
		}
	})

	t.Run("update and delete", func(t *testing.T) {
		before := digest()
		r := patch(
			map[string]any{"operation": "update", "key": "a", "description": "now described"},
			map[string]any{"operation": "update", "key": "c", "value": false},
			map[string]any{"operation": "delete", "key": "n"},
		)
		if r.status != 200 {
			t.Fatalf("patch = %d %s", r.status, r.raw)
		}
		a := v.do(http.MethodGet, base+"/item/a", nil).body
		if a["value"] != "uno" || str(a, "description") != "now described" {
			t.Errorf("a description-only update keeps the value: %v", a)
		}
		if c := v.do(http.MethodGet, base+"/item/c", nil).body; c["value"] != false {
			t.Errorf("c = %v", c)
		}
		if g := v.do(http.MethodGet, base+"/item/n", nil); g.status != 404 {
			t.Errorf("deleted = %d", g.status)
		}
		if got := v.do(http.MethodGet, base, nil).body["itemCount"]; got != float64(3) {
			t.Errorf("itemCount = %v, want 3", got)
		}
		if digest() == before {
			t.Error("digest")
		}
	})

	t.Run("a change that cannot apply changes nothing", func(t *testing.T) {
		before := digest()
		for name, c := range map[string]struct {
			items []map[string]any
			want  int
		}{
			"create over an existing key": {[]map[string]any{{"operation": "create", "key": "new", "value": 1}, {"operation": "create", "key": "a", "value": 1}}, 409},
			"update a missing key":        {[]map[string]any{{"operation": "update", "key": "ghost", "value": 1}}, 404},
			"delete a missing key":        {[]map[string]any{{"operation": "delete", "key": "ghost"}}, 404},
			"an unknown operation":        {[]map[string]any{{"operation": "rename", "key": "a", "value": 1}}, 400},
			"a bad key":                   {[]map[string]any{{"operation": "create", "key": "no spaces", "value": 1}}, 400},
			"create with no value":        {[]map[string]any{{"operation": "create", "key": "x"}}, 400},
			"a key twice":                 {[]map[string]any{{"operation": "upsert", "key": "x", "value": 1}, {"operation": "delete", "key": "x"}}, 400},
			"nothing":                     {[]map[string]any{}, 400},
		} {
			if r := patch(c.items...); r.status != c.want {
				t.Errorf("%s = %d %s, want %d", name, r.status, r.raw, c.want)
			}
		}
		if g := v.do(http.MethodGet, base+"/item/new", nil); g.status != 404 {
			t.Errorf("the create beside a failing one was applied: %d", g.status)
		}
		if digest() != before {
			t.Error("a refused change moved the digest")
		}
		if r := v.do(http.MethodPatch, "/v1/edge-config/ecfg_nope/items", map[string]any{"items": []map[string]any{{"operation": "create", "key": "k", "value": 1}}}); r.status != 404 {
			t.Errorf("unknown config = %d", r.status)
		}
	})
}

// TestVercelEdgeConfigTokensAndSchema holds the tokens a config hands out and
// the schema it can carry.
func TestVercelEdgeConfigTokensAndSchema(t *testing.T) {
	v := newVX(t, "vercel-edge-config-tokens")
	id := edgeConfig(t, v, "/v1/edge-config", map[string]any{"slug": "cfg"})
	base := "/v1/edge-config/" + id

	t.Run("a token is shown once", func(t *testing.T) {
		mk := v.do(http.MethodPost, base+"/token", map[string]any{"label": "reader"})
		if mk.status != 201 || str(mk.body, "token") == "" || str(mk.body, "id") == "" || str(mk.body, "token") == str(mk.body, "id") {
			t.Fatalf("create = %d %s", mk.status, mk.raw)
		}
		tok, tid := str(mk.body, "token"), str(mk.body, "id")
		if len(tok) != 36 || strings.Count(tok, "-") != 4 {
			t.Errorf("token %q is not a UUID", tok)
		}
		list := jsonList(t, v.do(http.MethodGet, base+"/tokens", nil))
		if len(list) != 1 || str(list[0], "id") != tid || str(list[0], "label") != "reader" || str(list[0], "partialToken") != tok[:3]+"********" {
			t.Fatalf("list = %v", list)
		}
		if _, has := list[0]["token"]; has {
			t.Errorf("a listing does not carry the token: %v", list[0])
		}
		one := v.do(http.MethodGet, "/v1/global-config/"+id+"/token/"+tok, nil)
		if one.status != 200 || str(one.body, "token") != tok || str(one.body, "id") != tid {
			t.Fatalf("get = %d %s", one.status, one.raw)
		}
		if g := v.do(http.MethodGet, base+"/token/nope", nil); g.status != 404 {
			t.Errorf("unknown token = %d", g.status)
		}

		t.Run("delete by id and by token", func(t *testing.T) {
			second := v.do(http.MethodPost, base+"/token", map[string]any{"label": "writer"}).body
			third := v.do(http.MethodPost, base+"/token", map[string]any{"label": "ci"}).body
			if d := v.do(http.MethodDelete, base+"/tokens", map[string]any{"ids": []string{tid}}); d.status != 204 {
				t.Fatalf("delete by id = %d %s", d.status, d.raw)
			}
			if d := v.do(http.MethodDelete, base+"/tokens", map[string]any{"tokens": []string{str(second, "token")}}); d.status != 204 {
				t.Fatalf("delete by token = %d %s", d.status, d.raw)
			}
			left := jsonList(t, v.do(http.MethodGet, base+"/tokens", nil))
			if len(left) != 1 || str(left[0], "id") != str(third, "id") {
				t.Errorf("left = %v", left)
			}
			if d := v.do(http.MethodDelete, base+"/tokens", map[string]any{}); d.status != 400 {
				t.Errorf("neither = %d", d.status)
			}
		})
	})

	t.Run("a label is required", func(t *testing.T) {
		for _, body := range []map[string]any{{}, {"label": ""}, {"label": strings.Repeat("x", 53)}} {
			if r := v.do(http.MethodPost, base+"/token", body); r.status != 400 {
				t.Errorf("%v = %d", body, r.status)
			}
		}
	})

	t.Run("a schema is stored and returned", func(t *testing.T) {
		if r := v.do(http.MethodGet, base+"/schema", nil); r.status != 200 || strings.TrimSpace(string(r.raw)) != "null" {
			t.Fatalf("no schema yet = %d %q", r.status, r.raw)
		}
		def := map[string]any{"type": "object", "properties": map[string]any{"greeting": map[string]any{"type": "string"}}}
		p := v.do(http.MethodPost, base+"/schema", map[string]any{"definition": def})
		if p.status != 200 || !reflect.DeepEqual(p.body, def) {
			t.Fatalf("patch = %d %s", p.status, p.raw)
		}
		if g := v.do(http.MethodGet, "/v1/global-config/"+id+"/schema", nil); !reflect.DeepEqual(g.body, def) {
			t.Fatalf("get = %s", g.raw)
		}
		if bad := v.do(http.MethodPost, base+"/schema", map[string]any{"definition": "nope"}); bad.status != 400 {
			t.Errorf("a schema that is not an object = %d", bad.status)
		}
		if d := v.do(http.MethodDelete, base+"/schema", nil); d.status != 204 {
			t.Fatalf("delete = %d", d.status)
		}
		if r := v.do(http.MethodGet, base+"/schema", nil); strings.TrimSpace(string(r.raw)) != "null" {
			t.Errorf("after delete = %q", r.raw)
		}
	})
}

// TestVercelEdgeConfigIsScopedToTheTeam holds that a team's configs are its own.
func TestVercelEdgeConfigIsScopedToTheTeam(t *testing.T) {
	v := newVX(t, "vercel-edge-config-team")
	team := str(v.do(http.MethodPost, "/v1/teams", map[string]any{"slug": "acme"}).body, "id")
	id := edgeConfig(t, v, "/v1/edge-config?teamId="+team, map[string]any{"slug": "teams"})
	if r := v.do(http.MethodGet, "/v1/edge-config/"+id+"?teamId="+team, nil); r.status != 200 || str(r.body, "ownerId") != team {
		t.Fatalf("the team's = %d %s", r.status, r.raw)
	}
	if r := v.do(http.MethodGet, "/v1/edge-config/"+id, nil); r.status != 404 {
		t.Fatalf("the personal account sees the team's config: %d", r.status)
	}
	if list := jsonList(t, v.do(http.MethodGet, "/v1/edge-config", nil)); len(list) != 0 {
		t.Fatalf("personal listing = %v", list)
	}
	own := edgeConfig(t, v, "/v1/edge-config", map[string]any{"slug": "mine"})
	if r := v.do(http.MethodGet, "/v1/edge-config/"+own, nil); !strings.HasPrefix(str(r.body, "ownerId"), "usr_") {
		t.Fatalf("a personal config is a user's: %s", r.raw)
	}
}
