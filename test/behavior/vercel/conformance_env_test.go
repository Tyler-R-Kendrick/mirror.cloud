package vercel

import (
	"net/http"
	"testing"
)

func init() { registerArea("env", (*conf).env) }

// env drives a project's variables through every type, every address the
// operations answer at, and the refusals, holding each answer to the document.
func (c *conf) env(t *testing.T) {
	pid := str(c.call(t, http.MethodPost, "/v11/projects", map[string]any{"name": "conform-env"}, 200).body, "id")

	// Created at each version it answers at, one of every type.
	prod := []string{"production"}
	all := []string{"production", "preview", "development"}
	plain := c.call(t, http.MethodPost, "/v10/projects/"+pid+"/env", map[string]any{"key": "PLAIN", "value": "p", "type": "plain", "target": all, "comment": "a plain one"}, 201)
	c.call(t, http.MethodPost, "/v9/projects/"+pid+"/env", map[string]any{"key": "ENC", "value": "e", "type": "encrypted", "target": prod}, 201)
	c.call(t, http.MethodPost, "/v8/projects/conform-env/env", map[string]any{"key": "SECRET", "value": "s", "type": "secret", "target": prod}, 201)
	c.call(t, http.MethodPost, "/v10/projects/"+pid+"/env", map[string]any{"key": "SENS", "value": "x", "type": "sensitive", "target": prod}, 201)
	c.call(t, http.MethodPost, "/v10/projects/"+pid+"/env", map[string]any{"key": "SYS", "value": "y", "type": "system", "target": prod}, 201)
	c.call(t, http.MethodPost, "/v10/projects/"+pid+"/env", map[string]any{"key": "BRANCHED", "value": "b", "type": "plain", "target": []string{"preview"}, "gitBranch": "feature"}, 201)
	c.call(t, http.MethodPost, "/v10/projects/"+pid+"/env", map[string]any{"key": "DEFAULTED", "value": "d"}, 201)
	// One request, several variables: the answer lists what was made and what was not.
	c.call(t, http.MethodPost, "/v10/projects/"+pid+"/env", []map[string]any{
		{"key": "BULK1", "value": "1", "type": "plain", "target": prod},
		{"key": "BULK2", "value": "2", "type": "plain", "target": prod},
		{"key": "PLAIN", "value": "dup", "type": "plain", "target": prod},
	}, 201)
	c.call(t, http.MethodPost, "/v10/projects/"+pid+"/env?upsert=true", map[string]any{"key": "PLAIN", "value": "p2", "type": "plain", "target": all}, 201)

	// The refusals.
	c.call(t, http.MethodPost, "/v10/projects/"+pid+"/env", map[string]any{"key": "PLAIN", "value": "dup", "type": "plain", "target": prod}, 409)
	c.call(t, http.MethodPost, "/v10/projects/"+pid+"/env", map[string]any{"value": "no key"}, 400)
	c.call(t, http.MethodPost, "/v10/projects/"+pid+"/env", map[string]any{"key": "A", "value": "v", "type": "wishful"}, 400)
	c.call(t, http.MethodPost, "/v10/projects/prj_nope/env", map[string]any{"key": "A", "value": "v"}, 404)

	// Listed at each version, with each spelling of the filters.
	c.call(t, http.MethodGet, "/v10/projects/"+pid+"/env", nil, 200)
	c.call(t, http.MethodGet, "/v9/projects/conform-env/env?decrypt=true", nil, 200)
	c.call(t, http.MethodGet, "/v8/projects/"+pid+"/env?gitBranch=feature", nil, 200)
	c.call(t, http.MethodGet, "/v10/projects/prj_nope/env", nil, 404)

	// One variable, at each version it is read at.
	id := str(plain.body["created"].(map[string]any), "id")
	c.call(t, http.MethodGet, "/v1/projects/"+pid+"/env/"+id, nil, 200)
	c.call(t, http.MethodGet, "/v10/projects/"+pid+"/env/"+id, nil, 200)
	c.call(t, http.MethodGet, "/v9/projects/conform-env/env/"+id, nil, 200)
	c.call(t, http.MethodGet, "/v8/projects/"+pid+"/env/"+id, nil, 200)
	c.call(t, http.MethodGet, "/v1/projects/"+pid+"/env/env_nope", nil, 404)

	// Edited at each version it answers at.
	c.call(t, http.MethodPatch, "/v9/projects/"+pid+"/env/"+id, map[string]any{"value": "edited", "comment": "changed"}, 200)
	c.call(t, http.MethodPatch, "/v10/projects/"+pid+"/env/"+id, map[string]any{"target": all}, 200)
	c.call(t, http.MethodPatch, "/v8/projects/"+pid+"/env/"+id, map[string]any{"type": "plain"}, 200)
	c.call(t, http.MethodPatch, "/v9/projects/"+pid+"/env/env_nope", map[string]any{"value": "x"}, 404)
	c.call(t, http.MethodPatch, "/v9/projects/"+pid+"/env/"+id, map[string]any{"key": "ENC", "target": prod}, 409)

	// Removed one at a time at each version, then several at once.
	rm := func(key string) string {
		made := c.call(t, http.MethodPost, "/v10/projects/"+pid+"/env", map[string]any{"key": key, "value": "r", "type": "plain", "target": prod}, 201)
		return str(made.body["created"].(map[string]any), "id")
	}
	c.call(t, http.MethodDelete, "/v9/projects/"+pid+"/env/"+rm("RM1"), nil, 200)
	c.call(t, http.MethodDelete, "/v10/projects/"+pid+"/env/"+rm("RM2"), nil, 200)
	c.call(t, http.MethodDelete, "/v8/projects/"+pid+"/env/"+rm("RM3"), nil, 200)
	c.call(t, http.MethodDelete, "/v9/projects/"+pid+"/env/env_nope", nil, 404)
	c.call(t, http.MethodDelete, "/v1/projects/"+pid+"/env", map[string]any{"ids": []string{rm("RM4"), rm("RM5")}}, 200)
	c.call(t, http.MethodDelete, "/v1/projects/"+pid+"/env", map[string]any{"ids": []string{}}, 400)
}
