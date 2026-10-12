package vercel

import (
	"net/http"
	"testing"
)

func init() { registerArea("projects", (*conf).projects) }

// projects drives a project through its life at every version it answers at.
func (c *conf) projects(t *testing.T) {
	app := c.call(t, http.MethodPost, "/v11/projects", map[string]any{
		"name": "conform-app", "framework": "nextjs", "buildCommand": "npm run build", "rootDirectory": "web",
	}, 200)
	id := str(app.body, "id")
	c.call(t, http.MethodPost, "/v10/projects", map[string]any{"name": "conform-v10"}, 200)
	c.call(t, http.MethodPost, "/v9/projects", map[string]any{"name": "conform-v9"}, 200)
	c.call(t, http.MethodPost, "/v11/projects", map[string]any{"name": "conform-app"}, 409)
	c.call(t, http.MethodPost, "/v11/projects", map[string]any{"name": ""}, 400)

	c.call(t, http.MethodGet, "/v9/projects/"+id, nil, 200)
	c.call(t, http.MethodGet, "/v9/projects/conform-app", nil, 200)
	c.call(t, http.MethodGet, "/v9/projects/prj_nope", nil, 404)

	c.call(t, http.MethodGet, "/v10/projects", nil, 200)
	page := c.call(t, http.MethodGet, "/v10/projects?limit=2", nil, 200)
	if p, _ := page.body["pagination"].(map[string]any); p["next"] == nil {
		t.Errorf("a page of 2 of 3 has a next cursor: %s", page.raw)
	}
	c.call(t, http.MethodGet, "/v10/projects?search=v10", nil, 200)

	c.call(t, http.MethodPatch, "/v9/projects/"+id, map[string]any{"name": "conform-renamed", "buildCommand": nil, "nodeVersion": "20.x"}, 200)
	c.call(t, http.MethodPatch, "/v9/projects/conform-renamed", map[string]any{"name": "conform-v10"}, 409)
	c.call(t, http.MethodPatch, "/v9/projects/prj_nope", map[string]any{"name": "x"}, 404)

	c.call(t, http.MethodDelete, "/v9/projects/conform-renamed", nil, 204)
	c.call(t, http.MethodDelete, "/v9/projects/conform-renamed", nil, 404)
	c.call(t, http.MethodGet, "/v9/projects/"+id, nil, 404)
}
