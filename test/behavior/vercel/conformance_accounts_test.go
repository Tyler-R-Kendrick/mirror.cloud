package vercel

import (
	"net/http"
	"testing"
)

func init() { registerArea("accounts", (*conf).accounts) }

// accounts drives the user and the teams: who is calling, the teams that exist,
// who is in them, and the addresses the CLI reaches them at.
func (c *conf) accounts(t *testing.T) {
	c.call(t, http.MethodGet, "/v2/user", nil, 200)
	c.call(t, http.MethodPatch, "/v2/user", map[string]any{"name": "Pat", "email": "pat@example.com"}, 200)
	user := c.call(t, http.MethodGet, "/v2/user", nil, 200)
	if u, _ := user.body["user"].(map[string]any); str(u, "name") != "Pat" {
		t.Errorf("the update did not move the user: %s", user.raw)
	}

	acme := c.call(t, http.MethodPost, "/v2/teams", map[string]any{"slug": "acme", "name": "Acme"}, 200)
	id := str(acme.body, "id")
	c.call(t, http.MethodPost, "/v1/teams", map[string]any{"slug": "acme-two"}, 200)
	c.call(t, http.MethodPost, "/v2/teams", map[string]any{"slug": "acme"}, 409)
	c.call(t, http.MethodPost, "/v2/teams", map[string]any{"slug": "  "}, 400)

	c.call(t, http.MethodGet, "/v2/teams", nil, 200)
	c.call(t, http.MethodGet, "/v1/teams", nil, 200)
	c.call(t, http.MethodGet, "/v2/teams?limit=1", nil, 200)
	c.call(t, http.MethodGet, "/v2/teams/"+id, nil, 200)
	c.call(t, http.MethodGet, "/v2/teams/acme", nil, 200)
	c.call(t, http.MethodGet, "/v2/teams/team_nope", nil, 404)

	c.call(t, http.MethodPatch, "/v2/teams/"+id, map[string]any{"name": "Acme Inc", "slug": "acme-inc", "description": "builds things"}, 200)
	c.call(t, http.MethodPatch, "/v2/teams/"+id, map[string]any{"slug": "acme-two"}, 409)
	c.call(t, http.MethodPatch, "/v2/teams/team_nope", map[string]any{"name": "x"}, 404)

	c.call(t, http.MethodGet, "/v2/teams/"+id+"/members", nil, 200)
	c.call(t, http.MethodGet, "/v3/teams/"+id+"/members", nil, 200)
	c.call(t, http.MethodPost, "/v2/teams/"+id+"/members", []map[string]any{{"email": "dev@example.com", "role": "DEVELOPER"}}, 200)
	c.call(t, http.MethodPost, "/v2/teams/"+id+"/members", []map[string]any{{"email": "dev@example.com"}}, 409)
	c.call(t, http.MethodGet, "/v3/teams/"+id+"/members", nil, 200)
	c.call(t, http.MethodGet, "/v2/teams/team_nope/members", nil, 404)
}
