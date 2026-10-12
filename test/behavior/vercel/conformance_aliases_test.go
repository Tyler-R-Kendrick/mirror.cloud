package vercel

import (
	"net/http"
	"testing"
	"time"
)

func init() { registerArea("aliases", (*conf).aliases) }

// aliases drives a hostname through assignment, a move between deployments,
// every listing and the removal, at each address the operations answer at.
func (c *conf) aliases(t *testing.T) {
	proj := c.call(t, http.MethodPost, "/v11/projects", map[string]any{"name": "conform-alias"}, 200)
	_ = proj
	first := str(c.call(t, http.MethodPost, "/v13/deployments", map[string]any{"name": "conform-alias", "project": "conform-alias"}, 200).body, "id")

	// Not yet built.
	c.call(t, http.MethodPost, "/v2/deployments/"+first+"/aliases", map[string]any{"alias": "early.conform.dev"}, 400)
	c.advance(3 * time.Second)

	a := c.call(t, http.MethodPost, "/v2/deployments/"+first+"/aliases", map[string]any{"alias": "www.conform.dev"}, 200)
	uid := str(a.body, "uid")
	c.call(t, http.MethodPost, "/v2/deployments/"+first+"/aliases", map[string]any{"alias": "www.conform.dev"}, 409)
	c.call(t, http.MethodPost, "/v2/deployments/"+first+"/aliases", map[string]any{"alias": "not a host"}, 400)
	c.call(t, http.MethodPost, "/v2/deployments/dpl_nope/aliases", map[string]any{"alias": "www.conform.dev"}, 404)

	// Moved to a second deployment, assigned at the later version.
	second := str(c.call(t, http.MethodPost, "/v13/deployments", map[string]any{"name": "conform-alias", "project": "conform-alias"}, 200).body, "id")
	c.advance(3 * time.Second)
	c.call(t, http.MethodPost, "/v3/deployments/"+second+"/aliases", map[string]any{"alias": "www.conform.dev"}, 200)
	c.call(t, http.MethodPost, "/v2/deployments/"+second+"/aliases", map[string]any{"alias": "app.conform.dev", "redirect": "www.conform.dev"}, 200)
	// The CLI's address (no version, a /now/ segment), naming the deployment by
	// the host it was given rather than by id.
	host := str(c.call(t, http.MethodGet, "/v13/deployments/"+second, nil, 200).body, "url")
	c.call(t, http.MethodPost, "/now/deployments/"+host+"/aliases", map[string]any{"alias": "cli.conform.dev"}, 200)
	c.call(t, http.MethodGet, "/v3/now/aliases?limit=20", nil, 200)

	// Listed and read at each version.
	c.call(t, http.MethodGet, "/v4/aliases", nil, 200)
	c.call(t, http.MethodGet, "/v3/aliases?limit=1", nil, 200)
	c.call(t, http.MethodGet, "/v2/aliases?domain=conform.dev", nil, 200)
	c.call(t, http.MethodGet, "/v4/aliases?projectId="+str(proj.body, "id"), nil, 200)
	c.call(t, http.MethodGet, "/v4/aliases/"+uid, nil, 200)
	c.call(t, http.MethodGet, "/v3/aliases/www.conform.dev", nil, 200)
	c.call(t, http.MethodGet, "/v2/aliases/app.conform.dev", nil, 200)
	c.call(t, http.MethodGet, "/v4/aliases/nope.conform.dev", nil, 404)
	c.call(t, http.MethodGet, "/v2/deployments/"+second+"/aliases", nil, 200)
	c.call(t, http.MethodGet, "/v3/deployments/"+second+"/aliases", nil, 200)
	c.call(t, http.MethodGet, "/v2/deployments/dpl_nope/aliases", nil, 404)
	c.call(t, http.MethodGet, "/v13/deployments/"+second, nil, 200)

	// Removed at each version.
	c.call(t, http.MethodDelete, "/v2/aliases/"+uid, nil, 200)
	c.call(t, http.MethodDelete, "/v2/aliases/"+uid, nil, 404)
	other := str(c.call(t, http.MethodGet, "/v4/aliases/app.conform.dev", nil, 200).body, "uid")
	c.call(t, http.MethodDelete, "/v4/aliases/"+other, nil, 200)
}
