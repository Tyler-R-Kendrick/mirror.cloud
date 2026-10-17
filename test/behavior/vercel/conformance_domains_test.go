package vercel

import (
	"net/http"
	"testing"
)

func init() { registerArea("domains", (*conf).domains) }

// domains drives a project's domains and the account's own through every
// address they answer at, holding each answer to the document: the challenge
// a new domain carries, the same domain once verified, a redirect, and the
// account-level records.
func (c *conf) domains(t *testing.T) {
	pid := str(c.call(t, http.MethodPost, "/v11/projects", map[string]any{"name": "conform-domains"}, 200).body, "id")

	// Added at each version it answers at.
	c.call(t, http.MethodPost, "/v10/projects/"+pid+"/domains", map[string]any{"name": "www.conform.dev"}, 200)
	c.call(t, http.MethodPost, "/v9/projects/"+pid+"/domains", map[string]any{"name": "app.conform.dev"}, 200)
	c.call(t, http.MethodPost, "/v8/projects/conform-domains/domains", map[string]any{"name": "blog.conform.dev", "gitBranch": "staging"}, 200)
	c.call(t, http.MethodPost, "/v10/projects/"+pid+"/domains", map[string]any{"name": "conform-domains.vercel.app"}, 200)
	c.call(t, http.MethodPost, "/v10/projects/"+pid+"/domains", map[string]any{"name": "www.conform.dev"}, 409)
	c.call(t, http.MethodPost, "/v10/projects/"+pid+"/domains", map[string]any{"name": "nodots"}, 400)
	c.call(t, http.MethodPost, "/v10/projects/"+pid+"/domains", map[string]any{"name": "x.conform.dev", "gitBranch": "main", "redirect": "y.conform.dev"}, 400)
	c.call(t, http.MethodPost, "/v10/projects/prj_nope/domains", map[string]any{"name": "z.conform.dev"}, 404)

	// Listed and read at each.
	c.call(t, http.MethodGet, "/v9/projects/"+pid+"/domains", nil, 200)
	c.call(t, http.MethodGet, "/v10/projects/conform-domains/domains?limit=2&order=ASC", nil, 200)
	c.call(t, http.MethodGet, "/v8/projects/"+pid+"/domains?verified=false", nil, 200)
	c.call(t, http.MethodGet, "/v9/projects/prj_nope/domains", nil, 404)
	c.call(t, http.MethodGet, "/v9/projects/"+pid+"/domains/www.conform.dev", nil, 200)
	c.call(t, http.MethodGet, "/v10/projects/"+pid+"/domains/app.conform.dev", nil, 200)
	c.call(t, http.MethodGet, "/v8/projects/"+pid+"/domains/blog.conform.dev", nil, 200)
	c.call(t, http.MethodGet, "/v9/projects/"+pid+"/domains/nope.conform.dev", nil, 404)

	// Edited at each, with a redirect set and cleared.
	c.call(t, http.MethodPatch, "/v9/projects/"+pid+"/domains/www.conform.dev", map[string]any{"redirect": "app.conform.dev", "redirectStatusCode": 308}, 200)
	c.call(t, http.MethodPatch, "/v10/projects/"+pid+"/domains/www.conform.dev", map[string]any{"redirect": nil, "redirectStatusCode": nil, "gitBranch": "main"}, 200)
	c.call(t, http.MethodPatch, "/v8/projects/"+pid+"/domains/app.conform.dev", map[string]any{"gitBranch": nil}, 200)
	c.call(t, http.MethodPatch, "/v9/projects/"+pid+"/domains/www.conform.dev", map[string]any{"redirect": "elsewhere.dev"}, 400)
	c.call(t, http.MethodPatch, "/v9/projects/"+pid+"/domains/nope.conform.dev", map[string]any{"redirect": "elsewhere.dev"}, 404)

	// Verified at each, which the challenge list shows going away.
	c.call(t, http.MethodPost, "/v9/projects/"+pid+"/domains/www.conform.dev/verify", nil, 200)
	c.call(t, http.MethodPost, "/v10/projects/"+pid+"/domains/app.conform.dev/verify", nil, 200)
	c.call(t, http.MethodPost, "/v8/projects/"+pid+"/domains/blog.conform.dev/verify", nil, 200)
	c.call(t, http.MethodPost, "/v9/projects/"+pid+"/domains/nope.conform.dev/verify", nil, 404)
	c.call(t, http.MethodGet, "/v9/projects/"+pid+"/domains", nil, 200)

	// The account's own domains.
	c.call(t, http.MethodPost, "/v7/domains", map[string]any{"name": "account.conform.dev", "cdnEnabled": true, "method": "add"}, 200)
	c.call(t, http.MethodPost, "/v4/domains", map[string]any{"name": "second.conform.dev"}, 200)
	c.call(t, http.MethodPost, "/v7/domains", map[string]any{"name": "account.conform.dev"}, 409)
	c.call(t, http.MethodPost, "/v7/domains", map[string]any{"name": "nodots"}, 400)
	c.call(t, http.MethodGet, "/v5/domains", nil, 200)
	c.call(t, http.MethodGet, "/v6/domains?limit=1", nil, 200)
	c.call(t, http.MethodGet, "/v4/domains", nil, 200)
	c.call(t, http.MethodGet, "/v5/domains/account.conform.dev", nil, 200)
	c.call(t, http.MethodGet, "/v6/domains/account.conform.dev", nil, 200)
	c.call(t, http.MethodGet, "/v4/domains/account.conform.dev", nil, 200)
	c.call(t, http.MethodGet, "/v5/domains/nope.conform.dev", nil, 404)
	c.call(t, http.MethodPatch, "/v3/domains/account.conform.dev", map[string]any{"op": "update", "renew": true, "customNameservers": []string{"ns1.conform.dev", "ns2.conform.dev"}, "echMode": "disabled"}, 200)
	c.call(t, http.MethodPatch, "/v3/domains/nope.conform.dev", map[string]any{"renew": true}, 404)
	c.call(t, http.MethodGet, "/v6/domains/www.conform.dev/config", nil, 200)
	c.call(t, http.MethodGet, "/v6/domains/account.conform.dev/config?strict=true", nil, 200)
	c.call(t, http.MethodGet, "/v9/domains/account.conform.dev/verification", nil, 200)
	c.call(t, http.MethodGet, "/v9/domains/nope.conform.dev/verification", nil, 404)
	c.call(t, http.MethodDelete, "/v6/domains/account.conform.dev", nil, 200)
	c.call(t, http.MethodDelete, "/v5/domains/second.conform.dev", nil, 200)
	c.call(t, http.MethodDelete, "/v4/domains/second.conform.dev", nil, 404)

	// The older address the CLI adds a domain at (authored: the document has
	// AddProjectDomain, with another body and answer), and takes one off at.
	c.call(t, http.MethodPost, "/projects/"+pid+"/alias", map[string]any{"target": "PRODUCTION", "domain": "alias.conform.dev"}, 200)
	c.call(t, http.MethodPost, "/projects/conform-domains/alias", map[string]any{"target": "PRODUCTION", "domain": "alias.conform.dev"}, 409)
	c.call(t, http.MethodPost, "/projects/"+pid+"/alias", map[string]any{"domain": "nodots"}, 400)
	c.call(t, http.MethodPost, "/projects/prj_nope/alias", map[string]any{"domain": "z.conform.dev"}, 404)
	c.call(t, http.MethodDelete, "/projects/"+pid+"/alias?domain=alias.conform.dev", nil, 200)
	c.call(t, http.MethodDelete, "/projects/"+pid+"/alias?domain=alias.conform.dev", nil, 404)

	// The project domains under an apex the account holds, which `domains
	// inspect` shows and `domains rm` counts, and the older address it deletes at.
	c.call(t, http.MethodPost, "/v7/domains", map[string]any{"name": "conform.dev"}, 200)
	c.call(t, http.MethodGet, "/v1/domains/conform.dev/project-domains", nil, 200)
	c.call(t, http.MethodGet, "/v1/domains/conform.dev/project-domains?limit=1&since=1&until=99999999999999", nil, 200)
	c.call(t, http.MethodGet, "/v1/domains/nope.dev/project-domains", nil, 404)
	c.call(t, http.MethodDelete, "/v3/domains/conform.dev", nil, 200)
	c.call(t, http.MethodDelete, "/v3/domains/conform.dev", nil, 404)

	// Removed at each.
	c.call(t, http.MethodDelete, "/v9/projects/"+pid+"/domains/www.conform.dev", nil, 200)
	c.call(t, http.MethodDelete, "/v10/projects/"+pid+"/domains/app.conform.dev", nil, 200)
	c.call(t, http.MethodDelete, "/v8/projects/"+pid+"/domains/blog.conform.dev", nil, 200)
	c.call(t, http.MethodDelete, "/v9/projects/"+pid+"/domains/blog.conform.dev", nil, 404)
}
