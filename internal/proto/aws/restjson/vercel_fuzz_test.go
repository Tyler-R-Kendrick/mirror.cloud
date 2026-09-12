package restjson

import (
	"net/http"
	"net/url"
	"strings"
	"testing"

	generatedvc "github.com/tyler-r-kendrick/mirror.cloud/internal/generated/vercel/api"
	generatedvckv "github.com/tyler-r-kendrick/mirror.cloud/internal/generated/vercel/kv"
)

// FuzzVercelRoute drives routing over the generated models rather than over
// an empty service. It fuzzed the hand-written route table until that table
// went with the pack; pointed at a service with no operations it would have
// kept passing while exercising nothing, so it now fuzzes httpuri.Match
// against the document's own patterns -- the largest model in the tree,
// 12,363 shapes behind 26 operations.
func FuzzVercelRoute(f *testing.F) {
	f.Add("GET", "/v2/user")
	f.Add("POST", "/v11/projects")
	f.Add("GET", "/v10/projects")
	f.Add("GET", "/v9/projects/app")
	f.Add("DELETE", "/v9/projects/app/env/env_1")
	f.Add("POST", "/v13/deployments")
	f.Add("GET", "/v7/deployments")
	f.Add("GET", "/v9/projects/app/domains")
	vc := generatedvc.Model()
	kv := generatedvckv.Model()
	f.Fuzz(func(t *testing.T, method, path string) {
		if method == "" {
			method = http.MethodGet
		}
		path = "/" + strings.TrimPrefix(path, "/")
		u, err := url.Parse("http://api.vercel.com" + path)
		if err != nil || u.Host == "" {
			return
		}
		req, err := http.NewRequest(method, u.String(), nil)
		if err != nil {
			return
		}
		_, _ = (Codec{}).Route(vc, req)
		// The KV data plane is one endpoint, and the command body is the
		// other half of what the pack parsed.
		if method == http.MethodPost {
			_, _ = (Codec{}).Route(kv, req)
		}
	})
}
