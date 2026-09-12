package restjson

import (
	"net/http"
	"net/url"
	"strings"
	"testing"
)

// FuzzVercelRoute drives routing over the generated model rather than over an
// empty service. It fuzzed the hand-written route table until that table went
// with the pack; pointed at a service with no operations it would have kept
// passing while exercising nothing, so it now fuzzes httpuri.Match against the
// document's own patterns.
//
// The seeds are deliberately the versions the document declares AND the ones
// the pack answered, because those disagree on four paths. The pack's table
// stripped the leading version segment before matching, so /v9/projects and
// /v10/projects were the same route; routing from the model tells them apart,
// and a fuzzer seeded only with the winners would never walk the boundary.
func FuzzVercelRoute(f *testing.F) {
	f.Add("GET", "/v10/projects")
	f.Add("GET", "/v9/projects")
	f.Add("POST", "/v11/projects")
	f.Add("GET", "/v9/projects/app")
	f.Add("DELETE", "/v9/projects/app")
	f.Add("GET", "/v10/projects/app/env")
	f.Add("GET", "/v9/projects/app/env")
	f.Add("DELETE", "/v9/projects/app/env/env_1")
	f.Add("GET", "/v9/projects/app/domains")
	f.Add("POST", "/v10/projects/app/domains")
	f.Add("POST", "/v13/deployments")
	f.Add("GET", "/v7/deployments")
	f.Add("GET", "/v6/deployments")
	f.Add("DELETE", "/v13/deployments/dpl_1")
	f.Add("GET", "/v2/user")
	f.Add("POST", "/")
	vercel := generatedModel(f, "vercel.api")
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
		_, _ = (Codec{}).Route(vercel, req)
	})
}
