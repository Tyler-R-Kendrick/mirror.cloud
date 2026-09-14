package restjson

import (
	"net/http"
	"net/url"
	"strings"
	"testing"
)

// FuzzDigitalOceanRoute drives routing over the generated model rather than
// over an empty service. It fuzzed the hand-written route table until that
// table went with the pack; pointed at a service with no operations it would
// have kept passing while exercising nothing, so it now fuzzes httpuri.Match
// against the document's own forty patterns -- the deepest set of nested
// labels any modelled service here has.
func FuzzDigitalOceanRoute(f *testing.F) {
	f.Add("POST", "/v2/droplets")
	f.Add("GET", "/v2/droplets")
	f.Add("GET", "/v2/droplets/1")
	f.Add("DELETE", "/v2/droplets/1")
	f.Add("POST", "/v2/domains")
	f.Add("GET", "/v2/domains/ex.test")
	f.Add("DELETE", "/v2/domains/ex.test")
	f.Add("PATCH", "/v2/domains/ex.test/records/7")
	f.Add("GET", "/v2/droplets/1/destroy_with_associated_resources/status")
	do := generatedModel(f, "digitalocean.v2")
	f.Fuzz(func(t *testing.T, method, path string) {
		if method == "" {
			method = http.MethodGet
		}
		path = "/" + strings.TrimPrefix(path, "/")
		u, err := url.Parse("http://api.digitalocean.com" + path)
		if err != nil || u.Host == "" {
			return
		}
		req, err := http.NewRequest(method, u.String(), nil)
		if err != nil {
			return
		}
		_, _ = (Codec{}).Route(do, req)
	})
}
