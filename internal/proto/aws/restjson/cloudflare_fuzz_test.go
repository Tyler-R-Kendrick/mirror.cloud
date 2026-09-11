package restjson

import (
	"net/http"
	"net/url"
	"strings"
	"testing"

	generatedcf "github.com/tyler-r-kendrick/mirror.cloud/internal/generated/cloudflare/api"
)

// FuzzCloudflareRoute used to drive a hand-written route table. That table
// went with the pack, so this drives what replaced it: httpuri.Match over the
// fourteen patterns Cloudflare's own document declares for Workers KV. The
// seeds are the same paths, which is the point -- the corpus keeps its meaning
// across the migration.
func FuzzCloudflareRoute(f *testing.F) {
	f.Add("GET", "/client/v4/accounts/a/storage/kv/namespaces")
	f.Add("POST", "/client/v4/accounts/a/storage/kv/namespaces")
	f.Add("GET", "/client/v4/accounts/a/storage/kv/namespaces/nid")
	f.Add("PUT", "/client/v4/accounts/a/storage/kv/namespaces/nid/values/k")
	f.Add("GET", "/client/v4/accounts/a/storage/kv/namespaces/nid/values/k")
	f.Add("DELETE", "/client/v4/accounts/a/storage/kv/namespaces/nid/values/k")
	f.Add("GET", "/client/v4/accounts/a/storage/kv/namespaces/nid/keys")
	cf := generatedcf.Model()
	f.Fuzz(func(t *testing.T, method, path string) {
		if method == "" {
			method = http.MethodGet
		}
		path = "/" + strings.TrimPrefix(path, "/")
		u, err := url.Parse("http://api.cloudflare.com" + path)
		if err != nil || u.Host == "" {
			return
		}
		req, err := http.NewRequest(method, u.String(), nil)
		if err != nil {
			return
		}
		_, _ = (Codec{}).Route(cf, req)
	})
}
