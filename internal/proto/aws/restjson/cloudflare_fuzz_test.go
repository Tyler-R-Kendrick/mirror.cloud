package restjson

import (
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
)

func FuzzCloudflareRoute(f *testing.F) {
	f.Add("GET", "/client/v4/accounts/a/storage/kv/namespaces")
	f.Add("POST", "/client/v4/accounts/a/storage/kv/namespaces")
	f.Add("GET", "/client/v4/accounts/a/storage/kv/namespaces/nid")
	f.Add("PUT", "/client/v4/accounts/a/storage/kv/namespaces/nid/values/k")
	f.Add("GET", "/client/v4/accounts/a/storage/kv/namespaces/nid/values/k")
	f.Add("DELETE", "/client/v4/accounts/a/storage/kv/namespaces/nid/values/k")
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
		_, _ = (Codec{}).Route(&model.Service{ID: "cloudflare.kv"}, req)
	})
}
