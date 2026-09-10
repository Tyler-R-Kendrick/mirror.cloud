package restjson

import (
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
)

func FuzzHetznerRoute(f *testing.F) {
	f.Add("POST", "/v1/servers")
	f.Add("GET", "/v1/servers")
	f.Add("GET", "/v1/servers/1")
	f.Add("DELETE", "/v1/servers/1")
	f.Add("POST", "/v1/ssh_keys")
	f.Add("GET", "/v1/ssh_keys/1")
	f.Add("DELETE", "/v1/ssh_keys/1")
	f.Fuzz(func(t *testing.T, method, path string) {
		if method == "" {
			method = http.MethodGet
		}
		path = "/" + strings.TrimPrefix(path, "/")
		u, err := url.Parse("http://api.hetzner.cloud" + path)
		if err != nil || u.Host == "" {
			return
		}
		req, err := http.NewRequest(method, u.String(), nil)
		if err != nil {
			return
		}
		_, _ = (Codec{}).Route(&model.Service{ID: "hetzner.v1"}, req)
	})
}
