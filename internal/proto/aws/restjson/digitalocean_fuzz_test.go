package restjson

import (
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
)

func FuzzDigitalOceanRoute(f *testing.F) {
	f.Add("POST", "/v2/droplets")
	f.Add("GET", "/v2/droplets")
	f.Add("GET", "/v2/droplets/1")
	f.Add("DELETE", "/v2/droplets/1")
	f.Add("POST", "/v2/domains")
	f.Add("GET", "/v2/domains/ex.test")
	f.Add("DELETE", "/v2/domains/ex.test")
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
		_, _ = (Codec{}).Route(&model.Service{ID: "digitalocean.v2"}, req)
	})
}
