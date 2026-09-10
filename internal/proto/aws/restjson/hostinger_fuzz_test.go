package restjson

import (
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
)

func FuzzHostingerRoute(f *testing.F) {
	f.Add("GET", "/api/domains/v1/portfolio")
	f.Add("POST", "/api/domains/v1/portfolio")
	f.Add("GET", "/api/domains/v1/portfolio/ex.test")
	f.Add("GET", "/api/dns/v1/zones/ex.test")
	f.Add("PUT", "/api/dns/v1/zones/ex.test")
	f.Add("DELETE", "/api/dns/v1/zones/ex.test")
	f.Fuzz(func(t *testing.T, method, path string) {
		if method == "" {
			method = http.MethodGet
		}
		path = "/" + strings.TrimPrefix(path, "/")
		u, err := url.Parse("http://api.hostinger.com" + path)
		if err != nil || u.Host == "" {
			return
		}
		req, err := http.NewRequest(method, u.String(), nil)
		if err != nil {
			return
		}
		_, _ = (Codec{}).Route(&model.Service{ID: "hostinger.dns"}, req)
	})
}
