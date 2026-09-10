package restjson

import (
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
)

func FuzzVercelRoute(f *testing.F) {
	f.Add("GET", "/v9/projects")
	f.Add("POST", "/v11/projects")
	f.Add("GET", "/v9/projects/app/env")
	f.Add("POST", "/")
	f.Add("DELETE", "/v13/deployments/dpl_1")
	f.Add("GET", "/v2/user")
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
		_, _ = (Codec{}).Route(&model.Service{ID: "vercel.api"}, req)
	})
}
