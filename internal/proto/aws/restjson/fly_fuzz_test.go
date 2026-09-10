package restjson

import (
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
)

func FuzzFlyRoute(f *testing.F) {
	f.Add("POST", "/v1/apps")
	f.Add("GET", "/v1/apps")
	f.Add("GET", "/v1/apps/web")
	f.Add("DELETE", "/v1/apps/web")
	f.Add("POST", "/v1/apps/web/machines")
	f.Add("GET", "/v1/apps/web/machines/1")
	f.Add("DELETE", "/v1/apps/web/machines/1")
	f.Fuzz(func(t *testing.T, method, path string) {
		if method == "" {
			method = http.MethodGet
		}
		path = "/" + strings.TrimPrefix(path, "/")
		u, err := url.Parse("http://api.machines.dev" + path)
		if err != nil || u.Host == "" {
			return
		}
		req, err := http.NewRequest(method, u.String(), nil)
		if err != nil {
			return
		}
		_, _ = (Codec{}).Route(&model.Service{ID: "fly.machines"}, req)
	})
}
