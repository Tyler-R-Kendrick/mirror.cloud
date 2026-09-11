package restxml

import (
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
)

func FuzzAzureRoute(f *testing.F) {
	f.Add("PUT", "/c?restype=container")
	f.Add("GET", "/?comp=list")
	f.Add("GET", "/c?restype=container")
	f.Add("GET", "/c?restype=container&comp=list")
	f.Add("PUT", "/c/o")
	f.Add("GET", "/c/o")
	f.Add("DELETE", "/c/o")
	f.Add("PUT", "/c?restype=container&comp=metadata")
	f.Add("GET", "/c?restype=container&comp=acl")
	f.Add("PUT", "/c?restype=container&comp=lease")
	f.Add("GET", "/?restype=service&comp=properties")
	f.Add("PUT", "/?restype=service&comp=properties")
	f.Add("GET", "/?restype=account&comp=properties")
	f.Fuzz(func(t *testing.T, method, path string) {
		if method == "" {
			method = http.MethodGet
		}
		path = "/" + strings.TrimPrefix(path, "/")
		u, err := url.Parse("http://acct.blob.core.windows.net" + path)
		if err != nil || u.Host == "" {
			return
		}
		req, err := http.NewRequest(method, u.String(), nil)
		if err != nil {
			return
		}
		_, _ = (Codec{}).Route(&model.Service{ID: "azure.blobs"}, req)
	})
}
