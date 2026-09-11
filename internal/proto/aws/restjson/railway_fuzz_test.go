package restjson

import (
	"net/http"
	"strings"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
)

func FuzzRailwayRoute(f *testing.F) {
	f.Add("POST", "/graphql/v2", `{"query":"mutation { projectCreate(input:{name:\"web\"}) { id name } }"}`)
	f.Add("POST", "/graphql/v2", `{"query":"{ projects { edges { node { id } } } }"}`)
	f.Add("POST", "/graphql/v2", `{"query":"{ project(id:\"x\") { id } }"}`)
	f.Add("POST", "/graphql/v2", `{"query":"mutation { projectDelete(id:\"x\") }"}`)
	f.Add("POST", "/graphql/v2", `{"query":"mutation { serviceCreate(input:{name:\"api\"}) { id } }"}`)
	f.Add("POST", "/graphql/v2", `{"query":"mutation { serviceDelete(id:\"x\") }"}`)
	f.Add("GET", "/graphql/v2", `{}`)
	f.Fuzz(func(t *testing.T, method, path, body string) {
		if method == "" {
			method = http.MethodPost
		}
		path = "/" + strings.TrimPrefix(path, "/")
		req, err := http.NewRequest(method, "http://backboard.railway.com"+path, strings.NewReader(body))
		if err != nil {
			return
		}
		_, _ = (Codec{}).Route(&model.Service{ID: "railway.graphql"}, req)
	})
}
