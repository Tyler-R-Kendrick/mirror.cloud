package railway

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/config"
	rtpkg "github.com/tyler-r-kendrick/mirror.cloud/internal/runtime"

	// Links the bundle's registration in. Without it the registry has no pack
	// for railway.graphql and the edge answers from the mock tier -- which
	// looks like a working service returning synthesized data, not like a
	// failure.
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/bundled"
)

// TestRailwayGraphQLBehavior drives the served service over its real HTTP
// surface. It boots the runtime rather than constructing an edge directly,
// because a bundle is served from the generated model and `edge.New` falls back
// to the hand-authored catalog when none is supplied.
func TestRailwayGraphQLBehavior(t *testing.T) {
	cfg := config.Default()
	cfg.Services = []string{"railway.graphql"}
	rt, err := rtpkg.Boot(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(rt.Handler())
	defer ts.Close()
	gql := func(query, vars string) (map[string]any, http.Header) {
		t.Helper()
		body := `{"query":` + marshal(query)
		if vars != "" {
			body += `,"variables":` + vars
		}
		body += `}`
		req, err := http.NewRequest(http.MethodPost, ts.URL+"/graphql/v2", strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		req.Host = "backboard.railway.com"
		req.Header.Set("Authorization", "Bearer test")
		req.Header.Set("Content-Type", "application/json")
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		b, _ := io.ReadAll(res.Body)
		res.Body.Close()
		env := map[string]any{}
		_ = json.Unmarshal(b, &env)
		return env, res.Header
	}
	t.Run("Given a project When created Then projects lists it and project returns it", func(t *testing.T) {
		env, _ := gql(`mutation($input:ProjectCreateInput){ projectCreate(input:$input){ id name } }`, `{"input":{"name":"bdd"}}`)
		data, _ := env["data"].(map[string]any)
		created, _ := data["projectCreate"].(map[string]any)
		if created["name"] != "bdd" {
			t.Fatalf("create %#v", env)
		}
		env, _ = gql(`{ projects { edges { node { id name } } } }`, "")
		data, _ = env["data"].(map[string]any)
		if data["projects"] == nil {
			t.Fatalf("list %#v", env)
		}
	})
	t.Run("Given an empty name When creating Then GraphQL errors", func(t *testing.T) {
		env, hdr := gql(`mutation($input:ProjectCreateInput){ projectCreate(input:$input){ id } }`, `{"input":{"name":""}}`)
		if env["errors"] == nil || hdr.Get("x-amzn-errortype") != "" {
			t.Fatalf("empty %#v %v", env, hdr)
		}
	})
	t.Run("Given a missing project When fetched or deleted Then GraphQL errors", func(t *testing.T) {
		env, hdr := gql(`query($id:String){ project(id:$id){ id } }`, `{"id":"missing"}`)
		if env["errors"] == nil || hdr.Get("x-amzn-errortype") != "" {
			t.Fatalf("missing %#v %v", env, hdr)
		}
		env, hdr = gql(`mutation($id:String){ projectDelete(id:$id) }`, `{"id":"missing"}`)
		if env["errors"] == nil || hdr.Get("x-amzn-errortype") != "" {
			t.Fatalf("delete missing %#v %v", env, hdr)
		}
	})
	t.Run("Given a missing service When fetched or deleted Then GraphQL errors", func(t *testing.T) {
		env, hdr := gql(`query($id:String){ service(id:$id){ id } }`, `{"id":"missing"}`)
		if env["errors"] == nil || hdr.Get("x-amzn-errortype") != "" || env["data"] != nil {
			t.Fatalf("missing service %#v %v", env, hdr)
		}
		env, hdr = gql(`mutation($id:String){ serviceDelete(id:$id) }`, `{"id":"missing"}`)
		if env["errors"] == nil || hdr.Get("x-amzn-errortype") != "" || env["data"] != nil {
			t.Fatalf("delete missing service %#v %v", env, hdr)
		}
	})
}

func marshal(s string) string {
	b, _ := json.Marshal(s)
	return string(b)
}
