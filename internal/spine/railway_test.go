package spine

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/config"
	rtpkg "github.com/tyler-r-kendrick/mirror.cloud/internal/runtime"

	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/railway/graphql"
)

func TestBootedServerRailwayGraphQL(t *testing.T) {
	cfg := config.Default()
	cfg.Services = []string{"railway.graphql"}
	cfg.Seed = "rw-1"
	rt, err := rtpkg.Boot(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(rt.Handler())
	defer ts.Close()
	gql := func(query, vars string) (int, map[string]any, http.Header) {
		t.Helper()
		body := `{"query":` + jsonString(query)
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
		return res.StatusCode, env, res.Header
	}
	_, env, _ := gql(`mutation($input:ProjectCreateInput){ projectCreate(input:$input){ id name } }`, `{"input":{"name":"web"}}`)
	data, _ := env["data"].(map[string]any)
	created, _ := data["projectCreate"].(map[string]any)
	if created["name"] != "web" {
		t.Fatalf("create %#v", env)
	}
	pid, _ := created["id"].(string)
	_, env, _ = gql(`{ projects { edges { node { id name } } } }`, "")
	data, _ = env["data"].(map[string]any)
	projects, _ := data["projects"].(map[string]any)
	edges, _ := projects["edges"].([]any)
	if len(edges) != 1 {
		t.Fatalf("list %#v", env)
	}
	_, env, _ = gql(`query($id:String){ project(id:$id){ id name } }`, `{"id":"`+pid+`"}`)
	data, _ = env["data"].(map[string]any)
	proj, _ := data["project"].(map[string]any)
	if proj["id"] != pid || proj["name"] != "web" {
		t.Fatalf("get %#v", env)
	}
	_, env, _ = gql(`mutation($input:ServiceCreateInput){ serviceCreate(input:$input){ id name } }`, `{"input":{"projectId":"`+pid+`","name":"api"}}`)
	data, _ = env["data"].(map[string]any)
	svc, _ := data["serviceCreate"].(map[string]any)
	sid, _ := svc["id"].(string)
	if svc["name"] != "api" {
		t.Fatalf("service create %#v", env)
	}
	_, env, _ = gql(`query($id:String){ service(id:$id){ id name } }`, `{"id":"`+sid+`"}`)
	data, _ = env["data"].(map[string]any)
	if data["service"].(map[string]any)["id"] != sid {
		t.Fatalf("service get %#v", env)
	}
	_, env, h := gql(`query($id:String){ project(id:$id){ id } }`, `{"id":"missing"}`)
	if env["errors"] == nil || h.Get("x-amzn-errortype") != "" || env["data"] != nil {
		t.Fatalf("missing project %#v %v", env, h)
	}
	_, env, h = gql(`mutation($id:String){ projectDelete(id:$id) }`, `{"id":"missing"}`)
	if env["errors"] == nil || h.Get("x-amzn-errortype") != "" {
		t.Fatalf("delete missing %#v %v", env, h)
	}
}

func jsonString(s string) string {
	b, _ := json.Marshal(s)
	return string(b)
}
