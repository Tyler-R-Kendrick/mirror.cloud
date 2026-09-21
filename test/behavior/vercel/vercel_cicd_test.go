package vercel

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/config"
	rtpkg "github.com/tyler-r-kendrick/mirror.cloud/internal/runtime"

	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/bundled"
)

// TestVercelCICDParity gates the CI/CD surface: deployment lifecycle on the
// engine clock, alias/check/artifact/rolling-release/runtime-log routes, and
// a concurrent promote race on the production pointer.
func TestVercelCICDParity(t *testing.T) {
	t.Setenv("MIRROR_CLOCK", "controllable")
	cfg := config.Default()
	cfg.Services = []string{"vercel.api"}
	cfg.Seed = "vercel-cicd"
	rt, err := rtpkg.Boot(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(rt.Handler())
	defer ts.Close()
	call := func(method, path, body string) (int, map[string]any) {
		t.Helper()
		var rdr io.Reader
		if body != "" {
			rdr = strings.NewReader(body)
		}
		req, err := http.NewRequest(method, ts.URL+path, rdr)
		if err != nil {
			t.Fatal(err)
		}
		req.Host = "api.vercel.com"
		req.Header.Set("Authorization", "Bearer test")
		req.Header.Set("Content-Type", "application/json")
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		b, _ := io.ReadAll(res.Body)
		res.Body.Close()
		m := map[string]any{}
		_ = json.Unmarshal(b, &m)
		return res.StatusCode, m
	}

	code, prj := call(http.MethodPost, "/v11/projects", `{"name":"cicd-app"}`)
	if code != 200 {
		t.Fatalf("create project %d %#v", code, prj)
	}
	pid := prj["id"].(string)

	t.Run("alias assign list get delete", func(t *testing.T) {
		code, dpl := call(http.MethodPost, "/v13/deployments", `{"name":"cicd-app","project":"cicd-app"}`)
		if code != 200 {
			t.Fatalf("deploy %d %#v", code, dpl)
		}
		did := dpl["id"].(string)
		code, als := call(http.MethodPost, "/v2/deployments/"+did+"/aliases", `{"alias":"cicd.example"}`)
		if code != 200 || als["alias"] != "cicd.example" {
			t.Fatalf("assign %d %#v", code, als)
		}
		code, listed := call(http.MethodGet, "/v4/aliases", "")
		if code != 200 || listed["aliases"] == nil {
			t.Fatalf("list %d %#v", code, listed)
		}
		uid := als["uid"].(string)
		code, got := call(http.MethodGet, "/v4/aliases/"+uid, "")
		if code != 200 || got["uid"] != uid {
			t.Fatalf("get %d %#v", code, got)
		}
		code, del := call(http.MethodDelete, "/v2/aliases/"+uid, "")
		if code != 200 || del["status"] != "SUCCESS" {
			t.Fatalf("delete %d %#v", code, del)
		}
	})

	t.Run("checks v1 and project checks", func(t *testing.T) {
		code, dpl := call(http.MethodPost, "/v13/deployments", `{"name":"cicd-app","project":"cicd-app"}`)
		if code != 200 {
			t.Fatalf("deploy %d %#v", code, dpl)
		}
		did := dpl["id"].(string)
		code, chk := call(http.MethodPost, "/v1/deployments/"+did+"/checks", `{"name":"lint","blocking":false}`)
		if code != 200 || chk["id"] == nil {
			t.Fatalf("create check %d %#v", code, chk)
		}
		code, all := call(http.MethodGet, "/v1/deployments/"+did+"/checks", "")
		if code != 200 || all["checks"] == nil {
			t.Fatalf("list checks %d %#v", code, all)
		}
		code, pchk := call(http.MethodPost, "/v2/projects/"+pid+"/checks", `{"name":"gate","requires":"build-ready"}`)
		if code != 200 || pchk["id"] == nil {
			t.Fatalf("project check %d %#v", code, pchk)
		}
		code, runs := call(http.MethodGet, "/v2/projects/"+pid+"/checks/"+pchk["id"].(string)+"/runs", "")
		if code != 200 || runs["runs"] == nil {
			t.Fatalf("list runs %d %#v", code, runs)
		}
		code, crun := call(http.MethodPost, "/v2/deployments/"+did+"/check-runs", `{"checkId":"`+chk["id"].(string)+`"}`)
		if code != 200 || crun["id"] == nil {
			t.Fatalf("check-run %d %#v", code, crun)
		}
	})

	t.Run("artifacts and rolling release and runtime logs", func(t *testing.T) {
		req, err := http.NewRequest(http.MethodPut, ts.URL+"/v8/artifacts/deadbeef", strings.NewReader("cache"))
		if err != nil {
			t.Fatal(err)
		}
		req.Host = "api.vercel.com"
		req.Header.Set("Authorization", "Bearer test")
		req.Header.Set("Content-Length", "5")
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		res.Body.Close()
		if res.StatusCode != 200 && res.StatusCode != 202 {
			t.Fatalf("upload artifact %d", res.StatusCode)
		}
		code, st := call(http.MethodGet, "/v8/artifacts/status", "")
		if code != 200 || st["status"] != "enabled" {
			t.Fatalf("artifact status %d %#v", code, st)
		}
		code, cfg := call(http.MethodPatch, "/v1/projects/"+pid+"/rolling-release/config", `{"target":"production","stages":[]}`)
		if code != 200 || cfg["rollingRelease"] == nil {
			t.Fatalf("rr config %d %#v", code, cfg)
		}
		code, dpl := call(http.MethodPost, "/v13/deployments", `{"name":"cicd-app","project":"cicd-app"}`)
		if code != 200 {
			t.Fatalf("deploy %d %#v", code, dpl)
		}
		code, started := call(http.MethodPost, "/v1/projects/"+pid+"/rolling-release/start", `{"canaryDeploymentId":"`+dpl["id"].(string)+`"}`)
		if code != 200 || started["rollingRelease"] == nil {
			t.Fatalf("rr start %d %#v", code, started)
		}
		code, logs := call(http.MethodGet, "/v1/projects/"+pid+"/deployments/"+dpl["id"].(string)+"/runtime-logs", "")
		if code != 200 || logs["message"] == nil {
			t.Fatalf("runtime logs %d %#v", code, logs)
		}
	})

	t.Run("concurrent promote leaves one production pointer", func(t *testing.T) {
		code, a := call(http.MethodPost, "/v13/deployments", `{"name":"cicd-app","project":"cicd-app"}`)
		code2, b := call(http.MethodPost, "/v13/deployments", `{"name":"cicd-app","project":"cicd-app"}`)
		if code != 200 || code2 != 200 {
			t.Fatalf("deploys %d %#v / %d %#v", code, a, code2, b)
		}
		var wg sync.WaitGroup
		for _, id := range []string{a["id"].(string), b["id"].(string)} {
			wg.Add(1)
			go func(id string) {
				defer wg.Done()
				_, _ = call(http.MethodPost, "/v10/projects/"+pid+"/promote/"+id, "")
			}(id)
		}
		wg.Wait()
		code, aliases := call(http.MethodGet, "/v1/projects/"+pid+"/promote/aliases", "")
		if code != 200 {
			t.Fatalf("promote aliases %d %#v", code, aliases)
		}
		promoted := 0
		for _, row := range aliases["aliases"].([]any) {
			if row.(map[string]any)["status"] == "PROMOTED" {
				promoted++
			}
		}
		if promoted != 1 {
			t.Fatalf("want one PROMOTED alias after concurrent promote, got %d in %#v", promoted, aliases)
		}
		_ = rt.Deps.Clock.Advance(time.Nanosecond) // keep controllable clock referenced
	})
}
