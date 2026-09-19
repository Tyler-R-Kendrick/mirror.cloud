package vercel

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/config"
	rtpkg "github.com/tyler-r-kendrick/mirror.cloud/internal/runtime"

	// Links the bundles' registration in. Without it the registry has no pack
	// for either Vercel service and the edge answers from the mock tier --
	// which looks like a working service returning synthesized data, not like
	// a failure.
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/bundled"
)

// TestVercelProjectDeployKVBehavior drives both served services over their
// real HTTP surfaces. It boots the runtime rather than constructing an edge
// directly, because a bundle is served from the generated model and `edge.New`
// falls back to the hand-authored catalog when none is supplied.
func TestVercelProjectDeployKVBehavior(t *testing.T) {
	cfg := config.Default()
	// Two services, because the pack's one registration carried two products
	// on two hosts: the REST API on api.vercel.com and the KV data plane on
	// kv.vercel-storage.com, which is Upstash Redis.
	cfg.Services = []string{"vercel.api", "vercel.kv"}
	cfg.Seed = "vercel-bdd"
	rt, err := rtpkg.Boot(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(rt.Handler())
	defer ts.Close()
	call := func(method, path, body, host string) (int, map[string]any, http.Header) {
		t.Helper()
		var rdr io.Reader
		if body != "" {
			rdr = strings.NewReader(body)
		}
		req, err := http.NewRequest(method, ts.URL+path, rdr)
		if err != nil {
			t.Fatal(err)
		}
		if host == "" {
			host = "api.vercel.com"
		}
		req.Host = host
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
		return res.StatusCode, m, res.Header
	}

	t.Run("Given a project name When created Then it is listed and fetched by name", func(t *testing.T) {
		code, created, _ := call(http.MethodPost, "/v11/projects", `{"name":"bdd-app"}`, "")
		if code != 200 || created["name"] != "bdd-app" {
			t.Fatalf("create %d %#v", code, created)
		}
		// /v10, which is the version the document declares. The pack answered
		// /v9 because its route table stripped the version segment before
		// matching, so every version of the path listed projects.
		code, listed, _ := call(http.MethodGet, "/v10/projects", "", "")
		if code != 200 || len(listed["projects"].([]any)) != 1 {
			t.Fatalf("list %d %#v", code, listed)
		}
		code, got, _ := call(http.MethodGet, "/v9/projects/bdd-app", "", "")
		if code != 200 || got["id"] != created["id"] {
			t.Fatalf("get %d %#v", code, got)
		}
	})
	t.Run("Given a duplicate project name When created Then conflict is returned", func(t *testing.T) {
		code, body, _ := call(http.MethodPost, "/v11/projects", `{"name":"bdd-app"}`, "")
		if code != 409 {
			t.Fatalf("dup %d %#v", code, body)
		}
	})
	t.Run("Given a project When deployed Then readyState is READY", func(t *testing.T) {
		code, dpl, _ := call(http.MethodPost, "/v13/deployments", `{"name":"bdd-app","project":"bdd-app"}`, "")
		if code != 200 || dpl["readyState"] != "READY" || dpl["url"] == nil {
			t.Fatalf("deploy %d %#v", code, dpl)
		}
	})
	t.Run("Given an undeclared version When a path is fetched Then it is not served", func(t *testing.T) {
		// The pack's version-stripping made this indistinguishable from
		// /v10/projects. Routing from the model tells them apart, which breaks
		// a client that relied on the laxity -- the same shape as Cloudflare's
		// percent-encoded slash, and recorded as a quirk for the same reason.
		if code, body, _ := call(http.MethodGet, "/v99/projects", "", ""); code == 200 {
			t.Fatalf("an undeclared version answered %d %#v", code, body)
		}
	})
	t.Run("Given KV SET When GET Then the value is returned", func(t *testing.T) {
		code, set, _ := call(http.MethodPost, "/", `["SET","k","v"]`, "kv.vercel-storage.com")
		if code != 200 || set["result"] != "OK" {
			t.Fatalf("set %d %#v", code, set)
		}
		code, get, _ := call(http.MethodPost, "/", `["GET","k"]`, "kv.vercel-storage.com")
		if code != 200 || get["result"] != "v" {
			t.Fatalf("get %d %#v", code, get)
		}
		code, del, _ := call(http.MethodPost, "/", `["DEL","k"]`, "kv.vercel-storage.com")
		if code != 200 || del["result"] != float64(1) {
			t.Fatalf("del %d %#v", code, del)
		}
		code, gone, _ := call(http.MethodPost, "/", `["GET","k"]`, "kv.vercel-storage.com")
		if code != 200 || gone["result"] != nil {
			t.Fatalf("get after del %d %#v", code, gone)
		}
	})
	t.Run("Given an unimplemented verb When sent Then 501 says so", func(t *testing.T) {
		// Three commands of Redis, not Redis. INCR is a real command the real
		// service answers, so the refusal says the emulator has not got to it
		// -- not that the caller sent something malformed.
		code, body, hdr := call(http.MethodPost, "/", `["INCR","k"]`, "kv.vercel-storage.com")
		if code != 501 || hdr.Get("x-mirror-not-implemented") == "" {
			t.Fatalf("incr %d %#v %#v", code, hdr, body)
		}
		// KV's errors are a plain string, which is what its document declares
		// and what Upstash answers -- not the REST API's {error:{code,message}}.
		if _, isString := body["error"].(string); !isString {
			t.Fatalf("kv error envelope %#v", body)
		}
	})
	t.Run("Given a missing project When fetched Then not_found is returned", func(t *testing.T) {
		code, body, hdr := call(http.MethodGet, "/v9/projects/nope", "", "")
		if code != 404 || hdr.Get("x-amzn-errortype") != "" {
			t.Fatalf("missing %d %#v %#v", code, hdr, body)
		}
		errObj, _ := body["error"].(map[string]any)
		if errObj["code"] != "not_found" {
			t.Fatalf("shape %#v", body)
		}
	})
	t.Run("Given a missing project When deleted Then not_found is returned", func(t *testing.T) {
		code, body, hdr := call(http.MethodDelete, "/v9/projects/nope", "", "")
		if code != 404 || hdr.Get("x-amzn-errortype") != "" {
			t.Fatalf("delete missing %d %#v %#v", code, hdr, body)
		}
		errObj, _ := body["error"].(map[string]any)
		if errObj["code"] != "not_found" {
			t.Fatalf("delete missing shape %#v", body)
		}
	})
	t.Run("Given a project When updated and renamed Then the new name resolves and the old one does not", func(t *testing.T) {
		code, upd, _ := call(http.MethodPatch, "/v9/projects/bdd-app", `{"framework":"astro"}`, "")
		if code != 200 || upd["framework"] != "astro" || upd["name"] != "bdd-app" {
			t.Fatalf("update %d %#v", code, upd)
		}
		code, ren, _ := call(http.MethodPatch, "/v9/projects/bdd-app", `{"name":"bdd-app2"}`, "")
		if code != 200 || ren["name"] != "bdd-app2" || ren["framework"] != "astro" {
			t.Fatalf("rename %d %#v", code, ren)
		}
		if code, body, _ := call(http.MethodGet, "/v9/projects/bdd-app", "", ""); code != 404 {
			t.Fatalf("the old name still resolves %d %#v", code, body)
		}
		code, got, _ := call(http.MethodGet, "/v9/projects/bdd-app2", "", "")
		if code != 200 || got["id"] != ren["id"] {
			t.Fatalf("get by new name %d %#v", code, got)
		}
	})
	t.Run("Given a custom environment When addressed by slug or id Then both answer", func(t *testing.T) {
		code, cenv, _ := call(http.MethodPost, "/v9/projects/bdd-app2/custom-environments", `{"slug":"staging"}`, "")
		if code != 201 || cenv["slug"] != "staging" {
			t.Fatalf("create %d %#v", code, cenv)
		}
		code, bySlug, _ := call(http.MethodGet, "/v9/projects/bdd-app2/custom-environments/staging", "", "")
		if code != 200 || bySlug["id"] != cenv["id"] {
			t.Fatalf("by slug %d %#v", code, bySlug)
		}
		code, byID, _ := call(http.MethodGet, "/v9/projects/bdd-app2/custom-environments/"+cenv["id"].(string), "", "")
		if code != 200 || byID["slug"] != "staging" {
			t.Fatalf("by id %d %#v", code, byID)
		}
		code, _, _ = call(http.MethodDelete, "/v9/projects/bdd-app2/custom-environments/staging", "", "")
		if code != 200 {
			t.Fatalf("remove %d", code)
		}
		if code, body, _ := call(http.MethodGet, "/v9/projects/bdd-app2/custom-environments/staging", "", ""); code != 404 {
			t.Fatalf("still there after remove %d %#v", code, body)
		}
	})
	t.Run("Given a deployment When promoted Then 201 and an unknown one is not_found", func(t *testing.T) {
		code, dpl, _ := call(http.MethodPost, "/v13/deployments", `{"name":"bdd-app2","project":"bdd-app2"}`, "")
		if code != 200 {
			t.Fatalf("deploy %d %#v", code, dpl)
		}
		code, prj, _ := call(http.MethodGet, "/v9/projects/bdd-app2", "", "")
		if code != 200 {
			t.Fatalf("get project %d %#v", code, prj)
		}
		promote := "/v10/projects/" + prj["id"].(string) + "/promote/"
		code, body, _ := call(http.MethodPost, promote+dpl["id"].(string), "", "")
		if code != 201 {
			t.Fatalf("promote %d %#v", code, body)
		}
		code, body, hdr := call(http.MethodPost, promote+"dpl_nope", "", "")
		errObj, _ := body["error"].(map[string]any)
		if code != 404 || errObj["code"] != "not_found" || hdr.Get("x-amzn-errortype") != "" {
			t.Fatalf("promote missing %d %#v %#v", code, hdr, body)
		}
	})
	// Events and files answer bare arrays, which the map-shaped helper cannot
	// read.
	callList := func(method, path, body string) (int, []any) {
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
		l := []any{}
		_ = json.Unmarshal(b, &l)
		return res.StatusCode, l
	}
	t.Run("Given no teams When listed Then the listing is empty and an unknown team is not_found", func(t *testing.T) {
		// Teams are account fixtures and the vendored document declares no
		// create, so the listing starts empty.
		code, body, _ := call(http.MethodGet, "/v2/teams", "", "")
		if code != 200 || len(body["teams"].([]any)) != 0 {
			t.Fatalf("teams %d %#v", code, body)
		}
		code, body, _ = call(http.MethodGet, "/v2/teams/team_nope", "", "")
		if code != 404 {
			t.Fatalf("team %d %#v", code, body)
		}
		code, body, _ = call(http.MethodPatch, "/v2/teams/team_nope", `{"name":"x"}`, "")
		if code != 404 {
			t.Fatalf("patch team %d %#v", code, body)
		}
	})
	t.Run("Given a deployment with a file When its sub-resources are read Then they answer", func(t *testing.T) {
		code, dpl, _ := call(http.MethodPost, "/v13/deployments", `{"name":"bdd-sub","project":"bdd-app2","files":[{"file":"index.html","sha":"aa11bb22","size":42}]}`, "")
		if code != 200 {
			t.Fatalf("deploy %d %#v", code, dpl)
		}
		id := dpl["id"].(string)
		code, aliases, _ := call(http.MethodGet, "/v2/deployments/"+id+"/aliases", "", "")
		if code != 200 || len(aliases["aliases"].([]any)) != 1 {
			t.Fatalf("aliases %d %#v", code, aliases)
		}
		code, events := callList(http.MethodGet, "/v3/deployments/"+id+"/events", "")
		// Newest first, the oracle's default direction: ready, building, created.
		if code != 200 || len(events) != 3 || events[0].(map[string]any)["type"] != "ready" {
			t.Fatalf("events %d %#v", code, events)
		}
		code, files := callList(http.MethodGet, "/v6/deployments/"+id+"/files", "")
		root := files[0].(map[string]any)
		if code != 200 || root["type"] != "directory" || len(root["children"].([]any)) != 1 {
			t.Fatalf("files %d %#v", code, files)
		}
		// The vendor's emulator refuses to cancel a READY deployment, and so
		// does mirror: the QUEUED/BUILDING guard can never fire when every
		// deployment is born READY. The loosened guard answered 200 here until
		// the differential corpus caught it.
		code, canceled, _ := call(http.MethodPatch, "/v12/deployments/"+id+"/cancel", "", "")
		if code != 400 {
			t.Fatalf("cancel of a READY deployment %d %#v", code, canceled)
		}
		code, after, _ := call(http.MethodGet, "/v13/deployments/"+id, "", "")
		if code != 200 || after["readyState"] != "READY" {
			t.Fatalf("get after refused cancel %d %#v", code, after)
		}
	})
	t.Run("Given a file digest When registered Then 200 and a missing digest is bad_request", func(t *testing.T) {
		req, err := http.NewRequest(http.MethodPost, ts.URL+"/v2/files", strings.NewReader("hello"))
		if err != nil {
			t.Fatal(err)
		}
		req.Host = "api.vercel.com"
		req.Header.Set("Authorization", "Bearer test")
		req.Header.Set("x-vercel-digest", "00112233445566778899aabbccddeeff")
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		res.Body.Close()
		if res.StatusCode != 200 {
			t.Fatalf("upload %d", res.StatusCode)
		}
		code, body, _ := call(http.MethodPost, "/v2/files", `{"x":1}`, "")
		if code != 400 {
			t.Fatalf("upload without digest %d %#v", code, body)
		}
	})
	t.Run("Given a project When promote aliases and protection bypass are read Then they answer", func(t *testing.T) {
		code, prj, _ := call(http.MethodGet, "/v9/projects/bdd-app2", "", "")
		if code != 200 {
			t.Fatalf("get project %d %#v", code, prj)
		}
		code, aliases, _ := call(http.MethodGet, "/v1/projects/"+prj["id"].(string)+"/promote/aliases", "", "")
		if code != 200 || aliases["aliases"] == nil || aliases["pagination"] == nil {
			t.Fatalf("promote aliases %d %#v", code, aliases)
		}
		code, pb, _ := call(http.MethodPatch, "/v1/projects/bdd-app2/protection-bypass", `{"generate":{"secret":"00112233445566778899aabbccddeeff","note":"ci"}}`, "")
		got, _ := pb["protectionBypass"].(map[string]any)
		if code != 200 || got["00112233445566778899aabbccddeeff"] == nil {
			t.Fatalf("protection bypass %d %#v", code, pb)
		}
		code, pb, _ = call(http.MethodPatch, "/v1/projects/bdd-app2/protection-bypass", `{"revoke":{"secret":"00112233445566778899aabbccddeeff"}}`, "")
		if code != 200 || len(pb["protectionBypass"].(map[string]any)) != 0 {
			t.Fatalf("revoke %d %#v", code, pb)
		}
	})
}
