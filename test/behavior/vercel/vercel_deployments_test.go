package vercel

import (
	"crypto/sha1"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/config"
)

// sha1Of is the name Vercel gives a file: the SHA-1 of its bytes.
func sha1Of(content string) string {
	sum := sha1.Sum([]byte(content))
	return hex.EncodeToString(sum[:])
}

// upload sends a file the way `vercel` does, as an octet stream named by digest.
func (v *vx) upload(content string) reply {
	v.t.Helper()
	return v.do(http.MethodPost, "/v2/files", []byte(content),
		"x-vercel-digest", sha1Of(content), "x-vercel-size", fmt.Sprint(len(content)))
}

// strs reads a list-of-strings member.
func strs(m map[string]any, key string) []string {
	var out []string
	list, _ := m[key].([]any)
	for _, e := range list {
		s, _ := e.(string)
		out = append(out, s)
	}
	return out
}

// TestVercelDeploymentUploadProtocol holds the exchange every Vercel client
// deploys by: ask for a deployment naming files by digest, learn which the
// server does not have, upload those, ask again.
func TestVercelDeploymentUploadProtocol(t *testing.T) {
	v := newVX(t, "vercel-upload")
	content := "<h1>hello</h1>"
	sha := sha1Of(content)
	deploy := map[string]any{
		"name":  "site",
		"files": []map[string]any{{"file": "index.html", "sha": sha, "size": len(content)}},
	}

	t.Run("Given a file nobody uploaded When a deployment names it Then the answer lists what is missing", func(t *testing.T) {
		r := v.do(http.MethodPost, "/v13/deployments", deploy)
		e := errorOf(r)
		if r.status != 400 || str(e, "code") != "missing_files" {
			t.Fatalf("deploy before upload = %d %s, want 400 missing_files", r.status, r.raw)
		}
		if got := strs(e, "missing"); !reflect.DeepEqual(got, []string{sha}) {
			t.Fatalf("missing = %v, want [%s]", got, sha)
		}
	})
	t.Run("Given a digest that is not the file's When it is uploaded Then it is refused", func(t *testing.T) {
		r := v.do(http.MethodPost, "/v2/files", []byte(content), "x-vercel-digest", sha1Of("other"))
		if r.status != 400 {
			t.Fatalf("mismatched digest = %d %s", r.status, r.raw)
		}
		if r := v.do(http.MethodPost, "/v2/files", []byte(content)); r.status != 400 {
			t.Fatalf("no digest = %d %s", r.status, r.raw)
		}
	})
	t.Run("Given the upload When it is repeated Then it is idempotent", func(t *testing.T) {
		for i := 0; i < 2; i++ {
			if r := v.upload(content); r.status != 200 {
				t.Fatalf("upload %d = %d %s", i, r.status, r.raw)
			}
		}
	})
	t.Run("Given the upload When the deployment is asked for again Then it is created and lists the file", func(t *testing.T) {
		r := v.do(http.MethodPost, "/v13/deployments", deploy)
		if r.status != 200 || !strings.HasPrefix(str(r.body, "id"), "dpl_") || str(r.body, "readyState") != "QUEUED" {
			t.Fatalf("deploy = %d %s", r.status, r.raw)
		}
		files := v.do(http.MethodGet, "/v6/deployments/"+str(r.body, "id")+"/files", nil)
		var tree []map[string]any
		if err := jsonInto(files.raw, &tree); err != nil || len(tree) != 1 {
			t.Fatalf("files = %d %s", files.status, files.raw)
		}
		children, _ := tree[0]["children"].([]any)
		if len(children) != 1 || children[0].(map[string]any)["name"] != "index.html" || children[0].(map[string]any)["uid"] != "f_"+sha {
			t.Fatalf("the file is not under the root: %s", files.raw)
		}
	})
	t.Run("Given inlined files When a deployment is created Then they are hashed and referenceable by sha", func(t *testing.T) {
		b64 := base64.StdEncoding.EncodeToString([]byte("\x00\x01binary"))
		r := v.do(http.MethodPost, "/v13/deployments", map[string]any{"name": "inline", "files": []map[string]any{
			{"file": "a.txt", "data": "plain text"},
			{"file": "b.bin", "data": b64, "encoding": "base64"},
		}})
		if r.status != 200 {
			t.Fatalf("inline deploy = %d %s", r.status, r.raw)
		}
		// A second deployment can refer to the first one's file by the digest
		// the first one computed, without uploading it.
		again := v.do(http.MethodPost, "/v13/deployments", map[string]any{"name": "inline2", "files": []map[string]any{
			{"file": "a.txt", "sha": sha1Of("plain text"), "size": 10},
			{"file": "b.bin", "sha": sha1Of("\x00\x01binary"), "size": 8},
		}})
		if again.status != 200 {
			t.Fatalf("a deployment naming inlined files by sha = %d %s", again.status, again.raw)
		}
	})
	t.Run("Given a file entry with neither contents nor sha When it is sent Then it is a 400", func(t *testing.T) {
		r := v.do(http.MethodPost, "/v13/deployments", map[string]any{"name": "bad", "files": []map[string]any{{"file": "x"}}})
		if r.status != 400 {
			t.Fatalf("empty entry = %d %s", r.status, r.raw)
		}
	})
}

// jsonInto decodes a body that is not an object.
func jsonInto(raw []byte, into any) error { return unmarshalJSON(raw, into) }

// TestVercelDeploymentLifecycleIsClockDriven holds the QUEUED to BUILDING to
// READY machine to the injected clock, the way a polling client sees it.
func TestVercelDeploymentLifecycleIsClockDriven(t *testing.T) {
	v := newVX(t, "vercel-lifecycle")
	created := v.do(http.MethodPost, "/v13/deployments", map[string]any{"name": "app", "target": "production"})
	if created.status != 200 {
		t.Fatalf("create = %d %s", created.status, created.raw)
	}
	id, host := str(created.body, "id"), str(created.body, "url")
	state := func() string {
		return str(v.do(http.MethodGet, "/v13/deployments/"+id, nil).body, "readyState")
	}

	if got := state(); got != "QUEUED" {
		t.Fatalf("at creation: %s", got)
	}
	v.advance(1 * time.Second)
	if got := state(); got != "BUILDING" {
		t.Fatalf("after 1s: %s", got)
	}
	v.advance(1 * time.Second)
	if got := state(); got != "READY" {
		t.Fatalf("after 2s: %s", got)
	}

	t.Run("it is addressable by its URL, with or without the scheme", func(t *testing.T) {
		// A client sends the URL percent-encoded, as one path segment.
		for _, key := range []string{host, url.PathEscape("https://" + host)} {
			r := v.do(http.MethodGet, "/v13/deployments/"+key, nil)
			if r.status != 200 || str(r.body, "id") != id {
				t.Errorf("GET by %q = %d %s", key, r.status, r.raw)
			}
		}
	})
	t.Run("a production deployment is aliased once it is ready, with its ready instant", func(t *testing.T) {
		r := v.do(http.MethodGet, "/v13/deployments/"+id, nil)
		if r.body["aliasAssigned"] != true || !reflect.DeepEqual(strs(r.body, "alias"), []string{"app.vercel.app"}) {
			t.Errorf("alias = %v assigned = %v", r.body["alias"], r.body["aliasAssigned"])
		}
		if r.body["ready"] == nil || r.body["target"] != "production" {
			t.Errorf("ready = %v target = %v", r.body["ready"], r.body["target"])
		}
	})
	t.Run("the events are the states it passed through, in order", func(t *testing.T) {
		r := v.do(http.MethodGet, "/v3/deployments/"+id+"/events", nil)
		var events []map[string]any
		if err := jsonInto(r.raw, &events); err != nil || len(events) < 4 {
			t.Fatalf("events = %d %s", r.status, r.raw)
		}
		var states []string
		for _, e := range events {
			if e["type"] == "deployment-state" {
				p, _ := e["payload"].(map[string]any)
				info, _ := p["info"].(map[string]any)
				states = append(states, str(info, "readyState"))
			}
		}
		if !reflect.DeepEqual(states, []string{"QUEUED", "BUILDING", "READY"}) {
			t.Errorf("states = %v", states)
		}
	})
	t.Run("a deployment that is ready cannot be canceled", func(t *testing.T) {
		if r := v.do(http.MethodPatch, "/v12/deployments/"+id+"/cancel", nil); r.status != 400 {
			t.Fatalf("cancel of a ready deployment = %d %s", r.status, r.raw)
		}
	})
	t.Run("a deployment that is queued can", func(t *testing.T) {
		q := v.do(http.MethodPost, "/v13/deployments", map[string]any{"name": "app"})
		r := v.do(http.MethodPatch, "/v12/deployments/"+str(q.body, "id")+"/cancel", nil)
		if r.status != 200 || str(r.body, "readyState") != "CANCELED" || r.body["canceledAt"] == nil {
			t.Fatalf("cancel = %d %s", r.status, r.raw)
		}
		v.advance(5 * time.Second)
		if got := str(v.do(http.MethodGet, "/v13/deployments/"+str(q.body, "id"), nil).body, "readyState"); got != "CANCELED" {
			t.Errorf("a canceled deployment became %s", got)
		}
	})
	t.Run("deleting it removes it and what hung off it", func(t *testing.T) {
		if r := v.do(http.MethodDelete, "/v13/deployments/"+id, nil); r.status != 200 || str(r.body, "state") != "DELETED" {
			t.Fatalf("delete = %d %s", r.status, r.raw)
		}
		if r := v.do(http.MethodGet, "/v13/deployments/"+id, nil); r.status != 404 {
			t.Fatalf("get after delete = %d", r.status)
		}
		if r := v.do(http.MethodGet, "/v13/deployments/"+host, nil); r.status != 404 {
			t.Fatalf("get by url after delete = %d", r.status)
		}
		if r := v.do(http.MethodDelete, "/v13/deployments/"+id, nil); r.status != 404 {
			t.Fatalf("second delete = %d", r.status)
		}
	})
}

// TestVercelDeploymentsListNewestFirstAndPage holds the listing's order, its
// filters and the cursor a client pages with.
func TestVercelDeploymentsListNewestFirstAndPage(t *testing.T) {
	v := newVX(t, "vercel-listing")
	var ids []string
	var created []float64
	for i := 0; i < 5; i++ {
		name := "alpha"
		if i%2 == 1 {
			name = "beta"
		}
		r := v.do(http.MethodPost, "/v13/deployments", map[string]any{"name": name})
		if r.status != 200 {
			t.Fatalf("create %d = %d %s", i, r.status, r.raw)
		}
		ids = append(ids, str(r.body, "id"))
		created = append(created, r.body["createdAt"].(float64))
		v.advance(10 * time.Millisecond)
	}
	list := func(query string) (got []string, pagination map[string]any) {
		r := v.do(http.MethodGet, "/v7/deployments"+query, nil)
		if r.status != 200 {
			t.Fatalf("list%s = %d %s", query, r.status, r.raw)
		}
		items, _ := r.body["deployments"].([]any)
		for _, it := range items {
			got = append(got, str(it.(map[string]any), "uid"))
		}
		pagination, _ = r.body["pagination"].(map[string]any)
		return
	}
	rev := func(in []string) []string {
		out := make([]string, len(in))
		for i, s := range in {
			out[len(in)-1-i] = s
		}
		return out
	}

	all, p := list("")
	if !reflect.DeepEqual(all, rev(ids)) || p["next"] != nil || p["prev"] != nil || p["count"] != float64(5) {
		t.Fatalf("unfiltered = %v pagination %v, want newest first %v", all, p, rev(ids))
	}

	// The cursor is a number that still reads as the creation instant of the last
	// item on the page (its integer part), with the item's place among those made
	// in the same millisecond in its fraction. It goes back as it came.
	cursor := func(p map[string]any) string { return strconv.FormatFloat(p["next"].(float64), 'f', -1, 64) }
	first, p := list("?limit=2")
	if !reflect.DeepEqual(first, rev(ids)[:2]) || p["count"] != float64(2) || math.Floor(p["next"].(float64)) != created[3] {
		t.Fatalf("page one = %v %v", first, p)
	}
	// A client that keeps only the integer part sends the old bare timestamp, and
	// gets what it always did: the items made before that millisecond.
	if bare, _ := list(fmt.Sprintf("?limit=2&until=%d", int64(p["next"].(float64)))); !reflect.DeepEqual(bare, rev(ids)[2:4]) {
		t.Fatalf("page two from the bare timestamp = %v", bare)
	}
	second, p := list("?limit=2&until=" + cursor(p))
	if !reflect.DeepEqual(second, rev(ids)[2:4]) || p["prev"] == nil {
		t.Fatalf("page two = %v %v", second, p)
	}
	last, p := list("?limit=2&until=" + cursor(p))
	if !reflect.DeepEqual(last, rev(ids)[4:]) || p["next"] != nil {
		t.Fatalf("last page = %v %v", last, p)
	}
	newer, _ := list(fmt.Sprintf("?since=%d", int64(created[2])))
	if !reflect.DeepEqual(newer, rev(ids)[:2]) {
		t.Fatalf("since = %v", newer)
	}
	if byApp, _ := list("?app=beta"); len(byApp) != 2 {
		t.Fatalf("app filter = %v", byApp)
	}
	if byState, _ := list("?state=READY,ERROR"); len(byState) != 0 {
		t.Fatalf("a queued deployment matched state=READY,ERROR: %v", byState)
	}
	v.advance(5 * time.Second)
	if byState, _ := list("?state=READY"); len(byState) != 5 {
		t.Fatalf("after the builds = %v", byState)
	}
	// The older address answers the same list.
	if r := v.do(http.MethodGet, "/v6/deployments?limit=1", nil); r.status != 200 {
		t.Fatalf("v6 = %d %s", r.status, r.raw)
	}
}

// TestVercelDeploymentFailuresAreInjectable holds the control surface: a test
// that needs a deployment to end badly does not have to race the build clock.
func TestVercelDeploymentFailuresAreInjectable(t *testing.T) {
	faults := withConfig(func(c *config.Config) { c.VercelFaults = true })
	v := newVXWith(t, "vercel-faults", []string{"vercel.api"}, faults)
	deploy := func() string {
		r := v.do(http.MethodPost, "/v13/deployments", map[string]any{"name": "flaky"})
		if r.status != 200 {
			t.Fatalf("create = %d %s", r.status, r.raw)
		}
		return str(r.body, "id")
	}
	get := func(id string) map[string]any { return v.do(http.MethodGet, "/v13/deployments/"+id, nil).body }

	t.Run("Given an armed failure When the next deployment builds Then it ends in ERROR with the code", func(t *testing.T) {
		armed := v.do(http.MethodPost, "/_mirror/vercel/deployments/fail-next", map[string]any{"count": 1, "errorCode": "BUILD_UTILS_SPAWN_1", "errorMessage": "npm run build exited with 1"}, "Authorization", "")
		if armed.status != 200 || armed.body["count"] != float64(1) {
			t.Fatalf("arm = %d %s", armed.status, armed.raw)
		}
		id, other := deploy(), deploy()
		v.advance(3 * time.Second)
		d := get(id)
		if d["readyState"] != "ERROR" || d["errorCode"] != "BUILD_UTILS_SPAWN_1" || d["errorMessage"] != "npm run build exited with 1" {
			t.Fatalf("armed deployment = %v", d)
		}
		if got := get(other)["readyState"]; got != "READY" {
			t.Fatalf("the failure leaked past its count: second deployment %v", got)
		}
	})
	t.Run("Given an armed cancel When the build runs Then it ends in CANCELED", func(t *testing.T) {
		v.do(http.MethodPost, "/_mirror/vercel/deployments/fail-next", map[string]any{"readyState": "CANCELED"})
		id := deploy()
		v.advance(3 * time.Second)
		if d := get(id); d["readyState"] != "CANCELED" || d["canceledAt"] == nil {
			t.Fatalf("armed cancel = %v", d)
		}
	})
	t.Run("Given a disarm When a deployment is created Then it builds normally", func(t *testing.T) {
		v.do(http.MethodPost, "/_mirror/vercel/deployments/fail-next", map[string]any{"count": 3})
		if r := v.do(http.MethodDelete, "/_mirror/vercel/deployments/fail-next", nil); r.status != 200 || r.body["count"] != float64(0) {
			t.Fatalf("disarm = %d %s", r.status, r.raw)
		}
		id := deploy()
		v.advance(3 * time.Second)
		if got := get(id)["readyState"]; got != "READY" {
			t.Fatalf("after disarm: %v", got)
		}
	})
	t.Run("Given a running deployment When it is moved to ERROR Then it is, now", func(t *testing.T) {
		id := deploy()
		r := v.do(http.MethodPost, "/_mirror/vercel/deployments/"+id+"/state", map[string]any{"readyState": "ERROR", "errorMessage": "out of memory"})
		if r.status != 200 || str(r.body, "readyState") != "ERROR" {
			t.Fatalf("move = %d %s", r.status, r.raw)
		}
		if d := get(id); d["readyState"] != "ERROR" || d["errorMessage"] != "out of memory" || d["errorCode"] != "BUILD_FAILED" {
			t.Fatalf("moved deployment = %v", d)
		}
		v.advance(5 * time.Second)
		if got := get(id)["readyState"]; got != "ERROR" {
			t.Fatalf("an errored deployment became %v", got)
		}
	})
	t.Run("Given a ready deployment When it is moved Then the move is refused", func(t *testing.T) {
		id := deploy()
		v.advance(3 * time.Second)
		if r := v.do(http.MethodPost, "/_mirror/vercel/deployments/"+id+"/state", map[string]any{"readyState": "ERROR"}); r.status != 400 {
			t.Fatalf("move of a ready deployment = %d %s", r.status, r.raw)
		}
		if r := v.do(http.MethodPost, "/_mirror/vercel/deployments/dpl_nope/state", map[string]any{"readyState": "ERROR"}); r.status != 404 {
			t.Fatalf("move of an unknown deployment = %d %s", r.status, r.raw)
		}
	})
	t.Run("Given a running deployment When it is moved to READY Then it is ready without waiting", func(t *testing.T) {
		id := deploy()
		if r := v.do(http.MethodPost, "/_mirror/vercel/deployments/"+id+"/state", map[string]any{"readyState": "READY"}); r.status != 200 {
			t.Fatalf("ready = %d %s", r.status, r.raw)
		}
		if got := get(id)["readyState"]; got != "READY" {
			t.Fatalf("after the move: %v", got)
		}
	})
}

// TestVercelDeploymentIdsAreDeterministic holds the seed to its word: the same
// requests against the same seed produce the same identifiers and instants.
func TestVercelDeploymentIdsAreDeterministic(t *testing.T) {
	run := func() []string {
		v := newVX(t, "vercel-determinism")
		var out []string
		for _, name := range []string{"a", "b"} {
			r := v.do(http.MethodPost, "/v13/deployments", map[string]any{"name": name})
			out = append(out, str(r.body, "id"), str(r.body, "url"), str(r.body, "projectId"), fmt.Sprint(r.body["createdAt"]))
		}
		return out
	}
	first, second := run(), run()
	if !reflect.DeepEqual(first, second) {
		t.Fatalf("one seed, two runs:\n%v\n%v", first, second)
	}
}
