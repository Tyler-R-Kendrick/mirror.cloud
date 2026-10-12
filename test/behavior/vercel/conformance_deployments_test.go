package vercel

import (
	"net/http"
	"net/url"
	"testing"
	"time"
)

func init() { registerArea("deployments", (*conf).deployments) }

// deployments drives the deployment operations and the control surface through
// one build: upload, deploy, poll to READY, read what hangs off it, cancel,
// fail on purpose, delete.
func (c *conf) deployments(t *testing.T) {
	content := "<h1>conformance</h1>"
	sha := sha1Of(content)

	c.call(t, http.MethodPost, "/v2/files", []byte(content), 200, "x-vercel-digest", sha, "x-vercel-size", "20")
	missing := c.call(t, http.MethodPost, "/v13/deployments", map[string]any{
		"name": "conform", "files": []map[string]any{{"file": "index.html", "sha": sha1Of("never uploaded"), "size": 3}},
	}, 400)
	if str(errorOf(missing), "code") != "missing_files" {
		t.Errorf("a deployment naming an unuploaded file = %s", missing.raw)
	}

	made := c.call(t, http.MethodPost, "/v13/deployments", map[string]any{
		"name": "conform", "target": "production", "meta": map[string]any{"commit": "abc123"},
		"files": []map[string]any{{"file": "index.html", "sha": sha, "size": len(content)}},
	}, 200)
	id, host := str(made.body, "id"), str(made.body, "url")

	c.call(t, http.MethodGet, "/v13/deployments/"+id, nil, 200)
	c.call(t, http.MethodGet, "/v13/deployments/"+url.PathEscape("https://"+host), nil, 200)
	c.call(t, http.MethodGet, "/v13/deployments/dpl_nope", nil, 404)
	c.call(t, http.MethodGet, "/v7/deployments?limit=1", nil, 200)
	c.call(t, http.MethodGet, "/v6/deployments", nil, 200)
	c.call(t, http.MethodGet, "/v3/deployments/"+id+"/events", nil, 200)
	c.call(t, http.MethodGet, "/v6/deployments/"+id+"/files", nil, 200)
	c.call(t, http.MethodGet, "/v2/deployments/"+id+"/aliases", nil, 200)
	c.call(t, http.MethodGet, "/v8/deployments/"+id+"/files/f_"+sha, nil, 200)

	// The same reads once it has built, which is when the optional members the
	// states add -- ready, the production alias -- are present.
	c.advance(2 * time.Second)
	ready := c.call(t, http.MethodGet, "/v13/deployments/"+id, nil, 200)
	if str(ready.body, "readyState") != "READY" {
		t.Fatalf("after the build: %s", ready.raw)
	}
	c.call(t, http.MethodGet, "/v3/deployments/"+id+"/events", nil, 200)
	c.call(t, http.MethodGet, "/v7/deployments?state=READY&target=production", nil, 200)
	c.call(t, http.MethodPatch, "/v12/deployments/"+id+"/cancel", nil, 400)

	queued := c.call(t, http.MethodPost, "/v13/deployments", map[string]any{"name": "conform"}, 200)
	canceled := c.call(t, http.MethodPatch, "/v12/deployments/"+str(queued.body, "id")+"/cancel", nil, 200)
	if str(canceled.body, "readyState") != "CANCELED" {
		t.Errorf("cancel = %s", canceled.raw)
	}
	c.call(t, http.MethodGet, "/v3/deployments/"+str(queued.body, "id")+"/events", nil, 200)

	// A build that ends badly: armed ahead, and moved by hand.
	c.call(t, http.MethodPost, "/_mirror/vercel/deployments/fail-next", map[string]any{"errorCode": "BUILD_FAILED", "errorMessage": "exit 1"}, 200)
	failing := c.call(t, http.MethodPost, "/v13/deployments", map[string]any{"name": "conform"}, 200)
	c.advance(3 * time.Second)
	failed := c.call(t, http.MethodGet, "/v13/deployments/"+str(failing.body, "id"), nil, 200)
	if str(failed.body, "readyState") != "ERROR" || str(failed.body, "errorCode") != "BUILD_FAILED" {
		t.Errorf("armed failure = %s", failed.raw)
	}
	c.call(t, http.MethodGet, "/v3/deployments/"+str(failing.body, "id")+"/events", nil, 200)
	c.call(t, http.MethodGet, "/v7/deployments?state=ERROR", nil, 200)
	c.call(t, http.MethodDelete, "/_mirror/vercel/deployments/fail-next", nil, 200)
	moving := c.call(t, http.MethodPost, "/v13/deployments", map[string]any{"name": "conform"}, 200)
	c.call(t, http.MethodPost, "/_mirror/vercel/deployments/"+str(moving.body, "id")+"/state", map[string]any{"readyState": "CANCELED"}, 200)
	c.call(t, http.MethodPost, "/_mirror/vercel/deployments/dpl_nope/state", map[string]any{"readyState": "ERROR"}, 404)

	c.call(t, http.MethodDelete, "/v13/deployments/"+id, nil, 200)
	c.call(t, http.MethodDelete, "/v13/deployments/"+id, nil, 404)
}
