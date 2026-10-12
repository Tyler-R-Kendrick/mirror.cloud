package vercel

import (
	"bytes"
	"net/http"
	"reflect"
	"testing"
	"time"
)

// TestVercelChecksFollowTheirLifecycle holds what an integration drives: a check
// is registered while a build runs, moves to running and completed, can be put
// back, and a project's check runs against each deployment.
func TestVercelChecksFollowTheirLifecycle(t *testing.T) {
	v := newVX(t, "vercel-checks")
	pid := envProject(t, v, "gated")
	did := deploy(t, v, "gated", false)

	made := v.do(http.MethodPost, "/v1/deployments/"+did+"/checks", map[string]any{"name": "lint", "blocking": true, "rerequestable": true, "path": "/health"})
	if made.status != 200 || made.body["status"] != "registered" || made.body["blocking"] != true || made.body["deploymentId"] != did ||
		made.body["integrationId"] == nil || made.body["conclusion"] != nil || made.body["path"] != "/health" {
		t.Fatalf("create = %d %s", made.status, made.raw)
	}
	if _, has := made.body["conclusion"]; has {
		t.Errorf("a check with no conclusion has no member for it: %s", made.raw)
	}
	cid := str(made.body, "id")
	base := "/v1/deployments/" + did + "/checks/" + cid

	if r := v.do(http.MethodPost, "/v1/deployments/"+did+"/checks", map[string]any{"blocking": true}); r.status != 400 {
		t.Errorf("a check with no name = %d", r.status)
	}

	t.Run("running, then completed with a conclusion", func(t *testing.T) {
		run := v.do(http.MethodPatch, base, map[string]any{"status": "running"})
		if run.status != 200 || run.body["status"] != "running" || run.body["startedAt"] == nil || run.body["completedAt"] != nil {
			t.Fatalf("running = %d %s", run.status, run.raw)
		}
		v.advance(time.Second)
		done := v.do(http.MethodPatch, base, map[string]any{"status": "completed", "conclusion": "failed", "detailsUrl": "https://ci.example/run/1"})
		if done.status != 200 || done.body["conclusion"] != "failed" || done.body["completedAt"] == nil || done.body["startedAt"] != run.body["startedAt"] ||
			str(done.body, "detailsUrl") != "https://ci.example/run/1" {
			t.Fatalf("completed = %d %s", done.status, done.raw)
		}
		list := v.do(http.MethodGet, "/v1/deployments/"+did+"/checks", nil)
		checks := list.body["checks"].([]any)
		if len(checks) != 1 || checks[0].(map[string]any)["conclusion"] != "failed" {
			t.Fatalf("list = %s", list.raw)
		}
	})

	t.Run("a rerequest starts it over", func(t *testing.T) {
		if r := v.do(http.MethodPost, base+"/rerequest", nil); r.status != 200 {
			t.Fatalf("rerequest = %d %s", r.status, r.raw)
		}
		g := v.do(http.MethodGet, base, nil)
		if g.body["status"] != "registered" || g.body["conclusion"] != nil || g.body["startedAt"] != nil || g.body["completedAt"] != nil {
			t.Fatalf("after rerequest = %s", g.raw)
		}
		if r := v.do(http.MethodPost, "/v1/deployments/"+did+"/checks/chk_nope/rerequest", nil); r.status != 404 {
			t.Errorf("rerequest an unknown check = %d", r.status)
		}
	})

	t.Run("a finished deployment takes no new checks", func(t *testing.T) {
		v.advance(3 * time.Second)
		if r := v.do(http.MethodPost, "/v1/deployments/"+did+"/checks", map[string]any{"name": "late", "blocking": false}); r.status != 400 {
			t.Fatalf("late check = %d %s", r.status, r.raw)
		}
	})

	t.Run("a project check runs against a deployment", func(t *testing.T) {
		pc := v.do(http.MethodPost, "/v2/projects/"+pid+"/checks", map[string]any{"name": "e2e", "requires": "build-ready", "blocks": "deployment-promotion", "targets": []string{"production"}, "timeout": 120000})
		if pc.status != 200 || pc.body["blocks"] != "deployment-promotion" || pc.body["timeout"] != float64(120000) || pc.body["projectId"] != pid ||
			pc.body["sourceKind"] != "webhook" || pc.body["isRerequestable"] != false {
			t.Fatalf("project check = %d %s", pc.status, pc.raw)
		}
		pcid := str(pc.body, "id")
		for name, body := range map[string]map[string]any{
			"an unknown requires": {"name": "x", "requires": "never"},
			"an unknown blocks":   {"name": "x", "requires": "none", "blocks": "everything"},
		} {
			if r := v.do(http.MethodPost, "/v2/projects/"+pid+"/checks", body); r.status != 400 {
				t.Errorf("%s = %d", name, r.status)
			}
		}
		if got := v.do(http.MethodGet, "/v2/projects/"+pid+"/checks?blocks=build-start", nil).body["checks"].([]any); len(got) != 0 {
			t.Errorf("blocks filter = %v", got)
		}

		run := v.do(http.MethodPost, "/v2/deployments/"+did+"/check-runs", map[string]any{"checkId": pcid})
		if run.status != 200 || run.body["status"] != "queued" || run.body["name"] != "e2e" || run.body["checkId"] != pcid ||
			run.body["deploymentId"] != did || run.body["timeout"] != float64(120000) || run.body["conclusion"] != nil {
			t.Fatalf("run = %d %s", run.status, run.raw)
		}
		rid := str(run.body, "id")
		upd := v.do(http.MethodPatch, "/v2/deployments/"+did+"/check-runs/"+rid, map[string]any{"status": "completed", "conclusion": "succeeded", "conclusionText": "all green", "externalUrl": "https://ci.example/9"})
		if upd.status != 200 || upd.body["conclusion"] != "succeeded" || upd.body["completedAt"] == nil || upd.body["conclusionText"] != "all green" {
			t.Fatalf("update run = %d %s", upd.status, upd.raw)
		}
		if got := v.do(http.MethodGet, "/v2/projects/"+pid+"/checks/"+pcid+"/runs", nil).body["runs"].([]any); len(got) != 1 {
			t.Errorf("the project check's runs = %v", got)
		}
		if got := v.do(http.MethodGet, "/v2/deployments/"+did+"/check-runs", nil).body["runs"].([]any); len(got) != 1 {
			t.Errorf("the deployment's runs = %v", got)
		}
		other := deploy(t, v, "gated", false)
		if r := v.do(http.MethodGet, "/v2/deployments/"+other+"/check-runs/"+rid, nil); r.status != 404 {
			t.Errorf("a run read through another deployment = %d", r.status)
		}
		if r := v.do(http.MethodPost, "/v2/deployments/"+did+"/check-runs", map[string]any{"checkId": "pchk_nope"}); r.status != 404 {
			t.Errorf("a run of an unknown check = %d", r.status)
		}
		upc := v.do(http.MethodPatch, "/v2/projects/"+pid+"/checks/"+pcid, map[string]any{"name": "e2e-renamed", "timeout": 60000})
		if upc.status != 200 || upc.body["name"] != "e2e-renamed" || upc.body["timeout"] != float64(60000) || upc.body["blocks"] != "deployment-promotion" {
			t.Fatalf("update project check = %d %s", upc.status, upc.raw)
		}
		if d := v.do(http.MethodDelete, "/v2/projects/"+pid+"/checks/"+pcid, nil); d.status != 200 || d.body["success"] != true {
			t.Fatalf("delete = %d %s", d.status, d.raw)
		}
		if d := v.do(http.MethodDelete, "/v2/projects/"+pid+"/checks/"+pcid, nil); d.status != 404 {
			t.Fatalf("delete again = %d", d.status)
		}
	})
}

// TestVercelRollingReleaseStages holds the release a client steps through: it
// needs stages, a built canary, and approvals in order, and completing it moves
// production.
func TestVercelRollingReleaseStages(t *testing.T) {
	v := newVX(t, "vercel-rolling")
	pid := envProject(t, v, "rolling")
	base := "/v1/projects/" + pid + "/rolling-release"

	t.Run("a release needs configured stages", func(t *testing.T) {
		if g := v.do(http.MethodGet, base+"/config", nil); g.status != 200 || g.body["rollingRelease"] != nil {
			t.Fatalf("no config = %d %s", g.status, g.raw)
		}
		canary := deploy(t, v, "rolling", true)
		if r := v.do(http.MethodPost, base+"/start", map[string]any{"canaryDeploymentId": canary}); r.status != 400 {
			t.Fatalf("start with no config = %d %s", r.status, r.raw)
		}
		for name, stages := range map[string][]map[string]any{
			"a final stage under 100": {{"targetPercentage": 25}},
			"stages that fall":        {{"targetPercentage": 50}, {"targetPercentage": 25}, {"targetPercentage": 100}},
			"a stage over 100":        {{"targetPercentage": 150}},
		} {
			if r := v.do(http.MethodPatch, base+"/config", map[string]any{"stages": stages}); r.status != 400 {
				t.Errorf("%s = %d %s", name, r.status, r.raw)
			}
		}
		cfg := v.do(http.MethodPatch, base+"/config", map[string]any{"rollingRelease": map[string]any{"target": "production", "stages": []map[string]any{
			{"targetPercentage": 10, "requireApproval": true}, {"targetPercentage": 50, "duration": 5}, {"targetPercentage": 100},
		}}})
		if cfg.status != 200 || cfg.body["rollingRelease"].(map[string]any)["target"] != "production" {
			t.Fatalf("config = %d %s", cfg.status, cfg.raw)
		}
	})

	canary := deploy(t, v, "rolling", true)
	building := deploy(t, v, "rolling", false)
	t.Run("the canary must have built", func(t *testing.T) {
		if r := v.do(http.MethodPost, base+"/start", map[string]any{"canaryDeploymentId": building}); r.status != 400 {
			t.Errorf("a building canary = %d %s", r.status, r.raw)
		}
		if r := v.do(http.MethodPost, base+"/start", map[string]any{"canaryDeploymentId": "dpl_nope"}); r.status != 404 {
			t.Errorf("an unknown canary = %d", r.status)
		}
	})

	var rr map[string]any
	t.Run("a started release is on its first stage", func(t *testing.T) {
		r := v.do(http.MethodPost, base+"/start", map[string]any{"canaryDeploymentId": canary})
		if r.status != 200 {
			t.Fatalf("start = %d %s", r.status, r.raw)
		}
		rr = r.body["rollingRelease"].(map[string]any)
		active := rr["activeStage"].(map[string]any)
		next := rr["nextStage"].(map[string]any)
		stages := rr["stages"].([]any)
		if rr["state"] != "ACTIVE" || rr["advancementType"] != "manual-approval" || len(stages) != 3 || active["index"] != float64(0) ||
			active["targetPercentage"] != float64(10) || next["index"] != float64(1) || rr["currentCanaryPercentage"] != float64(10) ||
			stages[2].(map[string]any)["isFinalStage"] != true || rr["canaryDeployment"].(map[string]any)["id"] != canary {
			t.Fatalf("release = %s", r.raw)
		}
		if again := v.do(http.MethodPost, base+"/start", map[string]any{"canaryDeploymentId": canary}); again.status != 409 {
			t.Errorf("a second start = %d", again.status)
		}
		got := v.do(http.MethodGet, base, nil)
		if got.status != 200 || !reflect.DeepEqual(got.body["rollingRelease"], rr) {
			t.Errorf("get = %s", got.raw)
		}
		if g := v.do(http.MethodGet, base+"?state=COMPLETE", nil); g.body["rollingRelease"] != nil {
			t.Errorf("a state that does not match = %s", g.raw)
		}
	})

	t.Run("approvals go one stage at a time", func(t *testing.T) {
		for name, body := range map[string]map[string]any{
			"skipping a stage":   {"canaryDeploymentId": canary, "nextStageIndex": 2},
			"going back":         {"canaryDeploymentId": canary, "nextStageIndex": 0},
			"another deployment": {"canaryDeploymentId": building, "nextStageIndex": 1},
		} {
			if r := v.do(http.MethodPost, base+"/approve-stage", body); r.status != 400 {
				t.Errorf("%s = %d %s", name, r.status, r.raw)
			}
		}
		r := v.do(http.MethodPost, base+"/approve-stage", map[string]any{"canaryDeploymentId": canary, "nextStageIndex": 1})
		if r.status != 200 {
			t.Fatalf("approve = %d %s", r.status, r.raw)
		}
		got := r.body["rollingRelease"].(map[string]any)
		if got["activeStage"].(map[string]any)["index"] != float64(1) || got["currentCanaryPercentage"] != float64(50) {
			t.Fatalf("after approve = %s", r.raw)
		}
	})

	t.Run("completing moves production", func(t *testing.T) {
		r := v.do(http.MethodPost, base+"/complete", map[string]any{"canaryDeploymentId": canary})
		if r.status != 200 {
			t.Fatalf("complete = %d %s", r.status, r.raw)
		}
		got := r.body["rollingRelease"].(map[string]any)
		if got["state"] != "COMPLETE" || got["currentCanaryPercentage"] != float64(100) || got["nextStage"] != nil {
			t.Fatalf("complete = %s", r.raw)
		}
		aliases := v.do(http.MethodGet, "/v1/projects/"+pid+"/promote/aliases", nil)
		promoted := 0
		for _, a := range aliases.body["aliases"].([]any) {
			if a.(map[string]any)["status"] == "PROMOTED" {
				promoted++
			}
		}
		if promoted != 1 {
			t.Errorf("production should be the canary: %s", aliases.raw)
		}
		if r := v.do(http.MethodPost, base+"/complete", map[string]any{"canaryDeploymentId": canary}); r.status != 404 {
			t.Errorf("completing a finished release = %d", r.status)
		}
		// With the first release over, another may begin, now with a current deployment.
		next := deploy(t, v, "rolling", true)
		s := v.do(http.MethodPost, base+"/start", map[string]any{"canaryDeploymentId": next})
		cur := s.body["rollingRelease"].(map[string]any)["currentDeployment"].(map[string]any)
		if s.status != 200 || cur["id"] != canary {
			t.Errorf("a second release's current deployment = %s", s.raw)
		}
	})

	t.Run("the config can be taken away", func(t *testing.T) {
		d := v.do(http.MethodDelete, base+"/config", nil)
		if d.status != 200 || d.body["rollingRelease"] != nil {
			t.Fatalf("delete = %d %s", d.status, d.raw)
		}
		if g := v.do(http.MethodGet, base+"/config", nil); g.body["rollingRelease"] != nil {
			t.Errorf("after delete = %s", g.raw)
		}
	})
}

// TestVercelArtifactsKeepTheirBytes holds the remote cache: what a build
// uploads is what the next one downloads, byte for byte, including bytes that
// are not text.
func TestVercelArtifactsKeepTheirBytes(t *testing.T) {
	v := newVX(t, "vercel-artifacts")
	payload := []byte{0x28, 0xb5, 0x2f, 0xfd, 0x00, 0xff, 0xfe, 0x80, 'c', 'a', 'c', 'h', 'e', 0x00}
	put := v.do(http.MethodPut, "/v8/artifacts/abc123", payload, "x-artifact-duration", "250", "x-artifact-tag", "sig")
	if put.status != 202 || len(put.body["urls"].([]any)) != 1 {
		t.Fatalf("upload = %d %s", put.status, put.raw)
	}
	v.do(http.MethodPut, "/v8/artifacts/def456", []byte("second"))

	t.Run("download returns the bytes", func(t *testing.T) {
		g := v.do(http.MethodGet, "/v8/artifacts/abc123", nil)
		if g.status != 200 || !bytes.Equal(g.raw, payload) {
			t.Fatalf("download = %d % x, want % x", g.status, g.raw, payload)
		}
		if h := v.do(http.MethodHead, "/v8/artifacts/abc123", nil); h.status != 200 {
			t.Errorf("head = %d", h.status)
		}
		if g := v.do(http.MethodGet, "/v8/artifacts/nope", nil); g.status != 404 {
			t.Errorf("unknown = %d", g.status)
		}
		if h := v.do(http.MethodHead, "/v8/artifacts/nope", nil); h.status != 404 {
			t.Errorf("head unknown = %d", h.status)
		}
	})

	t.Run("a query says what the cache holds", func(t *testing.T) {
		q := v.do(http.MethodPost, "/v8/artifacts", map[string]any{"hashes": []string{"abc123", "nope"}})
		hit := q.body["abc123"].(map[string]any)
		if q.status != 200 || hit["size"] != float64(len(payload)) || hit["taskDurationMs"] != float64(250) || hit["tag"] != "sig" {
			t.Fatalf("query = %d %s", q.status, q.raw)
		}
		if miss := q.body["nope"].(map[string]any); miss["error"].(map[string]any)["message"] == "" {
			t.Errorf("a miss says why: %s", q.raw)
		}
	})

	t.Run("delete takes them all", func(t *testing.T) {
		d := v.do(http.MethodDelete, "/v8/artifacts", nil)
		if d.status != 200 || d.body["deletedCount"] != float64(2) {
			t.Fatalf("delete = %d %s", d.status, d.raw)
		}
		if g := v.do(http.MethodGet, "/v8/artifacts/abc123", nil); g.status != 404 {
			t.Errorf("after delete = %d", g.status)
		}
	})
}
