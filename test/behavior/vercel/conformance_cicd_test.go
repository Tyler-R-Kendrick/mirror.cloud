package vercel

import (
	"net/http"
	"testing"
	"time"
)

func init() {
	registerArea("custom-environments", (*conf).customEnvironments)
	registerArea("promotion", (*conf).promotion)
	registerArea("checks", (*conf).checks)
	registerArea("artifacts", (*conf).artifacts)
	registerArea("rolling-release", (*conf).rollingRelease)
}

// customEnvironments drives a project's custom environments: made, read by slug
// and by id, listed, edited, removed.
func (c *conf) customEnvironments(t *testing.T) {
	pid := str(c.call(t, http.MethodPost, "/v11/projects", map[string]any{"name": "conform-cenv"}, 200).body, "id")
	base := "/v9/projects/" + pid + "/custom-environments"
	made := c.call(t, http.MethodPost, base, map[string]any{
		"slug": "staging", "description": "Pre-production", "branchMatcher": map[string]any{"type": "startsWith", "pattern": "staging-"},
	}, 201)
	id := str(made.body, "id")
	c.call(t, http.MethodPost, base, map[string]any{"slug": "staging"}, 409)
	c.call(t, http.MethodPost, base, map[string]any{"description": "no slug"}, 400)
	c.call(t, http.MethodPost, "/v9/projects/prj_nope/custom-environments", map[string]any{"slug": "x"}, 404)
	c.call(t, http.MethodGet, base, nil, 200)
	c.call(t, http.MethodGet, "/projects/"+pid+"/custom-environments", nil, 200) // the CLI's address, no version
	c.call(t, http.MethodGet, base+"/staging", nil, 200)
	c.call(t, http.MethodGet, base+"/"+id, nil, 200)
	c.call(t, http.MethodGet, base+"/nope", nil, 404)
	c.call(t, http.MethodPatch, base+"/staging", map[string]any{"description": "Pre-production, v2", "branchMatcher": map[string]any{"type": "endsWith", "pattern": "-stage"}}, 200)
	c.call(t, http.MethodPatch, base+"/nope", map[string]any{"description": "x"}, 404)
	c.call(t, http.MethodDelete, base+"/staging", nil, 200)
	c.call(t, http.MethodDelete, base+"/staging", nil, 404)
}

// promotion drives promote, rollback and the protection bypass over a project
// with two built deployments.
func (c *conf) promotion(t *testing.T) {
	pid := str(c.call(t, http.MethodPost, "/v11/projects", map[string]any{"name": "conform-promote"}, 200).body, "id")
	deploy := func() string {
		id := str(c.call(t, http.MethodPost, "/v13/deployments", map[string]any{"name": "conform-promote", "project": "conform-promote"}, 200).body, "id")
		c.advance(3 * time.Second)
		return id
	}
	first, second := deploy(), deploy()
	c.call(t, http.MethodPost, "/v10/projects/"+pid+"/promote/"+first, nil, 201)
	c.call(t, http.MethodPost, "/v10/projects/"+pid+"/promote/dpl_nope", nil, 404)
	c.call(t, http.MethodGet, "/v1/projects/"+pid+"/promote/aliases", nil, 200)
	c.call(t, http.MethodGet, "/v1/projects/prj_nope/promote/aliases", nil, 404)
	c.call(t, http.MethodPost, "/v10/projects/"+pid+"/promote/"+second, nil, 201)
	c.call(t, http.MethodPost, "/v1/projects/"+pid+"/rollback/"+first, nil, 201)
	c.call(t, http.MethodPost, "/v1/projects/"+pid+"/rollback/dpl_nope", nil, 404)
	c.call(t, http.MethodPatch, "/v1/projects/"+pid+"/rollback/"+first+"/update-description", map[string]any{"description": "rolled back after a bad deploy"}, 200)
	c.call(t, http.MethodPatch, "/v1/projects/"+pid+"/protection-bypass", map[string]any{"generate": map[string]any{"note": "ci"}}, 200)

	// The deployment's own URL is an alias with a protection bypass to set.
	aliases := c.call(t, http.MethodGet, "/v2/deployments/"+first+"/aliases", nil, 200)
	row := aliases.body["aliases"].([]any)[0].(map[string]any)
	c.call(t, http.MethodPatch, "/aliases/"+str(row, "uid")+"/protection-bypass", map[string]any{"ttl": 3600, "revoke": false}, 200)
	c.call(t, http.MethodPatch, "/aliases/als_nope/protection-bypass", map[string]any{"revoke": true}, 404)
}

// checks drives the deployment checks, the project checks and the check runs.
func (c *conf) checks(t *testing.T) {
	pid := str(c.call(t, http.MethodPost, "/v11/projects", map[string]any{"name": "conform-checks"}, 200).body, "id")
	did := str(c.call(t, http.MethodPost, "/v13/deployments", map[string]any{"name": "conform-checks", "project": "conform-checks"}, 200).body, "id")

	chk := c.call(t, http.MethodPost, "/v1/deployments/"+did+"/checks", map[string]any{"name": "lint", "blocking": false, "rerequestable": true}, 200)
	cid := str(chk.body, "id")
	c.call(t, http.MethodPost, "/v1/deployments/"+did+"/checks", map[string]any{"blocking": true}, 400)
	c.call(t, http.MethodPost, "/v1/deployments/dpl_nope/checks", map[string]any{"name": "lint", "blocking": false}, 404)
	c.call(t, http.MethodGet, "/v1/deployments/"+did+"/checks", nil, 200)
	c.call(t, http.MethodGet, "/v1/deployments/"+did+"/checks/"+cid, nil, 200)
	c.call(t, http.MethodGet, "/v1/deployments/"+did+"/checks/chk_nope", nil, 404)
	c.call(t, http.MethodPatch, "/v1/deployments/"+did+"/checks/"+cid, map[string]any{"status": "completed", "conclusion": "succeeded"}, 200)
	c.call(t, http.MethodPost, "/v1/deployments/"+did+"/checks/"+cid+"/rerequest", nil, 200)

	pchk := c.call(t, http.MethodPost, "/v2/projects/"+pid+"/checks", map[string]any{"name": "gate", "requires": "build-ready"}, 200)
	pcid := str(pchk.body, "id")
	c.call(t, http.MethodGet, "/v2/projects/"+pid+"/checks", nil, 200)
	c.call(t, http.MethodGet, "/v2/projects/"+pid+"/checks/"+pcid, nil, 200)
	c.call(t, http.MethodGet, "/v2/projects/"+pid+"/checks/"+pcid+"/runs", nil, 200)
	c.call(t, http.MethodPatch, "/v2/projects/"+pid+"/checks/"+pcid, map[string]any{"name": "gate-renamed"}, 200)
	c.call(t, http.MethodGet, "/v2/projects/"+pid+"/checks/chk_nope", nil, 404)

	run := c.call(t, http.MethodPost, "/v2/deployments/"+did+"/check-runs", map[string]any{"checkId": pcid}, 200)
	rid := str(run.body, "id")
	c.call(t, http.MethodGet, "/v2/deployments/"+did+"/check-runs", nil, 200)
	c.call(t, http.MethodGet, "/v2/deployments/"+did+"/check-runs/"+rid, nil, 200)
	c.call(t, http.MethodPatch, "/v2/deployments/"+did+"/check-runs/"+rid, map[string]any{"status": "completed", "conclusion": "succeeded"}, 200)
	c.call(t, http.MethodGet, "/v2/deployments/"+did+"/check-runs/run_nope", nil, 404)
	c.call(t, http.MethodDelete, "/v2/projects/"+pid+"/checks/"+pcid, nil, 200)
	c.call(t, http.MethodDelete, "/v2/projects/"+pid+"/checks/"+pcid, nil, 404)
}

// artifacts drives the remote cache: status, upload, probe, query, download,
// events and the wipe.
func (c *conf) artifacts(t *testing.T) {
	c.call(t, http.MethodGet, "/v8/artifacts/status", nil, 200)
	c.call(t, http.MethodPut, "/v8/artifacts/deadbeef", []byte("cache"), 202, "Content-Length", "5", "x-artifact-duration", "12")
	c.call(t, http.MethodPut, "/v8/artifacts/feedface", []byte("more"), 202, "Content-Length", "4")
	c.call(t, http.MethodHead, "/v8/artifacts/deadbeef", nil, 200)
	c.call(t, http.MethodHead, "/v8/artifacts/0000", nil, 404)
	c.call(t, http.MethodGet, "/v8/artifacts/deadbeef", nil, 200)
	c.call(t, http.MethodGet, "/v8/artifacts/0000", nil, 404)
	c.call(t, http.MethodPost, "/v8/artifacts", map[string]any{"hashes": []string{"deadbeef", "0000"}}, 200)
	c.call(t, http.MethodPost, "/v8/artifacts/events", []map[string]any{{"sessionId": "s1", "source": "REMOTE", "event": "HIT", "hash": "deadbeef", "duration": 3}}, 200)
	c.call(t, http.MethodDelete, "/v8/artifacts", nil, 200)
}

// rollingRelease drives the configuration and the release it governs.
func (c *conf) rollingRelease(t *testing.T) {
	pid := str(c.call(t, http.MethodPost, "/v11/projects", map[string]any{"name": "conform-rolling"}, 200).body, "id")
	base := "/v1/projects/" + pid + "/rolling-release"
	c.call(t, http.MethodGet, base+"/billing", nil, 200)
	c.call(t, http.MethodGet, base+"/config", nil, 200)
	c.call(t, http.MethodPatch, base+"/config", map[string]any{"target": "production", "stages": []map[string]any{
		{"targetPercentage": 25, "requireApproval": true, "duration": 10},
		{"targetPercentage": 100, "requireApproval": false},
	}}, 200)
	c.call(t, http.MethodGet, base+"/config", nil, 200)
	did := str(c.call(t, http.MethodPost, "/v13/deployments", map[string]any{"name": "conform-rolling", "project": "conform-rolling"}, 200).body, "id")
	c.advance(3 * time.Second)
	c.call(t, http.MethodGet, base, nil, 200)
	c.call(t, http.MethodPost, base+"/start", map[string]any{"canaryDeploymentId": did}, 200)
	c.call(t, http.MethodGet, base, nil, 200)
	c.call(t, http.MethodPost, base+"/approve-stage", map[string]any{"canaryDeploymentId": did, "nextStageIndex": 1}, 200)
	c.call(t, http.MethodPost, base+"/complete", map[string]any{"canaryDeploymentId": did}, 200)
	c.call(t, http.MethodGet, "/v1/projects/"+pid+"/deployments/"+did+"/runtime-logs", nil, 200)
	c.call(t, http.MethodDelete, base+"/config", nil, 200)
}
