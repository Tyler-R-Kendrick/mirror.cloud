package api

import (
	"context"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/golden"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spitest"
)

func TestVercelLifecycleCharacterization(t *testing.T) {
	p := New(spitest.Deps(t))
	ctx := context.Background()
	id := spi.Identity{Account: "000000000000", Region: "us-east-1"}
	inv := func(op string, in map[string]any) any {
		t.Helper()
		res, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: op, Input: in})
		if err != nil {
			f := err.(*spi.Fault)
			return map[string]any{"error": f.Code, "status": f.HTTPStatus, "message": f.Message}
		}
		return res.Output
	}
	golden.AssertJSON(t, map[string]any{
		"user":            inv("GetUser", nil),
		"create":          inv("CreateProject", map[string]any{"name": "snap", "framework": "nextjs"}),
		"duplicate":       inv("CreateProject", map[string]any{"name": "snap"}),
		"empty":           inv("CreateProject", map[string]any{}),
		"get":             inv("GetProject", map[string]any{"id": "snap"}),
		"list":            inv("ListProjects", nil),
		"env":             inv("CreateProjectEnv", map[string]any{"id": "snap", "key": "FOO", "value": "bar"}),
		"env_empty":       inv("CreateProjectEnv", map[string]any{"id": "snap"}),
		"envs":            inv("ListProjectEnv", map[string]any{"id": "snap"}),
		"domain":          inv("AddProjectDomain", map[string]any{"id": "snap", "name": "snap.example.test"}),
		"domains":         inv("ListProjectDomains", map[string]any{"id": "snap"}),
		"deploy":          inv("CreateDeployment", map[string]any{"name": "snap", "project": "snap"}),
		"get_deploy":      inv("GetDeployment", map[string]any{"id": inv("ListDeployments", nil).(map[string]any)["deployments"].([]any)[0].(map[string]any)["id"]}),
		"deploys":         inv("ListDeployments", nil),
		"kv_set":          inv("KvCommand", map[string]any{"_redis": []any{"SET", "k", "v"}}),
		"kv_get":          inv("KvCommand", map[string]any{"_redis": []any{"GET", "k"}}),
		"kv_del":          inv("KvCommand", map[string]any{"_redis": []any{"DEL", "k"}}),
		"kv_after_del":    inv("KvCommand", map[string]any{"_redis": []any{"GET", "k"}}),
		"kv_missing":      inv("KvCommand", map[string]any{"_redis": []any{"GET", "nope"}}),
		"missing_project": inv("GetProject", map[string]any{"id": "nope"}),
		"missing_deploy":  inv("DeleteDeployment", map[string]any{"id": "dpl_nope"}),
	})
}
