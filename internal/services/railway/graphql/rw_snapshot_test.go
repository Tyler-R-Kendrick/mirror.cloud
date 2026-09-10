package graphql

import (
	"context"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/golden"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spitest"
)

func TestRailwayGraphQLCharacterization(t *testing.T) {
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
		return map[string]any{"status": res.Status, "output": res.Output}
	}
	create := inv("projectCreate", map[string]any{"name": "web"})
	pid := ""
	if m, ok := create.(map[string]any); ok {
		if out, ok := m["output"].(map[string]any); ok {
			if rec, ok := out["projectCreate"].(map[string]any); ok {
				pid = str(rec["id"])
			}
		}
	}
	svc := inv("serviceCreate", map[string]any{"name": "api", "projectId": pid})
	sid := ""
	if m, ok := svc.(map[string]any); ok {
		if out, ok := m["output"].(map[string]any); ok {
			if rec, ok := out["serviceCreate"].(map[string]any); ok {
				sid = str(rec["id"])
			}
		}
	}
	golden.AssertJSON(t, map[string]any{
		"create":   create,
		"get":      inv("project", map[string]any{"id": pid}),
		"list":     inv("projects", nil),
		"empty":    inv("projectCreate", map[string]any{}),
		"service":  svc,
		"get_svc":  inv("service", map[string]any{"id": sid}),
		"delete":   inv("projectDelete", map[string]any{"id": pid}),
		"missing":  inv("project", map[string]any{"id": pid}),
		"del_miss": inv("projectDelete", map[string]any{"id": "missing"}),
		"miss_svc": inv("service", map[string]any{"id": "missing"}),
	})
}
