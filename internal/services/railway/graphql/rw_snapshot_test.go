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
	// An operation's output is the field's value itself, with no field name
	// wrapped around it -- which field answered is what the codec knows from
	// the operation.
	newID := func(res any) string {
		m, ok := res.(map[string]any)
		if !ok {
			return ""
		}
		out, ok := m["output"].(map[string]any)
		if !ok {
			return ""
		}
		return str(out["id"])
	}
	create := inv("projectCreate", map[string]any{"name": "web"})
	pid := newID(create)
	if pid == "" {
		t.Fatalf("projectCreate answered no id, so every step below would read a miss: %#v", create)
	}
	svc := inv("serviceCreate", map[string]any{"name": "api", "projectId": pid})
	sid := newID(svc)
	if sid == "" {
		t.Fatalf("serviceCreate answered no id: %#v", svc)
	}
	golden.AssertJSON(t, map[string]any{
		"create":      create,
		"get":         inv("project", map[string]any{"id": pid}),
		"list":        inv("projects", nil),
		"empty":       inv("projectCreate", map[string]any{}),
		"service":     svc,
		"get_svc":     inv("service", map[string]any{"id": sid}),
		"del_svc":     inv("serviceDelete", map[string]any{"id": sid}),
		"miss_after":  inv("service", map[string]any{"id": sid}),
		"delete":      inv("projectDelete", map[string]any{"id": pid}),
		"missing":     inv("project", map[string]any{"id": pid}),
		"del_miss":    inv("projectDelete", map[string]any{"id": "missing"}),
		"miss_svc":    inv("service", map[string]any{"id": "missing"}),
		"del_miss_sv": inv("serviceDelete", map[string]any{"id": "missing"}),
	})
}
