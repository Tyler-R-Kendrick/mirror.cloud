package machines

import (
	"context"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/golden"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spitest"
)

func TestFlyMachinesCharacterization(t *testing.T) {
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
	create := inv("CreateApp", map[string]any{"app_name": "web", "org_slug": "personal"})
	mach := inv("CreateMachine", map[string]any{"app_name": "web", "config": map[string]any{"image": "nginx"}})
	mid := ""
	if m, ok := mach.(map[string]any); ok {
		if out, ok := m["output"].(map[string]any); ok {
			if rec, ok := out["machine"].(map[string]any); ok {
				mid = str(rec["id"])
			}
		}
	}
	golden.AssertJSON(t, map[string]any{
		"create":     create,
		"get":        inv("GetApp", map[string]any{"app_name": "web"}),
		"list":       inv("ListApps", nil),
		"empty":      inv("CreateApp", map[string]any{}),
		"duplicate":  inv("CreateApp", map[string]any{"app_name": "web"}),
		"machine":    mach,
		"get_mach":   inv("GetMachine", map[string]any{"id": mid}),
		"machines":   inv("ListMachines", map[string]any{"app_name": "web"}),
		"del_mach":   inv("DeleteMachine", map[string]any{"id": mid}),
		"miss_mach":  inv("GetMachine", map[string]any{"id": mid}),
		"delete":     inv("DeleteApp", map[string]any{"app_name": "web"}),
		"missing":    inv("GetApp", map[string]any{"app_name": "web"}),
		"del_miss_a": inv("DeleteApp", map[string]any{"app_name": "missing"}),
		"del_miss_m": inv("DeleteMachine", map[string]any{"id": "missing"}),
	})
}
