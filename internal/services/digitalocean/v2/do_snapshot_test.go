package v2

import (
	"context"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/golden"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spitest"
)

func TestDigitalOceanV2Characterization(t *testing.T) {
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
	create := inv("CreateDroplet", map[string]any{"name": "web", "region": "nyc3", "size": "s-1vcpu-1gb"})
	did := ""
	if m, ok := create.(map[string]any); ok {
		if out, ok := m["output"].(map[string]any); ok {
			if drop, ok := out["droplet"].(map[string]any); ok {
				did = strID(drop["id"])
			}
		}
	}
	golden.AssertJSON(t, map[string]any{
		"create":     create,
		"get":        inv("GetDroplet", map[string]any{"id": did}),
		"list":       inv("ListDroplets", nil),
		"delete":     inv("DeleteDroplet", map[string]any{"id": did}),
		"missing":    inv("GetDroplet", map[string]any{"id": did}),
		"domain":     inv("CreateDomain", map[string]any{"name": "snap.test"}),
		"duplicate":  inv("CreateDomain", map[string]any{"name": "snap.test"}),
		"empty":      inv("CreateDomain", map[string]any{}),
		"get_domain": inv("GetDomain", map[string]any{"name": "snap.test"}),
		"domains":    inv("ListDomains", nil),
		"del_domain": inv("DeleteDomain", map[string]any{"name": "snap.test"}),
		"nodomain":   inv("GetDomain", map[string]any{"name": "nope.test"}),
		"del_miss_d": inv("DeleteDroplet", map[string]any{"id": "missing"}),
		"del_miss_n": inv("DeleteDomain", map[string]any{"name": "missing.test"}),
	})
}
