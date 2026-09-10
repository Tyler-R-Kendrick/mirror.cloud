package api

import (
	"context"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/golden"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spitest"
)

func TestHostingerDNSCharacterization(t *testing.T) {
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
	zone := []any{map[string]any{"name": "@", "type": "A", "ttl": 300, "records": []any{map[string]any{"content": "1.2.3.4"}}}}
	golden.AssertJSON(t, map[string]any{
		"create":    inv("CreateDomain", map[string]any{"domain": "snap.test"}),
		"duplicate": inv("CreateDomain", map[string]any{"domain": "snap.test"}),
		"empty":     inv("CreateDomain", map[string]any{}),
		"get":       inv("GetDomain", map[string]any{"domain": "snap.test"}),
		"list":      inv("ListDomains", nil),
		"update":    inv("UpdateDNSRecords", map[string]any{"domain": "snap.test", "overwrite": true, "zone": zone}),
		"records":   inv("GetDNSRecords", map[string]any{"domain": "snap.test"}),
		"delete":    inv("DeleteDNSRecords", map[string]any{"domain": "snap.test"}),
		"after_del": inv("GetDNSRecords", map[string]any{"domain": "snap.test"}),
		"missing":   inv("GetDomain", map[string]any{"domain": "nope.test"}),
	})
}
