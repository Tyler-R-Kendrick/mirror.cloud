package api

import (
	"context"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/golden"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spitest"
)

func TestCloudflareKVCharacterization(t *testing.T) {
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
	create := inv("CreateNamespace", map[string]any{"account_id": "acct1", "title": "snap"}).(map[string]any)
	nid := create["id"]
	golden.AssertJSON(t, map[string]any{
		"create":     create,
		"duplicate":  inv("CreateNamespace", map[string]any{"account_id": "acct1", "title": "snap"}),
		"empty":      inv("CreateNamespace", map[string]any{"account_id": "acct1"}),
		"get":        inv("GetNamespace", map[string]any{"account_id": "acct1", "namespace_id": nid}),
		"list":       inv("ListNamespaces", map[string]any{"account_id": "acct1"}),
		"put":        inv("PutValue", map[string]any{"account_id": "acct1", "namespace_id": nid, "key": "k", "value": "v"}),
		"get_value":  inv("GetValue", map[string]any{"account_id": "acct1", "namespace_id": nid, "key": "k"}),
		"del":        inv("DeleteValue", map[string]any{"account_id": "acct1", "namespace_id": nid, "key": "k"}),
		"after_del":  inv("GetValue", map[string]any{"account_id": "acct1", "namespace_id": nid, "key": "k"}),
		"missing_ns": inv("GetNamespace", map[string]any{"account_id": "acct1", "namespace_id": "nope"}),
	})
}
