package api

import (
	"context"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spitest"
)

func FuzzKVValue(f *testing.F) {
	f.Add("k", "v")
	f.Add("", "v")
	f.Add("k", "")
	f.Add("a/b", "x")
	f.Fuzz(func(t *testing.T, key, value string) {
		p := New(spitest.Deps(t))
		ctx := context.Background()
		id := spi.Identity{Account: "000000000000", Region: "us-east-1"}
		ns, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateNamespace", Input: map[string]any{"account_id": "acct1", "title": "fuzz"}})
		if err != nil {
			return
		}
		nid := ns.Output["id"]
		_, _ = p.Invoke(ctx, &spi.Request{Identity: id, Operation: "PutValue", Input: map[string]any{"account_id": "acct1", "namespace_id": nid, "key": key, "value": value}})
		_, _ = p.Invoke(ctx, &spi.Request{Identity: id, Operation: "GetValue", Input: map[string]any{"account_id": "acct1", "namespace_id": nid, "key": key}})
		_, _ = p.Invoke(ctx, &spi.Request{Identity: id, Operation: "DeleteValue", Input: map[string]any{"account_id": "acct1", "namespace_id": nid, "key": key}})
	})
}
