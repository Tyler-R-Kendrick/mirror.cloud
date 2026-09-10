package api

import (
	"context"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spitest"
)

func FuzzDNSRecords(f *testing.F) {
	f.Add("ex.test", "@", "A", "1.2.3.4")
	f.Add("", "@", "A", "1.2.3.4")
	f.Add("ex.test", "www", "CNAME", "ex.test.")
	f.Fuzz(func(t *testing.T, domain, name, typ, content string) {
		p := New(spitest.Deps(t))
		ctx := context.Background()
		id := spi.Identity{Account: "000000000000", Region: "us-east-1"}
		_, _ = p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateDomain", Input: map[string]any{"domain": domain}})
		zone := []any{map[string]any{"name": name, "type": typ, "ttl": 300, "records": []any{map[string]any{"content": content}}}}
		_, _ = p.Invoke(ctx, &spi.Request{Identity: id, Operation: "UpdateDNSRecords", Input: map[string]any{"domain": domain, "overwrite": true, "zone": zone}})
		_, _ = p.Invoke(ctx, &spi.Request{Identity: id, Operation: "GetDNSRecords", Input: map[string]any{"domain": domain}})
		_, _ = p.Invoke(ctx, &spi.Request{Identity: id, Operation: "DeleteDNSRecords", Input: map[string]any{"domain": domain}})
	})
}
