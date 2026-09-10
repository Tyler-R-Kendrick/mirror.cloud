package v2

import (
	"context"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spitest"
)

func FuzzCreateBody(f *testing.F) {
	f.Add("web", "nyc3")
	f.Add("", "nyc3")
	f.Add("ex.test", "")
	f.Fuzz(func(t *testing.T, name, region string) {
		p := New(spitest.Deps(t))
		ctx := context.Background()
		id := spi.Identity{Account: "000000000000", Region: "us-east-1"}
		_, _ = p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateDroplet", Input: map[string]any{"name": name, "region": region}})
		_, _ = p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateDomain", Input: map[string]any{"name": name}})
		_, _ = p.Invoke(ctx, &spi.Request{Identity: id, Operation: "GetDomain", Input: map[string]any{"name": name}})
		_, _ = p.Invoke(ctx, &spi.Request{Identity: id, Operation: "ListDroplets", Input: map[string]any{}})
	})
}
