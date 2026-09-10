package machines

import (
	"context"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spitest"
)

func FuzzCreateBody(f *testing.F) {
	f.Add("web", "nginx")
	f.Add("", "nginx")
	f.Add("web", "")
	f.Fuzz(func(t *testing.T, name, image string) {
		p := New(spitest.Deps(t))
		ctx := context.Background()
		id := spi.Identity{Account: "000000000000", Region: "us-east-1"}
		_, _ = p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateApp", Input: map[string]any{"app_name": name}})
		_, _ = p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateMachine", Input: map[string]any{"app_name": name, "config": map[string]any{"image": image}}})
		_, _ = p.Invoke(ctx, &spi.Request{Identity: id, Operation: "ListApps", Input: map[string]any{}})
	})
}
