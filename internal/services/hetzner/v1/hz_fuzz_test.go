package v1

import (
	"context"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spitest"
)

func FuzzCreateBody(f *testing.F) {
	f.Add("web", "cx22", "ssh-ed25519 AAAA")
	f.Add("", "cx22", "ssh-ed25519 AAAA")
	f.Add("web", "", "")
	f.Fuzz(func(t *testing.T, name, serverType, pub string) {
		p := New(spitest.Deps(t))
		ctx := context.Background()
		id := spi.Identity{Account: "000000000000", Region: "us-east-1"}
		_, _ = p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateServer", Input: map[string]any{"name": name, "server_type": serverType}})
		_, _ = p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateSSHKey", Input: map[string]any{"name": name, "public_key": pub}})
		_, _ = p.Invoke(ctx, &spi.Request{Identity: id, Operation: "ListServers", Input: map[string]any{}})
	})
}
