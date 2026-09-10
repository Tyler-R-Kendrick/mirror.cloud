package api

import (
	"context"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spitest"
)

func FuzzKvCommand(f *testing.F) {
	f.Add("GET", "k", "")
	f.Add("SET", "k", "v")
	f.Add("DEL", "k", "")
	f.Add("PING", "", "")
	f.Fuzz(func(t *testing.T, op, key, value string) {
		p := New(spitest.Deps(t))
		cmd := []any{op, key}
		if op == "SET" {
			cmd = []any{op, key, value}
		}
		_, _ = p.Invoke(context.Background(), &spi.Request{
			Identity:  spi.Identity{Account: "000000000000", Region: "us-east-1"},
			Operation: "KvCommand",
			Input:     map[string]any{"_redis": cmd},
		})
	})
}
