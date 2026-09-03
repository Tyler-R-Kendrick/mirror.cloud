package dynamodb

import (
	"context"
	"encoding/hex"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spitest"
)

func FuzzTableLifecycle(f *testing.F) {
	f.Add([]byte("table"))
	f.Fuzz(func(t *testing.T, raw []byte) {
		p := New(spitest.Deps(t))
		ctx := context.Background()
		id := spi.Identity{Account: "000000000000", Region: "us-east-1"}
		name := hex.EncodeToString(raw)
		call := func(operation string) error {
			_, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: operation, Input: map[string]any{"TableName": name}})
			return err
		}
		created, duplicate := call("CreateTable"), call("CreateTable")
		deleted, missing := call("DeleteTable"), call("DeleteTable")
		if created != nil || duplicate == nil || deleted != nil || missing == nil {
			t.Fatal("table lifecycle was not create/conflict/delete/missing")
		}
	})
}
