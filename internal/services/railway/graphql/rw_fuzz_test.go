package graphql

import (
	"context"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spitest"
)

func FuzzCreateBody(f *testing.F) {
	f.Add("web")
	f.Add("")
	f.Add("api")
	f.Fuzz(func(t *testing.T, name string) {
		p := New(spitest.Deps(t))
		ctx := context.Background()
		id := spi.Identity{Account: "000000000000", Region: "us-east-1"}
		created, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "projectCreate", Input: map[string]any{"name": name}})
		if err == nil && created != nil {
			rec, _ := created.Output["projectCreate"].(map[string]any)
			pid := str(rec["id"])
			_, _ = p.Invoke(ctx, &spi.Request{Identity: id, Operation: "serviceCreate", Input: map[string]any{"name": name, "projectId": pid}})
			_, _ = p.Invoke(ctx, &spi.Request{Identity: id, Operation: "project", Input: map[string]any{"id": pid}})
			_, _ = p.Invoke(ctx, &spi.Request{Identity: id, Operation: "projects", Input: map[string]any{}})
		}
		_ = FieldName("mutation { projectCreate(input:{name:\"" + name + "\"}) { id } }")
	})
}
