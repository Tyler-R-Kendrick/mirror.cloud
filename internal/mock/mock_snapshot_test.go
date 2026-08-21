package mock

import (
	"context"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/golden"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spitest"
)

func TestSynthesizeSnapshot(t *testing.T) {
	svc := &model.Service{
		ID: "aws.demo",
		Operations: []model.Operation{
			{Name: "CreateThing", Input: "In", Output: "Out"},
			{Name: "GetThing", Input: "In", Output: "Out"},
		},
		Shapes: map[string]model.Shape{
			"In":  {ID: "In", Kind: model.KindStructure, Members: map[string]model.Member{"Name": {Shape: "S", Required: true}}},
			"Out": {ID: "Out", Kind: model.KindStructure, Members: map[string]model.Member{"Name": {Shape: "S"}, "Id": {Shape: "S"}}},
			"S":   {ID: "S", Kind: model.KindString},
		},
	}
	p := New(svc, spitest.Deps(t), false)
	ctx := context.Background()
	in := map[string]any{"Name": "n"}
	a, err := p.Invoke(ctx, &spi.Request{ServiceID: svc.ID, Operation: "CreateThing", Input: in, Identity: spi.Identity{Account: "a", Region: "r"}})
	if err != nil {
		t.Fatal(err)
	}
	b, err := p.Invoke(ctx, &spi.Request{ServiceID: svc.ID, Operation: "CreateThing", Input: in, Identity: spi.Identity{Account: "a", Region: "r"}})
	if err != nil {
		t.Fatal(err)
	}
	golden.AssertJSON(t, map[string]any{"first": a.Output, "second": b.Output})
}
