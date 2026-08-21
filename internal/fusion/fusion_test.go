package fusion

import (
	"context"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
)

func TestFusePrecedence(t *testing.T) {
	a := []model.Service{{
		ID:         "aws.demo",
		Operations: []model.Operation{{Name: "Ping", Confidence: model.ConfDeclared, Source: model.SourceRef{Path: "a"}}},
		Shapes:     map[string]model.Shape{"In": {ID: "In"}},
	}}
	b := []model.Service{{
		ID:         "aws.demo",
		Operations: []model.Operation{{Name: "Ping", Confidence: model.ConfVerified, Source: model.SourceRef{Path: "b"}}, {Name: "Pong", Confidence: model.ConfDeclared}},
	}}
	out, _, err := Fuse(context.Background(), model.ProviderAWS, [][]model.Service{a, b})
	if err != nil {
		t.Fatal(err)
	}
	if len(out.Services) != 1 {
		t.Fatalf("%d services", len(out.Services))
	}
	var ping model.Operation
	for _, op := range out.Services[0].Operations {
		if op.Name == "Ping" {
			ping = op
		}
	}
	if ping.Confidence != model.ConfVerified {
		t.Fatalf("want verified, got %s", ping.Confidence)
	}
}
