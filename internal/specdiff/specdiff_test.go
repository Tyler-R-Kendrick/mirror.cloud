package specdiff

import (
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
)

func TestDiff(t *testing.T) {
	a := model.Bundle{Services: []model.Service{{
		ID:         "aws.demo",
		Operations: []model.Operation{{Name: "Ping", Input: "In"}},
		Shapes:     map[string]model.Shape{"In": {ID: "In", Kind: model.KindStructure, Members: map[string]model.Member{"N": {Shape: "S", Required: true}}}},
	}}}
	b := model.Bundle{Services: []model.Service{{
		ID:         "aws.demo",
		Operations: []model.Operation{{Name: "Ping", Input: "In2"}, {Name: "Pong"}},
		Shapes: map[string]model.Shape{
			"In":  {ID: "In", Kind: model.KindStructure, Members: map[string]model.Member{"N": {Shape: "S", Required: false}}},
			"In2": {ID: "In2", Kind: model.KindStructure},
		},
	}}}
	r := Diff(a, b)
	if len(r.AddedOps) != 1 || r.AddedOps[0] != "aws.demo.Pong" {
		t.Fatalf("added %v", r.AddedOps)
	}
	if len(r.ChangedOps) != 1 {
		t.Fatalf("changed %v", r.ChangedOps)
	}
	if len(r.ChangedShapes) != 1 {
		t.Fatalf("shapes %v", r.ChangedShapes)
	}
	if r.String() == "" || r.JSON() == "" {
		t.Fatal("empty render")
	}
}
