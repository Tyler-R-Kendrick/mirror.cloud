package specdiff

import (
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/golden"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
)

func TestDiffSnapshot(t *testing.T) {
	a := model.Bundle{Services: []model.Service{{
		ID:         "aws.demo",
		Operations: []model.Operation{{Name: "Ping", Input: "In"}},
		Shapes:     map[string]model.Shape{"In": {ID: "In", Kind: model.KindStructure, Members: map[string]model.Member{"N": {Shape: "S", Required: true}}}},
	}}}
	b := model.Bundle{Services: []model.Service{{
		ID:         "aws.demo",
		Operations: []model.Operation{{Name: "Ping", Input: "In2"}, {Name: "Pong"}},
		Shapes: map[string]model.Shape{
			"In":  {ID: "In", Kind: model.KindStructure, Members: map[string]model.Member{"N": {Shape: "S"}}},
			"In2": {ID: "In2", Kind: model.KindStructure},
		},
	}}}
	golden.AssertJSON(t, Diff(a, b))
}
