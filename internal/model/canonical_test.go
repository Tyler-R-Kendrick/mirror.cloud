package model_test

import (
	"reflect"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
)

func canonicalService() *model.Service {
	return &model.Service{
		ID: "aws.shaped",
		Shapes: map[string]model.Shape{
			"String": {Kind: model.KindString},
			"Result": {Kind: model.KindStructure, Members: map[string]model.Member{
				"Vpcs":  {Shape: "VpcList", Binding: model.MemberBinding{Name: "vpcSet"}},
				"Plain": {Shape: "String"},
				"Tags":  {Shape: "TagMap", Binding: model.MemberBinding{Name: "tagSet"}},
				// Two members claiming one wire name. Which one a `both` key
				// stands for is not answerable from the model.
				"Left":  {Shape: "String", Binding: model.MemberBinding{Name: "both"}},
				"Right": {Shape: "String", Binding: model.MemberBinding{Name: "both"}},
			}},
			"VpcList": {Kind: model.KindList, Member: "Vpc"},
			// A map of records, so the walk has to carry the value's shape
			// through the entry: canonicalizing the outside and leaving the
			// members within in wire names is the failure this catches.
			"TagMap": {Kind: model.KindMap, Key: "String", Member: "Vpc"},
			"Vpc": {Kind: model.KindStructure, Members: map[string]model.Member{
				"VpcId": {Shape: "String", Binding: model.MemberBinding{Name: "vpcId"}},
				"State": {Shape: "String", Binding: model.MemberBinding{Name: "state"}},
			}},
		},
	}
}

func TestCanonicalRewritesWireNamesToDeclaredOnes(t *testing.T) {
	got := model.Canonical(canonicalService(), "Result", map[string]any{
		"vpcSet": []any{map[string]any{"vpcId": "vpc-1", "state": "available"}},
		"Plain":  "p",
		"tagSet": map[string]any{"vpcId": map[string]any{"vpcId": "vpc-2"}},
		"Extra":  "kept",
		"both":   "ambiguous",
	})
	want := map[string]any{
		// Renamed, and the rename follows into the list's element shape.
		"Vpcs":  []any{map[string]any{"VpcId": "vpc-1", "State": "available"}},
		"Plain": "p",
		// A map's keys are data -- this one is deliberately spelled like a
		// member name -- and its values are records, whose members are not.
		"Tags":  map[string]any{"vpcId": map[string]any{"VpcId": "vpc-2"}},
		"Extra": "kept",
		// Two members claim `both`; picking one would invent a fact.
		"both": "ambiguous",
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("canonical form is\n%#v\nwant\n%#v", got, want)
	}
}

func TestCanonicalLeavesDeclaredNamesAndUnknownShapesAlone(t *testing.T) {
	svc := canonicalService()
	already := map[string]any{"Vpcs": []any{map[string]any{"VpcId": "vpc-1"}}, "Plain": "p"}
	if got := model.Canonical(svc, "Result", already); !reflect.DeepEqual(got, already) {
		t.Errorf("a value already in declared names changed: %#v", got)
	}
	// No shape to walk: an operation with no output, or a member the model
	// does not carry. Nothing is renamed and nothing is dropped.
	loose := map[string]any{"vpcSet": []any{"a"}}
	if got := model.Canonical(svc, "Absent", loose); !reflect.DeepEqual(got, loose) {
		t.Errorf("an undescribed value changed: %#v", got)
	}
	if got := model.Canonical(nil, "Result", loose); !reflect.DeepEqual(got, loose) {
		t.Errorf("a nil service changed the value: %#v", got)
	}
	if got := model.Canonical(svc, "String", "scalar"); got != "scalar" {
		t.Errorf("a scalar changed: %#v", got)
	}
}
