package cloudformation

import (
	"encoding/json"
	"strings"
	"testing"
)

// Fn::Select "returns a single object from a list of objects by index", its
// index a literal or a Ref, its list a literal, a Fn::Split's, a
// Fn::FindInMap's or a Fn::GetAZs'; Fn::Split returns "A list of string
// values", "a||c|" by "|" being ["a", "", "c", ""] (Template Reference). An
// index out of the list's bounds is the stack's error, not a template's
// (ValidateTemplate takes it). Before, neither function was evaluated, and a
// property was the function's map.
func TestCloudFormationSelectsAndSplits(t *testing.T) {
	h := newStacks(t)
	sel := func(i, list any) any { return map[string]any{"Fn::Select": []any{i, list}} }
	split := func(d, s any) any { return map[string]any{"Fn::Split": []any{d, s}} }
	template := func(outputs map[string]any) string {
		body, _ := json.Marshal(map[string]any{"Parameters": map[string]any{"Idx": map[string]any{"Type": "Number", "Default": "2"}},
			"Mappings":  map[string]any{"M": map[string]any{"k": map[string]any{"l": []any{"m0", "m1"}}}},
			"Resources": map[string]any{"T": map[string]any{"Type": "AWS::SNS::Topic"}}, "Outputs": outputs})
		return string(body)
	}
	value := func(v any) map[string]any { return map[string]any{"Value": v} }
	outs := map[string]any{
		"Literal": value(sel("1", []any{"apples", "grapes", "oranges"})),
		"Number":  value(sel(0, []any{"first", "second"})),
		"ByRef":   value(sel(map[string]any{"Ref": "Idx"}, split("|", "a|b|c"))),
		"Mapped":  value(sel(1, map[string]any{"Fn::FindInMap": []any{"M", "k", "l"}})),
		"Zone":    value(sel(1, map[string]any{"Fn::GetAZs": ""})),
		"Region":  value(sel(0, map[string]any{"Fn::GetAZs": map[string]any{"Ref": "AWS::Region"}})),
		"Empties": value(map[string]any{"Fn::Join": []any{",", split("|", "a||c|")}}),
	}
	if _, err := h.invoke("CreateStack", map[string]any{"StackName": "sel", "TemplateBody": template(outs)}); err != nil {
		t.Fatal(err)
	}
	want := map[string]any{"Literal": "grapes", "Number": "first", "ByRef": "c", "Mapped": "m1", "Zone": "us-east-1b", "Region": "us-east-1a", "Empties": "a,,c,"}
	if got := h.outputs("sel"); len(got) != len(want) {
		t.Errorf("outputs = %v, want %v", got, want)
	} else {
		for k, v := range want {
			if got[k] != v {
				t.Errorf("output %s = %v, want %v", k, got[k], v)
			}
		}
	}
	for _, i := range []any{3, -1, "x"} {
		body := template(map[string]any{"O": value(sel(i, []any{"a", "b", "c"}))})
		if _, err := h.invoke("ValidateTemplate", map[string]any{"TemplateBody": body}); err != nil {
			t.Errorf("ValidateTemplate of Fn::Select %v of three answered %v", i, err)
		}
		if _, err := h.invoke("CreateStack", map[string]any{"StackName": "unsel", "TemplateBody": body}); code(err) != "ValidationError" || !strings.Contains(err.Error(), "Fn::Select cannot select nonexistent value") {
			t.Errorf("CreateStack of Fn::Select %v of three answered %v, want ValidationError", i, err)
		}
		if record, left := h.stored("unsel"); left {
			t.Errorf("the refused Fn::Select %v left %v", i, record)
		}
	}
}
