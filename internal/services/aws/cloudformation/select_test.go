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

// A Fn::Select's list may hold what only exists once a resource is made: a
// queue's URL (its Ref), split at "/", or its ARN, split at ":". Its index is
// checked against that list once it is made, not before, when a Ref to the
// queue is still its logical id; an index out of that list is the stack's
// ValidationError then, and the stack rolls back. A CommaDelimitedList
// parameter is a list: "a Ref would result in ["test","dev","prod"]", "each
// member string is space trimmed" (User Guide, Parameters), so a Fn::Select
// or a Fn::Join of it reads its members. Before, all three were refused
// before anything was made: "Fn::Select cannot select nonexistent value".
func TestCloudFormationSelectsFromWhatIsMade(t *testing.T) {
	h := newStacks(t)
	sel := func(i int, list any) any { return map[string]any{"Fn::Select": []any{i, list}} }
	split := func(d string, s any) any { return map[string]any{"Fn::Split": []any{d, s}} }
	template := func(index int, queue string) string {
		body, _ := json.Marshal(map[string]any{
			"Parameters": map[string]any{"L": map[string]any{"Type": "CommaDelimitedList", "Default": "a, b ,c"}},
			"Resources": map[string]any{"Q": map[string]any{"Type": "AWS::SQS::Queue", "Properties": map[string]any{"QueueName": queue}},
				"P": map[string]any{"Type": "AWS::SSM::Parameter", "Properties": map[string]any{"Name": queue + "-p", "Type": "String", "Value": sel(index, split("/", map[string]any{"Ref": "Q"}))}}},
			"Outputs": map[string]any{"Name": map[string]any{"Value": sel(5, split(":", map[string]any{"Fn::GetAtt": []any{"Q", "Arn"}}))},
				"Member": map[string]any{"Value": sel(1, map[string]any{"Ref": "L"})},
				"Joined": map[string]any{"Value": map[string]any{"Fn::Join": []any{"+", map[string]any{"Ref": "L"}}}}}})
		return string(body)
	}
	if _, err := h.invoke("CreateStack", map[string]any{"StackName": "made", "TemplateBody": template(4, "selected-q")}); err != nil {
		t.Fatal(err)
	}
	if got := h.outputs("made"); got["Name"] != "selected-q" || got["Member"] != "b" || got["Joined"] != "a+b+c" {
		t.Errorf("outputs = %v, want Name selected-q, Member b, Joined a+b+c", got)
	}
	got, err := h.service("aws.ssm", "GetParameter", map[string]any{"Name": "selected-q-p"})
	if v := str(asMap(got["Parameter"])["Value"]); err != nil || v != "selected-q" {
		t.Errorf("the parameter holds %q (%v), want the queue's name, selected-q", v, err)
	}
	if _, err := h.invoke("DeleteStack", map[string]any{"StackName": "made"}); err != nil {
		t.Fatal(err)
	}
	if _, err := h.invoke("CreateStack", map[string]any{"StackName": "beyond", "TemplateBody": template(9, "beyond-q")}); code(err) != "ValidationError" || !strings.Contains(err.Error(), "Fn::Select cannot select nonexistent value") {
		t.Errorf("CreateStack selecting index 9 of the queue's URL answered %v, want ValidationError", err)
	}
	if status, _, _ := h.status("beyond"); status != "ROLLBACK_COMPLETE" || len(h.resources("beyond")) != 0 {
		t.Errorf("the refused stack is %s holding %v, want ROLLBACK_COMPLETE holding nothing", status, h.resources("beyond"))
	}
	if _, err := h.service("aws.sqs", "GetQueueUrl", map[string]any{"QueueName": "beyond-q"}); err == nil {
		t.Errorf("the rolled-back stack left its queue")
	}
}
