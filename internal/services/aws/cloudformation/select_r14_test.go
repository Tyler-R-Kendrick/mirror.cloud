package cloudformation

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

// resolve has three outcomes: a value, a Fn::Select of no element of a list
// it knows, or unknown, what reads a resource not made yet. A Fn::Select is
// refused before anything is made only when its list is known there and its
// index falls outside it; one over what is not made yet is checked when the
// resource is made, and the stack fails then and rolls back. Each list source:
// a literal, AWS::NoValue in a list and in a Fn::If, a Fn::Sub, a Fn::Split
// of a Ref, of a Fn::Sub of one and of a Fn::GetAtt, Fn::GetAZs, Fn::FindInMap and a
// CommaDelimitedList. Before, a list holding a ${AWS::Region} or an
// AWS::NoValue, and every list read through a Fn::GetAtt on the follow-up
// branch, read as unknown at every resolution, so an index out of it
// answered "" and the stack was made.
func TestCloudFormationSelectsByWhatItKnows(t *testing.T) {
	h := newStacks(t)
	ref := func(n string) any { return map[string]any{"Ref": n} }
	cases := []struct {
		name      string
		list      func(q string) any
		in        int
		want      string
		out       int
		afterMade bool // the list is known only once the queue is made
	}{
		{"literal", func(string) any { return []any{"x", "y", "z"} }, 1, "y", 3, false},
		{"novalue", func(string) any { return []any{"a", ref("AWS::NoValue")} }, 0, "a", 1, false},
		{"if", func(string) any { return []any{"a", map[string]any{"Fn::If": []any{"Off", "b", ref("AWS::NoValue")}}} }, 0, "a", 1, false},
		{"sub", func(string) any {
			return map[string]any{"Fn::Split": []any{"-", map[string]any{"Fn::Sub": "a-${AWS::Region}"}}}
		}, 1, "us", 9, false},
		{"getazs", func(string) any { return map[string]any{"Fn::GetAZs": ""} }, 2, "us-east-1c", 3, false},
		{"findinmap", func(string) any { return map[string]any{"Fn::FindInMap": []any{"M", "k", "l"}} }, 1, "m1", 2, false},
		{"commadelimited", func(string) any { return ref("L") }, 2, "c", 3, false},
		{"split-ref", func(string) any { return map[string]any{"Fn::Split": []any{"/", ref("Q")}} }, 4, "", 9, true},
		{"sub-ref", func(string) any { return map[string]any{"Fn::Split": []any{"/", map[string]any{"Fn::Sub": "${Q}"}}} }, 4, "", 9, true},
		{"split-getatt", func(string) any {
			return map[string]any{"Fn::Split": []any{":", map[string]any{"Fn::GetAtt": []any{"Q", "Arn"}}}}
		}, 5, "", 40, true},
	}
	for _, c := range cases {
		for _, index := range []int{c.in, c.out} {
			queue := fmt.Sprintf("sel-%s-%d", c.name, index)
			body, _ := json.Marshal(map[string]any{
				"Parameters": map[string]any{"L": map[string]any{"Type": "CommaDelimitedList", "Default": "a, b ,c"}},
				"Conditions": map[string]any{"Off": map[string]any{"Fn::Equals": []any{"x", "y"}}},
				"Mappings":   map[string]any{"M": map[string]any{"k": map[string]any{"l": []any{"m0", "m1"}}}},
				"Resources":  map[string]any{"Q": map[string]any{"Type": "AWS::SQS::Queue", "Properties": map[string]any{"QueueName": queue}}},
				"Outputs":    map[string]any{"O": map[string]any{"Value": map[string]any{"Fn::Select": []any{index, c.list(queue)}}}}})
			_, err := h.invoke("CreateStack", map[string]any{"StackName": queue, "TemplateBody": string(body)})
			switch status, _, _ := h.status(queue); {
			case index == c.in:
				if want := map[bool]string{false: c.want, true: queue}[c.afterMade]; err != nil || h.outputs(queue)["O"] != want {
					t.Errorf("%s: index %d answered %v, the output %v, want %q", c.name, index, err, h.outputs(queue)["O"], want)
				}
			case code(err) != "ValidationError" || !strings.Contains(err.Error(), "Fn::Select cannot select nonexistent value"):
				t.Errorf("%s: index %d out of the list answered %v, want ValidationError", c.name, index, err)
			case !c.afterMade && status != "":
				t.Errorf("%s: the refusal before anything is made left the stack %s", c.name, status)
			case c.afterMade && (status != "ROLLBACK_COMPLETE" || len(h.resources(queue)) != 0):
				t.Errorf("%s: after the queue was made the stack is %s holding %v, want ROLLBACK_COMPLETE holding nothing", c.name, status, h.resources(queue))
			}
		}
	}
}

// "CloudFormation creates the resource only when the condition evaluates to
// true" (Template Reference, Condition attribute), so what a resource its
// condition leaves out would hold is not checked: its Fn::Select of no
// element refuses nothing. Before, sound checked it, and the stack was
// refused for a resource it would never make.
func TestCloudFormationChecksOnlyWhatItMakes(t *testing.T) {
	h := newStacks(t)
	body, _ := json.Marshal(map[string]any{
		"Conditions": map[string]any{"Off": map[string]any{"Fn::Equals": []any{"x", "y"}}},
		"Resources": map[string]any{"T": map[string]any{"Type": "AWS::SNS::Topic"},
			"R": map[string]any{"Type": "AWS::SSM::Parameter", "Condition": "Off", "Properties": map[string]any{"Type": "String", "Value": map[string]any{"Fn::Select": []any{5, []any{"a"}}}}}}})
	if _, err := h.invoke("CreateStack", map[string]any{"StackName": "unmade", "TemplateBody": string(body)}); err != nil {
		t.Errorf("CreateStack with the Fn::Select in a resource its condition leaves out answered %v", err)
	}
}

// An output's OutputValue is a String (the model's Output shape): a Ref to a
// list, a CommaDelimitedList or AWS::NotificationARNs, answers its members
// comma-joined, as a CommaDelimitedList is written, and so does a ${Name} of
// one in a Fn::Sub, as a Fn::Join of it does. Every DescribeStacks answer the
// tests read is held to the model's shapes (conforms). Before, the output
// held the list, which no String shape can, and the Fn::Sub the parameter's
// text as given, spaces and all.
func TestCloudFormationAnOutputIsText(t *testing.T) {
	h := newStacks(t)
	ref := func(n string) any { return map[string]any{"Ref": n} }
	body, _ := json.Marshal(map[string]any{"Parameters": map[string]any{"L": map[string]any{"Type": "CommaDelimitedList", "Default": "a, b ,c"}},
		"Resources": map[string]any{"T": map[string]any{"Type": "AWS::SNS::Topic"}},
		"Outputs": map[string]any{"L": map[string]any{"Value": ref("L")}, "N": map[string]any{"Value": ref("AWS::NotificationARNs")},
			"S": map[string]any{"Value": map[string]any{"Fn::Sub": "<${L}>"}}, "J": map[string]any{"Value": map[string]any{"Fn::Join": []any{",", ref("L")}}}}})
	if _, err := h.invoke("CreateStack", map[string]any{"StackName": "texts", "TemplateBody": string(body), "NotificationARNs": []any{"arn:a", "arn:b"}}); err != nil {
		t.Fatal(err)
	}
	if got := h.outputs("texts"); got["L"] != "a,b,c" || got["N"] != "arn:a,arn:b" || got["S"] != "<a,b,c>" || got["J"] != "a,b,c" {
		t.Errorf("outputs = %v, want L a,b,c, N arn:a,arn:b, S <a,b,c> and J a,b,c", got)
	}
}

// sound checks only what the stack makes: an output whose Condition is false,
// and a resource skipped because it names one its condition leaves out ("any
// resources that depend on that resource aren't created either", Template
// Reference, Condition attribute), may hold a Fn::Select of no element.
// Before, both were refused: "Fn::Select cannot select nonexistent value".
func TestCloudFormationChecksOnlyWhatItMakesOrAnswers(t *testing.T) {
	h := newStacks(t)
	sel := map[string]any{"Fn::Select": []any{9, []any{"a"}}}
	off := map[string]any{"Off": map[string]any{"Fn::Equals": []any{"x", "y"}}}
	for name, tpl := range map[string]map[string]any{
		"output-off": {"Conditions": off, "Resources": map[string]any{"T": map[string]any{"Type": "AWS::SNS::Topic"}},
			"Outputs": map[string]any{"O": map[string]any{"Condition": "Off", "Value": sel}}},
		"skipped-by-dep": {"Conditions": off, "Resources": map[string]any{"T": map[string]any{"Type": "AWS::SNS::Topic"},
			"R": map[string]any{"Type": "AWS::SNS::Topic", "Condition": "Off"},
			"S": map[string]any{"Type": "AWS::SSM::Parameter", "Properties": map[string]any{"Type": "String", "Name": map[string]any{"Ref": "R"}, "Value": sel}}}},
	} {
		if err := rvMake(t, h, name, tpl); err != nil {
			t.Errorf("%s answered %v, want the stack made", name, err)
		}
	}
}

func rvMake(t *testing.T, h *stacks, name string, tpl map[string]any) error {
	t.Helper()
	body, _ := json.Marshal(tpl)
	_, err := h.invoke("CreateStack", map[string]any{"StackName": name, "TemplateBody": string(body)})
	return err
}

// A template's own strings are values whatever characters they hold: the
// noncharacters U+FFFF and U+FFFE, which the engine once used to mark an id
// not yet known and AWS::NoValue, are text like any other. Before, a list
// holding "x<U+FFFF>" was read as not yet known, so a Fn::Select of no element
// of it was made, and a parameter of "<U+FFFE>" was AWS::NoValue, so the
// property it gave was removed.
func TestCloudFormationATemplateStringIsNeverAMark(t *testing.T) {
	h := newStacks(t)
	sel := func(i int, l any) any { return map[string]any{"Fn::Select": []any{i, l}} }
	topic := map[string]any{"T": map[string]any{"Type": "AWS::SNS::Topic"}}
	if err := rvMake(t, h, "ffff-in", map[string]any{"Resources": topic, "Outputs": map[string]any{"O": map[string]any{"Value": sel(0, []any{"x\uffff"})}}}); err != nil || h.outputs("ffff-in")["O"] != "x\uffff" {
		t.Errorf("a Fn::Select of a string holding U+FFFF answered %v, the output %q", err, h.outputs("ffff-in")["O"])
	}
	for name, tpl := range map[string]map[string]any{
		"ffff-out": {"Resources": topic, "Outputs": map[string]any{"O": map[string]any{"Value": sel(5, []any{"x\uffff"})}}},
		"ffff-param": {"Parameters": map[string]any{"P": map[string]any{"Type": "String", "Default": "a\uffff"}}, "Resources": topic,
			"Outputs": map[string]any{"O": map[string]any{"Value": sel(7, []any{map[string]any{"Ref": "P"}})}}},
	} {
		if err := rvMake(t, h, name, tpl); code(err) != "ValidationError" {
			t.Errorf("%s: a Fn::Select of no element answered %v, want ValidationError", name, err)
		}
	}
	if err := rvMake(t, h, "fffe", map[string]any{"Parameters": map[string]any{"P": map[string]any{"Type": "String", "Default": "\ufffe"}},
		"Resources": map[string]any{"S": map[string]any{"Type": "AWS::SSM::Parameter", "Properties": map[string]any{"Name": "fffe", "Type": "String", "Value": map[string]any{"Ref": "P"}}}}}); err != nil {
		t.Errorf("a parameter of U+FFFE answered %v", err)
	} else if got, err := h.service("aws.ssm", "GetParameter", map[string]any{"Name": "fffe"}); err != nil || asMap(got["Parameter"])["Value"] != "\ufffe" {
		t.Errorf("the parameter is %v (%v), want U+FFFE", got, err)
	}
}
