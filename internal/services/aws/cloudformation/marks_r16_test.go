package cloudformation

import (
	"encoding/json"
	"fmt"
	"testing"
)

// Every intrinsic that builds a value from others answers the mark of what it
// is built on, combined in one order: an error (AWS::NoValue inside a value,
// or a Fn::Select of no element) before a value not known yet, and that
// before a value. A template whose output holds an error, at any depth, is
// refused before anything is made; one built on an id not known yet is made,
// with that id in it. The table puts each mark two deep in each pair of
// intrinsics, Fn::GetAZs among them since round 17. One pack serves the whole table; its cases run in turn, since
// the harness's random source is not shared across goroutines.
// Before, Fn::Join and Fn::Sub passed on no AWS::NoValue already inside a
// string, and an unknown hid an error, so marks reached providers, the stack
// record and answers.
func TestCloudFormationCombinesMarksInEveryIntrinsic(t *testing.T) {
	h := newStacks(t)
	wrap := map[string]func(any) any{
		"sub":    func(v any) any { return map[string]any{"Fn::Sub": []any{"x${V}", map[string]any{"V": v}}} },
		"join":   func(v any) any { return map[string]any{"Fn::Join": []any{"-", []any{"a", v}}} },
		"sep":    func(v any) any { return map[string]any{"Fn::Join": []any{v, []any{"a", "b"}}} },
		"split":  func(v any) any { return map[string]any{"Fn::Split": []any{",", v}} },
		"select": func(v any) any { return map[string]any{"Fn::Select": []any{0, []any{v}}} },
		"if":     func(v any) any { return map[string]any{"Fn::If": []any{"On", v, "z"}} },
		"getazs": func(v any) any { return map[string]any{"Fn::GetAZs": v} }, // round 17
	}
	marks := map[string]struct {
		v      any
		refuse bool
	}{
		"unknown":    {map[string]any{"Ref": "T"}, false},
		"unselected": {map[string]any{"Fn::Select": []any{9, []any{"a"}}}, true},
		"novalue":    {map[string]any{"Fn::Sub": "b${AWS::NoValue}"}, true},
	}
	n := 0
	for mn, m := range marks {
		for in, inner := range wrap {
			for on, outer := range wrap {
				n++
				name := fmt.Sprintf("m%d", n)
				body, _ := json.Marshal(map[string]any{"Conditions": map[string]any{"On": map[string]any{"Fn::Equals": []any{"x", "x"}}},
					"Resources": map[string]any{"T": map[string]any{"Type": "AWS::SNS::Topic"}},
					"Outputs":   map[string]any{"O": map[string]any{"Value": outer(inner(m.v))}}})
				_, err := h.invoke("CreateStack", map[string]any{"StackName": name, "TemplateBody": string(body)})
				if _, made := h.stored(name); m.refuse && (code(err) != "ValidationError" || made) {
					t.Errorf("%s in %s in %s: answered %v (stack recorded %v), want ValidationError before anything is made", mn, in, on, err, made)
				} else if !m.refuse && err != nil {
					t.Errorf("%s in %s in %s: answered %v, want the stack made", mn, in, on, err)
				}
			}
		}
	}
}
