package cfntemplate

import (
	"reflect"
	"testing"
)

func TestParseYAMLRefGetAtt(t *testing.T) {
	m, err := parseYAML("Resources:\n  Q:\n    Type: AWS::SQS::Queue\n    Properties:\n      QueueName: !Ref Name\n")
	if err != nil {
		t.Fatal(err)
	}
	res := m["Resources"].(map[string]any)
	q := res["Q"].(map[string]any)
	props := q["Properties"].(map[string]any)
	ref := props["QueueName"].(map[string]any)
	if Str(ref["Ref"]) != "Name" {
		t.Fatalf("%v", m)
	}
}

// A YAML template is YAML: flow collections, block scalars and the short-form
// intrinsics (!Ref, !GetAtt, !Sub, and !Name for any other Fn::Name) all read
// as their JSON-template equivalents. The hand-written subset this replaced
// read a flow sequence as a string and refused a block scalar.
func TestParseYAMLReadsYAML(t *testing.T) {
	m, err := parseYAML(`Resources:
  Q:
    Type: AWS::SQS::Queue
    Properties:
      Tags: [{Key: env, Value: dev}, {Key: team, Value: "a: b"}]
      Policy: |
        line one
        line two
      Arn: !GetAtt [Other, Arn]
      Joined: !Join ["-", [a, !Ref B]]
      Port: 8080
      Ratio: 0.5
      On: true
`)
	if err != nil {
		t.Fatal(err)
	}
	props := m["Resources"].(map[string]any)["Q"].(map[string]any)["Properties"].(map[string]any)
	want := map[string]any{
		"Tags":   []any{map[string]any{"Key": "env", "Value": "dev"}, map[string]any{"Key": "team", "Value": "a: b"}},
		"Policy": "line one\nline two\n",
		"Arn":    map[string]any{"Fn::GetAtt": []any{"Other", "Arn"}},
		"Joined": map[string]any{"Fn::Join": []any{"-", []any{"a", map[string]any{"Ref": "B"}}}},
		"Port":   8080, "Ratio": 0.5, "On": true,
	}
	if !reflect.DeepEqual(props, want) {
		t.Fatalf("parsed %#v\nwant   %#v", props, want)
	}
}

// An Integer may be negative: "-5" is -5, as a Number parameter's text can
// be, and "-", "+5", "- 5" and "5-" are still none. Before, "-5" was refused.
func TestCloudFormationReadsANegativeInteger(t *testing.T) {
	for text, want := range map[string]any{"-5": -5, "016": 16, "-": nil, "+5": nil, "- 5": nil, "5-": nil} {
		got, ok := Coerce(text, "Integer")
		if want == nil && ok || want != nil && (!ok || got != want) {
			t.Errorf("Coerce(%q, Integer) is %v, %t, want %v", text, got, ok, want)
		}
	}
}

// AWS::NotificationARNs is a list, as JSON: [] with none given.
func TestPseudoNotificationARNsWithNoneGiven(t *testing.T) {
	if got := (&Reader{}).pseudo("s", "id")["AWS::NotificationARNs"]; got != "[]" {
		t.Errorf("AWS::NotificationARNs with none given = %q, want []", got)
	}
}

// Same reads two scalars by their text and everything else as it is: a scalar inside a list or an object
// keeps its type, a string is never what it reads as, and a type no template holds is compared by its JSON.
func TestSameComparesScalarsByTextAndTheRestExactly(t *testing.T) {
	for _, c := range []struct {
		a, b any
		want bool
	}{
		{nil, nil, true}, {"null", nil, false}, {"", nil, false}, {"", "", true}, {0.0, nil, false}, {false, nil, false}, {false, "", false},
		{3.0, "3", true}, {3, 3.0, true}, {3.0, "3.0", false}, {true, "true", true}, {"True", true, false}, {int64(9007199254740993), "9007199254740993", true},
		{"[]", []any{}, false}, {"{}", map[string]any{}, false}, {"[1,2]", []any{1.0, 2.0}, false}, {[]any{"1"}, "1", false}, {"a,b", []any{"a", "b"}, false},
		{[]any{3.0}, []any{"3"}, false}, {[]any{true}, []any{"true"}, false}, {[]any{3.0}, []any{3}, true}, {[]any{1.0, 2.0}, []any{2.0, 1.0}, false},
		{map[string]any{"a": 1.0, "b": []any{"x"}}, map[string]any{"b": []any{"x"}, "a": 1.0}, true}, {map[string]any{"a": 1.0}, map[string]any{"a": "1"}, false},
		{map[string]any{"a": nil}, map[string]any{}, false}, {[]any{nil}, []any{""}, false}, {[]any{}, nil, false},
		{[]string{"a"}, []string{"b"}, false}, {[]string{"a"}, []string{"a"}, true}, {map[string]string{"a": "1"}, map[string]string{"a": "2"}, false},
		{int32(1), int32(2), false}, {int32(1), int32(1), true}, {float32(1), float32(2), false},
	} {
		if got := Same(c.a, c.b); got != c.want || Same(c.b, c.a) != c.want {
			t.Errorf("Same(%#v, %#v) is %v, and %v the other way; want %v", c.a, c.b, got, Same(c.b, c.a), c.want)
		}
	}
}
