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
