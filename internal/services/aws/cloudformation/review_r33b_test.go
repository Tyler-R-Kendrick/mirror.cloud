package cloudformation

import (
	"fmt"
	"strings"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

// Compiles on origin/main too: a scalar inside a Json-typed property (an event pattern) changes type.
func TestCloudFormationReview6PatternScalarTypeChangeAtDepth(t *testing.T) {
	h := newStacks(t)
	rule := func(pattern map[string]any) map[string]any {
		return map[string]any{"E": map[string]any{"Type": "AWS::Events::Rule", "Properties": map[string]any{"EventPattern": pattern}}}
	}
	if err := rvMake(t, h, "ep", map[string]any{"Resources": rule(map[string]any{"source": []any{"x"}, "detail": map[string]any{"n": []any{3.0}, "f": []any{true}}})}); err != nil {
		t.Fatal(err)
	}
	name := spi.LastSlash(rv31IDs(h, "ep")["E"])
	pat := func() string {
		return fmt.Sprint(rv31Get(h, "aws.events", "DescribeRule", map[string]any{"Name": name})["EventPattern"])
	}
	before := pat()
	err := rv28Update(h, "ep", rule(map[string]any{"source": []any{"x"}, "detail": map[string]any{"n": []any{"3"}, "f": []any{"true"}}}))
	after := pat()
	st, _, _ := h.status("ep")
	t.Logf("update: %v status=%s before=%s after=%s", err, st, before, after)
	if !strings.Contains(after, `"3"`) || !strings.Contains(after, `"true"`) {
		t.Errorf("the rule's pattern is still %s after the template changed 3 to \"3\" and true to \"true\"", after)
	}
}

// Compiles on main too: a role's policy condition value.
func TestCloudFormationReview6NestedNumberVsParamIsNoUpdate(t *testing.T) {
	h := newStacks(t)
	tpl := func(rcu any) string {
		return rv19Body(map[string]any{"Parameters": map[string]any{"N": map[string]any{"Type": "Number", "Default": 5}, "B": map[string]any{"Type": "String", "Default": "true"}},
			"Resources": map[string]any{
				"T": rv29Table(map[string]any{"ProvisionedThroughput": map[string]any{"ReadCapacityUnits": rcu, "WriteCapacityUnits": 5}}),
			}})
	}
	if _, err := h.invoke("CreateStack", map[string]any{"StackName": "np", "TemplateBody": tpl(5)}); err != nil {
		t.Fatal(err)
	}
	_, err := h.invoke("UpdateStack", map[string]any{"StackName": "np", "TemplateBody": tpl(map[string]any{"Ref": "N"})})
	t.Logf("literal 5 -> Ref Number 5: err=%v events=%v", err, h.events("np", "T"))
}

func TestCloudFormationReview6ReplaceByBooleanSpelling(t *testing.T) {
	h := newStacks(t)
	tpl := func(v any) string {
		return rv19Body(map[string]any{"Parameters": map[string]any{"B": map[string]any{"Type": "String", "Default": "true"}},
			"Resources": map[string]any{"Q": map[string]any{"Type": "AWS::SQS::Queue", "Properties": map[string]any{"FifoQueue": v}},
				"K": map[string]any{"Type": "AWS::KMS::Key", "Properties": map[string]any{"MultiRegion": v, "Description": "d"}}}})
	}
	if _, err := h.invoke("CreateStack", map[string]any{"StackName": "rb", "TemplateBody": tpl(true)}); err != nil {
		t.Fatal(err)
	}
	ids := rv31IDs(h, "rb")
	for _, v := range []any{map[string]any{"Ref": "B"}, "true", true} {
		_, err := h.invoke("UpdateStack", map[string]any{"StackName": "rb", "TemplateBody": tpl(v)})
		now := rv31IDs(h, "rb")
		t.Logf("%v: err=%v Q=%v K=%v", v, err, h.events("rb", "Q"), h.events("rb", "K"))
		if now["Q"] != ids["Q"] || now["K"] != ids["K"] {
			t.Errorf("%v: a resource was replaced: %v -> %v", v, ids, now)
		}
	}
}

func TestCloudFormationReview6TargetsIntrinsics(t *testing.T) {
	h := newStacks(t)
	one := map[string]any{"Id": "a", "Arn": "arn:aws:sqs:us-east-1:000000000000:q"}
	two := map[string]any{"Id": "b", "Arn": "arn:aws:sqs:us-east-1:000000000000:q"}
	tpl := func(targets any) string {
		return rv19Body(map[string]any{
			"Parameters": map[string]any{"On": map[string]any{"Type": "String", "Default": "yes"}, "L": map[string]any{"Type": "CommaDelimitedList", "Default": "x,y"}},
			"Conditions": map[string]any{"C": map[string]any{"Fn::Equals": []any{map[string]any{"Ref": "On"}, "yes"}}},
			"Resources":  map[string]any{"E": map[string]any{"Type": "AWS::Events::Rule", "Properties": map[string]any{"ScheduleExpression": "rate(5 minutes)", "Targets": targets}}}})
	}
	ids := func() string {
		name := spi.LastSlash(rv31IDs(h, "ti")["E"])
		out := []string{}
		for _, target := range spi.Slice(rv31Get(h, "aws.events", "ListTargetsByRule", map[string]any{"Rule": name})["Targets"]) {
			out = append(out, str(asMap(target)["Id"]))
		}
		return fmt.Sprint(out)
	}
	ifList := map[string]any{"Fn::If": []any{"C", []any{one, two}, map[string]any{"Ref": "AWS::NoValue"}}}
	if _, err := h.invoke("CreateStack", map[string]any{"StackName": "ti", "TemplateBody": tpl(ifList)}); err != nil || ids() != "[a b]" {
		t.Fatalf("Fn::If to a list: %v %s", err, ids())
	}
	if _, err := h.service("aws.events", "PutTargets", map[string]any{"Rule": spi.LastSlash(rv31IDs(h, "ti")["E"]), "Targets": []any{map[string]any{"Id": "outside", "Arn": "arn:aws:sqs:us-east-1:000000000000:o"}}}); err != nil {
		t.Fatal(err)
	}
	// flips to NoValue: the template's targets go, the outside one stays
	_, err := h.invoke("UpdateStack", map[string]any{"StackName": "ti", "TemplateBody": tpl(ifList), "Parameters": []any{map[string]any{"ParameterKey": "On", "ParameterValue": "no"}}})
	if st, _, _ := h.status("ti"); err != nil || ids() != "[outside]" || st != "UPDATE_COMPLETE" {
		t.Errorf("Fn::If to NoValue: %v %s %s", err, ids(), st)
	}
	// a list whose element is an Fn::If to NoValue
	inner := []any{one, map[string]any{"Fn::If": []any{"C", two, map[string]any{"Ref": "AWS::NoValue"}}}}
	_, err = h.invoke("UpdateStack", map[string]any{"StackName": "ti", "TemplateBody": tpl(inner), "Parameters": []any{map[string]any{"ParameterKey": "On", "ParameterValue": "no"}}})
	if st, _, _ := h.status("ti"); err != nil || ids() != "[outside a]" && ids() != "[a outside]" || st != "UPDATE_COMPLETE" {
		t.Errorf("element NoValue: %v %s %s", err, ids(), st)
	}
	// Fn::If to an object (not a list): refused, nothing touched
	obj := map[string]any{"Fn::If": []any{"C", one, []any{two}}}
	_, err = h.invoke("UpdateStack", map[string]any{"StackName": "ti", "TemplateBody": tpl(obj), "Parameters": []any{map[string]any{"ParameterKey": "On", "ParameterValue": "yes"}}})
	st, reason, _ := h.status("ti")
	t.Logf("Fn::If to an object: %v %s %s %s", err, st, reason, ids())
	if err == nil || st != "UPDATE_ROLLBACK_COMPLETE" || ids() != "[outside a]" && ids() != "[a outside]" {
		t.Errorf("object refused? %v %s %s", err, st, ids())
	}
	// Ref to a CommaDelimitedList: a list (of strings) - passes the list check, the service judges it
	_, err = h.invoke("UpdateStack", map[string]any{"StackName": "ti", "TemplateBody": tpl(map[string]any{"Ref": "L"}), "Parameters": []any{map[string]any{"ParameterKey": "On", "ParameterValue": "no"}}})
	st, reason, _ = h.status("ti")
	t.Logf("Ref to a list parameter: %v %s %s %s", err, st, reason, ids())
	if err != nil && strings.Contains(fmt.Sprint(err, reason), "must be a list") {
		t.Errorf("a list parameter was refused as not a list: %v", err)
	}
	// empty list
	_, err = h.invoke("UpdateStack", map[string]any{"StackName": "ti", "TemplateBody": tpl([]any{}), "Parameters": []any{map[string]any{"ParameterKey": "On", "ParameterValue": "no"}}})
	st, reason, _ = h.status("ti")
	t.Logf("empty list: %v %s %s %s", err, st, reason, ids())
}

// A record with Gave but no Took (written before this PR) continues as before: no crash, settles.
func TestCloudFormationReview6RecordWithoutTook(t *testing.T) {
	h := newStacks(t)
	sec := func(p map[string]any) map[string]any { return rveProps("AWS::SecretsManager::Secret", p) }
	if err := rveCreate(h, "lg", map[string]any{"R": sec(map[string]any{"SecretString": "A", "Description": "one"})}); err != nil {
		t.Fatal(err)
	}
	id := rv31IDs(h, "lg")["R"]
	_, _ = h.service("aws.secretsmanager", "PutSecretValue", map[string]any{"SecretId": id, "SecretString": "ROT"})
	rveFailed(t, h, "lg", sec(map[string]any{"Description": "two"}), true)
	rec := h.record("lg")
	for _, r := range spi.Slice(rec["Resources"]) {
		delete(asMap(r), "Took")
	}
	if err := h.deps.Store.Scope(h.id.Account, h.id.Region).Collection("cfn").Put(h.ctx, "lg", []byte(spi.JSONText(rec))); err != nil {
		t.Fatal(err)
	}
	rveContinue(t, h, "lg")
	t.Logf("legacy (no Took): %s", s458Secret(h, id))
	if got := s458Secret(h, id); got != "value=ROT desc=one" {
		t.Errorf("a record without Took ends %s; before this PR it ended value=ROT desc=one", got)
	}
}

func TestCloudFormationReview6Same(t *testing.T) {
	big := 9007199254740993.0
	for _, c := range []struct {
		a, b any
		want bool
	}{
		{"null", nil, false}, {"[]", []any{}, false}, {"{}", map[string]any{}, false}, {"true", true, true}, {"True", true, false},
		{3.0, "3", true}, {3, "3", true}, {3, 3.0, true}, {3.0, "3.0", false}, {3.0, "03", false}, {1e3, 1000.0, true}, {1e3, "1e3", false}, {1e21, "1e+21", true},
		{[]any{[]any{1.0, 2.0}}, []any{[]any{2.0, 1.0}}, false}, {map[string]any{"a": 1.0, "b": 2.0}, map[string]any{"b": 2.0, "a": 1.0}, true},
		{big, "9007199254740993", false}, {int64(9007199254740993), "9007199254740993", true}, {"", nil, false}, {"", "", true},
		{[]any{}, nil, false}, {map[string]any{}, nil, false}, {[]any{nil}, []any{}, false}, {[]any{nil}, []any{""}, false}, {map[string]any{"a": nil}, map[string]any{"a": ""}, false},
		{0.3, "0.3", true}, {false, "false", true}, {false, nil, false}, {0.0, nil, false}, {0.0, "", false}, {false, "", false},
		{[]any{"a,b"}, []any{"a", "b"}, false}, {"a,b", []any{"a", "b"}, false}, {[]any{"1"}, "1", false}, {"\"3\"", "3", false}, {-0.0, 0.0, true},
		{[]string{"a"}, []string{"b"}, false}, {map[string]string{"a": "1"}, map[string]string{"a": "2"}, false}, {int32(1), int32(2), false}, {float32(1), float32(2), false},
	} {
		if got := same(c.a, c.b); got != c.want {
			t.Errorf("same(%#v, %#v) = %v, want %v", c.a, c.b, got, c.want)
		}
		if same(c.a, c.b) != same(c.b, c.a) {
			t.Errorf("same(%#v, %#v) is not symmetric", c.a, c.b)
		}
	}
}
