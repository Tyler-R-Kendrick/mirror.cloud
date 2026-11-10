package cloudformation

import (
	"fmt"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

// The tests of the fifth review of the engine (of 22553ebc), one for each of its findings; the reviewer's
// own walks are review_r32{a,b,c}_test.go.

// A rollback and a continued one agree on a value the failed update took away: the template gives the
// secret's SecretString, the failed update gave none, and both put the template's value back, over one
// rotated outside the template since. Before, a continued rollback knew only what the failed update gave,
// read the value as unchanged, and left the rotated one, where the rollback itself restored the template's.
func TestCloudFormationARollbackAndAContinuedOneAgreeOnAValueTakenAway(t *testing.T) {
	t.Parallel()
	secret := func(p map[string]any) map[string]any {
		return map[string]any{"Type": "AWS::SecretsManager::Secret", "Properties": p}
	}
	for name, failRestore := range map[string]bool{"a rollback": false, "a continued rollback": true} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			h := newStacks(t)
			if err := rvMake(t, h, "ta", map[string]any{"Resources": map[string]any{"S": secret(map[string]any{"SecretString": "A", "Description": "one"})}}); err != nil {
				t.Fatal(err)
			}
			id := rv31IDs(h, "ta")["S"]
			if _, err := h.service("aws.secretsmanager", "PutSecretValue", map[string]any{"SecretId": id, "SecretString": "ROT"}); err != nil {
				t.Fatal(err)
			}
			var writes atomic.Int32 // of the secret: the update's lands, and the restore's fails when it is to
			off := faultStore(h, func(op, c, k string) bool {
				return failRestore && (op == "put" || op == "txput") && strings.Contains(c, "secret") && writes.Add(1) == 2
			})
			err := invoked(t, h, "UpdateStack", map[string]any{"StackName": "ta", "TemplateBody": rv19Body(map[string]any{"Resources": map[string]any{
				"S": secret(map[string]any{"Description": "two"}), "Z": rv31Dep(rv31Bad, "S")}})})
			off()
			if st, _, _ := h.status("ta"); err == nil || (st == "UPDATE_ROLLBACK_FAILED") != failRestore {
				t.Fatalf("the failed update answered %v and left %s (%d secret writes)", err, st, writes.Load())
			}
			if failRestore {
				if rec := spi.JSONText(h.record("ta")["Resources"]); !strings.Contains(rec, `"Gave":["Description"]`) || !strings.Contains(rec, `"Took":["SecretString"]`) {
					t.Errorf("the record is %s; want the description given and the value taken away, by name", rec)
				}
				if _, err := h.invoke("ContinueUpdateRollback", map[string]any{"StackName": "ta"}); err != nil {
					t.Fatal(err)
				}
			}
			value := rv31Get(h, "aws.secretsmanager", "GetSecretValue", map[string]any{"SecretId": id})["SecretString"]
			if st, _, _ := h.status("ta"); st != "UPDATE_ROLLBACK_COMPLETE" || value != "A" || strings.Contains(spi.JSONText(h.record("ta")["Resources"]), "Took") {
				t.Errorf("the stack is %s, the secret's value %v and its record %s; want UPDATE_ROLLBACK_COMPLETE, the template's value A, and nothing recorded any more", st, value, spi.JSONText(h.record("ta")["Resources"]))
			}
		})
	}
}

// A rollback that ends before it has put everything back marks what it had not, by what the failed update
// gave each against the template's own properties as the stack held them: the template's are read with the
// stack's values, not the failed update's. The engine panics as the rollback restores the bucket; the
// rule and the secret after it are marked. The secret's description alone changed (through a parameter),
// so a value rotated since outlives the continued rollback, and the rule's target put outside it stays.
func TestCloudFormationATakeoverMarksWhatTheFailedUpdateChanged(t *testing.T) {
	t.Parallel()
	h := newStacks(t)
	template := func(origin, target string, failing bool) string {
		resources := map[string]any{"A": rv30CorsBucket(origin),
			"R": map[string]any{"Type": "AWS::Events::Rule", "DependsOn": "A", "Properties": map[string]any{"ScheduleExpression": "rate(5 minutes)",
				"Targets": []any{map[string]any{"Id": target, "Arn": "arn:aws:sqs:us-east-1:000000000000:q"}}}},
			"S": map[string]any{"Type": "AWS::SecretsManager::Secret", "DependsOn": "R", "Properties": map[string]any{"SecretString": "v", "Description": map[string]any{"Ref": "D"}}}}
		if failing {
			resources["Z"] = rv31Dep(rv31Bad, "S")
		}
		return rv19Body(map[string]any{"Parameters": map[string]any{"D": map[string]any{"Type": "String", "Default": "one"}}, "Resources": resources})
	}
	if _, err := h.invoke("CreateStack", map[string]any{"StackName": "tk", "TemplateBody": template("one", "a", false)}); err != nil {
		t.Fatal(err)
	}
	ids := rv31IDs(h, "tk")
	rule := spi.LastSlash(ids["R"])
	if _, err := h.service("aws.events", "PutTargets", map[string]any{"Rule": rule, "Targets": []any{map[string]any{"Id": "outside", "Arn": "arn:aws:sqs:us-east-1:000000000000:o"}}}); err != nil {
		t.Fatal(err)
	}
	var writes atomic.Int32 // of the bucket's CORS: the update's, then the rollback's, which panics
	off := faultStore(h, func(op, c, k string) bool { return op == "put" && c == "bktcfg" && writes.Add(1) == 2 })
	err := invoked(t, h, "UpdateStack", map[string]any{"StackName": "tk", "TemplateBody": template("two", "b", true), "Parameters": []any{map[string]any{"ParameterKey": "D", "ParameterValue": "two"}}})
	off()
	if !isInternalFailure(err) {
		t.Fatalf("the update whose rollback panics answered %v", err)
	}
	_, _ = h.invoke("UpdateStack", map[string]any{"StackName": "tk", "TemplateBody": template("one", "a", false)}) // refused: it takes the stack over
	rec := map[string]string{}
	for _, r := range spi.Slice(h.record("tk")["Resources"]) {
		rec[str(asMap(r)["Logical"])] = spi.JSONText(r)
	}
	if st, _, _ := h.status("tk"); st != "UPDATE_ROLLBACK_FAILED" || !strings.Contains(rec["S"], `"Gave":["Description"]`) || strings.Contains(rec["S"], "SecretString") ||
		!strings.Contains(rec["R"], `"Gave":["Targets"]`) || !strings.Contains(rec["R"], `"Targets":["b"]`) {
		t.Fatalf("the stack taken over is %s, recording %s and %s; want the secret's description and the rule's targets as what changed, and the value not", st, rec["S"], rec["R"])
	}
	if _, err := h.service("aws.secretsmanager", "PutSecretValue", map[string]any{"SecretId": ids["S"], "SecretString": "ROT"}); err != nil {
		t.Fatal(err)
	}
	if _, err := h.invoke("ContinueUpdateRollback", map[string]any{"StackName": "tk"}); err != nil {
		t.Fatal(err)
	}
	var targets []string
	for _, target := range spi.Slice(rv31Get(h, "aws.events", "ListTargetsByRule", map[string]any{"Rule": rule})["Targets"]) {
		targets = append(targets, str(asMap(target)["Id"]))
	}
	value := rv31Get(h, "aws.secretsmanager", "GetSecretValue", map[string]any{"SecretId": ids["S"]})["SecretString"]
	described := rv31Get(h, "aws.secretsmanager", "DescribeSecret", map[string]any{"SecretId": ids["S"]})["Description"]
	cors, _, _ := h.deps.Store.Scope(h.id.Account, h.id.Region).Collection("bktcfg").Get(h.ctx, ids["A"]+"/cors")
	if st, _, _ := h.status("tk"); st != "UPDATE_ROLLBACK_COMPLETE" || value != "ROT" || described != "one" || len(targets) != 2 || !strings.Contains(strings.Join(targets, " "), "outside") || !strings.Contains(string(cors), `"one"`) {
		t.Errorf("the stack is %s, the secret %v described %v, the rule's targets %v and the bucket's CORS %s; want UPDATE_ROLLBACK_COMPLETE, the rotated value, the description one, the targets a and outside, the origin one", st, value, described, targets, cors)
	}
}

// A property's value is compared as a value. The comparison was of JSON text with a string's own text
// standing for it, so the string "null" was the absent property, and a string that reads as a list or an
// object was that list or object: an update that took such a property away, or gave one, was judged to
// change nothing. A number and a Boolean are still their text: "the parameter value becomes a string"
// when a Number parameter is referenced (User Guide, Parameters), so 3 and "3" are one value.
func TestCloudFormationAStringThatReadsAsNothingIsAValue(t *testing.T) {
	t.Parallel()
	for _, c := range []struct {
		a, b any
		want bool
	}{{nil, nil, true}, {"null", nil, false}, {nil, "null", false}, {"", nil, false}, {"[1,2]", []any{1.0, 2.0}, false}, {`{"x":1}`, map[string]any{"x": 1.0}, false},
		{3.0, "3", true}, {true, "true", true}, {"3", "3", true}, {[]any{1.0, 2.0}, []any{2.0, 1.0}, false}, {[]any{1.0, "a"}, []any{"1", "a"}, true},
		{map[string]any{"x": 1.0, "y": []any{true}}, map[string]any{"y": []any{"true"}, "x": "1"}, true}, {map[string]any{"x": nil}, map[string]any{}, false}} {
		if got := same(c.a, c.b); got != c.want {
			t.Errorf("same(%#v, %#v) is %v", c.a, c.b, got)
		}
	}
	names, _, took := gave(map[string]any{"A": 3.0, "B": "null", "C": "[1]", "G": "same"}, map[string]any{"A": "3", "C": []any{1.0}, "G": "same", "H": "only-now"})
	if fmt.Sprint(*names, *took) != "[B C] [H]" {
		t.Errorf("gave lists %v as changed and %v as taken away; want [B C] and [H]", *names, *took)
	}
	if was := (prior{props: map[string]any{"D": "null"}}); !was.differs("D", nil) || was.differs("D", "null") {
		t.Errorf(`a template that gave "null" differs from nothing: %v, and from "null": %v`, was.differs("D", nil), was.differs("D", "null"))
	}

	// End to end: a description that is the text "null", then taken away by an update.
	h := newStacks(t)
	secret := func(properties map[string]any) map[string]any {
		return map[string]any{"S": map[string]any{"Type": "AWS::SecretsManager::Secret", "Properties": properties}}
	}
	if err := rvMake(t, h, "nl", map[string]any{"Resources": secret(map[string]any{"SecretString": "v", "Description": "null"})}); err != nil {
		t.Fatal(err)
	}
	described := func() any {
		return rv31Get(h, "aws.secretsmanager", "DescribeSecret", map[string]any{"SecretId": rv31IDs(h, "nl")["S"]})["Description"]
	}
	if described() != "null" {
		t.Fatalf("the secret is described %v", described())
	}
	if err := rv28Update(h, "nl", secret(map[string]any{"SecretString": "v"})); err != nil || described() != nil {
		t.Errorf("the update that takes the description away answered %v and left %v", err, described())
	}
}
