package cloudformation

import (
	"fmt"
	"maps"
	"slices"
	"strings"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

// The tests of the sixth review of the engine (of c044b72b), one for each of its findings; the reviewer's
// own are review_r33{a,b}_test.go.

// A scalar inside a property keeps its type. The comparison of two property values read every number and
// Boolean as its text at any depth, so an event pattern that went from [3] to ["3"] was judged unchanged:
// the update answered UPDATE_COMPLETE, stored the new template and left the rule its old pattern, and in
// EventBridge the two match different events ("{"x": [1]} ... matches numeric", EventBridge User Guide,
// event patterns; quoted as remembered). The same holds for a policy document.
//
// What is compared is what the service is sent. A member its create operation's model types is read as
// that type (call types it so, typed), so a table's ReadCapacityUnits of 5 and of a Ref to a Number
// parameter, which is the string "5" ("the parameter value becomes a string", User Guide, Parameters), are
// one value and no update; inside a document the model does not type, the string is sent, and is a change.
func TestCloudFormationAScalarInsideADocumentKeepsItsType(t *testing.T) {
	t.Parallel()
	h := newStacks(t)
	policy := func(age any) map[string]any {
		return map[string]any{"Version": "2012-10-17", "Statement": []any{map[string]any{"Effect": "Allow", "Principal": map[string]any{"AWS": "arn:aws:iam::000000000000:root"},
			"Action": "sts:AssumeRole", "Condition": map[string]any{"NumericLessThan": map[string]any{"aws:MultiFactorAuthAge": age}}}}}
	}
	template := func(n, age, units any) string {
		return rv19Body(map[string]any{"Parameters": map[string]any{"N": map[string]any{"Type": "Number", "Default": 5}}, "Resources": map[string]any{
			"E": map[string]any{"Type": "AWS::Events::Rule", "Properties": map[string]any{"EventPattern": map[string]any{"source": []any{"x"}, "detail": map[string]any{"n": []any{n}, "f": []any{true}}}}},
			"R": map[string]any{"Type": "AWS::IAM::Role", "Properties": map[string]any{"AssumeRolePolicyDocument": policy(age)}},
			"T": rv29Table(map[string]any{"ProvisionedThroughput": map[string]any{"ReadCapacityUnits": units, "WriteCapacityUnits": 5}})}})
	}
	if _, err := h.invoke("CreateStack", map[string]any{"StackName": "sd", "TemplateBody": template(5.0, 5.0, 5.0), "Capabilities": []any{"CAPABILITY_IAM"}}); err != nil {
		t.Fatal(err)
	}
	ids := rv31IDs(h, "sd")
	state := func() string {
		pattern := rv31Get(h, "aws.events", "DescribeRule", map[string]any{"Name": spi.LastSlash(ids["E"])})["EventPattern"]
		trust := spi.JSONText(asMap(rv31Get(h, "aws.iam", "GetRole", map[string]any{"RoleName": spi.LastSlash(ids["R"])})["Role"])["AssumeRolePolicyDocument"])
		trust = trust[strings.Index(trust, "MultiFactorAuthAge"):]
		return fmt.Sprint("pattern=", pattern, " policy=", trust[:strings.IndexAny(trust, "}%")])
	}
	ref := map[string]any{"Ref": "N"}
	number, text := `pattern={"detail":{"f":[true],"n":[5]},"source":["x"]} policy=MultiFactorAuthAge":5`, `pattern={"detail":{"f":[true],"n":["5"]},"source":["x"]} policy=MultiFactorAuthAge":"5"`
	if got := state(); got != number {
		t.Fatalf("as made: %s", got)
	}
	for _, step := range []struct {
		what          string
		n, age, units any
		want, wrote   string // the rule and the role, and the services written to
	}{
		{"the numbers as text in the pattern and the policy, and a Ref to a Number parameter for the table's units", "5", "5", ref, text, "[events iam]"},
		{"a Ref to the Number parameter in the pattern and the policy, which is that text, and the units as text", ref, ref, "5", text, "[]"},
		{"numbers again, written 5.0", 5.0, 5.0, 5.0, number, "[events iam]"},
		{"six units", 5.0, 5.0, 6.0, number, "[dynamodb]"},
	} {
		wrote := map[string]bool{} // the service of each resource a store write was made for
		off := faultStore(h, func(op, collection, key string) bool {
			for logical, service := range map[string]string{"E": "events", "R": "iam", "T": "dynamodb"} {
				if strings.HasSuffix(op, "put") && !strings.HasPrefix(collection, "cfn") && strings.Contains(key, spi.LastSlash(ids[logical])) {
					wrote[service] = true
				}
			}
			return false
		})
		_, err := h.invoke("UpdateStack", map[string]any{"StackName": "sd", "TemplateBody": template(step.n, step.age, step.units), "Capabilities": []any{"CAPABILITY_IAM"}})
		off()
		if status, _, _ := h.status("sd"); err != nil || status != "UPDATE_COMPLETE" {
			t.Fatalf("%s: the update answered %v and left the stack %s", step.what, err, status)
		}
		if got, services := state(), fmt.Sprint(slices.Sorted(maps.Keys(wrote))); got != step.want || services != step.wrote {
			t.Errorf("%s:\n got %s, writing to %s\nwant %s, writing to %s", step.what, got, services, step.want, step.wrote)
		}
		if now := rv31IDs(h, "sd"); now["E"] != ids["E"] || now["R"] != ids["R"] || now["T"] != ids["T"] {
			t.Fatalf("%s: a resource was replaced: %v, was %v", step.what, now, ids)
		}
	}
}

// The refusal of a rule whose Targets is not a list is of a template as it is given, never of a stored one
// as it is restored. It ran wherever a rule was written, so a stack whose stored template holds such a
// value (one recorded before the refusal) was not restored at all: the restore was refused before the
// rule was written. It is read again as it always was: the rule is written, with its description, and its
// targets are EventBridge's to judge, which refuses this one as it did before ("Targets must contain
// between 1 and 10 entries": an object is no list of targets). An update that leaves that rule as it is
// does not meet the refusal either.
func TestCloudFormationAStoredTemplateIsRestoredAsItWasRead(t *testing.T) {
	t.Parallel()
	one := map[string]any{"Id": "a", "Arn": "arn:aws:sqs:us-east-1:000000000000:q"}
	rule := func(description string, targets any) map[string]any {
		return map[string]any{"Type": "AWS::Events::Rule", "Properties": map[string]any{"ScheduleExpression": "rate(5 minutes)", "Description": description, "Targets": targets}}
	}
	for _, continued := range []bool{false, true} {
		h := newStacks(t)
		if err := rvMake(t, h, "ol", map[string]any{"Resources": map[string]any{"R": rule("one", []any{one})}}); err != nil {
			t.Fatal(err)
		}
		// The older record, made by hand: its template gives the one target as an object, not a list of one.
		rec := h.record("ol")
		rec["Template"] = rv19Body(map[string]any{"Resources": map[string]any{"R": rule("one", one)}})
		if err := h.deps.Store.Scope(h.id.Account, h.id.Region).Collection("cfn").Put(h.ctx, "ol", []byte(spi.JSONText(rec))); err != nil {
			t.Fatal(err)
		}
		name := spi.LastSlash(rv31IDs(h, "ol")["R"])
		described := func() any {
			return rv31Get(h, "aws.events", "DescribeRule", map[string]any{"Name": name})["Description"]
		}
		told := func(err error) string {
			_, reason, _ := h.status("ol")
			return fmt.Sprint(err, " ", reason, " ", spi.JSONText(h.record("ol")["Resources"]))
		}
		if err := rv28Update(h, "ol", map[string]any{"R": rule("one", one), "K": rv28Topic}); err != nil {
			t.Errorf("continued=%v: an update that leaves the rule as its stored template has it answered %v", continued, err)
		}
		rveFailed(t, h, "ol", rule("two", []any{one}), continued)
		var err error
		if continued {
			_, err = h.invoke("ContinueUpdateRollback", map[string]any{"StackName": "ol"})
		}
		if got := told(err); strings.Contains(got, "must be a list") || !strings.Contains(got, "between 1 and 10 entries") || described() != "one" {
			t.Errorf("continued=%v: restored from its stored template the stack tells %s and the rule is described %v; want the rule written, described one, and EventBridge's own answer for its targets", continued, got, described())
		}
	}
}
