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

// A property given as null is a property not given, as one that is a Ref to AWS::NoValue is ("Removes the
// corresponding resource property"). Three readers disagreed: what a failed update gave counted a null as
// taken away, while the call that sends a resource's properties and the bucket's configuration took the
// key as given, so a secret whose SecretString was null was sent an empty value ("must be at least 1
// characters") and a rule whose Targets was null an empty PutTargets. All three now read it as absent, in
// a template given and in a stored one. (The service itself refuses a template that holds a null, "'null'
// values are not allowed in templates", as users report it; that refusal is not made here.)
func TestCloudFormationAPropertyGivenAsNullIsNotGiven(t *testing.T) {
	t.Parallel()
	h := newStacks(t)
	one := map[string]any{"Id": "a", "Arn": "arn:aws:sqs:us-east-1:000000000000:q"}
	resources := func(value, targets, tags, block any) map[string]any {
		return map[string]any{
			"S": map[string]any{"Type": "AWS::SecretsManager::Secret", "Properties": map[string]any{"SecretString": value, "Description": "d"}},
			"E": map[string]any{"Type": "AWS::Events::Rule", "Properties": map[string]any{"ScheduleExpression": "rate(5 minutes)", "Targets": targets}},
			"B": map[string]any{"Type": "AWS::S3::Bucket", "Properties": map[string]any{"Tags": tags, "PublicAccessBlockConfiguration": block}}}
	}
	given := []any{"v", []any{one}, []any{map[string]any{"Key": "k", "Value": "v"}}, map[string]any{"BlockPublicAcls": true}}
	if err := rvMake(t, h, "nu", map[string]any{"Resources": resources(nil, nil, nil, nil)}); err != nil {
		t.Fatalf("a stack whose properties are null answered %v", err)
	}
	ids := rv31IDs(h, "nu")
	state := func() string {
		value := rv31Get(h, "aws.secretsmanager", "GetSecretValue", map[string]any{"SecretId": ids["S"]})["SecretString"]
		targets := len(spi.Slice(rv31Get(h, "aws.events", "ListTargetsByRule", map[string]any{"Rule": spi.LastSlash(ids["E"])})["Targets"]))
		return fmt.Sprint("value=", value, " targets=", targets, " ", rv29Bucket(h, ids["B"]))
	}
	made := state()
	if !strings.HasPrefix(made, "value=<nil> targets=0 ") || !strings.HasSuffix(made, "tags=none") {
		t.Fatalf("made with null properties: %s", made)
	}
	if err := rv28Update(h, "nu", resources(given[0], given[1], given[2], given[3])); err != nil || !strings.HasPrefix(state(), "value=v targets=1 ") || !strings.HasSuffix(state(), "tags=set") {
		t.Fatalf("the update that gives each property answered %v and left %s", err, state())
	}
	// Null again: each is cleared as a property taken out of the template is, and the secret keeps its value,
	// which nothing takes away.
	if err := rv28Update(h, "nu", resources(nil, nil, nil, nil)); err != nil || !strings.HasPrefix(state(), "value=v targets=0 ") || !strings.Contains(state(), "block=none") || !strings.HasSuffix(state(), "tags=none") {
		t.Errorf("the update that gives each property as null answered %v and left %s", err, state())
	}
	names, _, took, _ := gave(h.pack.Given(map[string]any{"A": nil, "B": "b"}, nil, "nu"), h.pack.Given(map[string]any{"A": "a", "B": nil}, nil, "nu"))
	if fmt.Sprint(*names, *took) != "[B] [A]" {
		t.Errorf("what a failed update gave is %v, and took away %v; want [B] and [A]", *names, *took)
	}
}
