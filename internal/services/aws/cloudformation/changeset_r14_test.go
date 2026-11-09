package cloudformation

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

// changeSets is a stack "s" of a topic and the change sets named, each its template given; the stack is
// made through h, whose store a test may have gated.
func changeSets(t *testing.T, h *stacks, sets map[string]string) {
	t.Helper()
	topic, _ := json.Marshal(map[string]any{"Resources": map[string]any{"T": map[string]any{"Type": "AWS::SNS::Topic"}}})
	if _, err := h.invoke("CreateStack", map[string]any{"StackName": "s", "TemplateBody": string(topic)}); err != nil {
		t.Fatal(err)
	}
	for cs, body := range sets {
		if _, err := h.invoke("CreateChangeSet", map[string]any{"StackName": "s", "ChangeSetName": cs, "TemplateBody": cmpOr(body, string(topic))}); err != nil {
			t.Fatal(err)
		}
	}
}

func cmpOr(a, b string) string {
	if a != "" {
		return a
	}
	return b
}

// describe answers DescribeChangeSet of the stack s's change set, or its error.
func (h *stacks) describe(cs string) (map[string]any, error) {
	return h.invoke("DescribeChangeSet", map[string]any{"StackName": "s", "ChangeSetName": cs})
}

// bucketed is a template of the topic and a bucket with a CORS configuration, whose write gated holds.
func bucketed() string {
	body, _ := json.Marshal(map[string]any{"Resources": paused(0)})
	var m map[string]any
	_ = json.Unmarshal(body, &m)
	asMap(m["Resources"])["T"] = map[string]any{"Type": "AWS::SNS::Topic"}
	raw, _ := json.Marshal(m)
	return string(raw)
}

// Every write of a change set, and every write of the stack with its change
// sets, is a compare-and-set in one store transaction, so an operation that
// comes while a change set is being executed finds it so:
//   - DeleteChangeSet of the change set being executed is InvalidChangeSetStatus;
//   - a second change set executed meanwhile deletes the first, "all other
//     change sets associated with the stack" (ExecuteChangeSet), and the first,
//     finding the stack changed, writes nothing: neither the stack nor its
//     deleted change set;
//   - an UpdateStack meanwhile leaves the available change sets OBSOLETE and
//     the one being executed EXECUTE_FAILED once its execution finds the stack
//     changed, with why as its StatusReason.
//
// Before, DeleteChangeSet deleted the change set mid-execution and the
// execution then wrote it back; the first execution wrote its deleted change
// set back as EXECUTE_FAILED; and an execution's failure carried no reason.
func TestCloudFormationChangeSetsRace(t *testing.T) {
	execute := func(h *stacks, cs string) chan error {
		done := make(chan error, 1)
		go func() {
			_, err := h.invoke("ExecuteChangeSet", map[string]any{"StackName": "s", "ChangeSetName": cs})
			done <- err
		}()
		return done
	}
	t.Run("delete", func(t *testing.T) {
		h, g := gated(t)
		changeSets(t, h, map[string]string{"a": bucketed()})
		done := execute(h, "a")
		within(t, g.reached, "a's execution reaching its bucket")
		if _, err := h.invoke("DeleteChangeSet", map[string]any{"StackName": "s", "ChangeSetName": "a"}); code(err) != "InvalidChangeSetStatus" {
			t.Errorf("DeleteChangeSet of the change set being executed answered %v, want InvalidChangeSetStatus", err)
		}
		close(g.release)
		if err := within(t, done, "a's execution"); err != nil {
			t.Fatal(err)
		}
		if out, err := h.describe("a"); err != nil || out["ExecutionStatus"] != "EXECUTE_COMPLETE" {
			t.Errorf("after its execution a is %v, %v, want EXECUTE_COMPLETE", out, err)
		}
	})
	t.Run("execute", func(t *testing.T) {
		h, g := gated(t)
		changeSets(t, h, map[string]string{"a": bucketed(), "b": ""})
		done := execute(h, "a")
		within(t, g.reached, "a's execution reaching its bucket")
		if err := within(t, execute(h, "b"), "b's execution"); err != nil {
			t.Fatal(err)
		}
		close(g.release)
		if err := within(t, done, "a's execution"); code(err) != "ValidationError" {
			t.Errorf("a's execution, finding the stack changed under it, answered %v, want ValidationError", err)
		}
		if _, err := h.describe("a"); code(err) != "ChangeSetNotFound" {
			t.Errorf("a, which b's execution deleted, answered %v, want ChangeSetNotFound", err)
		}
		if out, err := h.describe("b"); err != nil || out["ExecutionStatus"] != "EXECUTE_COMPLETE" {
			t.Errorf("b is %v, %v, want EXECUTE_COMPLETE", out, err)
		}
	})
	t.Run("update", func(t *testing.T) {
		h, g := gated(t)
		changeSets(t, h, map[string]string{"a": bucketed(), "c": ""})
		done := execute(h, "a")
		within(t, g.reached, "a's execution reaching its bucket")
		topic, _ := json.Marshal(map[string]any{"Resources": map[string]any{"T": map[string]any{"Type": "AWS::SNS::Topic"}, "U": map[string]any{"Type": "AWS::SNS::Topic"}}})
		if _, err := h.invoke("UpdateStack", map[string]any{"StackName": "s", "TemplateBody": string(topic)}); err != nil {
			t.Fatal(err)
		}
		close(g.release)
		if err := within(t, done, "a's execution"); code(err) != "ValidationError" {
			t.Errorf("a's execution, finding the stack updated under it, answered %v, want ValidationError", err)
		}
		if out, err := h.describe("a"); err != nil || out["ExecutionStatus"] != "EXECUTE_FAILED" || !strings.Contains(str(out["StatusReason"]), "can not be updated") {
			t.Errorf("a is %v, %v, want EXECUTE_FAILED with why as its StatusReason", out, err)
		}
		if out, err := h.describe("c"); err != nil || out["ExecutionStatus"] != "OBSOLETE" {
			t.Errorf("c is %v, %v, want OBSOLETE", out, err)
		}
	})
}

// A CreateChangeSet of type CREATE and a CreateStack of the name never both
// succeed: the stack in review is written by a compare-and-set in the
// transaction that finds the name free, as the create's reservation is. A gate
// on that write holds it where it is a write of its own; where it is part of
// the transaction there is none, and the change set's operation ends first.
// Before, CreateChangeSet read the name free, CreateStack made the stack, and
// the change set's write then put the stack back in review.
func TestCloudFormationAStackInReviewIsReserved(t *testing.T) {
	h := newStacks(t)
	g := h.gateOn(func(collection, _ string, val []byte) bool {
		return collection == "cfn" && strings.Contains(string(val), "REVIEW_IN_PROGRESS")
	})
	body, _ := json.Marshal(map[string]any{"Resources": map[string]any{"T": map[string]any{"Type": "AWS::SNS::Topic"}}})
	csDone := make(chan error, 1)
	go func() {
		_, err := h.invoke("CreateChangeSet", map[string]any{"StackName": "r", "ChangeSetName": "c", "ChangeSetType": "CREATE", "TemplateBody": string(body)})
		csDone <- err
	}()
	var csErr, stackErr error
	select {
	case <-g.reached: // the stack in review is a write of its own: the create comes between
		_, stackErr = h.invoke("CreateStack", map[string]any{"StackName": "r", "TemplateBody": string(body)})
		close(g.release)
		csErr = within(t, csDone, "CreateChangeSet")
	case csErr = <-csDone:
		_, stackErr = h.invoke("CreateStack", map[string]any{"StackName": "r", "TemplateBody": string(body)})
	}
	status, _, _ := h.status("r")
	if csErr == nil && stackErr == nil || status != map[bool]string{true: "REVIEW_IN_PROGRESS", false: "CREATE_COMPLETE"}[csErr == nil] {
		t.Errorf("CreateChangeSet answered %v and CreateStack %v, and the stack is %s: want exactly one, and the stack its", csErr, stackErr, status)
	}
}

// A refusal of an execution that changes nothing leaves the change set
// AVAILABLE, with why as its StatusReason; an execution that fails after it
// started is EXECUTE_FAILED, with why. A stack's change sets are deleted with
// it, in the transaction that deletes it. Before, a refused execution left
// its change set EXECUTE_FAILED with no reason, and DeleteStack left the
// stack's change sets behind.
func TestCloudFormationAChangeSetSaysWhyItFailed(t *testing.T) {
	h := newStacks(t)
	kept, _ := json.Marshal(map[string]any{"Resources": map[string]any{"B": map[string]any{"Type": "AWS::S3::Bucket", "Properties": map[string]any{"BucketName": "kept-b"}}}})
	taken, _ := json.Marshal(map[string]any{"Resources": map[string]any{"B": map[string]any{"Type": "AWS::S3::Bucket", "Properties": map[string]any{"BucketName": "kept-b"}},
		"P": map[string]any{"Type": "AWS::SSM::Parameter", "Properties": map[string]any{"Name": "taken", "Type": "String", "Value": "v"}}}})
	if _, err := h.service("aws.ssm", "PutParameter", map[string]any{"Name": "taken", "Type": "String", "Value": "v"}); err != nil {
		t.Fatal(err)
	}
	if _, err := h.invoke("CreateStack", map[string]any{"StackName": "s", "TemplateBody": string(kept)}); err != nil {
		t.Fatal(err)
	}
	for _, cs := range []string{"failing", "refused"} {
		if _, err := h.invoke("CreateChangeSet", map[string]any{"StackName": "s", "ChangeSetName": cs, "TemplateBody": string(taken)}); err != nil {
			t.Fatal(err)
		}
	}
	for _, cs := range []string{"failing", "refused"} {
		if cs == "refused" { // a stack whose delete failed takes no update: "is in DELETE_FAILED state"
			h.fill("kept-b", true)
			if _, err := h.invoke("DeleteStack", map[string]any{"StackName": "s"}); err == nil {
				t.Fatal("DeleteStack of a stack holding a full bucket succeeded")
			}
		}
		_, err := h.invoke("ExecuteChangeSet", map[string]any{"StackName": "s", "ChangeSetName": cs})
		want := map[string]string{"failing": "EXECUTE_FAILED", "refused": "AVAILABLE"}[cs]
		if out, _ := h.describe(cs); err == nil || out["ExecutionStatus"] != want || out["StatusReason"] != err.Error() {
			t.Errorf("after ExecuteChangeSet of %s answered %v, it is %v, want %s with that error as its StatusReason", cs, err, out, want)
		}
	}
	h.fill("kept-b", false)
	if _, err := h.invoke("DeleteStack", map[string]any{"StackName": "s"}); err != nil {
		t.Fatal(err)
	}
	for _, cs := range []string{"failing", "refused"} {
		if _, err := h.describe(cs); code(err) != "ChangeSetNotFound" {
			t.Errorf("after DeleteStack, DescribeChangeSet of %s answered %v, want ChangeSetNotFound", cs, err)
		}
	}
}

// An UPDATE change set without Tags keeps the stack's: "If you don't specify
// this parameter, CloudFormation doesn't modify the stack's tags" (UpdateStack),
// the change set holding no Tags, as it holds no NotificationARNs. A
// UsePreviousValue beside an empty ParameterValue is taken: the service refuses
// only a "non empty value" beside it. ListChangeSets answers ChangeSetSummary's
// members and no others. Before, the change set held an empty Tags list that
// removed the stack's; UsePreviousValue beside "" was refused; and the
// summaries carried the whole record, TemplateBody and Parameters among it.
func TestCloudFormationAChangeSetKeepsWhatItIsNotGiven(t *testing.T) {
	h := newStacks(t)
	body, _ := json.Marshal(map[string]any{"Parameters": map[string]any{"Key": map[string]any{"Type": "String"}},
		"Resources": map[string]any{"T": map[string]any{"Type": "AWS::SNS::Topic"}}, "Outputs": map[string]any{"K": map[string]any{"Value": map[string]any{"Ref": "Key"}}}})
	team := []any{map[string]any{"Key": "team", "Value": "a"}}
	if _, err := h.invoke("CreateStack", map[string]any{"StackName": "s", "TemplateBody": string(body), "Tags": team, "Parameters": givenParams("Key", "kept")}); err != nil {
		t.Fatal(err)
	}
	empty := []any{map[string]any{"ParameterKey": "Key", "ParameterValue": "", "UsePreviousValue": "true"}}
	if _, err := h.invoke("CreateChangeSet", map[string]any{"StackName": "s", "ChangeSetName": "plain", "TemplateBody": string(body), "Parameters": empty}); err != nil {
		t.Errorf("CreateChangeSet with UsePreviousValue beside an empty value answered %v", err)
		if _, err := h.invoke("CreateChangeSet", map[string]any{"StackName": "s", "ChangeSetName": "plain", "TemplateBody": string(body), "Parameters": givenParams("Key", "kept")}); err != nil {
			t.Fatal(err)
		}
	}
	out, err := h.invoke("ListChangeSets", map[string]any{"StackName": "s"})
	for _, summary := range spiSlice(out["Summaries"]) {
		for member := range asMap(summary) {
			if !strings.Contains(" StackId StackName ChangeSetId ChangeSetName ExecutionStatus Status StatusReason ", " "+member+" ") {
				t.Errorf("ListChangeSets answered %s, which ChangeSetSummary does not declare (%v)", member, err)
			}
		}
	}
	if _, err := h.invoke("ExecuteChangeSet", map[string]any{"StackName": "s", "ChangeSetName": "plain"}); err != nil {
		t.Fatal(err)
	}
	if got := h.outputs("s")["K"]; got != "kept" {
		t.Errorf("K = %v, want kept", got)
	}
	stacks, _ := h.invoke("DescribeStacks", map[string]any{"StackName": "s"})
	if got := asMap(spiSlice(stacks["Stacks"])[0])["Tags"]; !reflect.DeepEqual(got, team) {
		t.Errorf("after a change set without Tags, the stack's Tags are %v, want %v kept", got, team)
	}
	if _, err := h.invoke("UpdateStack", map[string]any{"StackName": "s", "TemplateBody": string(body), "Parameters": empty}); err != nil {
		t.Errorf("UpdateStack with UsePreviousValue beside an empty value answered %v", err)
	}
}

// A change set stored before it had an ExecutionStatus reads as AVAILABLE on
// every path: DescribeChangeSet, ListChangeSets and ExecuteChangeSet, which
// claims it; executed, it is refused a second execution, which leaves it as
// it was. Before, ListChangeSets answered such a change set with no
// ExecutionStatus at all.
func TestCloudFormationAChangeSetWithoutAStatusIsAvailable(t *testing.T) {
	h := newStacks(t)
	changeSets(t, h, nil)
	topic, _ := json.Marshal(map[string]any{"Resources": map[string]any{"T": map[string]any{"Type": "AWS::SNS::Topic"}}})
	stack, _ := h.stored("s")
	old, _ := json.Marshal(map[string]any{"ChangeSetName": "old", "ChangeSetId": "arn:aws:cloudformation:us-east-1:000000000000:changeSet/old/1", "StackName": "s",
		"StackId": stack["ID"], "ChangeSetType": "UPDATE", "Status": "CREATE_COMPLETE", "TemplateBody": string(topic), "StatusReason": "refused before"})
	if err := h.deps.Store.Scope(h.id.Account, h.id.Region).Collection("cfn-cs").Put(h.ctx, "s:old", old); err != nil {
		t.Fatal(err)
	}
	if out, err := h.describe("old"); err != nil || out["ExecutionStatus"] != "AVAILABLE" {
		t.Errorf("DescribeChangeSet answered %v, %v, want AVAILABLE", out, err)
	}
	if out, err := h.invoke("ListChangeSets", map[string]any{"StackName": "s"}); err != nil || len(spiSlice(out["Summaries"])) != 1 || asMap(spiSlice(out["Summaries"])[0])["ExecutionStatus"] != "AVAILABLE" {
		t.Errorf("ListChangeSets answered %v, %v, want old AVAILABLE", out, err)
	}
	if _, err := h.invoke("ExecuteChangeSet", map[string]any{"StackName": "s", "ChangeSetName": "old"}); err != nil {
		t.Errorf("ExecuteChangeSet answered %v", err)
	}
	if _, err := h.invoke("ExecuteChangeSet", map[string]any{"StackName": "s", "ChangeSetName": "old"}); code(err) != "InvalidChangeSetStatus" {
		t.Errorf("ExecuteChangeSet of the executed change set answered %v, want InvalidChangeSetStatus", err)
	}
	if out, err := h.describe("old"); err != nil || out["ExecutionStatus"] != "EXECUTE_COMPLETE" || out["StatusReason"] != nil {
		t.Errorf("after its execution and a refused second one, DescribeChangeSet answered %v, %v, want EXECUTE_COMPLETE with no StatusReason", out, err)
	}
}

func spiSlice(v any) []any { s, _ := v.([]any); return s }

// A change set made from a TemplateURL keeps the template it read, and its
// execution makes the stack from it: CreateChangeSet takes "TemplateBody or a
// TemplateURL" (API reference, CreateChangeSet). Before, the native
// CreateChangeSet kept only a TemplateBody given, so the execution of one made
// from a TemplateURL had no template.
func TestCloudFormationAChangeSetKeepsTheTemplateItRead(t *testing.T) {
	h := newStacks(t)
	body, _ := json.Marshal(map[string]any{"Resources": map[string]any{"T": map[string]any{"Type": "AWS::SNS::Topic"}}})
	url := h.upload("cs.json", string(body))
	if _, err := h.invoke("CreateChangeSet", map[string]any{"StackName": "u", "ChangeSetName": "c", "ChangeSetType": "CREATE", "TemplateURL": url}); err != nil {
		t.Fatal(err)
	}
	if _, err := h.invoke("ExecuteChangeSet", map[string]any{"StackName": "u", "ChangeSetName": "c"}); err != nil {
		t.Errorf("ExecuteChangeSet of a change set made from a TemplateURL answered %v", err)
	}
	if status, _, _ := h.status("u"); status != "CREATE_COMPLETE" {
		t.Errorf("the stack is %q, want CREATE_COMPLETE", status)
	}
}

// A stack is made only under CreateStack's name rule, "alphanumeric
// characters (case sensitive) and hyphens ... can't be longer than 128
// characters" (API reference, CreateStack), by CreateStack and by a CREATE
// change set, so no stack's name holds the colon a change set's key,
// <stack>:<change set>, joins with, and retiring one stack's change sets
// touches no other's. Before, stacks a:x, 1a, a_b and one of 129 characters
// were each made.
func TestCloudFormationAChangeSetKeyIsItsStacks(t *testing.T) {
	h := newStacks(t)
	topic, _ := json.Marshal(map[string]any{"Resources": map[string]any{"T": map[string]any{"Type": "AWS::SNS::Topic"}}})
	for _, name := range []string{"a:x", "1a", "a_b", "a" + strings.Repeat("b", 128)} {
		if _, err := h.invoke("CreateStack", map[string]any{"StackName": name, "TemplateBody": string(topic)}); code(err) != "ValidationError" {
			t.Errorf("CreateStack of %q answered %v, want ValidationError", name, err)
		}
		if _, err := h.invoke("CreateChangeSet", map[string]any{"StackName": name, "ChangeSetName": "c", "ChangeSetType": "CREATE", "TemplateBody": string(topic)}); code(err) != "ValidationError" {
			t.Errorf("a CREATE change set of %q answered %v, want ValidationError", name, err)
		}
	}
	if _, err := h.invoke("CreateStack", map[string]any{"StackName": "a" + strings.Repeat("b", 127), "TemplateBody": string(topic)}); err != nil {
		t.Errorf("CreateStack of a 128-character name answered %v", err)
	}
}
