package cloudformation

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

// A change set is executed with what CreateChangeSet took that CreateStack
// and UpdateStack take: its parameters, a required one and an overridden
// Default, UsePreviousValue on an UPDATE change set, its NotificationARNs,
// tags and capabilities; DescribeChangeSet answers them, Parameters as
// "A list of Parameter structures that describes the input parameters and
// their values used to create the change set" (API reference). A CREATE
// change set makes the stack under the StackId it answered, the id its
// AWS::StackId reads (NIT: minted at CreateChangeSet). Before, only the
// TemplateBody was kept, so the execution answered "Parameters: [Key] must
// have values", and a CREATE change set was executed as an update.
func TestCloudFormationAChangeSetIsExecutedWithItsRequest(t *testing.T) {
	h := newStacks(t)
	body, _ := json.Marshal(map[string]any{
		"Parameters": map[string]any{"Key": map[string]any{"Type": "String"}, "Def": map[string]any{"Type": "String", "Default": "dflt"}},
		"Resources":  map[string]any{"T": map[string]any{"Type": "AWS::SNS::Topic"}},
		"Outputs": map[string]any{"K": map[string]any{"Value": map[string]any{"Ref": "Key"}}, "D": map[string]any{"Value": map[string]any{"Ref": "Def"}},
			"Id": map[string]any{"Value": map[string]any{"Ref": "AWS::StackId"}},
			"N":  map[string]any{"Value": map[string]any{"Fn::Join": []any{",", map[string]any{"Ref": "AWS::NotificationARNs"}}}}}})
	a := "arn:aws:sns:us-east-1:000000000000:a"
	params, tags, caps := givenParams("Key", "given", "Def", "over"), []any{map[string]any{"Key": "k", "Value": "v"}}, []any{"CAPABILITY_IAM"}
	out, err := h.invoke("CreateChangeSet", map[string]any{"StackName": "cs", "ChangeSetName": "make", "ChangeSetType": "CREATE", "TemplateBody": string(body),
		"Parameters": params, "NotificationARNs": []any{a}, "Tags": tags, "Capabilities": caps})
	if err != nil {
		t.Fatal(err)
	}
	minted := str(out["StackId"])
	desc, err := h.invoke("DescribeChangeSet", map[string]any{"StackName": "cs", "ChangeSetName": "make"})
	if err != nil || !reflect.DeepEqual(desc["Parameters"], params) || !reflect.DeepEqual(desc["NotificationARNs"], []any{a}) ||
		!reflect.DeepEqual(desc["Tags"], tags) || !reflect.DeepEqual(desc["Capabilities"], caps) || desc["StackId"] != minted {
		t.Errorf("DescribeChangeSet = %v, %v, want the request's Parameters, NotificationARNs, Tags, Capabilities and StackId %s", desc, err, minted)
	}
	if _, err := h.invoke("ExecuteChangeSet", map[string]any{"StackName": "cs", "ChangeSetName": "make"}); err != nil {
		t.Fatalf("ExecuteChangeSet of the CREATE change set answered %v", err)
	}
	if got := h.outputs("cs"); got["K"] != "given" || got["D"] != "over" || got["Id"] != minted || got["N"] != a {
		t.Errorf("outputs of the executed CREATE change set = %v, want K given, D over, Id %s and N %s", got, minted, a)
	}
	if st := h.record("cs"); st["ID"] != minted || st["Status"] != "CREATE_COMPLETE" || !reflect.DeepEqual(st["NotificationARNs"], []any{a}) {
		t.Errorf("the stack made is %v, %v, want CREATE_COMPLETE under the id %s the change set answered, and its NotificationARNs", st["ID"], st["Status"], minted)
	}
	previous := []any{map[string]any{"ParameterKey": "Key", "UsePreviousValue": "true"}, map[string]any{"ParameterKey": "Def", "ParameterValue": "again"}}
	out, err = h.invoke("CreateChangeSet", map[string]any{"StackName": "cs", "ChangeSetName": "change", "TemplateBody": string(body), "Parameters": previous})
	if err != nil || out["StackId"] != minted {
		t.Fatalf("CreateChangeSet of an UPDATE change set answered %v, %v, want the stack's id %s", out, err, minted)
	}
	if _, err := h.invoke("ExecuteChangeSet", map[string]any{"StackName": "cs", "ChangeSetName": "change"}); err != nil {
		t.Fatalf("ExecuteChangeSet of the UPDATE change set answered %v", err)
	}
	if got := h.outputs("cs"); got["K"] != "given" || got["D"] != "again" || got["Id"] != minted || got["N"] != a {
		t.Errorf("outputs of the executed UPDATE change set = %v, want K given (UsePreviousValue), D again, Id %s and N %s kept", got, minted, a)
	}
	if status, _, _ := h.status("cs"); status != "UPDATE_COMPLETE" {
		t.Errorf("after the UPDATE change set the stack is %s, want UPDATE_COMPLETE", status)
	}
}

// A change set executes once. "When you execute a change set, CloudFormation
// deletes all other change sets associated with the stack because they
// aren't valid for the updated stack" (API reference, ExecuteChangeSet), and
// one executed is InvalidChangeSetStatus, "The specified change set can't be
// used to update the stack". DescribeChangeSet answers its ExecutionStatus,
// AVAILABLE and then EXECUTE_COMPLETE, or OBSOLETE "because the stack was
// already updated" (DescribeChangeSet). A UsePreviousValue is read when the
// change set is made: its Parameters are "the input parameters and their
// values used to create the change set" (DescribeChangeSet). Before, a
// second change set executed after the first reverted it, one change set
// executed any number of times, and one made before an UpdateStack undid it.
func TestCloudFormationAChangeSetExecutesOnce(t *testing.T) {
	h := newStacks(t)
	body, _ := json.Marshal(map[string]any{"Parameters": map[string]any{"Key": map[string]any{"Type": "String"}},
		"Resources": map[string]any{"T": map[string]any{"Type": "AWS::SNS::Topic"}}, "Outputs": map[string]any{"K": map[string]any{"Value": map[string]any{"Ref": "Key"}}}})
	create := func(cs string, params []any) {
		t.Helper()
		if _, err := h.invoke("CreateChangeSet", map[string]any{"StackName": "once", "ChangeSetName": cs, "TemplateBody": string(body), "Parameters": params}); err != nil {
			t.Fatal(err)
		}
	}
	status := func(cs string) any {
		out, err := h.invoke("DescribeChangeSet", map[string]any{"StackName": "once", "ChangeSetName": cs})
		if err != nil {
			return err
		}
		return out["ExecutionStatus"]
	}
	if _, err := h.invoke("CreateStack", map[string]any{"StackName": "once", "TemplateBody": string(body), "Parameters": givenParams("Key", "first")}); err != nil {
		t.Fatal(err)
	}
	create("a", givenParams("Key", "a"))
	create("b", givenParams("Key", "b"))
	create("prev", []any{map[string]any{"ParameterKey": "Key", "UsePreviousValue": "true"}})
	if got := status("a"); got != "AVAILABLE" {
		t.Errorf("a's ExecutionStatus = %v, want AVAILABLE", got)
	}
	desc, _ := h.invoke("DescribeChangeSet", map[string]any{"StackName": "once", "ChangeSetName": "prev"})
	if want := givenParams("Key", "first"); !reflect.DeepEqual(desc["Parameters"], want) {
		t.Errorf("prev's Parameters = %v, want %v, the value read when it was made", desc["Parameters"], want)
	}
	if _, err := h.invoke("ExecuteChangeSet", map[string]any{"StackName": "once", "ChangeSetName": "b"}); err != nil {
		t.Fatal(err)
	}
	if got := status("b"); got != "EXECUTE_COMPLETE" {
		t.Errorf("b's ExecutionStatus = %v, want EXECUTE_COMPLETE", got)
	}
	for _, cs := range []string{"a", "prev"} {
		if _, err := h.invoke("ExecuteChangeSet", map[string]any{"StackName": "once", "ChangeSetName": cs}); code(err) != "ChangeSetNotFound" {
			t.Errorf("ExecuteChangeSet of %s after b answered %v, want ChangeSetNotFound: it was deleted", cs, err)
		}
	}
	if _, err := h.invoke("ExecuteChangeSet", map[string]any{"StackName": "once", "ChangeSetName": "b"}); code(err) != "InvalidChangeSetStatus" {
		t.Errorf("a second ExecuteChangeSet of b answered %v, want InvalidChangeSetStatus", err)
	}
	if got := h.outputs("once")["K"]; got != "b" {
		t.Errorf("K = %v, want b", got)
	}
	create("later", []any{map[string]any{"ParameterKey": "Key", "UsePreviousValue": true}})
	if _, err := h.invoke("UpdateStack", map[string]any{"StackName": "once", "TemplateBody": string(body), "Parameters": givenParams("Key", "c")}); err != nil {
		t.Fatal(err)
	}
	if got := status("later"); got != "OBSOLETE" {
		t.Errorf("after an UpdateStack, later's ExecutionStatus = %v, want OBSOLETE", got)
	}
	if _, err := h.invoke("ExecuteChangeSet", map[string]any{"StackName": "once", "ChangeSetName": "later"}); code(err) != "InvalidChangeSetStatus" {
		t.Errorf("ExecuteChangeSet of an obsolete change set answered %v, want InvalidChangeSetStatus", err)
	}
	if got := h.outputs("once")["K"]; got != "c" {
		t.Errorf("K = %v, want c, the update's", got)
	}
}

// "If you create a change set for a new stack, CloudFormation creates a stack
// with a unique stack ID, but no template or resources. The stack will be in
// the REVIEW_IN_PROGRESS state until you execute the change set", and "You
// can't use the UPDATE type to create a change set for a new stack or the
// CREATE type to create a change set for an existing stack" (API reference,
// CreateChangeSet). So a second CREATE change set for the name targets that
// stack, CreateStack of the name is AlreadyExistsException, the change set's
// execution makes the stack under its id, and DeleteStack of a stack in
// review deletes it. Before, a CREATE change set made no stack, and an
// UPDATE change set for a stack that did not exist was made.
func TestCloudFormationACreateChangeSetMakesAStackInReview(t *testing.T) {
	h := newStacks(t)
	body, _ := json.Marshal(map[string]any{"Resources": map[string]any{"T": map[string]any{"Type": "AWS::SNS::Topic"}},
		"Outputs": map[string]any{"Id": map[string]any{"Value": map[string]any{"Ref": "AWS::StackId"}}}})
	changeSet := func(stack, cs, typ string) (map[string]any, error) {
		return h.invoke("CreateChangeSet", map[string]any{"StackName": stack, "ChangeSetName": cs, "ChangeSetType": typ, "TemplateBody": string(body)})
	}
	first, err := changeSet("review", "one", "CREATE")
	if err != nil {
		t.Fatal(err)
	}
	if status, _, err := h.status("review"); err != nil || status != "REVIEW_IN_PROGRESS" || h.record("review")["ID"] != first["StackId"] {
		t.Errorf("after a CREATE change set the stack is %q (%v) under %v, want REVIEW_IN_PROGRESS under %v", status, err, h.record("review")["ID"], first["StackId"])
	}
	if second, err := changeSet("review", "two", "CREATE"); err != nil || second["StackId"] != first["StackId"] {
		t.Errorf("a second CREATE change set answered %v, %v, want the stack's id %v", second, err, first["StackId"])
	}
	if _, err := h.invoke("CreateStack", map[string]any{"StackName": "review", "TemplateBody": string(body)}); code(err) != "AlreadyExistsException" {
		t.Errorf("CreateStack of a stack in review answered %v, want AlreadyExistsException", err)
	}
	if _, err := changeSet("review", "upd", "UPDATE"); code(err) != "ValidationError" || !strings.Contains(err.Error(), "REVIEW_IN_PROGRESS state and can not be updated") {
		t.Errorf("an UPDATE change set for a stack in review answered %v, want ValidationError", err)
	}
	if _, err := changeSet("nowhere", "upd", ""); code(err) != "ValidationError" || !strings.Contains(err.Error(), "Stack [nowhere] does not exist") {
		t.Errorf("an UPDATE change set for a stack that does not exist answered %v, want ValidationError", err)
	}
	if _, err := h.invoke("ExecuteChangeSet", map[string]any{"StackName": "review", "ChangeSetName": "two"}); err != nil {
		t.Fatal(err)
	}
	if status, _, _ := h.status("review"); status != "CREATE_COMPLETE" || h.outputs("review")["Id"] != first["StackId"] {
		t.Errorf("after the execution the stack is %s, AWS::StackId %v, want CREATE_COMPLETE under %v", status, h.outputs("review")["Id"], first["StackId"])
	}
	if _, err := changeSet("review", "again", "CREATE"); code(err) != "AlreadyExistsException" {
		t.Errorf("a CREATE change set for a stack that exists answered %v, want AlreadyExistsException", err)
	}
	if _, err := changeSet("abandoned", "one", "CREATE"); err != nil {
		t.Fatal(err)
	}
	if _, err := h.invoke("DeleteStack", map[string]any{"StackName": "abandoned"}); err != nil {
		t.Fatal(err)
	}
	if _, left := h.stored("abandoned"); left {
		t.Errorf("DeleteStack of a stack in review left it")
	}
}
