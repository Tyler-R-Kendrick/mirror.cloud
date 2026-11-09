package cloudformation

import (
	"encoding/json"
	"reflect"
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
