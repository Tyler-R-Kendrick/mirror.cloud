package cloudformation

import (
	"encoding/json"
	"reflect"
	"regexp"
	"strings"
	"testing"
)

// An update compares each resource's properties with what the stack's
// stored template gave it, and that template's Fn::FindInMaps read its own
// Mappings: a mapping whose value the update changes changes the resource.
// Before the Mappings were read from the template once per operation, the
// stored Refs carried the old ones; reading the new template's for the old
// properties would find nothing changed.
func TestCloudFormationAStoredTemplateReadsItsOwnMappings(t *testing.T) {
	h := newStacks(t)
	template := func(value string) string {
		body, _ := json.Marshal(map[string]any{"Mappings": map[string]any{"M": map[string]any{"k": map[string]any{"a": value}}},
			"Resources": map[string]any{"P": map[string]any{"Type": "AWS::SSM::Parameter", "Properties": map[string]any{"Name": "own-mappings", "Type": "String",
				"Value": map[string]any{"Fn::FindInMap": []any{"M", "k", "a"}}}}}})
		return string(body)
	}
	if _, err := h.invoke("CreateStack", map[string]any{"StackName": "own", "TemplateBody": template("one")}); err != nil {
		t.Fatal(err)
	}
	if _, err := h.invoke("UpdateStack", map[string]any{"StackName": "own", "TemplateBody": template("two")}); err != nil {
		t.Fatal(err)
	}
	got, err := h.service("aws.ssm", "GetParameter", map[string]any{"Name": "own-mappings"})
	if v := str(asMap(got["Parameter"])["Value"]); err != nil || v != "two" {
		t.Errorf("after the update the parameter is %q (%v), want two", v, err)
	}
}

// CreateChangeSet checks the template as its execution will read it: an
// UPDATE change set's UsePreviousValue takes the stack's value, so the
// change set is CREATE_COMPLETE, not FAILED, and DescribeChangeSet answers
// the request's own Parameters. More than five NotificationARNs are refused,
// as CreateStack refuses them, and a CREATE change set's StackId is a stack
// id, arn:...:stack/<name>/<id>, minted for the stack its execution makes.
// Before, the check read UsePreviousValue as no value, and the change set
// FAILED with "Parameters: [Key] must have values"; the StackId was
// arn:...:stack/<name>, and NotificationARNs were not counted.
func TestCloudFormationAChangeSetIsCheckedAsItsExecutionReadsIt(t *testing.T) {
	h := newStacks(t)
	body, _ := json.Marshal(map[string]any{"Parameters": map[string]any{"Key": map[string]any{"Type": "String"}},
		"Resources": map[string]any{"T": map[string]any{"Type": "AWS::SNS::Topic"}}, "Outputs": map[string]any{"K": map[string]any{"Value": map[string]any{"Ref": "Key"}}}})
	if _, err := h.invoke("CreateStack", map[string]any{"StackName": "checked", "TemplateBody": string(body), "Parameters": givenParams("Key", "kept")}); err != nil {
		t.Fatal(err)
	}
	previous := []any{map[string]any{"ParameterKey": "Key", "UsePreviousValue": "true"}}
	if _, err := h.invoke("CreateChangeSet", map[string]any{"StackName": "checked", "ChangeSetName": "prev", "TemplateBody": string(body), "Parameters": previous}); err != nil {
		t.Fatal(err)
	}
	desc, err := h.invoke("DescribeChangeSet", map[string]any{"StackName": "checked", "ChangeSetName": "prev"})
	if err != nil || desc["Status"] != "CREATE_COMPLETE" || !reflect.DeepEqual(desc["Parameters"], givenParams("Key", "kept")) {
		t.Errorf("DescribeChangeSet = %v, %v, want CREATE_COMPLETE with the value read when it was made", desc, err)
	}
	if _, err := h.invoke("ExecuteChangeSet", map[string]any{"StackName": "checked", "ChangeSetName": "prev"}); err != nil {
		t.Fatal(err)
	}
	if got := h.outputs("checked")["K"]; got != "kept" {
		t.Errorf("after the change set, K = %v, want kept", got)
	}
	out, err := h.invoke("CreateChangeSet", map[string]any{"StackName": "fresh", "ChangeSetName": "make", "ChangeSetType": "CREATE", "TemplateBody": string(body), "Parameters": givenParams("Key", "k")})
	if id := str(out["StackId"]); err != nil || !regexp.MustCompile(`^arn:aws:cloudformation:us-east-1:000000000000:stack/fresh/[0-9a-f]{8}$`).MatchString(id) {
		t.Errorf("a CREATE change set answered the StackId %q (%v), want arn:...:stack/fresh/<id>", id, err)
	}
	six := []any{"a", "b", "c", "d", "e", "f"}
	if _, err := h.invoke("CreateChangeSet", map[string]any{"StackName": "checked", "ChangeSetName": "six", "TemplateBody": string(body), "NotificationARNs": six}); code(err) != "ValidationError" || !strings.Contains(err.Error(), "Member must have length less than or equal to 5") {
		t.Errorf("CreateChangeSet with six NotificationARNs answered %v, want ValidationError", err)
	}
}

// A change set the template check FAILs is UNAVAILABLE, the model's
// ExecutionStatus for one that cannot be executed, and a parameter the
// template does not declare is refused by CreateChangeSet itself, which
// reads the template: "Parameters: [X] do not exist in the template".
func TestCloudFormationAFailedChangeSetIsUnavailable(t *testing.T) {
	h := newStacks(t)
	body, _ := json.Marshal(map[string]any{"Parameters": map[string]any{"Key": map[string]any{"Type": "String"}},
		"Resources": map[string]any{"T": map[string]any{"Type": "AWS::SNS::Topic"}}})
	if _, err := h.invoke("CreateChangeSet", map[string]any{"StackName": "unavail", "ChangeSetName": "nokey", "ChangeSetType": "CREATE", "TemplateBody": string(body)}); err != nil {
		t.Fatal(err)
	}
	if desc, err := h.invoke("DescribeChangeSet", map[string]any{"StackName": "unavail", "ChangeSetName": "nokey"}); err != nil || desc["Status"] != "FAILED" || desc["ExecutionStatus"] != "UNAVAILABLE" {
		t.Errorf("DescribeChangeSet of a change set without its parameter's value = %v, %v, want FAILED and UNAVAILABLE", desc, err)
	}
	if _, err := h.invoke("CreateChangeSet", map[string]any{"StackName": "unavail", "ChangeSetName": "extra", "ChangeSetType": "CREATE", "TemplateBody": string(body),
		"Parameters": givenParams("Key", "k", "Nope", "n")}); code(err) != "ValidationError" || !strings.Contains(err.Error(), "Parameters: [Nope] do not exist in the template") {
		t.Errorf("CreateChangeSet with an undeclared parameter answered %v, want ValidationError", err)
	}
}
