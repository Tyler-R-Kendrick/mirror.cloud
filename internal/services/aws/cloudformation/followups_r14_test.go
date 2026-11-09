package cloudformation

import (
	"strings"
	"testing"
)

// A template that does not parse is the change set's error, FAILED with it as
// its StatusReason ("if your attempt to create a change set failed,
// CloudFormation shows the error message", StatusReason), even when the
// request gives parameters: the parameters are checked against a template
// only once it parses. Before, the parameters were checked against nothing
// first, and CreateChangeSet refused them as not in the template.
func TestCloudFormationAChangeSetParsesBeforeItReadsParameters(t *testing.T) {
	h := newStacks(t)
	if _, err := h.invoke("CreateChangeSet", map[string]any{"StackName": "p", "ChangeSetName": "c", "ChangeSetType": "CREATE",
		"TemplateBody": "{not a template", "Parameters": givenParams("Key", "v")}); err != nil {
		t.Fatalf("CreateChangeSet of a template that does not parse answered %v, want a FAILED change set", err)
	}
	if out, err := h.invoke("DescribeChangeSet", map[string]any{"StackName": "p", "ChangeSetName": "c"}); err != nil || out["Status"] != "FAILED" || strings.Contains(str(out["StatusReason"]), "Parameters") {
		t.Errorf("DescribeChangeSet answered %v, %v, want FAILED with the template's error", out, err)
	}
}
