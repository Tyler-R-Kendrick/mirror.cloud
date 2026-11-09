package cloudformation

import (
	"strings"
	"testing"
)

func TestRV15UpdateMissingPrecedence(t *testing.T) {
	h := newStacks(t)
	for name, in := range map[string]map[string]any{
		"param": {"StackName": "nope", "TemplateBody": `{"Parameters":{"P":{"Type":"String"}},"Resources":{"T":{"Type":"AWS::SNS::Topic"}}}`},
		"prev":  {"StackName": "nope", "TemplateBody": `{"Parameters":{"P":{"Type":"String"}},"Resources":{"T":{"Type":"AWS::SNS::Topic"}}}`, "Parameters": []any{map[string]any{"ParameterKey": "P", "UsePreviousValue": true}}},
		"iam":   {"StackName": "nope", "TemplateBody": `{"Resources":{"R":{"Type":"AWS::IAM::Role","Properties":{"AssumeRolePolicyDocument":{}}}}}`},
	} {
		_, err := h.invoke("UpdateStack", in)
		t.Logf("%s: %v", name, err)
		if err == nil || !strings.Contains(err.Error(), "Stack [nope] does not exist") {
			t.Errorf("BUG %s: UpdateStack of a missing stack answered %v, want that it does not exist first", name, err)
		}
	}
}
