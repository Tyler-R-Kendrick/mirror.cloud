package cloudformation

import (
	"encoding/json"
	"strings"
	"testing"
)

// A change set made from a TemplateURL of a template over TemplateBody's 51,200 bytes (a TemplateURL's is 1 MB).
func TestRV14LargeTemplateURLChangeSet(t *testing.T) {
	h := newStacks(t)
	body, _ := json.Marshal(map[string]any{"Description": strings.Repeat("d", 1000),
		"Metadata":  map[string]any{"pad": strings.Repeat("x", 60_000)},
		"Resources": map[string]any{"T": map[string]any{"Type": "AWS::SNS::Topic"}}})
	url := h.upload("big.json", string(body))
	if _, err := h.invoke("CreateStack", map[string]any{"StackName": "direct", "TemplateURL": url}); err != nil {
		t.Logf("CreateStack from the URL answered %v", err)
	}
	if _, err := h.invoke("CreateChangeSet", map[string]any{"StackName": "u", "ChangeSetName": "c", "ChangeSetType": "CREATE", "TemplateURL": url}); err != nil {
		t.Fatal(err)
	}
	_, err := h.invoke("ExecuteChangeSet", map[string]any{"StackName": "u", "ChangeSetName": "c"})
	st, _, _ := h.status("u")
	if err != nil || st != "CREATE_COMPLETE" {
		t.Errorf("BUG: ExecuteChangeSet of a change set made from a %d-byte TemplateURL answered %v, stack %s", len(body), err, st)
	}
}
