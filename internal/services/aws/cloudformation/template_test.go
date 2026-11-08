package cloudformation

import (
	"cmp"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/s3"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

// upload puts body in the templates bucket under key and answers its TemplateURL.
func (h *stacks) upload(key, body string) string {
	h.t.Helper()
	s3Pack := s3.New(h.deps)
	if _, err := s3Pack.Invoke(h.ctx, &spi.Request{Identity: h.id, Operation: "CreateBucket", Input: map[string]any{"Bucket": "templates"}}); err != nil && code(err) != "BucketAlreadyOwnedByYou" {
		h.t.Fatal(err)
	}
	in := map[string]any{"Bucket": "templates", "Key": key}
	if _, err := s3Pack.Invoke(h.ctx, &spi.Request{Identity: h.id, Operation: "PutObject", Input: in, Body: io.NopCloser(strings.NewReader(body))}); err != nil {
		h.t.Fatal(err)
	}
	return "s3://templates/" + key
}

// padded is a queue's template, YAML or JSON, padded with a comment or a
// Description to exactly size bytes.
func padded(size int, asJSON bool) string {
	if asJSON {
		head, tail := `{"Description":"`, `","Resources":{"Q":{"Type":"AWS::SQS::Queue"}}}`
		return head + strings.Repeat("x", size-len(head)-len(tail)) + tail
	}
	head := "Resources:\n  Q:\n    Type: AWS::SQS::Queue\n#"
	return head + strings.Repeat("x", size-len(head)-1) + "\n"
}

// CloudFormation bounds a template by its size, not by its shape: "Template
// body size in a request ... 51,200 bytes" and "Template body size in an
// Amazon S3 object ... 1 MB" (User Guide, Understand CloudFormation quotas);
// TemplateBody is "a maximum length of 51,200 bytes" and TemplateURL's
// template "max size: 1 MB" (API reference, CreateStack). A body over 51,200
// bytes is the member's constraint error, ValidationError, in YAML and JSON
// alike, on every operation that takes one; a body of exactly 51,200 bytes is
// a template. The quota page writes 204,800 bytes as "204.8 KB", so its 1 MB
// is 1,000,000 bytes: an S3 object of 1,000,000 bytes is a template and one
// of 1,000,001 is not. 200 mappings of 100 keys of two attributes and a
// queue, 642 KB, within the quotas' "200 mappings" and "200 mapping
// attributes", is a template through TemplateURL and too large as a
// TemplateBody. Before, that template was refused for its nodes wherever it
// came from, and a body of any size was read.
func TestCloudFormationBoundsATemplateBySize(t *testing.T) {
	h := newStacks(t)
	var b strings.Builder
	b.WriteString("Mappings:\n")
	for m := range 200 {
		fmt.Fprintf(&b, "  M%d:\n", m)
		for k := range 100 {
			fmt.Fprintf(&b, "    K%d: {A: 012345, B: 012345}\n", k)
		}
	}
	b.WriteString("Resources:\n  Q:\n    Type: AWS::SQS::Queue\n")
	mappings := b.String()
	if n := len(mappings); n < 600_000 || n > 700_000 {
		t.Fatalf("the mappings template is %d bytes, want about 642 KB", n)
	}
	url := h.upload("mappings.yaml", mappings)
	for _, op := range []string{"ValidateTemplate", "GetTemplateSummary", "CreateStack"} {
		if _, err := h.invoke(op, map[string]any{"StackName": "big", "TemplateURL": url}); err != nil {
			t.Errorf("%s of the 642 KB template through TemplateURL answered %v", op, err)
		}
	}
	for j, c := range []struct {
		name, body string
		ok         bool
	}{
		{"the mappings", mappings, false},
		{"51,200 bytes of YAML", padded(51_200, false), true},
		{"51,201 bytes of YAML", padded(51_201, false), false},
		{"51,200 bytes of JSON", padded(51_200, true), true},
		{"51,201 bytes of JSON", padded(51_201, true), false},
	} {
		for i, op := range []string{"ValidateTemplate", "GetTemplateSummary", "CreateStack", "UpdateStack", "CreateChangeSet"} {
			stack := map[string]string{"UpdateStack": "big"}[op] // an update of the stack made above
			in := map[string]any{"StackName": cmp.Or(stack, fmt.Sprintf("s%d-%d", j, i)), "ChangeSetName": "cs", "TemplateBody": c.body}
			_, err := h.invoke(op, in)
			if c.ok && err != nil {
				t.Errorf("%s of %s answered %v", op, c.name, err)
			} else if !c.ok && (code(err) != "ValidationError" || !strings.Contains(err.Error(), "Member must have length less than or equal to 51200")) {
				t.Errorf("%s of %s as a TemplateBody answered %v, want ValidationError: at most 51200", op, c.name, err)
			}
		}
	}
	for size, ok := range map[int]bool{1_000_000: true, 1_000_001: false} {
		url := h.upload(fmt.Sprintf("padded-%d.yaml", size), padded(size, false))
		_, err := h.invoke("ValidateTemplate", map[string]any{"TemplateURL": url})
		if ok && err != nil {
			t.Errorf("a template of %d bytes through TemplateURL answered %v", size, err)
		} else if !ok && (code(err) != "ValidationError" || !strings.Contains(err.Error(), "1 MB")) {
			t.Errorf("a template of %d bytes through TemplateURL answered %v, want ValidationError: 1 MB", size, err)
		}
	}
}

// yaml.v3 and encoding/json each refuse nesting deeper than 10,000 levels
// ("exceeded max depth"), so no walk of a template goes deeper than that, and
// Go's stack grows to a gigabyte: a template 9,990 levels deep is read and
// made, in YAML and JSON, and one 10,001 levels deep is ValidationError, the
// parser's. Before, any template deeper than 100 levels was refused.
func TestCloudFormationReadsADeepTemplate(t *testing.T) {
	h := newStacks(t)
	deep := func(levels int, asJSON bool) string {
		nest := strings.Repeat("[", levels) + strings.Repeat("]", levels)
		if asJSON {
			raw, _ := json.Marshal(map[string]any{"Resources": map[string]any{"Q": map[string]any{"Type": "AWS::SQS::Queue", "Properties": map[string]any{"Tags": "@"}}}})
			return strings.Replace(string(raw), `"@"`, nest, 1)
		}
		return "Resources:\n  Q:\n    Type: AWS::SQS::Queue\n    Properties:\n      Tags: " + nest + "\n"
	}
	for _, asJSON := range []bool{false, true} {
		for i, op := range []string{"ValidateTemplate", "GetTemplateSummary", "CreateStack"} {
			in := map[string]any{"StackName": fmt.Sprintf("deep%d%t", i, asJSON), "TemplateBody": deep(9_990, asJSON)}
			if _, err := h.invoke(op, in); err != nil {
				t.Errorf("%s of a template 9,990 levels deep (JSON %t) answered %v", op, asJSON, err)
			}
			in["TemplateBody"] = deep(10_001, asJSON)
			if _, err := h.invoke(op, in); code(err) != "ValidationError" || !strings.Contains(err.Error(), "depth") {
				t.Errorf("%s of a template 10,001 levels deep (JSON %t) answered %v, want ValidationError: depth", op, asJSON, err)
			}
		}
	}
}
