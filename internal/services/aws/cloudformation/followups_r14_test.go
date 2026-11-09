package cloudformation

import (
	"context"
	"io"
	"strings"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/bundled"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
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

// countedBlobs counts the reads of each blob.
type countedBlobs struct {
	spi.BlobStore
	reads map[string]int
}

func (b *countedBlobs) Get(ctx context.Context, key string) (io.ReadSeekCloser, spi.BlobInfo, error) {
	b.reads[key]++
	return b.BlobStore.Get(ctx, key)
}

// CreateChangeSet reads a TemplateURL's object once, and checks and keeps
// that one body. Before, it read the object to check its size, and again to
// check the template, so the two could be different objects.
func TestCloudFormationAChangeSetReadsItsTemplateOnce(t *testing.T) {
	h := newStacks(t)
	url := h.upload("once.json", `{"Resources":{"T":{"Type":"AWS::SNS::Topic"}}}`)
	b := &countedBlobs{BlobStore: h.deps.Blobs, reads: map[string]int{}}
	h.deps.Blobs = b
	h.pack = New(h.deps)
	h.cfn = bundled.Handler("aws.cloudformation", h.deps)
	if _, err := h.invoke("CreateChangeSet", map[string]any{"StackName": "once", "ChangeSetName": "c", "ChangeSetType": "CREATE", "TemplateURL": url}); err != nil {
		t.Fatal(err)
	}
	for key, n := range b.reads {
		if n != 1 {
			t.Errorf("CreateChangeSet read %s %d times, want once", key, n)
		}
	}
}
