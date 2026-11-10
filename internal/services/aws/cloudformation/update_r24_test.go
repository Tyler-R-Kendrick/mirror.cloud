package cloudformation

import (
	"strings"
	"sync/atomic"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

// A change set is refused for every stack UpdateStack refuses, a failed one among them: one a process
// left UPDATE_IN_PROGRESS, settled UPDATE_ROLLBACK_FAILED, takes CreateChangeSet no more than UpdateStack.
// (Round 24.)
func TestCloudFormationAChangeSetIsRefusedForAFailedStack(t *testing.T) {
	t.Parallel()
	h := newStacks(t)
	for name, status := range map[string]string{"cu": "UPDATE_IN_PROGRESS", "cr": "UPDATE_ROLLBACK_FAILED", "cd": "DELETE_FAILED", "cb": "ROLLBACK_COMPLETE"} {
		rec := stack{Name: name, ID: "arn:aws:cloudformation:us-east-1:000000000000:stack/" + name + "/1", Status: status, Template: topicTpl(), Resources: []res{}, Outputs: []any{}}
		_ = h.deps.Store.Scope(h.id.Account, h.id.Region).Collection("cfn").Put(h.ctx, name, []byte(spi.JSONText(rec)))
		settled := map[string]string{"UPDATE_IN_PROGRESS": "UPDATE_ROLLBACK_FAILED"}[status] // as the takeover settles one left in progress
		if settled == "" {
			settled = status
		}
		for _, op := range []string{"CreateChangeSet", "UpdateStack"} {
			_, err := h.invoke(op, map[string]any{"StackName": name, "ChangeSetName": "c", "TemplateBody": topicTpl("N")})
			if err == nil || !strings.Contains(err.Error(), "is in "+settled+" state and can not be updated") {
				t.Errorf("%s of a stack %s answered %v; want it refused, in %s", op, status, err, settled)
			}
		}
	}
}

// What operations keep is counted as it is kept and as it is written, and bounded: an update that could
// not settle adds one, the takeover that writes it takes it away, and with the count at its bound nothing
// is kept and the count stays. Not parallel: the count is the process's. (Round 24.)
func TestCloudFormationWhatOperationsKeepIsCounted(t *testing.T) {
	h := newStacks(t)
	unsettled := func(name string) { // an update of a new stack whose own writes all panic after its reservation
		t.Helper()
		if err := rvMake(t, h, name, map[string]any{"Resources": map[string]any{"K": rv22Topic}}); err != nil {
			t.Fatal(err)
		}
		var writes atomic.Int32
		off := faultStore(h, func(o, col, k string) bool { return o == "txput" && col == "cfn" && k == name && writes.Add(1) >= 2 })
		defer off()
		_ = invoked(t, h, "UpdateStack", map[string]any{"StackName": name, "TemplateBody": rv19Body(map[string]any{"Resources": map[string]any{"K": rv22Topic, "N": rv22Topic}})})
	}
	before := opKept.Load()
	unsettled("k1")
	if got := opKept.Load(); got != before+1 {
		t.Errorf("after an update that could not settle the count is %d; want %d", got, before+1)
	}
	if _, err := h.invoke("DeleteStack", map[string]any{"StackName": "k1"}); err != nil {
		t.Fatal(err)
	}
	if got := opKept.Load(); got != before {
		t.Errorf("after the takeover wrote what was kept the count is %d; want %d", got, before)
	}
	opKept.Store(1024)
	defer opKept.Store(before)
	unsettled("k2")
	if got := opKept.Load(); got != 1024 {
		t.Errorf("at its bound, after an update that could not settle, the count is %d; want 1024, nothing kept", got)
	}
	if _, err := h.invoke("DeleteStack", map[string]any{"StackName": "k2"}); err != nil { // taken over all the same, as its operation's failure
		t.Errorf("DeleteStack of the stack whose record was not kept answered %v", err)
	}
	if got := opKept.Load(); got != 1024 {
		t.Errorf("after a takeover with nothing kept the count is %d; want 1024", got)
	}
}
