package cloudformation

import (
	"strings"
	"sync/atomic"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

// A stack takes back what its update left only under the name it left it: the stack holds parameter X as
// p1; an update that renames X to p2 fails and cannot delete p2, which is left; the next update, X as the
// stack holds it and a new topic, updates p1 in place (PutParameter with Overwrite), completes, and
// deletes p2. (Round 24: the name was dropped from the match in round 23, wrongly.)
func TestCloudFormationAStackTakesBackWhatItLeftOnlyUnderItsName(t *testing.T) {
	t.Parallel()
	h := newStacks(t)
	param := func(name, value string) map[string]any {
		return map[string]any{"Type": "AWS::SSM::Parameter", "Properties": map[string]any{"Name": name, "Type": "String", "Value": value}}
	}
	if err := rvMake(t, h, "rl", map[string]any{"Resources": map[string]any{"X": param("/rv24/p1", "v1")}}); err != nil {
		t.Fatal(err)
	}
	off := faultStore(h, func(op, col, k string) bool { return strings.Contains(op, "delete") && strings.Contains(k, "rv24/p2") })
	_, err := h.invoke("UpdateStack", map[string]any{"StackName": "rl", "TemplateBody": rv19Body(map[string]any{"Resources": map[string]any{"X": param("/rv24/p2", "v2"), "Z": rv22Bad("rv24-nofifo", "X")}})})
	off()
	if err == nil || !strings.Contains(spi.JSONText(h.record("rl")["Left"]), "/rv24/p2") {
		t.Fatalf("the renaming update answered %v and left %v; want it failed, leaving /rv24/p2", err, h.record("rl")["Left"])
	}
	if _, err := h.invoke("UpdateStack", map[string]any{"StackName": "rl", "TemplateBody": rv19Body(map[string]any{"Resources": map[string]any{"X": param("/rv24/p1", "v3"), "T": rv22Topic}})}); err != nil {
		t.Fatalf("the update that keeps X under its name answered %v", err)
	}
	if rec := h.record("rl"); rec["Status"] != "UPDATE_COMPLETE" || rec["Left"] != nil {
		t.Errorf("the stack is %v, having left %v; want UPDATE_COMPLETE, with nothing left", rec["Status"], rec["Left"])
	}
	if out, err := h.service("aws.ssm", "GetParameter", map[string]any{"Name": "/rv24/p1"}); err != nil || asMap(out["Parameter"])["Value"] != "v3" {
		t.Errorf("parameter /rv24/p1 is %v (%v); want its value v3", out["Parameter"], err)
	}
	if _, err := h.service("aws.ssm", "GetParameter", map[string]any{"Name": "/rv24/p2"}); err == nil {
		t.Errorf("parameter /rv24/p2, which the failed update left, survives the next update")
	}
}

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
