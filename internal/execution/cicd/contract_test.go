package cicd_test

import (
	"context"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/execution/cicd"
)

// runContract exercises JobControl without provider switches.
func runContract(t *testing.T, jc cicd.JobControl, label string) {
	t.Helper()
	ctx := context.Background()
	scope := cicd.Scope{Environment: "local", Account: "a1", Region: "r1", Project: "p"}

	h1, err := jc.Submit(ctx, cicd.JobSpec{
		Scope:         scope,
		RunID:         "run-1",
		AttemptID:     "att-1",
		Generation:    "g1",
		SubmissionKey: label + ":key-1",
		Mode:          "execute",
		Dialect:       "command",
	})
	if err != nil {
		t.Fatalf("%s Submit: %v", label, err)
	}
	if h1.BackendID == "" || h1.RunID != "run-1" {
		t.Fatalf("%s handle incomplete: %+v", label, h1)
	}

	hDup, err := jc.Submit(ctx, cicd.JobSpec{
		Scope:         scope,
		RunID:         "run-1",
		AttemptID:     "att-1",
		Generation:    "g1",
		SubmissionKey: label + ":key-1",
	})
	if err != nil {
		t.Fatalf("%s Submit dup: %v", label, err)
	}
	if hDup != h1 {
		t.Fatalf("%s duplicate SubmissionKey: got %+v want %+v", label, hDup, h1)
	}

	obs, err := jc.Observe(ctx, h1, "")
	if err != nil {
		t.Fatalf("%s Observe: %v", label, err)
	}
	if !obs.Terminal {
		obs, err = jc.Reconcile(ctx, h1)
		if err != nil {
			t.Fatalf("%s Reconcile: %v", label, err)
		}
	}
	if !obs.Terminal || obs.Disposition != cicd.DispositionKnown {
		t.Fatalf("%s want known terminal, got %+v", label, obs)
	}
	if obs.Receipt == nil || obs.Receipt.Key == "" {
		t.Fatalf("%s missing receipt: %+v", label, obs)
	}
	if len(obs.Events) == 0 {
		t.Fatalf("%s want events", label)
	}

	// Non-destructive cursor: re-observe from NextCursor yields no new events.
	again, err := jc.Observe(ctx, h1, obs.NextCursor)
	if err != nil {
		t.Fatalf("%s Observe cursor: %v", label, err)
	}
	if len(again.Events) != 0 {
		t.Fatalf("%s cursor replay consumed events: %+v", label, again.Events)
	}
	if !again.Terminal {
		t.Fatalf("%s cursor observe lost terminal", label)
	}

	// Cancel path on a fresh job.
	h2, err := jc.Submit(ctx, cicd.JobSpec{
		Scope:         scope,
		RunID:         "run-2",
		AttemptID:     "att-1",
		SubmissionKey: label + ":key-2",
	})
	if err != nil {
		t.Fatalf("%s Submit2: %v", label, err)
	}
	if err := jc.Cancel(ctx, h2, "test"); err != nil {
		t.Fatalf("%s Cancel: %v", label, err)
	}
	cobs, err := jc.Reconcile(ctx, h2)
	if err != nil {
		t.Fatalf("%s Reconcile cancelled: %v", label, err)
	}
	if !cobs.Terminal {
		t.Fatalf("%s cancel not terminal: %+v", label, cobs)
	}
	found := false
	for _, ev := range cobs.Events {
		if ev.Kind == "cancelled" {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("%s missing cancelled event: %+v", label, cobs.Events)
	}

	_, err = jc.Observe(ctx, cicd.JobHandle{Backend: h1.Backend, BackendID: "missing"}, "")
	ce, ok := cicd.AsError(err)
	if !ok || ce.Kind != cicd.ErrAbsent {
		t.Fatalf("%s Observe absent: %v", label, err)
	}
}

func TestJobControl_TwoFakeBackendsSameContract(t *testing.T) {
	// Two unrelated MemoryJobControl instances (different Backend names) share
	// one contract helper — no provider conditionals in the test path.
	backends := []struct {
		name string
		jc   cicd.JobControl
	}{
		{"fake-alpha", cicd.NewMemoryJobControl("fake-alpha")},
		{"fake-beta", cicd.NewMemoryJobControl("fake-beta")},
	}
	for _, b := range backends {
		t.Run(b.name, func(t *testing.T) {
			runContract(t, b.jc, b.name)
		})
	}
}

func TestMemoryJobControl_ReceiptStored(t *testing.T) {
	ctx := context.Background()
	m := cicd.NewMemoryJobControl("mem")
	h, err := m.Submit(ctx, cicd.JobSpec{RunID: "r", SubmissionKey: "k"})
	if err != nil {
		t.Fatal(err)
	}
	obs, err := m.Observe(ctx, h, "")
	if err != nil {
		t.Fatal(err)
	}
	r, ok := m.Receipt(h.BackendID)
	if !ok || obs.Receipt == nil || r.Key != obs.Receipt.Key {
		t.Fatalf("receipt not stored: mem=%v obs=%v", r, obs.Receipt)
	}
}

func TestErrorKinds(t *testing.T) {
	kinds := []cicd.ErrorKind{
		cicd.ErrValidation, cicd.ErrUnsupported, cicd.ErrUnavailable, cicd.ErrAbsent,
		cicd.ErrConflict, cicd.ErrCancelled, cicd.ErrTimeout, cicd.ErrPolicyDenied,
		cicd.ErrIntegrity, cicd.ErrOutcomeUnknown,
	}
	for _, k := range kinds {
		e := &cicd.Error{Kind: k, Op: "x", Message: "y"}
		if e.Error() == "" {
			t.Fatalf("empty error for %s", k)
		}
	}
}
