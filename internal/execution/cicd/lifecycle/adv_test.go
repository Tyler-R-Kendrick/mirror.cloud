package lifecycle_test

import (
	"errors"
	"sync"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/execution/cicd/lifecycle"
)

// ADV-SUBMIT-RACE: concurrent identical SubmissionKey → one intent.
func TestADV_SUBMIT_RACE(t *testing.T) {
	TestConcurrentIdenticalSubmissionKeyOneIntent(t)
}

// ADV-STALE-WORKER: stale generation cannot complete.
func TestADV_STALE_WORKER(t *testing.T) {
	TestStaleGenerationCannotComplete(t)
}

// ADV-NOT-IDEMPOTENT: different fingerprints on same key conflict (not collapsed).
func TestADV_NOT_IDEMPOTENT(t *testing.T) {
	TestSameKeyDifferentFingerprintConflict(t)
}

// ADV-CANCEL-RACE: cancel overlapping complete — exactly one terminal writer wins.
func TestADV_CANCEL_RACE(t *testing.T) {
	s := lifecycle.NewMemoryStore()
	res, err := s.Admit(baseReq("sk-cancel", "fp-c"))
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	errs := make([]error, 2)
	wg.Add(2)
	go func() {
		defer wg.Done()
		errs[0] = s.CancelAttempt(res.Attempt.ID, res.Attempt.Generation, "race")
	}()
	go func() {
		defer wg.Done()
		errs[1] = s.CompleteAttempt(res.Attempt.ID, res.Attempt.Generation, lifecycle.ExecutionReceipt{
			Outcome: "succeeded", Conclusion: "success",
		})
	}()
	wg.Wait()
	okCancel, okComplete := errs[0] == nil, errs[1] == nil
	if okCancel == okComplete {
		t.Fatalf("want exactly one winner; cancel=%v complete=%v", errs[0], errs[1])
	}
	if !okCancel && !errors.Is(errs[0], lifecycle.ErrAlreadyTerminal) {
		t.Fatalf("losing cancel: %v", errs[0])
	}
	if !okComplete && !errors.Is(errs[1], lifecycle.ErrAlreadyTerminal) {
		t.Fatalf("losing complete: %v", errs[1])
	}
	rcpt, has := s.GetReceipt(res.Attempt.ID)
	if !has {
		t.Fatal("terminal attempt must have receipt")
	}
	if okComplete && rcpt.Outcome != "succeeded" {
		t.Fatalf("complete won: receipt %#v", rcpt)
	}
	if okCancel && rcpt.Outcome != "cancelled" {
		t.Fatalf("cancel won: receipt %#v", rcpt)
	}
}
