package lifecycle_test

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/execution/cicd"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/execution/cicd/lifecycle"
)

func baseReq(key, fp string) lifecycle.AdmitRequest {
	return lifecycle.AdmitRequest{
		Scope: cicd.Scope{
			Environment: "local",
			Account:     "acct",
			Project:     "proj",
			Repository:  "repo",
			TrustDomain: "test",
		},
		SubmissionKey: key,
		Fingerprint:   fp,
		Mode:          "process",
		Profile:       "default",
		ConfigRef:     "cfg-1",
		Action:        "execute",
		Backend:       "memory",
	}
}

func TestConcurrentIdenticalSubmissionKeyOneIntent(t *testing.T) {
	s := lifecycle.NewMemoryStore()
	const n = 64
	var (
		wg      sync.WaitGroup
		intents = make([]string, n)
		errs    = make([]error, n)
	)
	wg.Add(n)
	for i := 0; i < n; i++ {
		i := i
		go func() {
			defer wg.Done()
			res, err := s.Admit(baseReq("sk-1", "fp-same"))
			errs[i] = err
			if err == nil {
				intents[i] = res.Intent.ID
			}
		}()
	}
	wg.Wait()

	seen := map[string]struct{}{}
	for i, err := range errs {
		if err != nil {
			t.Fatalf("admit %d: %v", i, err)
		}
		if intents[i] == "" {
			t.Fatalf("admit %d: empty intent", i)
		}
		seen[intents[i]] = struct{}{}
	}
	if len(seen) != 1 {
		t.Fatalf("want 1 intent, got %d: %v", len(seen), seen)
	}
}

func TestSameKeyDifferentFingerprintConflict(t *testing.T) {
	s := lifecycle.NewMemoryStore()
	if _, err := s.Admit(baseReq("sk-2", "fp-a")); err != nil {
		t.Fatal(err)
	}
	_, err := s.Admit(baseReq("sk-2", "fp-b"))
	if !errors.Is(err, lifecycle.ErrConflict) {
		t.Fatalf("want ErrConflict, got %v", err)
	}
}

func TestStaleGenerationCannotComplete(t *testing.T) {
	s := lifecycle.NewMemoryStore()
	res, err := s.Admit(baseReq("sk-3", "fp-a"))
	if err != nil {
		t.Fatal(err)
	}
	err = s.CompleteAttempt(res.Attempt.ID, "not-"+res.Attempt.Generation, lifecycle.ExecutionReceipt{
		Outcome:    "succeeded",
		Conclusion: "success",
	})
	if !errors.Is(err, lifecycle.ErrStaleGeneration) {
		t.Fatalf("want ErrStaleGeneration, got %v", err)
	}
	if _, ok := s.GetReceipt(res.Attempt.ID); ok {
		t.Fatal("receipt must not exist after stale complete")
	}
	if err := s.CompleteAttempt(res.Attempt.ID, res.Attempt.Generation, lifecycle.ExecutionReceipt{
		Outcome:    "succeeded",
		Conclusion: "success",
	}); err != nil {
		t.Fatalf("matching generation: %v", err)
	}
}

func TestLeaseRaceOnlyOneWins(t *testing.T) {
	s := lifecycle.NewMemoryStore()
	res, err := s.Admit(baseReq("sk-4", "fp-a"))
	if err != nil {
		t.Fatal(err)
	}
	expiry := time.Unix(4_102_444_800, 0) // far future vs wall now; avoid time.Now in tests
	const n = 32
	var wins atomic.Int32
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		i := i
		go func() {
			defer wg.Done()
			_, err := s.TryAcquireLease(res.Intent.ID, fmt.Sprintf("worker-%d", i), expiry)
			if err == nil {
				wins.Add(1)
			} else if !errors.Is(err, lifecycle.ErrLeaseHeld) {
				t.Errorf("unexpected err: %v", err)
			}
		}()
	}
	wg.Wait()
	if wins.Load() != 1 {
		t.Fatalf("want exactly 1 lease winner, got %d", wins.Load())
	}
	intent, ok := s.GetIntent(res.Intent.ID)
	if !ok || intent.DeliveryState != lifecycle.DeliveryLeased || intent.Lease.Owner == "" {
		t.Fatalf("lease state: ok=%v intent=%+v", ok, intent)
	}
}
