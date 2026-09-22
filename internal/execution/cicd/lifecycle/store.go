package lifecycle

import (
	"fmt"
	"sync"
	"time"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/execution/cicd"
)

// MemoryStore is an in-memory Store for tests and local adapters.
type MemoryStore struct {
	mu       sync.Mutex
	seq      uint64
	byKey    map[string]string // SubmissionKey → IntentID
	runs     map[string]Run
	attempts map[string]Attempt
	intents  map[string]DispatchIntent
	receipts map[string]ExecutionReceipt // AttemptID → receipt
	now      func() time.Time
}

// NewMemoryStore returns an empty MemoryStore.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		byKey:    make(map[string]string),
		runs:     make(map[string]Run),
		attempts: make(map[string]Attempt),
		intents:  make(map[string]DispatchIntent),
		receipts: make(map[string]ExecutionReceipt),
		now:      time.Now,
	}
}

func (s *MemoryStore) nextID(prefix string) string {
	s.seq++
	return fmt.Sprintf("%s-%d", prefix, s.seq)
}

// Admit atomically creates Run+Attempt+Intent, or returns the existing triple
// when SubmissionKey matches with the same Fingerprint.
func (s *MemoryStore) Admit(req AdmitRequest) (AdmitResult, error) {
	if req.SubmissionKey == "" {
		return AdmitResult{}, fmt.Errorf("%w: empty submission key", ErrConflict)
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	if intentID, ok := s.byKey[req.SubmissionKey]; ok {
		intent := s.intents[intentID]
		if intent.Fingerprint != req.Fingerprint {
			return AdmitResult{}, ErrConflict
		}
		attempt := s.attempts[intent.AttemptID]
		run := s.runs[attempt.RunID]
		return AdmitResult{Run: run, Attempt: attempt, Intent: intent, Existing: true}, nil
	}

	now := s.now()
	gen := req.Generation
	if gen == "" {
		gen = s.nextID("gen")
	}
	runID := s.nextID("run")
	attemptID := s.nextID("attempt")
	intentID := s.nextID("intent")

	reqs := append([]cicd.Requirement(nil), req.Requirements...)
	run := Run{
		ID:              runID,
		Scope:           req.Scope,
		NativeService:   req.NativeService,
		NativeResource:  req.NativeResource,
		ConfigRef:       req.ConfigRef,
		Mode:            req.Mode,
		Profile:         req.Profile,
		InitiatingEvent: req.InitiatingEvent,
		SchedulerOwner:  req.SchedulerOwner,
		CurrentAttempt:  attemptID,
		AdmittedAt:      now,
		NativeStatus:    "accepted",
		SubmissionKey:   req.SubmissionKey,
		Fingerprint:     req.Fingerprint,
		Generation:      gen,
	}
	attempt := Attempt{
		ID:            attemptID,
		RunID:         runID,
		Generation:    gen,
		SubmissionKey: req.SubmissionKey,
		Fingerprint:   req.Fingerprint,
		Backend:       req.Backend,
		Requirements:  reqs,
		QueuedAt:      now,
	}
	digest := req.RequestDigest
	if digest == "" {
		digest = req.Fingerprint
	}
	intent := DispatchIntent{
		ID:                 intentID,
		AttemptID:          attemptID,
		Scope:              req.Scope,
		Action:             req.Action,
		InputRefs:          append([]string(nil), req.InputRefs...),
		RequestDigest:      digest,
		ExpectedGeneration: gen,
		DeliveryState:      DeliveryPending,
		RetryClass:         req.RetryClass,
		SubmissionKey:      req.SubmissionKey,
		Fingerprint:        req.Fingerprint,
	}

	s.runs[runID] = run
	s.attempts[attemptID] = attempt
	s.intents[intentID] = intent
	s.byKey[req.SubmissionKey] = intentID
	return AdmitResult{Run: run, Attempt: attempt, Intent: intent}, nil
}

// TryAcquireLease grants the lease to owner when free or expired.
func (s *MemoryStore) TryAcquireLease(intentID, owner string, expiry time.Time) (DispatchIntent, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	intent, ok := s.intents[intentID]
	if !ok {
		return DispatchIntent{}, ErrNotFound
	}
	now := s.now()
	held := intent.Lease.Owner != "" && intent.Lease.Expiry.After(now)
	if held && intent.Lease.Owner != owner {
		return DispatchIntent{}, ErrLeaseHeld
	}
	intent.Lease = Lease{Owner: owner, Expiry: expiry}
	intent.DeliveryState = DeliveryLeased
	s.intents[intentID] = intent
	return intent, nil
}

// CompleteAttempt finalizes an attempt only when generation matches.
func (s *MemoryStore) CompleteAttempt(attemptID, generation string, receipt ExecutionReceipt) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	attempt, ok := s.attempts[attemptID]
	if !ok {
		return ErrNotFound
	}
	if attempt.Terminal {
		return ErrAlreadyTerminal
	}
	if attempt.Generation != generation {
		return ErrStaleGeneration
	}
	now := s.now()
	attempt.Terminal = true
	attempt.FinishedAt = now
	if attempt.CompletionReason == "" {
		attempt.CompletionReason = receipt.Conclusion
		if attempt.CompletionReason == "" {
			attempt.CompletionReason = receipt.Outcome
		}
	}
	receipt.AttemptID = attemptID
	receipt.Generation = generation
	if receipt.FinishedAt.IsZero() {
		receipt.FinishedAt = now
	}
	s.attempts[attemptID] = attempt
	s.receipts[attemptID] = receipt
	s.markTerminalLocked(attempt)
	return nil
}

// CancelAttempt terminates an attempt under generation fencing (cancel-vs-complete race).
func (s *MemoryStore) CancelAttempt(attemptID, generation, reason string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	attempt, ok := s.attempts[attemptID]
	if !ok {
		return ErrNotFound
	}
	if attempt.Terminal {
		return ErrAlreadyTerminal
	}
	if attempt.Generation != generation {
		return ErrStaleGeneration
	}
	now := s.now()
	attempt.Terminal = true
	attempt.CancelRequested = true
	attempt.FinishedAt = now
	attempt.CompletionReason = "cancelled"
	s.attempts[attemptID] = attempt
	s.receipts[attemptID] = ExecutionReceipt{
		AttemptID:    attemptID,
		Generation:   generation,
		Outcome:      "cancelled",
		Conclusion:   "cancelled",
		Cancellation: reason,
		FinishedAt:   now,
	}
	s.markTerminalLocked(attempt)
	return nil
}

func (s *MemoryStore) markTerminalLocked(attempt Attempt) {
	if run, ok := s.runs[attempt.RunID]; ok {
		run.NativeStatus = "terminal"
		s.runs[attempt.RunID] = run
	}
	for id, intent := range s.intents {
		if intent.AttemptID == attempt.ID {
			intent.DeliveryState = DeliveryAcked
			intent.Lease = Lease{}
			s.intents[id] = intent
		}
	}
}

func (s *MemoryStore) GetRun(id string) (Run, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.runs[id]
	return r, ok
}

func (s *MemoryStore) GetAttempt(id string) (Attempt, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	a, ok := s.attempts[id]
	return a, ok
}

func (s *MemoryStore) GetIntent(id string) (DispatchIntent, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	i, ok := s.intents[id]
	return i, ok
}

func (s *MemoryStore) GetReceipt(attemptID string) (ExecutionReceipt, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.receipts[attemptID]
	return r, ok
}

// Compile-time check: MemoryStore implements Store.
var _ Store = (*MemoryStore)(nil)
