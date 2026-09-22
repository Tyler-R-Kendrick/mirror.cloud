package lifecycle

import (
	"errors"
	"time"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/execution/cicd"
)

var (
	// ErrConflict means the same SubmissionKey was admitted with a different fingerprint.
	ErrConflict = errors.New("lifecycle: submission key conflict")
	// ErrStaleGeneration means the caller's generation does not match the attempt.
	ErrStaleGeneration = errors.New("lifecycle: stale generation")
	// ErrLeaseHeld means another owner holds a non-expired lease.
	ErrLeaseHeld = errors.New("lifecycle: lease held")
	// ErrNotFound means the referenced record is absent.
	ErrNotFound = errors.New("lifecycle: not found")
	// ErrAlreadyTerminal means the attempt is already complete.
	ErrAlreadyTerminal = errors.New("lifecycle: already terminal")
)

// AdmitRequest is the atomic run+attempt+intent admission input.
type AdmitRequest struct {
	Scope           cicd.Scope
	SubmissionKey   string
	Fingerprint     string
	Generation      string // empty → store assigns
	Mode            string
	Profile         string
	ConfigRef       string
	NativeService   string
	NativeResource  string
	InitiatingEvent string
	SchedulerOwner  string
	Action          string
	InputRefs       []string
	RequestDigest   string
	RetryClass      string
	Backend         string
	Requirements    []cicd.Requirement
}

// AdmitResult is the admitted triple. Existing is true on idempotent replay.
type AdmitResult struct {
	Run      Run
	Attempt  Attempt
	Intent   DispatchIntent
	Existing bool
}

// Admittable atomically creates Run+Attempt+DispatchIntent under one SubmissionKey.
type Admittable interface {
	Admit(req AdmitRequest) (AdmitResult, error)
}

// Fence acquires delivery leases and completes attempts under generation fencing.
type Fence interface {
	TryAcquireLease(intentID, owner string, expiry time.Time) (DispatchIntent, error)
	CompleteAttempt(attemptID, generation string, receipt ExecutionReceipt) error
}

// Store is the combined durable lifecycle surface used by tests and adapters.
type Store interface {
	Admittable
	Fence
	GetRun(id string) (Run, bool)
	GetAttempt(id string) (Attempt, bool)
	GetIntent(id string) (DispatchIntent, bool)
	GetReceipt(attemptID string) (ExecutionReceipt, bool)
}
