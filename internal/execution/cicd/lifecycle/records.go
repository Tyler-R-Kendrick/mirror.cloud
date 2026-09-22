// Package lifecycle holds durable CI/CD run admission and fencing primitives.
// It is store-shaped interface surface, not full SPI wiring.
package lifecycle

import (
	"time"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/execution/cicd"
)

// DeliveryState is the finite dispatch-intent delivery projection.
type DeliveryState string

const (
	DeliveryPending DeliveryState = "pending"
	DeliveryLeased  DeliveryState = "leased"
	DeliveryAcked   DeliveryState = "acked"
	DeliveryFailed  DeliveryState = "failed"
)

// Lease is a fencing token on a DispatchIntent.
type Lease struct {
	Owner  string
	Expiry time.Time
}

// Run is one admitted native workflow instance.
type Run struct {
	ID              string
	Scope           cicd.Scope
	NativeService   string
	NativeResource  string
	ConfigRef       string // immutable submitted configuration reference
	Mode            string
	Profile         string
	InitiatingEvent string
	SchedulerOwner  string
	CurrentAttempt  string
	AdmittedAt      time.Time
	NativeStatus    string
	SubmissionKey   string
	Fingerprint     string
	Generation      string
}

// Attempt is one immutable execution try under a Run.
type Attempt struct {
	ID               string
	RunID            string
	ParentAttemptID  string
	Generation       string
	SubmissionKey    string
	Fingerprint      string
	Backend          string
	BackendHandle    string
	Requirements     []cicd.Requirement
	QueuedAt         time.Time
	StartedAt        time.Time
	FinishedAt       time.Time
	CancelRequested  bool
	CompletionReason string
	Terminal         bool
}

// DispatchIntent is durable work to deliver for an Attempt.
type DispatchIntent struct {
	ID                 string
	AttemptID          string
	Scope              cicd.Scope
	Action             string
	InputRefs          []string
	RequestDigest      string
	ExpectedGeneration string
	DeliveryState      DeliveryState
	Lease              Lease
	RetryClass         string
	SubmissionKey      string
	Fingerprint        string
}

// ExecutionReceipt is verified terminal evidence for one Attempt.
type ExecutionReceipt struct {
	Mode            string
	AttemptID       string
	Generation      string
	SourceDigest    string
	ConfigDigest    string
	ToolDigest      string
	ImageDigest     string
	SchedulerOwner  string
	EnvVarNames     []string
	EnvVersionRefs  []string
	ExitCode        int
	Signal          string
	Outcome         string
	Conclusion      string
	LogRefs         []cicd.BlobRef
	OutputManifests []cicd.BlobRef
	StartedAt       time.Time
	FinishedAt      time.Time
	Cancellation    string
	DestinationObs  []string
}
