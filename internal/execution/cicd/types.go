package cicd

// Scope is the trusted job placement boundary. Callers never pass host paths
// or remote URLs here; account names are not an authentication proof.
type Scope struct {
	Environment string
	Account     string
	Region      string
	Project     string
	Repository  string
	TrustDomain string
}

// BlobRef is an integrity-bearing scoped handle resolved by the storage owner.
type BlobRef struct {
	Key       string // trusted scoped handle, not a host path or remote URL
	SHA256    string
	SizeBytes int64
	MediaType string
}

// Requirement names a capability revision a job needs from its backend.
type Requirement struct {
	Capability string
	Revision   string
}

// JobSpec is one accepted execution intent. Submit returning a handle means
// the executor accepted the intent, not that the job completed.
type JobSpec struct {
	Scope          Scope
	RunID          string
	AttemptID      string
	Generation     string
	SubmissionKey  string
	Mode           string
	Dialect        string
	Profile        string
	SchedulerOwner string
	Source         BlobRef
	NativeConfig   BlobRef
	ToolLock       BlobRef
	InputManifest  BlobRef
	Requirements   []Requirement
	SecretRefs     []string // references only; never secret values
	SemanticDueAt  string   // RFC3339Nano or empty; interpreted with spi.Clock
	SafetyLimitNS  int64    // external watchdog budget, not virtual elapsed time
}

// JobHandle identifies one accepted attempt at a backend.
type JobHandle struct {
	Scope      Scope
	RunID      string
	AttemptID  string
	Generation string
	Backend    string
	BackendID  string
}

// JobEvent is one non-destructive observation unit.
type JobEvent struct {
	Handle     JobHandle
	EventID    string
	Producer   string
	Sequence   uint64
	Kind       string // finite, schema-checked vocabulary
	ObservedAt string
	Data       BlobRef // redacted, schema-checked event payload when present
}

// Observation dispositions for Reconcile/Observe terminals.
const (
	DispositionKnown          = "known"
	DispositionPending        = "pending"
	DispositionOutcomeUnknown = "outcome_unknown"
)

// Observation is a cursor-based, non-destructive view of job progress.
type Observation struct {
	Events      []JobEvent
	NextCursor  string
	More        bool
	Terminal    bool
	Receipt     *BlobRef
	Disposition string // known, pending, or outcome_unknown
}
