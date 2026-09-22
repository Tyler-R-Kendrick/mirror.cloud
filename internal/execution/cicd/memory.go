package cicd

import (
	"context"
	"fmt"
	"sync"
)

// MemoryJobControl is an in-memory JobControl for conformance tests.
// Duplicate SubmissionKey returns the same handle. Observe eventually
// reaches Terminal with a Receipt blob ref stored in memory.
type MemoryJobControl struct {
	Backend string

	mu      sync.Mutex
	seq     uint64
	byKey   map[string]*memJob // SubmissionKey → job
	byID    map[string]*memJob // BackendID → job
	receipt map[string]BlobRef // BackendID → receipt
}

type memJob struct {
	handle     JobHandle
	events     []JobEvent
	cancelReq  bool
	terminal   bool
	disposition string
}

// NewMemoryJobControl returns an empty fake backend.
func NewMemoryJobControl(backend string) *MemoryJobControl {
	if backend == "" {
		backend = "memory"
	}
	return &MemoryJobControl{
		Backend: backend,
		byKey:   map[string]*memJob{},
		byID:    map[string]*memJob{},
		receipt: map[string]BlobRef{},
	}
}

// Receipt returns the stored receipt for a backend id, if any.
func (m *MemoryJobControl) Receipt(backendID string) (BlobRef, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	r, ok := m.receipt[backendID]
	return r, ok
}

func (m *MemoryJobControl) Submit(_ context.Context, spec JobSpec) (JobHandle, error) {
	if spec.SubmissionKey == "" {
		return JobHandle{}, &Error{Kind: ErrValidation, Op: "Submit", Message: "submission_key required"}
	}
	if spec.RunID == "" {
		return JobHandle{}, &Error{Kind: ErrValidation, Op: "Submit", Message: "run_id required"}
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if j, ok := m.byKey[spec.SubmissionKey]; ok {
		return j.handle, nil
	}

	m.seq++
	id := fmt.Sprintf("%s-%d", m.Backend, m.seq)
	h := JobHandle{
		Scope:      spec.Scope,
		RunID:      spec.RunID,
		AttemptID:  spec.AttemptID,
		Generation: spec.Generation,
		Backend:    m.Backend,
		BackendID:  id,
	}
	j := &memJob{
		handle:      h,
		disposition: DispositionPending,
		events: []JobEvent{{
			Handle:   h,
			EventID:  id + ":accepted",
			Producer: m.Backend,
			Sequence: 1,
			Kind:     "accepted",
		}},
	}
	m.byKey[spec.SubmissionKey] = j
	m.byID[id] = j
	return h, nil
}

func (m *MemoryJobControl) Observe(_ context.Context, h JobHandle, cursor string) (Observation, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	j, ok := m.byID[h.BackendID]
	if !ok || j.handle.Backend != h.Backend {
		return Observation{}, &Error{Kind: ErrAbsent, Op: "Observe", Message: "job not found"}
	}
	m.progressLocked(j)
	return m.observationLocked(j, cursor), nil
}

func (m *MemoryJobControl) Cancel(_ context.Context, h JobHandle, _ string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	j, ok := m.byID[h.BackendID]
	if !ok || j.handle.Backend != h.Backend {
		return &Error{Kind: ErrAbsent, Op: "Cancel", Message: "job not found"}
	}
	if j.terminal {
		return nil // idempotent
	}
	j.cancelReq = true
	return nil
}

func (m *MemoryJobControl) Reconcile(_ context.Context, h JobHandle) (Observation, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	j, ok := m.byID[h.BackendID]
	if !ok || j.handle.Backend != h.Backend {
		return Observation{}, &Error{Kind: ErrAbsent, Op: "Reconcile", Message: "job not found"}
	}
	m.progressLocked(j)
	return m.observationLocked(j, ""), nil
}

func (m *MemoryJobControl) progressLocked(j *memJob) {
	if j.terminal {
		return
	}
	if j.cancelReq {
		j.terminal = true
		j.disposition = DispositionKnown
		seq := uint64(len(j.events) + 1)
		j.events = append(j.events, JobEvent{
			Handle:   j.handle,
			EventID:  j.handle.BackendID + ":cancelled",
			Producer: m.Backend,
			Sequence: seq,
			Kind:     "cancelled",
		})
		return
	}
	// Fake completes on first progress: store receipt, mark known terminal.
	receipt := BlobRef{
		Key:       "receipt/" + j.handle.BackendID,
		SHA256:    "sha256:" + j.handle.BackendID,
		SizeBytes: 1,
		MediaType: "application/vnd.mirror.cicd.receipt+json",
	}
	m.receipt[j.handle.BackendID] = receipt
	j.terminal = true
	j.disposition = DispositionKnown
	seq := uint64(len(j.events) + 1)
	j.events = append(j.events, JobEvent{
		Handle:   j.handle,
		EventID:  j.handle.BackendID + ":completed",
		Producer: m.Backend,
		Sequence: seq,
		Kind:     "completed",
		Data:     receipt,
	})
}

func (m *MemoryJobControl) observationLocked(j *memJob, cursor string) Observation {
	start := 0
	if cursor != "" {
		for i, ev := range j.events {
			if ev.EventID == cursor {
				start = i + 1
				break
			}
		}
	}
	events := append([]JobEvent(nil), j.events[start:]...)
	next := ""
	if n := len(j.events); n > 0 {
		next = j.events[n-1].EventID
	}
	obs := Observation{
		Events:      events,
		NextCursor:  next,
		More:        false,
		Terminal:    j.terminal,
		Disposition: j.disposition,
	}
	if j.terminal {
		if r, ok := m.receipt[j.handle.BackendID]; ok {
			cp := r
			obs.Receipt = &cp
		}
	}
	return obs
}
