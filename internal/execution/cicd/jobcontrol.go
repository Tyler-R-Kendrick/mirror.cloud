package cicd

import "context"

// JobControl is the provider-neutral CI/CD execution port.
//
// Submit accepts intent and returns a handle; it does not mean completion.
// Cancel returning nil means the request was accepted or already handled, not
// that termination was observed. Observe is cursor-based and non-destructive.
// Reconcile distinguishes absent, still running, known terminal, and unknown.
type JobControl interface {
	Submit(context.Context, JobSpec) (JobHandle, error)
	Observe(context.Context, JobHandle, string) (Observation, error)
	Cancel(context.Context, JobHandle, string) error
	Reconcile(context.Context, JobHandle) (Observation, error)
}
