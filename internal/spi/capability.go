package spi

import (
	"context"
	"fmt"
	"io"
)

// BodyMode declares how a capability carries a payload body.
type BodyMode string

const (
	BodyNone   BodyMode = "none"
	BodyBytes  BodyMode = "bytes"
	BodyStream BodyMode = "stream"
)

// EffectClass classifies side effects for composition rules.
type EffectClass string

const (
	EffectRead            EffectClass = "read"
	EffectWrite           EffectClass = "write"
	EffectIdempotentWrite EffectClass = "idempotent_write"
)

// CapabilityErrorKind is the typed failure class for external capabilities.
type CapabilityErrorKind string

const (
	CapErrValidation     CapabilityErrorKind = "validation"
	CapErrUnsupported    CapabilityErrorKind = "unsupported"
	CapErrUnavailable    CapabilityErrorKind = "unavailable"
	CapErrAbsent         CapabilityErrorKind = "absent"
	CapErrConflict       CapabilityErrorKind = "conflict"
	CapErrOutcomeUnknown CapabilityErrorKind = "outcome_unknown"
)

// CapabilityDescriptor is one finite named external action, declared as data.
type CapabilityDescriptor struct {
	Name               string
	InputSchema        string // schema ref or typed tag
	OutputSchema       string
	ResourceKind       string
	BodyMode           BodyMode
	FailureClass       string // provider wire-error mapping key
	EffectClass        EffectClass
	BackendRequirement string // e.g. "miniflare>=3" or "celld=0.5.1"
}

// CapabilityError is a typed external-capability failure.
type CapabilityError struct {
	Kind       CapabilityErrorKind
	Capability string
	Message    string
}

func (e *CapabilityError) Error() string {
	if e == nil {
		return ""
	}
	if e.Message != "" {
		return fmt.Sprintf("%s: %s", e.Kind, e.Message)
	}
	return string(e.Kind)
}

// ResourceRef is a trusted environment/account/resource handle resolved by mirror.
// Callers never pass an IPC address, method name, or filesystem path here.
type ResourceRef struct {
	Environment string
	Account     string
	Kind        string
	ID          string
	Generation  string // fencing metadata; not a persistent resource identity
}

// CapabilityCall is one validated external action invocation.
type CapabilityCall struct {
	Resource ResourceRef
	Action   string
	Args     map[string]any
	Body     io.ReadCloser // nil when BodyMode is none
}

// CapabilityResult is structured output and/or a streamed body.
type CapabilityResult struct {
	Output map[string]any
	Body   io.ReadCloser
}

// BackendIdentity pins the selected external runtime.
type BackendIdentity struct {
	Name    string
	Version string
}

// CapabilitySession is the provider-neutral external runtime seam.
// Miniflare, celld, and test fakes implement this. ComputeProvider stays
// unchanged for existing byte-oriented callers.
type CapabilitySession interface {
	Describe(ctx context.Context) (BackendIdentity, []CapabilityDescriptor, error)
	Call(ctx context.Context, req CapabilityCall) (*CapabilityResult, error)
}
