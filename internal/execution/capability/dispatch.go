// Package capability is a generic dispatcher over spi.CapabilitySession.
// It validates action identity, resource kind, and body mode against
// Describe() descriptors. No provider-specific engine branch lives here.
package capability

import (
	"context"
	"fmt"
	"sync"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

// Dispatcher routes CapabilityCall through a session after descriptor checks.
type Dispatcher struct {
	Session spi.CapabilitySession

	mu   sync.Mutex
	caps map[string]spi.CapabilityDescriptor
	id   spi.BackendIdentity
}

// Describe refreshes and returns the pinned backend identity and capabilities.
func (d *Dispatcher) Describe(ctx context.Context) (spi.BackendIdentity, []spi.CapabilityDescriptor, error) {
	if d == nil || d.Session == nil {
		return spi.BackendIdentity{}, nil, &spi.CapabilityError{
			Kind:    spi.CapErrUnavailable,
			Message: "no capability session",
		}
	}
	id, list, err := d.Session.Describe(ctx)
	if err != nil {
		return spi.BackendIdentity{}, nil, err
	}
	index := make(map[string]spi.CapabilityDescriptor, len(list))
	for _, c := range list {
		if c.Name == "" {
			return spi.BackendIdentity{}, nil, &spi.CapabilityError{
				Kind:    spi.CapErrValidation,
				Message: "descriptor missing name",
			}
		}
		if _, dup := index[c.Name]; dup {
			return spi.BackendIdentity{}, nil, &spi.CapabilityError{
				Kind:       spi.CapErrConflict,
				Capability: c.Name,
				Message:    "duplicate capability name",
			}
		}
		index[c.Name] = c
	}
	d.mu.Lock()
	d.caps = index
	d.id = id
	d.mu.Unlock()
	out := make([]spi.CapabilityDescriptor, len(list))
	copy(out, list)
	return id, out, nil
}

// Call validates req against cached descriptors (refreshing once if empty),
// then forwards to the session.
func (d *Dispatcher) Call(ctx context.Context, req spi.CapabilityCall) (*spi.CapabilityResult, error) {
	if d == nil || d.Session == nil {
		return nil, &spi.CapabilityError{
			Kind:    spi.CapErrUnavailable,
			Message: "no capability session",
		}
	}
	desc, err := d.lookup(ctx, req.Action)
	if err != nil {
		return nil, err
	}
	if desc.ResourceKind != "" && req.Resource.Kind != "" && desc.ResourceKind != req.Resource.Kind {
		return nil, &spi.CapabilityError{
			Kind:       spi.CapErrValidation,
			Capability: req.Action,
			Message:    fmt.Sprintf("resource kind %q incompatible with %q", req.Resource.Kind, desc.ResourceKind),
		}
	}
	switch desc.BodyMode {
	case spi.BodyNone:
		if req.Body != nil {
			return nil, &spi.CapabilityError{
				Kind:       spi.CapErrValidation,
				Capability: req.Action,
				Message:    "body not allowed",
			}
		}
	case spi.BodyBytes, spi.BodyStream, "":
		// optional / unset: no further body-mode check here
	default:
		return nil, &spi.CapabilityError{
			Kind:       spi.CapErrValidation,
			Capability: req.Action,
			Message:    fmt.Sprintf("unknown body mode %q", desc.BodyMode),
		}
	}
	return d.Session.Call(ctx, req)
}

func (d *Dispatcher) lookup(ctx context.Context, action string) (spi.CapabilityDescriptor, error) {
	d.mu.Lock()
	caps := d.caps
	d.mu.Unlock()
	if caps == nil {
		if _, _, err := d.Describe(ctx); err != nil {
			return spi.CapabilityDescriptor{}, err
		}
		d.mu.Lock()
		caps = d.caps
		d.mu.Unlock()
	}
	desc, ok := caps[action]
	if !ok {
		return spi.CapabilityDescriptor{}, &spi.CapabilityError{
			Kind:       spi.CapErrUnsupported,
			Capability: action,
			Message:    fmt.Sprintf("action %q not advertised by backend", action),
		}
	}
	return desc, nil
}

// Identity returns the last Describe() backend pin, or zero if never described.
func (d *Dispatcher) Identity() spi.BackendIdentity {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.id
}
