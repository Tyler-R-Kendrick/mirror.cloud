// Package miniflare is a CapabilitySession stub for an optional Miniflare/workerd backend.
// It never downloads packages or runs npx; callers must preprovision binaries.
package miniflare

import (
	"context"
	"os"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

// Session implements spi.CapabilitySession for Miniflare.
// Empty Workerd/Node/Miniflare paths mean "not provisioned".
type Session struct {
	Workerd   string // path to workerd binary
	Node      string // path to node binary
	Miniflare string // path to miniflare CLI/entry
}

// provisioned reports whether all required binaries are configured and present.
func (s *Session) provisioned() bool {
	if s == nil || s.Workerd == "" || s.Node == "" || s.Miniflare == "" {
		return false
	}
	for _, p := range []string{s.Workerd, s.Node, s.Miniflare} {
		if _, err := os.Stat(p); err != nil {
			return false
		}
	}
	return true
}

// Start validates preprovisioned paths only. It does not run npx, curl, or download.
func (s *Session) Start(context.Context) error {
	if !s.provisioned() {
		return &spi.CapabilityError{
			Kind:    spi.CapErrUnavailable,
			Message: "miniflare/workerd/node not provisioned; install explicitly (Start does not download or run npx)",
		}
	}
	return nil
}

// Describe reports backend "miniflare". Descriptors stay empty until provisioned+configured.
func (s *Session) Describe(context.Context) (spi.BackendIdentity, []spi.CapabilityDescriptor, error) {
	id := spi.BackendIdentity{Name: "miniflare"}
	if !s.provisioned() {
		return id, nil, nil
	}
	// Provisioned but no capability catalog wired yet — still empty/unsupported.
	return id, nil, nil
}

// Call rejects work when the runtime is not provisioned.
func (s *Session) Call(_ context.Context, req spi.CapabilityCall) (*spi.CapabilityResult, error) {
	if !s.provisioned() {
		return nil, &spi.CapabilityError{
			Kind:       spi.CapErrUnavailable,
			Capability: req.Action,
			Message:    "miniflare/workerd/node not provisioned; install explicitly (Start does not download or run npx)",
		}
	}
	return nil, &spi.CapabilityError{
		Kind:       spi.CapErrUnsupported,
		Capability: req.Action,
		Message:    "miniflare session has no configured capability descriptors",
	}
}

// Ensure Session satisfies CapabilitySession.
var _ spi.CapabilitySession = (*Session)(nil)
