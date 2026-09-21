// Package celld is a CapabilitySession adapter for an optional celld binary.
// Start records --version identity only; it does not claim Worker/DO capabilities.
package celld

import (
	"bytes"
	"context"
	"os"
	"os/exec"
	"strings"
	"sync"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

// Session implements spi.CapabilitySession for celld.
// Empty Binary means "not provisioned". Start does not download.
type Session struct {
	Binary string // path to celld executable

	mu      sync.Mutex
	version string
	started bool
}

func (s *Session) provisioned() bool {
	if s == nil || s.Binary == "" {
		return false
	}
	st, err := os.Stat(s.Binary)
	if err != nil || st.IsDir() {
		return false
	}
	return true
}

// Start fails CapErrUnavailable if Binary is missing.
// When present, runs `celld --version` (falls back to --help) and records identity.
func (s *Session) Start(ctx context.Context) error {
	if !s.provisioned() {
		return &spi.CapabilityError{
			Kind:    spi.CapErrUnavailable,
			Message: "celld binary not provisioned; install explicitly from tools/celld-runtime pin (Start does not download)",
		}
	}
	out, err := runIdentity(ctx, s.Binary)
	if err != nil {
		return &spi.CapabilityError{
			Kind:    spi.CapErrUnavailable,
			Message: "celld identity probe failed: " + err.Error(),
		}
	}
	s.mu.Lock()
	s.version = parseVersion(out)
	s.started = true
	s.mu.Unlock()
	return nil
}

func runIdentity(ctx context.Context, bin string) (string, error) {
	cmd := exec.CommandContext(ctx, bin, "--version")
	var buf bytes.Buffer
	cmd.Stdout = &buf
	cmd.Stderr = &buf
	if err := cmd.Run(); err == nil {
		return buf.String(), nil
	}
	buf.Reset()
	cmd = exec.CommandContext(ctx, bin, "--help")
	cmd.Stdout = &buf
	cmd.Stderr = &buf
	if err := cmd.Run(); err != nil {
		return "", err
	}
	return buf.String(), nil
}

func parseVersion(out string) string {
	// Prefer first line like "celld 0.5.1".
	line := strings.TrimSpace(strings.Split(out, "\n")[0])
	if line == "" {
		return ""
	}
	const p = "celld "
	if strings.HasPrefix(line, p) {
		return strings.TrimSpace(strings.TrimPrefix(line, p))
	}
	return line
}

// Describe reports backend "celld". Version filled after successful Start.
func (s *Session) Describe(context.Context) (spi.BackendIdentity, []spi.CapabilityDescriptor, error) {
	id := spi.BackendIdentity{Name: "celld"}
	if s == nil {
		return id, nil, nil
	}
	s.mu.Lock()
	id.Version = s.version
	s.mu.Unlock()
	return id, nil, nil
}

// Call rejects work when the binary is not provisioned.
// Provisioned sessions still return CapErrUnsupported: no DO/Worker catalog is wired.
func (s *Session) Call(_ context.Context, req spi.CapabilityCall) (*spi.CapabilityResult, error) {
	if !s.provisioned() {
		return nil, &spi.CapabilityError{
			Kind:       spi.CapErrUnavailable,
			Capability: req.Action,
			Message:    "celld binary not provisioned; install explicitly from tools/celld-runtime pin (Start does not download)",
		}
	}
	return nil, &spi.CapabilityError{
		Kind:       spi.CapErrUnsupported,
		Capability: req.Action,
		Message:    "celld session records identity only; Worker/DO capabilities not advertised",
	}
}

// Close is idempotent. Identity Start does not leave a child process.
func (s *Session) Close() error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	s.started = false
	s.mu.Unlock()
	return nil
}

// Ensure Session satisfies CapabilitySession.
var _ spi.CapabilitySession = (*Session)(nil)
