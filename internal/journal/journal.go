// Package journal records every request for diagnostics and drift analysis.
package journal

import (
	"sync"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

// Memory is an in-memory Journal.
type Memory struct {
	mu      sync.Mutex
	entries []spi.Entry
}

// New returns an empty journal.
func New() *Memory { return &Memory{} }

func (m *Memory) Record(e spi.Entry) {
	m.mu.Lock()
	m.entries = append(m.entries, e)
	m.mu.Unlock()
}

func (m *Memory) Query(f spi.Filter) []spi.Entry {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []spi.Entry
	for i := len(m.entries) - 1; i >= 0; i-- {
		e := m.entries[i]
		if f.ServiceID != "" && e.ServiceID != f.ServiceID {
			continue
		}
		if f.Operation != "" && e.Operation != f.Operation {
			continue
		}
		if !f.Since.IsZero() && e.At.Before(f.Since) {
			continue
		}
		out = append(out, e)
		if f.Limit > 0 && len(out) >= f.Limit {
			break
		}
	}
	return out
}

var _ spi.Journal = (*Memory)(nil)
