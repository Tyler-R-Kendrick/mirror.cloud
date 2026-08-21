// Package bus is in-process event delivery. Delivery is synchronous and
// ordered per topic so tests are deterministic.
package bus

import (
	"context"
	"sync"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

// Memory implements spi.Bus.
type Memory struct {
	mu   sync.Mutex
	subs map[string][]func(context.Context, []byte)
}

// New returns an empty bus.
func New() *Memory { return &Memory{subs: map[string][]func(context.Context, []byte){}} }

func (m *Memory) Publish(ctx context.Context, topic string, payload []byte) error {
	m.mu.Lock()
	fns := append([]func(context.Context, []byte){}, m.subs[topic]...)
	m.mu.Unlock()
	cp := make([]byte, len(payload))
	copy(cp, payload)
	for _, fn := range fns {
		fn(ctx, cp)
	}
	return nil
}

func (m *Memory) Subscribe(topic string, fn func(context.Context, []byte)) (cancel func()) {
	m.mu.Lock()
	m.subs[topic] = append(m.subs[topic], fn)
	idx := len(m.subs[topic]) - 1
	m.mu.Unlock()
	return func() {
		m.mu.Lock()
		defer m.mu.Unlock()
		s := m.subs[topic]
		if idx >= 0 && idx < len(s) {
			m.subs[topic] = append(s[:idx], s[idx+1:]...)
		}
	}
}

var _ spi.Bus = (*Memory)(nil)
