// Package registry maps service IDs to the pack serving them. Packs register
// themselves in package init via Register; the edge never imports a
// behavior pack directly.
package registry

import (
	"errors"
	"fmt"
	"sync"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

// Registry maps service IDs to the pack serving them.
type Registry interface {
	Register(factory Factory)
	// Resolve returns the pack for a service ID, honoring the enabled-service
	// set and the configured tier for that service.
	Resolve(serviceID string) (spi.BehaviorPack, bool)
	Enabled() []string
	Close() error
}

// Factory constructs a pack once dependencies are available.
type Factory struct {
	ServiceID string
	Tier      model.Tier
	New       func(spi.Deps) (spi.BehaviorPack, error)
}

var (
	mu        sync.Mutex
	factories []Factory
)

// Register appends a factory to the process-wide table. Called from pack init.
func Register(factory Factory) {
	mu.Lock()
	defer mu.Unlock()
	factories = append(factories, factory)
}

// Factories returns a snapshot of registered factories.
func Factories() []Factory {
	mu.Lock()
	defer mu.Unlock()
	out := make([]Factory, len(factories))
	copy(out, factories)
	return out
}

type mem struct {
	enabled map[string]spi.BehaviorPack
	order   []string
}

// New builds a registry from factories, constructing only enabled services.
func New(deps spi.Deps, enabled []string, tiers map[string]model.Tier) (Registry, error) {
	want := map[string]bool{}
	if len(enabled) == 0 {
		for _, f := range Factories() {
			want[f.ServiceID] = true
		}
	} else {
		for _, id := range enabled {
			want[id] = true
		}
	}
	r := &mem{enabled: map[string]spi.BehaviorPack{}}
	for _, f := range Factories() {
		if !want[f.ServiceID] {
			continue
		}
		// Two factories for one ID means two descriptions of one service —
		// during extraction, typically a hand-written pack that outlived its
		// bundle. Silently keeping the last one would decide that by import
		// order, so refuse instead.
		if _, dup := r.enabled[f.ServiceID]; dup {
			return nil, fmt.Errorf("registry: %s is registered twice; "+
				"a service is served either by a pack or by its behavior bundle, not both",
				f.ServiceID)
		}
		if t, ok := tiers[f.ServiceID]; ok && t != f.Tier && t != model.TierProxy {
			// Factory still used; mock pack is attached separately by the edge.
			_ = t
		}
		p, err := f.New(deps)
		if err != nil {
			return nil, err
		}
		r.enabled[f.ServiceID] = p
		r.order = append(r.order, f.ServiceID)
	}
	return r, nil
}

func (m *mem) Register(factory Factory) { Register(factory) }

func (m *mem) Resolve(serviceID string) (spi.BehaviorPack, bool) {
	p, ok := m.enabled[serviceID]
	return p, ok
}

func (m *mem) Enabled() []string {
	out := make([]string, len(m.order))
	copy(out, m.order)
	return out
}

func (m *mem) Close() error {
	var err error
	for i := len(m.order) - 1; i >= 0; i-- {
		if closer, ok := m.enabled[m.order[i]].(interface{ Close() error }); ok {
			err = errors.Join(err, closer.Close())
		}
	}
	return err
}
