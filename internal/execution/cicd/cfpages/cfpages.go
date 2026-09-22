// Package cfpages is a mirror-owned Cloudflare Pages-ish static publisher.
// Not Workers Builds / Pages GitHub App / wrangler pages deploy parity.
package cfpages

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/execution/cicd/delivery"
)

// Env is a deployment environment root.
type Env string

const (
	Preview    Env = "preview"
	Production Env = "production"
)

// Publisher stores preview vs production document roots separately.
type Publisher struct {
	mu   sync.Mutex
	base string
	prev *delivery.Site
	prod *delivery.Site
}

// New creates a publisher under base (temp dir if empty).
func New(base string) (*Publisher, error) {
	if base == "" {
		dir, err := os.MkdirTemp("", "mirror-cfpages-")
		if err != nil {
			return nil, err
		}
		base = dir
	}
	if err := os.MkdirAll(filepath.Join(base, string(Preview)), 0o755); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Join(base, string(Production)), 0o755); err != nil {
		return nil, err
	}
	return &Publisher{base: base}, nil
}

// Publish writes files into env root and serves them on loopback.
func (p *Publisher) Publish(env Env, files map[string][]byte) (*delivery.Site, error) {
	if env != Preview && env != Production {
		return nil, fmt.Errorf("cfpages: unknown env %q", env)
	}
	root := filepath.Join(p.base, string(env))

	p.mu.Lock()
	defer p.mu.Unlock()
	switch env {
	case Preview:
		if p.prev != nil {
			_ = p.prev.Close()
			p.prev = nil
		}
	case Production:
		if p.prod != nil {
			_ = p.prod.Close()
			p.prod = nil
		}
	}

	_ = os.RemoveAll(root)
	if err := os.MkdirAll(root, 0o755); err != nil {
		return nil, err
	}
	for name, body := range files {
		if err := writeRel(root, name, body); err != nil {
			return nil, err
		}
	}
	site, err := delivery.ServeDir(root)
	if err != nil {
		return nil, err
	}
	switch env {
	case Preview:
		p.prev = site
	case Production:
		p.prod = site
	}
	return site, nil
}

// Site returns the live site for env (may be nil).
func (p *Publisher) Site(env Env) *delivery.Site {
	p.mu.Lock()
	defer p.mu.Unlock()
	if env == Preview {
		return p.prev
	}
	return p.prod
}

// Close stops listeners; keeps base on disk for file VerifyNonce.
func (p *Publisher) Close() {
	if p == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.prev != nil {
		_ = p.prev.Close()
		p.prev = nil
	}
	if p.prod != nil {
		_ = p.prod.Close()
		p.prod = nil
	}
}

// Root returns the on-disk root for env.
func (p *Publisher) Root(env Env) string {
	return filepath.Join(p.base, string(env))
}

func writeRel(root, name string, body []byte) error {
	if filepath.IsAbs(name) || strings.Contains(name, "..") {
		return fmt.Errorf("cfpages: bad path %q", name)
	}
	path := filepath.Join(root, name)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	return os.WriteFile(path, body, 0o644)
}
