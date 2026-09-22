// Package azswa is a mirror-owned Azure Static Web Apps preview/production publisher.
package azswa

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/execution/cicd/delivery"
)

type Env string

const (
	Preview    Env = "preview"
	Production Env = "production"
)

// Publisher keeps preview and production roots separate.
type Publisher struct {
	mu   sync.Mutex
	base string
	prev *delivery.Site
	prod *delivery.Site
}

func New(base string) (*Publisher, error) {
	if base == "" {
		dir, err := os.MkdirTemp("", "mirror-azswa-")
		if err != nil {
			return nil, err
		}
		base = dir
	}
	for _, e := range []Env{Preview, Production} {
		if err := os.MkdirAll(filepath.Join(base, string(e)), 0o755); err != nil {
			return nil, err
		}
	}
	return &Publisher{base: base}, nil
}

func (p *Publisher) Publish(env Env, files map[string][]byte) (*delivery.Site, error) {
	if env != Preview && env != Production {
		return nil, fmt.Errorf("azswa: unknown env %q", env)
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
	if env == Preview {
		p.prev = site
	} else {
		p.prod = site
	}
	return site, nil
}

// ClosePreview removes the preview environment only.
func (p *Publisher) ClosePreview() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.prev != nil {
		_ = p.prev.Close()
		p.prev = nil
	}
	return os.RemoveAll(filepath.Join(p.base, string(Preview)))
}

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

func (p *Publisher) Root(env Env) string {
	return filepath.Join(p.base, string(env))
}

func (p *Publisher) Site(env Env) *delivery.Site {
	p.mu.Lock()
	defer p.mu.Unlock()
	if env == Preview {
		return p.prev
	}
	return p.prod
}

func writeRel(root, name string, body []byte) error {
	if filepath.IsAbs(name) || strings.Contains(name, "..") {
		return fmt.Errorf("azswa: bad path %q", name)
	}
	path := filepath.Join(root, name)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	return os.WriteFile(path, body, 0o644)
}
