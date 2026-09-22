// Package netlify is a mirror-owned Netlify-ish site/deploy adapter.
// Local Site Create + draft Deploy + production publish pointer only —
// not Netlify Deploy API / Build Hooks / Edge Functions parity.
package netlify

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/execution/cicd/delivery"
)

const (
	StatusReady    = "ready"
	StatusError    = "error"
	StatusBuilding = "building"
)

// Site holds deploys and a production publish pointer separate from drafts.
type Site struct {
	mu      sync.Mutex
	ID      string
	Name    string
	base    string
	seq     int
	deploys map[string]*Deploy
	prodID  string
	prod    *delivery.Site
}

// Deploy is one uploaded revision with its own loopback URL when ready.
type Deploy struct {
	ID     string
	Status string
	Draft  bool
	Site   *delivery.Site // deploy-specific URL; nil when not ready
	root   string
}

// CreateSite makes a local site under a temp root (name labels the id).
func CreateSite(name string) (*Site, error) {
	if name == "" {
		name = "site"
	}
	base, err := os.MkdirTemp("", "mirror-netlify-")
	if err != nil {
		return nil, err
	}
	return &Site{
		ID:      "site-" + name,
		Name:    name,
		base:    base,
		deploys: make(map[string]*Deploy),
	}, nil
}

// Deploy uploads a files map as a ready draft. Does not flip production.
func (s *Site) Deploy(files map[string][]byte) (*Deploy, error) {
	if s == nil {
		return nil, fmt.Errorf("netlify: nil site")
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	s.seq++
	id := fmt.Sprintf("deploy-%d", s.seq)
	root := filepath.Join(s.base, "deploys", id)
	if err := os.MkdirAll(root, 0o755); err != nil {
		return nil, err
	}
	for name, body := range files {
		if err := writeRel(root, name, body); err != nil {
			_ = os.RemoveAll(root)
			return nil, err
		}
	}
	live, err := delivery.ServeDir(root)
	if err != nil {
		_ = os.RemoveAll(root)
		return nil, err
	}
	d := &Deploy{ID: id, Status: StatusReady, Draft: true, Site: live, root: root}
	s.deploys[id] = d
	return d, nil
}

// FailDeploy records an error deploy with no URL. Publish must reject it.
func (s *Site) FailDeploy() *Deploy {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.seq++
	id := fmt.Sprintf("deploy-%d", s.seq)
	d := &Deploy{ID: id, Status: StatusError, Draft: true}
	s.deploys[id] = d
	return d
}

// IncompleteDeploy records a building deploy with no URL. Publish must reject it.
func (s *Site) IncompleteDeploy() *Deploy {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.seq++
	id := fmt.Sprintf("deploy-%d", s.seq)
	d := &Deploy{ID: id, Status: StatusBuilding, Draft: true}
	s.deploys[id] = d
	return d
}

// Publish points production at a ready deploy. Failed/incomplete deploys error.
func (s *Site) Publish(deployID string) error {
	if s == nil {
		return fmt.Errorf("netlify: nil site")
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	d, ok := s.deploys[deployID]
	if !ok {
		return fmt.Errorf("netlify: deploy %q not found", deployID)
	}
	if d.Status != StatusReady || d.Site == nil || d.root == "" {
		return fmt.Errorf("netlify: deploy %q not ready (status %q)", deployID, d.Status)
	}

	prodRoot := filepath.Join(s.base, "production")
	_ = os.RemoveAll(prodRoot)
	if err := copyTree(d.root, prodRoot); err != nil {
		return err
	}
	if s.prod != nil {
		_ = s.prod.Close()
		s.prod = nil
	}
	live, err := delivery.ServeDir(prodRoot)
	if err != nil {
		return err
	}
	d.Draft = false
	s.prodID = deployID
	s.prod = live
	return nil
}

// Production returns the live production Site (nil until Publish).
func (s *Site) Production() *delivery.Site {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.prod
}

// ProductionID is the published deploy id (empty until Publish).
func (s *Site) ProductionID() string {
	if s == nil {
		return ""
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.prodID
}

// Close stops listeners; leaves base on disk for file checks.
func (s *Site) Close() {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, d := range s.deploys {
		if d != nil && d.Site != nil {
			_ = d.Site.Close()
			d.Site = nil
		}
	}
	if s.prod != nil {
		_ = s.prod.Close()
		s.prod = nil
	}
}

func writeRel(root, name string, body []byte) error {
	if filepath.IsAbs(name) || strings.Contains(name, "..") {
		return fmt.Errorf("netlify: bad path %q", name)
	}
	path := filepath.Join(root, name)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	return os.WriteFile(path, body, 0o644)
}

func copyTree(src, dst string) error {
	return filepath.Walk(src, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}
		target := filepath.Join(dst, rel)
		if info.IsDir() {
			return os.MkdirAll(target, 0o755)
		}
		body, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
			return err
		}
		return os.WriteFile(target, body, info.Mode().Perm())
	})
}
