// Package render is a mirror-owned Render-ish site/service adapter.
// Local Service create + static Deploy + status + Redeploy only —
// not Render Services / Static Sites / Deploy Hooks API parity.
package render

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/execution/cicd/delivery"
)

const (
	StatusCreated  = "created"
	StatusLive     = "live"
	StatusFailed   = "failed"
	KindStaticSite = "static_site"
	KindWebService = "web_service"
)

// Service is one Render-ish site/service with deploys and a live pointer.
type Service struct {
	mu      sync.Mutex
	ID      string
	Name    string
	Kind    string
	base    string
	seq     int
	deploys map[string]*Deploy
	liveID  string
	live    *delivery.Site
	last    map[string][]byte
}

// Deploy is one revision; Site is set when live.
type Deploy struct {
	ID     string
	Status string
	Site   *delivery.Site
	root   string
}

// CreateSite makes a local static_site service.
func CreateSite(name string) (*Service, error) {
	return CreateService(name, KindStaticSite)
}

// CreateService makes a local service under a temp root.
func CreateService(name, kind string) (*Service, error) {
	if name == "" {
		name = "service"
	}
	if kind == "" {
		kind = KindStaticSite
	}
	base, err := os.MkdirTemp("", "mirror-render-")
	if err != nil {
		return nil, err
	}
	return &Service{
		ID:      "srv-" + name,
		Name:    name,
		Kind:    kind,
		base:    base,
		deploys: make(map[string]*Deploy),
	}, nil
}

// Deploy uploads files as a live deploy.
func (s *Service) Deploy(files map[string][]byte) (*Deploy, error) {
	if s == nil {
		return nil, fmt.Errorf("render: nil service")
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
	if s.live != nil {
		_ = s.live.Close()
		s.live = nil
	}
	d := &Deploy{ID: id, Status: StatusLive, Site: live, root: root}
	s.deploys[id] = d
	s.liveID = id
	s.live = live
	s.last = cloneFiles(files)
	return d, nil
}

// FailDeploy records a failed deploy; does not flip live.
func (s *Service) FailDeploy() *Deploy {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.seq++
	id := fmt.Sprintf("deploy-%d", s.seq)
	d := &Deploy{ID: id, Status: StatusFailed}
	s.deploys[id] = d
	return d
}

// Redeploy re-publishes the last successful files as a new live deploy.
func (s *Service) Redeploy() (*Deploy, error) {
	if s == nil {
		return nil, fmt.Errorf("render: nil service")
	}
	s.mu.Lock()
	files := cloneFiles(s.last)
	s.mu.Unlock()
	if len(files) == 0 {
		return nil, fmt.Errorf("render: nothing to redeploy")
	}
	return s.Deploy(files)
}

// Status returns service-level status derived from the live pointer.
func (s *Service) Status() string {
	if s == nil {
		return ""
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.live == nil || s.liveID == "" {
		return StatusCreated
	}
	d := s.deploys[s.liveID]
	if d == nil {
		return StatusCreated
	}
	return d.Status
}

// Live returns the current live Site (nil until first successful Deploy).
func (s *Service) Live() *delivery.Site {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.live
}

// LiveID is the current live deploy id.
func (s *Service) LiveID() string {
	if s == nil {
		return ""
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.liveID
}

// DeployStatus returns one deploy's status (empty if unknown).
func (s *Service) DeployStatus(deployID string) string {
	if s == nil {
		return ""
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	d := s.deploys[deployID]
	if d == nil {
		return ""
	}
	return d.Status
}

// Close stops listeners.
func (s *Service) Close() {
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
	s.live = nil
}

func writeRel(root, name string, body []byte) error {
	if filepath.IsAbs(name) || strings.Contains(name, "..") {
		return fmt.Errorf("render: bad path %q", name)
	}
	path := filepath.Join(root, name)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	return os.WriteFile(path, body, 0o644)
}

func cloneFiles(in map[string][]byte) map[string][]byte {
	if in == nil {
		return nil
	}
	out := make(map[string][]byte, len(in))
	for k, v := range in {
		cp := make([]byte, len(v))
		copy(cp, v)
		out[k] = cp
	}
	return out
}
