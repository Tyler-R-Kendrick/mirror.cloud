// Package gcpdeploy is a mirror-owned Cloud Deploy-ish staging→approval→production gate.
// Artifact bytes (or optional Cloud Build compose) to HTTP sites — not Skaffold/Cloud Run parity.
package gcpdeploy

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/execution/cicd/delivery"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/execution/cicd/gcpcloudbuild"
)

const (
	PhaseStaging    = "staging"
	PhasePending    = "pending_approval"
	PhaseApproved   = "approved"
	PhaseRejected   = "rejected"
	PhaseProduction = "production"
)

// Pipeline is one delivery pipeline with an explicit approval gate before promote.
type Pipeline struct {
	mu         sync.Mutex
	base       string
	StagingDir string
	ProdDir    string
	Staging    *delivery.Site
	Production *delivery.Site
	Phase      string
	Build      *gcpcloudbuild.RunResult
	prior      []byte // last production bytes for rollback
}

// New creates a pipeline under base (temp if empty).
func New(base string) (*Pipeline, error) {
	if base == "" {
		dir, err := os.MkdirTemp("", "mirror-gcpdeploy-")
		if err != nil {
			return nil, err
		}
		base = dir
	}
	p := &Pipeline{
		base:       base,
		StagingDir: filepath.Join(base, "staging"),
		ProdDir:    filepath.Join(base, "production"),
		Phase:      PhaseStaging,
	}
	for _, d := range []string{p.StagingDir, p.ProdDir} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			return nil, err
		}
	}
	return p, nil
}

// Stage publishes files to the staging site and moves phase to pending_approval.
func (p *Pipeline) Stage(files map[string][]byte) error {
	if p == nil {
		return fmt.Errorf("gcpdeploy: nil pipeline")
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := p.writeTree(p.StagingDir, files); err != nil {
		return err
	}
	if p.Staging != nil {
		_ = p.Staging.Close()
		p.Staging = nil
	}
	site, err := delivery.ServeDir(p.StagingDir)
	if err != nil {
		return err
	}
	p.Staging = site
	p.Phase = PhasePending
	return nil
}

// BuildAndStage runs local Cloud Build YAML then stages artifactRel as index.html.
func (p *Pipeline) BuildAndStage(ctx context.Context, yaml []byte, workDir, artifactRel string) error {
	if p == nil {
		return fmt.Errorf("gcpdeploy: nil pipeline")
	}
	cf, err := gcpcloudbuild.Parse(yaml)
	if err != nil {
		return err
	}
	res, err := gcpcloudbuild.Run(ctx, cf, gcpcloudbuild.Config{WorkDir: workDir})
	if err != nil {
		return err
	}
	p.mu.Lock()
	p.Build = res
	p.mu.Unlock()
	if res.Status != "SUCCESS" {
		return fmt.Errorf("gcpdeploy: build %s", res.Status)
	}
	body, err := os.ReadFile(filepath.Join(workDir, artifactRel))
	if err != nil {
		return err
	}
	return p.Stage(map[string][]byte{"index.html": body})
}

// Approve grants the pending release (required before Promote).
func (p *Pipeline) Approve() error {
	if p == nil {
		return fmt.Errorf("gcpdeploy: nil pipeline")
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.Phase != PhasePending || p.Staging == nil {
		return fmt.Errorf("gcpdeploy: nothing pending (phase %q)", p.Phase)
	}
	p.Phase = PhaseApproved
	return nil
}

// Reject blocks promotion of the pending staging release.
func (p *Pipeline) Reject() error {
	if p == nil {
		return fmt.Errorf("gcpdeploy: nil pipeline")
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.Phase != PhasePending {
		return fmt.Errorf("gcpdeploy: nothing pending (phase %q)", p.Phase)
	}
	p.Phase = PhaseRejected
	return nil
}

// Promote copies staging to production. Requires Approve; Reject/pending error.
func (p *Pipeline) Promote() error {
	if p == nil {
		return fmt.Errorf("gcpdeploy: nil pipeline")
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.Phase != PhaseApproved || p.Staging == nil {
		return fmt.Errorf("gcpdeploy: promote blocked (phase %q)", p.Phase)
	}
	body, err := os.ReadFile(filepath.Join(p.StagingDir, "index.html"))
	if err != nil {
		return err
	}
	if p.Production != nil {
		if prior, rerr := os.ReadFile(filepath.Join(p.ProdDir, "index.html")); rerr == nil {
			p.prior = prior
		}
		_ = p.Production.Close()
		p.Production = nil
	}
	if err := os.WriteFile(filepath.Join(p.ProdDir, "index.html"), body, 0o644); err != nil {
		return err
	}
	site, err := delivery.ServeDir(p.ProdDir)
	if err != nil {
		return err
	}
	p.Production = site
	p.Phase = PhaseProduction
	return nil
}

// Rollback restores prior production bytes (or supplied prior if non-nil).
func (p *Pipeline) Rollback(prior []byte) error {
	if p == nil {
		return fmt.Errorf("gcpdeploy: nil pipeline")
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if prior == nil {
		prior = p.prior
	}
	if prior == nil {
		return fmt.Errorf("gcpdeploy: nothing to rollback")
	}
	if p.Production != nil {
		_ = p.Production.Close()
		p.Production = nil
	}
	if err := os.MkdirAll(p.ProdDir, 0o755); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(p.ProdDir, "index.html"), prior, 0o644); err != nil {
		return err
	}
	site, err := delivery.ServeDir(p.ProdDir)
	if err != nil {
		return err
	}
	p.Production = site
	p.Phase = PhaseProduction
	return nil
}

// Close stops listeners.
func (p *Pipeline) Close() {
	if p == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.Staging != nil {
		_ = p.Staging.Close()
		p.Staging = nil
	}
	if p.Production != nil {
		_ = p.Production.Close()
		p.Production = nil
	}
}

func (p *Pipeline) writeTree(root string, files map[string][]byte) error {
	_ = os.RemoveAll(root)
	if err := os.MkdirAll(root, 0o755); err != nil {
		return err
	}
	for name, body := range files {
		if err := writeRel(root, name, body); err != nil {
			return err
		}
	}
	return nil
}

func writeRel(root, name string, body []byte) error {
	if filepath.IsAbs(name) || strings.Contains(name, "..") {
		return fmt.Errorf("gcpdeploy: bad path %q", name)
	}
	path := filepath.Join(root, name)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	return os.WriteFile(path, body, 0o644)
}
