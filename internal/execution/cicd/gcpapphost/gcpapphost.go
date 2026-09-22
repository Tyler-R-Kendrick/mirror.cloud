// Package gcpapphost composes Cloud Build YAML → mirror-owned static delivery.
// Not Firebase App Hosting / Cloud Deploy full parity.
package gcpapphost

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/execution/cicd/delivery"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/execution/cicd/gcpcloudbuild"
)

// Rollout is one staging→production promotion of build output.
type Rollout struct {
	Staging    *delivery.Site
	Production *delivery.Site
	Build      *gcpcloudbuild.RunResult
	StagingDir string
	ProdDir    string
}

// BuildAndStage runs cloudbuild.yaml in workDir, copies artifactRel to staging site.
func BuildAndStage(ctx context.Context, yaml []byte, workDir, artifactRel string) (*Rollout, error) {
	cf, err := gcpcloudbuild.Parse(yaml)
	if err != nil {
		return nil, err
	}
	res, err := gcpcloudbuild.Run(ctx, cf, gcpcloudbuild.Config{WorkDir: workDir})
	if err != nil {
		return nil, err
	}
	r := &Rollout{Build: res, StagingDir: filepath.Join(workDir, ".staging"), ProdDir: filepath.Join(workDir, ".prod")}
	if res.Status != "SUCCESS" {
		return r, nil
	}
	body, err := os.ReadFile(filepath.Join(workDir, artifactRel))
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(r.StagingDir, 0o755); err != nil {
		return nil, err
	}
	if err := os.WriteFile(filepath.Join(r.StagingDir, "index.html"), body, 0o644); err != nil {
		return nil, err
	}
	site, err := delivery.ServeDir(r.StagingDir)
	if err != nil {
		return nil, err
	}
	r.Staging = site
	return r, nil
}

// Promote copies staging bytes to production (approval stand-in already granted by caller).
func (r *Rollout) Promote() error {
	if r == nil || r.Staging == nil {
		return fmt.Errorf("gcpapphost: nothing to promote")
	}
	if r.Build != nil && r.Build.Status != "SUCCESS" {
		return fmt.Errorf("gcpapphost: cannot promote failed build")
	}
	body, err := os.ReadFile(filepath.Join(r.StagingDir, "index.html"))
	if err != nil {
		return err
	}
	if err := os.MkdirAll(r.ProdDir, 0o755); err != nil {
		return err
	}
	if r.Production != nil {
		_ = r.Production.Close()
		r.Production = nil
	}
	if err := os.WriteFile(filepath.Join(r.ProdDir, "index.html"), body, 0o644); err != nil {
		return err
	}
	site, err := delivery.ServeDir(r.ProdDir)
	if err != nil {
		return err
	}
	r.Production = site
	return nil
}

// Rollback restores prior production bytes.
func (r *Rollout) Rollback(prior []byte) error {
	if r == nil {
		return fmt.Errorf("gcpapphost: nil rollout")
	}
	if err := os.MkdirAll(r.ProdDir, 0o755); err != nil {
		return err
	}
	if r.Production != nil {
		_ = r.Production.Close()
		r.Production = nil
	}
	if err := os.WriteFile(filepath.Join(r.ProdDir, "index.html"), prior, 0o644); err != nil {
		return err
	}
	site, err := delivery.ServeDir(r.ProdDir)
	if err != nil {
		return err
	}
	r.Production = site
	return nil
}

// Close stops listeners.
func (r *Rollout) Close() {
	if r == nil {
		return
	}
	if r.Staging != nil {
		_ = r.Staging.Close()
	}
	if r.Production != nil {
		_ = r.Production.Close()
	}
}
