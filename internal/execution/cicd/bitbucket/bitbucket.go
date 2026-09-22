// Package bitbucket is a bounded local evaluator for bitbucket-pipelines.yml.
// Trusted process only — not full Pipelines runner parity. See capability.json.
package bitbucket

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"gopkg.in/yaml.v3"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/execution/cicd/shellstep"
)

const Dialect = "bitbucket.pipelines/yml-subset/1"

// File is the supported bitbucket-pipelines.yml subset.
type File struct {
	Pipelines Pipelines `yaml:"pipelines"`
}

// Pipelines holds default and branch pipelines.
type Pipelines struct {
	Default  []StepWrap            `yaml:"default"`
	Branches map[string][]StepWrap `yaml:"branches"`
}

// StepWrap is the YAML `{ step: ... }` wrapper.
type StepWrap struct {
	Step Step `yaml:"step"`
}

// Step is one pipeline step.
type Step struct {
	Name       string   `yaml:"name"`
	Script     []string `yaml:"script"`
	Artifacts  []string `yaml:"artifacts"`
	Trigger    string   `yaml:"trigger"` // "" | automatic | manual
	Deployment string   `yaml:"deployment"`
}

// Parse unmarshals bitbucket-pipelines.yml.
func Parse(data []byte) (*File, error) {
	var f File
	if err := yaml.Unmarshal(data, &f); err != nil {
		return nil, fmt.Errorf("bitbucket: parse: %w", err)
	}
	if len(f.Pipelines.Default) == 0 && len(f.Pipelines.Branches) == 0 {
		return nil, fmt.Errorf("bitbucket: no pipelines")
	}
	return &f, nil
}

// Config confines a local pipeline run.
type Config struct {
	WorkDir string
	Branch  string
	Env     map[string]string
	Output  io.Writer
	// ArtifactDir defaults to WorkDir/.mirror-bb-artifacts
	ArtifactDir string
	// Manual is consulted for trigger:manual steps. nil or false → blocked.
	Manual func(step string) bool
}

// StepResult is one step observation.
type StepResult struct {
	Name       string `json:"name"`
	Deployment string `json:"deployment,omitempty"`
	Status     string `json:"status"` // succeeded | failed | blocked | skipped
	ExitCode   int    `json:"exitCode,omitempty"`
	Logs       string `json:"logs,omitempty"`
}

// RunResult is the terminal pipeline observation.
type RunResult struct {
	Status        string       `json:"status"` // succeeded | failed | blocked | skipped
	Dialect       string       `json:"dialect"`
	PendingManual string       `json:"pendingManual,omitempty"`
	Steps         []StepResult `json:"steps"`
}

// Run selects pipelines.branches[Branch] or default, then executes steps in order.
func Run(ctx context.Context, f *File, cfg Config) (*RunResult, error) {
	if f == nil {
		return nil, fmt.Errorf("bitbucket: nil file")
	}
	if cfg.WorkDir == "" {
		return nil, fmt.Errorf("bitbucket: empty WorkDir")
	}
	steps := selectPipeline(f, cfg.Branch)
	if len(steps) == 0 {
		return &RunResult{Status: "skipped", Dialect: Dialect}, nil
	}
	art := cfg.ArtifactDir
	if art == "" {
		art = filepath.Join(cfg.WorkDir, ".mirror-bb-artifacts")
	}
	if err := os.MkdirAll(art, 0o755); err != nil {
		return nil, err
	}
	out := &RunResult{Status: "succeeded", Dialect: Dialect}
	overlay := map[string]string{
		"BITBUCKET_BRANCH": cfg.Branch,
		"MIRROR_ARTIFACTS": art,
	}
	for k, v := range cfg.Env {
		overlay[k] = v
	}
	env := shellstep.BaseEnv(overlay)
	blocked := false

	for _, wrap := range steps {
		st := wrap.Step
		name := st.Name
		if name == "" {
			name = "step"
		}
		if blocked {
			out.Steps = append(out.Steps, StepResult{Name: name, Deployment: st.Deployment, Status: "skipped"})
			continue
		}
		if st.Trigger == "manual" {
			ok := cfg.Manual != nil && cfg.Manual(name)
			if !ok {
				out.Steps = append(out.Steps, StepResult{Name: name, Deployment: st.Deployment, Status: "blocked"})
				out.Status = "blocked"
				out.PendingManual = name
				blocked = true
				continue
			}
		}
		_ = restoreArtifacts(art, cfg.WorkDir)
		var log strings.Builder
		w := io.Writer(&log)
		if cfg.Output != nil {
			w = io.MultiWriter(&log, cfg.Output)
		}
		exit := 0
		for _, cmd := range st.Script {
			code, err := shellstep.Run(ctx, cfg.WorkDir, cmd, env, w)
			if err != nil {
				return nil, err
			}
			if code != 0 {
				exit = code
				break
			}
		}
		sr := StepResult{Name: name, Deployment: st.Deployment, ExitCode: exit, Logs: log.String()}
		if exit != 0 {
			sr.Status = "failed"
			out.Status = "failed"
			blocked = true
		} else {
			sr.Status = "succeeded"
			if err := storeArtifacts(cfg.WorkDir, art, st.Artifacts); err != nil {
				return nil, err
			}
		}
		out.Steps = append(out.Steps, sr)
	}
	return out, nil
}

func selectPipeline(f *File, branch string) []StepWrap {
	if branch != "" && f.Pipelines.Branches != nil {
		if steps, ok := f.Pipelines.Branches[branch]; ok {
			return steps
		}
	}
	return f.Pipelines.Default
}

func storeArtifacts(work, art string, paths []string) error {
	for _, p := range paths {
		if filepath.IsAbs(p) || strings.Contains(p, "..") {
			return fmt.Errorf("bitbucket: bad artifact path %q", p)
		}
		src := filepath.Join(work, p)
		dst := filepath.Join(art, p)
		if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
			return err
		}
		b, err := os.ReadFile(src)
		if err != nil {
			return fmt.Errorf("bitbucket: artifact %s: %w", p, err)
		}
		if err := os.WriteFile(dst, b, 0o644); err != nil {
			return err
		}
	}
	return nil
}

func restoreArtifacts(art, work string) error {
	return filepath.Walk(art, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return err
		}
		rel, err := filepath.Rel(art, path)
		if err != nil {
			return err
		}
		dst := filepath.Join(work, rel)
		if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
			return err
		}
		b, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		return os.WriteFile(dst, b, 0o644)
	})
}
