// Package gitlabci is a bounded local evaluator for .gitlab-ci.yml.
// Trusted process only — not gitlab-ci-local parity. See capability.json.
package gitlabci

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/execution/cicd/shellstep"
)

const Dialect = "gitlab.ci/yml-subset/1"

var reserved = map[string]struct{}{
	"stages": {}, "variables": {}, "default": {}, "include": {},
	"workflow": {}, "image": {}, "services": {}, "cache": {},
	"before_script": {}, "after_script": {},
}

// Pipeline is the supported .gitlab-ci.yml subset.
type Pipeline struct {
	Stages []string
	Jobs   map[string]Job
}

// Job is one CI job.
type Job struct {
	Stage     string     `yaml:"stage"`
	Script    []string   `yaml:"script"`
	Needs     StringList `yaml:"needs"`
	Artifacts *Artifacts `yaml:"artifacts"`
	When      string     `yaml:"when"` // "" | on_success | manual
	Only      []string   `yaml:"only"`
	If        string     `yaml:"if"`
}

// Artifacts lists paths to retain after a successful job.
type Artifacts struct {
	Paths []string `yaml:"paths"`
}

// StringList accepts a YAML string or sequence.
type StringList []string

func (s *StringList) UnmarshalYAML(n *yaml.Node) error {
	if n.Kind == yaml.ScalarNode {
		*s = []string{n.Value}
		return nil
	}
	var list []string
	if err := n.Decode(&list); err != nil {
		return err
	}
	*s = list
	return nil
}

// Parse unmarshals .gitlab-ci.yml.
func Parse(data []byte) (*Pipeline, error) {
	var root yaml.Node
	if err := yaml.Unmarshal(data, &root); err != nil {
		return nil, fmt.Errorf("gitlabci: parse: %w", err)
	}
	if root.Kind != yaml.DocumentNode || len(root.Content) == 0 {
		return nil, fmt.Errorf("gitlabci: empty document")
	}
	doc := root.Content[0]
	if doc.Kind != yaml.MappingNode {
		return nil, fmt.Errorf("gitlabci: want mapping")
	}
	p := &Pipeline{Jobs: map[string]Job{}}
	for i := 0; i+1 < len(doc.Content); i += 2 {
		key := doc.Content[i].Value
		val := doc.Content[i+1]
		if key == "stages" {
			if err := val.Decode(&p.Stages); err != nil {
				return nil, err
			}
			continue
		}
		if _, skip := reserved[key]; skip {
			continue
		}
		if strings.HasPrefix(key, ".") {
			continue // hidden jobs
		}
		var j Job
		if err := val.Decode(&j); err != nil {
			return nil, fmt.Errorf("gitlabci: job %s: %w", key, err)
		}
		if len(j.Script) == 0 {
			continue
		}
		if j.Stage == "" {
			j.Stage = "test"
		}
		p.Jobs[key] = j
	}
	if len(p.Jobs) == 0 {
		return nil, fmt.Errorf("gitlabci: no jobs")
	}
	if len(p.Stages) == 0 {
		p.Stages = []string{"build", "test", "deploy"}
	}
	return p, nil
}

// Config confines a local pipeline run.
type Config struct {
	WorkDir string
	Branch  string
	Env     map[string]string
	Output  io.Writer
	// ArtifactDir defaults to WorkDir/.mirror-gl-artifacts
	ArtifactDir string
	// Manual is consulted for when:manual jobs. nil or false → status blocked.
	Manual func(job string) bool
}

// JobResult is one job observation.
type JobResult struct {
	Name     string `json:"name"`
	Stage    string `json:"stage"`
	Status   string `json:"status"` // succeeded | failed | skipped | blocked | selected-out
	ExitCode int    `json:"exitCode,omitempty"`
	Logs     string `json:"logs,omitempty"`
}

// RunResult is the terminal pipeline observation.
type RunResult struct {
	Status        string      `json:"status"` // succeeded | failed | blocked | skipped
	Dialect       string      `json:"dialect"`
	PendingManual string      `json:"pendingManual,omitempty"`
	Jobs          []JobResult `json:"jobs"`
}

// Run executes selected jobs in stage order (needs respected within/across stages).
func Run(ctx context.Context, p *Pipeline, cfg Config) (*RunResult, error) {
	if p == nil {
		return nil, fmt.Errorf("gitlabci: nil pipeline")
	}
	if cfg.WorkDir == "" {
		return nil, fmt.Errorf("gitlabci: empty WorkDir")
	}
	art := cfg.ArtifactDir
	if art == "" {
		art = filepath.Join(cfg.WorkDir, ".mirror-gl-artifacts")
	}
	if err := os.MkdirAll(art, 0o755); err != nil {
		return nil, err
	}
	out := &RunResult{Status: "succeeded", Dialect: Dialect}

	stageIdx := map[string]int{}
	for i, s := range p.Stages {
		stageIdx[s] = i
	}

	names := make([]string, 0, len(p.Jobs))
	for n := range p.Jobs {
		names = append(names, n)
	}
	sort.Slice(names, func(i, j int) bool {
		a, b := p.Jobs[names[i]], p.Jobs[names[j]]
		if stageIdx[a.Stage] != stageIdx[b.Stage] {
			return stageIdx[a.Stage] < stageIdx[b.Stage]
		}
		return names[i] < names[j]
	})

	jobOK := map[string]bool{}
	overlayBase := map[string]string{
		"CI_COMMIT_BRANCH": cfg.Branch,
		"CI_PROJECT_DIR":   cfg.WorkDir,
		"MIRROR_ARTIFACTS": art,
	}
	for k, v := range cfg.Env {
		overlayBase[k] = v
	}

	for _, name := range names {
		job := p.Jobs[name]
		if !jobSelected(job, cfg.Branch) {
			out.Jobs = append(out.Jobs, JobResult{Name: name, Stage: job.Stage, Status: "selected-out"})
			jobOK[name] = false
			continue
		}
		depsOK := true
		for _, dep := range job.Needs {
			if !jobOK[dep] {
				depsOK = false
				break
			}
		}
		if !depsOK {
			out.Jobs = append(out.Jobs, JobResult{Name: name, Stage: job.Stage, Status: "skipped"})
			jobOK[name] = false
			if out.Status == "succeeded" {
				out.Status = "failed"
			}
			continue
		}
		if job.When == "manual" {
			ok := cfg.Manual != nil && cfg.Manual(name)
			if !ok {
				out.Jobs = append(out.Jobs, JobResult{Name: name, Stage: job.Stage, Status: "blocked"})
				jobOK[name] = false
				out.Status = "blocked"
				out.PendingManual = name
				// dependents will skip; keep scanning to mark them
				continue
			}
		}

		// restore artifacts into workdir before script
		_ = restoreArtifacts(art, cfg.WorkDir)

		var log strings.Builder
		w := io.Writer(&log)
		if cfg.Output != nil {
			w = io.MultiWriter(&log, cfg.Output)
		}
		env := shellstep.BaseEnv(overlayBase)
		exit := 0
		for _, cmd := range job.Script {
			code, err := shellstep.Run(ctx, cfg.WorkDir, cmd, env, w)
			if err != nil {
				return nil, err
			}
			if code != 0 {
				exit = code
				break
			}
		}
		jr := JobResult{Name: name, Stage: job.Stage, ExitCode: exit, Logs: log.String()}
		if exit != 0 {
			jr.Status = "failed"
			jobOK[name] = false
			out.Status = "failed"
		} else {
			jr.Status = "succeeded"
			jobOK[name] = true
			if job.Artifacts != nil {
				if err := storeArtifacts(cfg.WorkDir, art, job.Artifacts.Paths); err != nil {
					return nil, err
				}
			}
		}
		out.Jobs = append(out.Jobs, jr)
	}
	return out, nil
}

func jobSelected(j Job, branch string) bool {
	if len(j.Only) > 0 {
		ok := false
		for _, b := range j.Only {
			if b == branch {
				ok = true
				break
			}
		}
		if !ok {
			return false
		}
	}
	if j.If != "" {
		return evalIfBranch(j.If, branch)
	}
	return true
}

var reIfBranch = regexp.MustCompile(`^\s*\$CI_COMMIT_BRANCH\s*==\s*"([^"]+)"\s*$`)

func evalIfBranch(expr, branch string) bool {
	m := reIfBranch.FindStringSubmatch(expr)
	if m == nil {
		// unsupported if → treat as false (fail closed for selection)
		return false
	}
	return m[1] == branch
}

func storeArtifacts(work, art string, paths []string) error {
	for _, p := range paths {
		if filepath.IsAbs(p) || strings.Contains(p, "..") {
			return fmt.Errorf("gitlabci: bad artifact path %q", p)
		}
		src := filepath.Join(work, p)
		dst := filepath.Join(art, p)
		if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
			return err
		}
		b, err := os.ReadFile(src)
		if err != nil {
			return fmt.Errorf("gitlabci: artifact %s: %w", p, err)
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
