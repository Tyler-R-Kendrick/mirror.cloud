package azurepipelines

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/execution/cicd/shellstep"
	"gopkg.in/yaml.v3"
)

const Dialect = "azure.pipelines/yaml-subset"

type Pipeline struct {
	Stages []Stage `yaml:"stages"`
	Jobs   []Job   `yaml:"jobs"` // top-level jobs when no stages
}

type Stage struct {
	Stage string `yaml:"stage"`
	Jobs  []Job  `yaml:"jobs"`
}

type Job struct {
	Job         string   `yaml:"job"`
	DependsOn   any      `yaml:"dependsOn"`
	Condition   string   `yaml:"condition"`
	Steps       []Step   `yaml:"steps"`
	Environment *struct {
		Name string `yaml:"name"`
	} `yaml:"environment"`
}

type Step struct {
	Script string `yaml:"script"`
	Bash   string `yaml:"bash"`
	DisplayName string `yaml:"displayName"`
}

type Config struct {
	WorkDir string
	Env     map[string]string
	Output  io.Writer
	// ApproveEnvironment returns true to pass environment deployment jobs.
	ApproveEnvironment func(name string) bool
}

type RunResult struct {
	Status string
	Jobs   []string
}

func Parse(data []byte) (*Pipeline, error) {
	var p Pipeline
	if err := yaml.Unmarshal(data, &p); err != nil {
		return nil, err
	}
	return &p, nil
}

func Run(ctx context.Context, p *Pipeline, cfg Config) (*RunResult, error) {
	if cfg.WorkDir == "" {
		return nil, fmt.Errorf("azurepipelines: empty WorkDir")
	}
	out := &RunResult{Status: "succeeded"}
	jobs := p.Jobs
	if len(p.Stages) > 0 {
		jobs = nil
		for _, st := range p.Stages {
			jobs = append(jobs, st.Jobs...)
		}
	}
	done := map[string]bool{}
	for _, j := range jobs {
		name := j.Job
		if name == "" {
			name = "job"
		}
		if j.Environment != nil && j.Environment.Name != "" {
			ok := cfg.ApproveEnvironment != nil && cfg.ApproveEnvironment(j.Environment.Name)
			if !ok {
				out.Status = "blocked"
				return out, nil
			}
		}
		if strings.Contains(j.Condition, "failed()") {
			// ignore exotic
		}
		for _, st := range j.Steps {
			cmd := st.Script
			if cmd == "" {
				cmd = st.Bash
			}
			if cmd == "" {
				continue
			}
			code, err := shellstep.Run(ctx, cfg.WorkDir, cmd, shellstep.BaseEnv(cfg.Env), cfg.Output)
			if err != nil {
				return nil, err
			}
			if code != 0 {
				out.Status = "failed"
				return out, nil
			}
		}
		done[name] = true
		out.Jobs = append(out.Jobs, name)
		_ = filepath.Separator
		_ = os.DevNull
	}
	return out, nil
}
