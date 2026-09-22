package gcpcloudbuild

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

const Dialect = "gcp.cloudbuild/yaml-subset"

type ConfigFile struct {
	Steps         []Step            `yaml:"steps"`
	Substitutions map[string]string `yaml:"substitutions"`
}

type Step struct {
	ID         string   `yaml:"id"`
	Name       string   `yaml:"name"`
	Entrypoint string   `yaml:"entrypoint"`
	Args       []string `yaml:"args"`
	Script     string   `yaml:"script"`
	WaitFor    []string `yaml:"waitFor"`
	Env        []string `yaml:"env"`
}

type Config struct {
	WorkDir string
	Env     map[string]string
	Output  io.Writer
}

type StepResult struct {
	ID       string
	ExitCode int
	Skipped  bool
}

type RunResult struct {
	Status string
	Steps  []StepResult
}

func Parse(data []byte) (*ConfigFile, error) {
	var c ConfigFile
	if err := yaml.Unmarshal(data, &c); err != nil {
		return nil, err
	}
	return &c, nil
}

func Run(ctx context.Context, cf *ConfigFile, cfg Config) (*RunResult, error) {
	if cfg.WorkDir == "" {
		return nil, fmt.Errorf("gcpcloudbuild: empty WorkDir")
	}
	out := &RunResult{Status: "SUCCESS"}
	done := map[string]bool{}
	failed := map[string]bool{}
	subs := map[string]string{}
	for k, v := range cf.Substitutions {
		subs[k] = v
	}
	for _, st := range cf.Steps {
		id := st.ID
		if id == "" {
			id = st.Name
		}
		ready := true
		for _, w := range st.WaitFor {
			if w == "-" {
				continue
			}
			if failed[w] || !done[w] {
				ready = false
				break
			}
		}
		if !ready {
			out.Steps = append(out.Steps, StepResult{ID: id, Skipped: true})
			out.Status = "FAILURE"
			failed[id] = true
			continue
		}
		cmd := st.Script
		if cmd == "" && st.Entrypoint != "" {
			cmd = st.Entrypoint + " " + strings.Join(st.Args, " ")
		}
		if cmd == "" && len(st.Args) > 0 {
			cmd = strings.Join(st.Args, " ")
		}
		for k, v := range subs {
			cmd = strings.ReplaceAll(cmd, "${"+k+"}", v)
			cmd = strings.ReplaceAll(cmd, "$"+k, v)
		}
		env := shellstep.BaseEnv(cfg.Env)
		for _, e := range st.Env {
			env = append(env, e)
		}
		code, err := shellstep.Run(ctx, cfg.WorkDir, cmd, env, cfg.Output)
		if err != nil {
			return nil, err
		}
		out.Steps = append(out.Steps, StepResult{ID: id, ExitCode: code})
		if code != 0 {
			out.Status = "FAILURE"
			failed[id] = true
		} else {
			done[id] = true
		}
		_ = filepath.Separator
		_ = os.DevNull
	}
	return out, nil
}
