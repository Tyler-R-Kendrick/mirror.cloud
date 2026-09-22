// Package githubactions is a bounded local evaluator for GitHub Actions workflow YAML.
// Trusted process only — not act / full runner parity. See capability.json.
package githubactions

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/execution/cicd/shellstep"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/execution/process"
)

const (
	// Dialect is the local profile id.
	Dialect = "github.actions/workflow-subset/1"
)

// Workflow is the supported workflow subset.
type Workflow struct {
	Name string         `yaml:"name"`
	On   On             `yaml:"on"`
	Jobs map[string]Job `yaml:"jobs"`
}

// On is the trigger block (push only in this profile).
type On struct {
	Push *Push `yaml:"push"`
}

// UnmarshalYAML accepts `on: push` or `on: { push: ... }`.
func (o *On) UnmarshalYAML(n *yaml.Node) error {
	if n.Kind == yaml.ScalarNode {
		if n.Value == "push" {
			o.Push = &Push{}
		}
		return nil
	}
	type plain On
	var p plain
	if err := n.Decode(&p); err != nil {
		return err
	}
	*o = On(p)
	return nil
}

// Push filters branches when set.
type Push struct {
	Branches []string `yaml:"branches"`
}

// Job is one jobs.<id> entry.
type Job struct {
	Needs    StringList        `yaml:"needs"`
	Strategy *Strategy         `yaml:"strategy"`
	Outputs  map[string]string `yaml:"outputs"`
	Steps    []Step            `yaml:"steps"`
}

// Strategy holds matrix axes plus include/exclude.
type Strategy struct {
	Matrix Matrix `yaml:"matrix"`
}

// Matrix is axes (string→list) plus include/exclude rows.
type Matrix struct {
	Axes    map[string][]string
	Include []map[string]string
	Exclude []map[string]string
}

// UnmarshalYAML accepts GHA matrix shape with include/exclude reserved keys.
func (m *Matrix) UnmarshalYAML(n *yaml.Node) error {
	var raw map[string]yaml.Node
	if err := n.Decode(&raw); err != nil {
		return err
	}
	m.Axes = map[string][]string{}
	for k, node := range raw {
		switch k {
		case "include", "exclude":
			var rows []map[string]any
			if err := node.Decode(&rows); err != nil {
				return err
			}
			out := make([]map[string]string, 0, len(rows))
			for _, r := range rows {
				out = append(out, anyMapToString(r))
			}
			if k == "include" {
				m.Include = out
			} else {
				m.Exclude = out
			}
		default:
			var vals []any
			if err := node.Decode(&vals); err != nil {
				return err
			}
			ss := make([]string, 0, len(vals))
			for _, v := range vals {
				ss = append(ss, fmt.Sprint(v))
			}
			m.Axes[k] = ss
		}
	}
	return nil
}

func anyMapToString(in map[string]any) map[string]string {
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = fmt.Sprint(v)
	}
	return out
}

// Step is a run: shell step or local uses: composite/js action.
type Step struct {
	ID               string `yaml:"id"`
	Name             string `yaml:"name"`
	Run              string `yaml:"run"`
	Uses             string `yaml:"uses"`
	ContinueOnError  bool   `yaml:"continue-on-error"`
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

// Parse unmarshals workflow YAML.
func Parse(data []byte) (*Workflow, error) {
	var wf Workflow
	if err := yaml.Unmarshal(data, &wf); err != nil {
		return nil, fmt.Errorf("githubactions: parse: %w", err)
	}
	if len(wf.Jobs) == 0 {
		return nil, fmt.Errorf("githubactions: no jobs")
	}
	return &wf, nil
}

// Config confines a local workflow run.
type Config struct {
	WorkDir string
	Branch  string // for on.push.branches
	Env     map[string]string
	Output  io.Writer
	// ArtifactDir defaults to WorkDir/.mirror-gha-artifacts
	ArtifactDir string
}

// StepResult is one executed step.
type StepResult struct {
	ID       string `json:"id,omitempty"`
	Name     string `json:"name,omitempty"`
	ExitCode int    `json:"exitCode"`
	Logs     string `json:"logs,omitempty"`
}

// JobResult is one job instance (matrix cell or single).
type JobResult struct {
	Name    string            `json:"name"`
	Matrix  map[string]string `json:"matrix,omitempty"`
	Status  string            `json:"status"` // succeeded | failed | skipped
	Steps   []StepResult      `json:"steps,omitempty"`
	Outputs map[string]string `json:"outputs,omitempty"`
}

// RunResult is the terminal workflow observation.
type RunResult struct {
	Status  string      `json:"status"` // succeeded | failed | skipped
	Dialect string      `json:"dialect"`
	Jobs    []JobResult `json:"jobs"`
}

// Run evaluates workflow for a push-like event on Config.Branch.
func Run(ctx context.Context, wf *Workflow, cfg Config) (*RunResult, error) {
	if wf == nil {
		return nil, fmt.Errorf("githubactions: nil workflow")
	}
	if cfg.WorkDir == "" {
		return nil, fmt.Errorf("githubactions: empty WorkDir")
	}
	out := &RunResult{Status: "succeeded", Dialect: Dialect}
	if !matchPush(wf.On, cfg.Branch) {
		out.Status = "skipped"
		return out, nil
	}
	art := cfg.ArtifactDir
	if art == "" {
		art = filepath.Join(cfg.WorkDir, ".mirror-gha-artifacts")
	}
	if err := os.MkdirAll(art, 0o755); err != nil {
		return nil, err
	}

	order, err := topoJobs(wf.Jobs)
	if err != nil {
		return nil, err
	}
	jobOK := map[string]bool{}
	for _, name := range order {
		job := wf.Jobs[name]
		skip := false
		for _, dep := range job.Needs {
			if !jobOK[dep] {
				out.Jobs = append(out.Jobs, JobResult{Name: name, Status: "skipped"})
				jobOK[name] = false
				out.Status = "failed"
				skip = true
				break
			}
		}
		if skip {
			continue
		}
		cells := expandMatrix(job.Strategy)
		allOK := true
		for _, cell := range cells {
			jr, err := runJob(ctx, name, job, cell, cfg, art)
			if err != nil {
				return nil, err
			}
			out.Jobs = append(out.Jobs, jr)
			if jr.Status != "succeeded" {
				allOK = false
				out.Status = "failed"
			}
		}
		jobOK[name] = allOK
	}
	return out, nil
}

func matchPush(on On, branch string) bool {
	if on.Push == nil {
		// Bare `on: push` often decodes oddly; treat missing push as allow if On empty.
		return true
	}
	if len(on.Push.Branches) == 0 {
		return true
	}
	for _, b := range on.Push.Branches {
		if b == branch {
			return true
		}
	}
	return false
}

func topoJobs(jobs map[string]Job) ([]string, error) {
	names := make([]string, 0, len(jobs))
	for n := range jobs {
		names = append(names, n)
	}
	sort.Strings(names)
	seen := map[string]int{} // 0=unseen 1=visiting 2=done
	var order []string
	var visit func(string) error
	visit = func(n string) error {
		st := seen[n]
		if st == 1 {
			return fmt.Errorf("githubactions: cycle at job %s", n)
		}
		if st == 2 {
			return nil
		}
		if _, ok := jobs[n]; !ok {
			return fmt.Errorf("githubactions: unknown job %s", n)
		}
		seen[n] = 1
		for _, dep := range jobs[n].Needs {
			if err := visit(dep); err != nil {
				return err
			}
		}
		seen[n] = 2
		order = append(order, n)
		return nil
	}
	for _, n := range names {
		if err := visit(n); err != nil {
			return nil, err
		}
	}
	return order, nil
}

func expandMatrix(st *Strategy) []map[string]string {
	if st == nil {
		return []map[string]string{nil}
	}
	m := st.Matrix
	keys := make([]string, 0, len(m.Axes))
	for k := range m.Axes {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var cells []map[string]string
	var rec func(int, map[string]string)
	rec = func(i int, cur map[string]string) {
		if i == len(keys) {
			cp := make(map[string]string, len(cur))
			for k, v := range cur {
				cp[k] = v
			}
			cells = append(cells, cp)
			return
		}
		k := keys[i]
		for _, v := range m.Axes[k] {
			cur[k] = v
			rec(i+1, cur)
		}
	}
	if len(keys) == 0 {
		// include-only matrix: do not seed an empty cell
		cells = nil
	} else {
		rec(0, map[string]string{})
	}
	for _, row := range m.Include {
		cells = append(cells, row)
	}
	var filtered []map[string]string
	for _, c := range cells {
		if excluded(c, m.Exclude) {
			continue
		}
		filtered = append(filtered, c)
	}
	if len(filtered) == 0 {
		return []map[string]string{nil}
	}
	return filtered
}

func excluded(cell map[string]string, excl []map[string]string) bool {
	for _, e := range excl {
		match := true
		for k, v := range e {
			if cell[k] != v {
				match = false
				break
			}
		}
		if match && len(e) > 0 {
			return true
		}
	}
	return false
}

var (
	reMatrix = regexp.MustCompile(`\$\{\{\s*matrix\.([A-Za-z0-9_]+)\s*\}\}`)
	reStepOut = regexp.MustCompile(`\$\{\{\s*steps\.([A-Za-z0-9_]+)\.outputs\.([A-Za-z0-9_]+)\s*\}\}`)
)

func runJob(ctx context.Context, name string, job Job, cell map[string]string, cfg Config, art string) (JobResult, error) {
	jr := JobResult{Name: name, Matrix: cell, Status: "succeeded", Outputs: map[string]string{}}
	stepOut := map[string]map[string]string{}
	ghOutFile := filepath.Join(cfg.WorkDir, ".mirror-github-output-"+name)
	_ = os.Remove(ghOutFile)

	overlay := map[string]string{
		"GITHUB_WORKSPACE": cfg.WorkDir,
		"GITHUB_OUTPUT":    ghOutFile,
		"MIRROR_ARTIFACTS": art,
	}
	for k, v := range cfg.Env {
		overlay[k] = v
	}
	if cell != nil {
		for k, v := range cell {
			overlay["MATRIX_"+strings.ToUpper(k)] = v
		}
	}
	env := shellstep.BaseEnv(overlay)

	for _, st := range job.Steps {
		_ = os.WriteFile(ghOutFile, nil, 0o644)
		var buf strings.Builder
		w := io.Writer(&buf)
		if cfg.Output != nil {
			w = io.MultiWriter(&buf, cfg.Output)
		}
		code := 0
		var err error
		switch {
		case st.Uses != "":
			code, err = runUses(ctx, cfg.WorkDir, st.Uses, env, ghOutFile, w)
		case st.Run != "":
			cmd := expandExprs(st.Run, cell, stepOut)
			code, err = shellstep.Run(ctx, cfg.WorkDir, cmd, env, w)
		default:
			return jr, fmt.Errorf("githubactions: job %s step missing run:/uses:", name)
		}
		if err != nil {
			return jr, err
		}
		sr := StepResult{ID: st.ID, Name: st.Name, ExitCode: code, Logs: buf.String()}
		jr.Steps = append(jr.Steps, sr)
		outs := readOutputFile(ghOutFile)
		if st.ID != "" {
			stepOut[st.ID] = outs
		}
		if code != 0 && !st.ContinueOnError {
			jr.Status = "failed"
			return jr, nil
		}
	}
	for k, expr := range job.Outputs {
		jr.Outputs[k] = expandExprs(expr, cell, stepOut)
	}
	return jr, nil
}

func expandExprs(s string, cell map[string]string, stepOut map[string]map[string]string) string {
	s = reMatrix.ReplaceAllStringFunc(s, func(m string) string {
		sub := reMatrix.FindStringSubmatch(m)
		if len(sub) < 2 || cell == nil {
			return m
		}
		if v, ok := cell[sub[1]]; ok {
			return v
		}
		return m
	})
	s = reStepOut.ReplaceAllStringFunc(s, func(m string) string {
		sub := reStepOut.FindStringSubmatch(m)
		if len(sub) < 3 {
			return m
		}
		if outs, ok := stepOut[sub[1]]; ok {
			if v, ok := outs[sub[2]]; ok {
				return v
			}
		}
		return m
	})
	return s
}

func readOutputFile(path string) map[string]string {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil
	}
	out := map[string]string{}
	for _, line := range strings.Split(string(b), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		k, v, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}
		out[k] = v
	}
	return out
}

// runUses executes a local ./path composite or node action (action.yml).
func runUses(ctx context.Context, workDir, uses string, env []string, ghOut string, w io.Writer) (int, error) {
	if !strings.HasPrefix(uses, "./") {
		return 1, fmt.Errorf("githubactions: only local uses: ./ supported, got %q", uses)
	}
	dir := filepath.Join(workDir, filepath.Clean(uses))
	workAbs, err := filepath.Abs(workDir)
	if err != nil {
		return 1, err
	}
	dirAbs, err := filepath.Abs(dir)
	if err != nil {
		return 1, err
	}
	sep := string(os.PathSeparator)
	if dirAbs != workAbs && !strings.HasPrefix(dirAbs, workAbs+sep) {
		return 1, fmt.Errorf("githubactions: uses %q escapes WorkDir", uses)
	}
	raw, err := os.ReadFile(filepath.Join(dir, "action.yml"))
	if err != nil {
		raw, err = os.ReadFile(filepath.Join(dir, "action.yaml"))
	}
	if err != nil {
		return 1, fmt.Errorf("githubactions: action.yml: %w", err)
	}
	var meta struct {
		Runs struct {
			Using string `yaml:"using"`
			Main  string `yaml:"main"`
			Steps []struct {
				Run string `yaml:"run"`
			} `yaml:"steps"`
		} `yaml:"runs"`
	}
	if err := yaml.Unmarshal(raw, &meta); err != nil {
		return 1, err
	}
	using := strings.ToLower(meta.Runs.Using)
	switch {
	case using == "composite":
		for _, st := range meta.Runs.Steps {
			if st.Run == "" {
				continue
			}
			code, err := shellstep.Run(ctx, workDir, st.Run, env, w)
			if err != nil || code != 0 {
				return code, err
			}
		}
		return 0, nil
	case strings.HasPrefix(using, "node"):
		main := meta.Runs.Main
		if main == "" {
			main = "index.js"
		}
		if filepath.IsAbs(main) || strings.Contains(main, "..") {
			return 1, fmt.Errorf("githubactions: bad runs.main %q", main)
		}
		mainPath := filepath.Join(dir, main)
		mainAbs, err := filepath.Abs(mainPath)
		if err != nil {
			return 1, err
		}
		if mainAbs != dirAbs && !strings.HasPrefix(mainAbs, dirAbs+string(os.PathSeparator)) {
			return 1, fmt.Errorf("githubactions: runs.main %q escapes action dir", main)
		}
		// Tiny harness: require the action module; if it writes GITHUB_OUTPUT itself we honor it.
		harness := fmt.Sprintf(`process.env.GITHUB_OUTPUT=%q; require(%q);`, ghOut, mainPath)
		node, err := execLookPath("node")
		if err != nil {
			return 1, fmt.Errorf("githubactions: node required for js actions: %w", err)
		}
		return process.Run(ctx, process.Config{Path: node, Args: []string{"-e", harness}, Dir: workDir, Env: env}, w)
	default:
		return 1, fmt.Errorf("githubactions: unsupported runs.using %q", meta.Runs.Using)
	}
}

func execLookPath(file string) (string, error) {
	return exec.LookPath(file)
}
