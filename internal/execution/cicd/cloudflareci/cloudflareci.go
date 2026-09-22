// Package cloudflareci is a bounded local evaluator for @cloudflare/ci 0.2.0
// runner/cache semantics on Artifacts push events.
//
// It is NOT Workers Builds / Pages Git integration, and it does not claim
// Sandbox/Workflows/R2 parity. Trusted local process stands in for Sandbox.
// Pin: github.com/cloudflare/ci @ 0.2.0 (Apache-2.0).
package cloudflareci

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/execution/process"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

const (
	// PackagePin is the npm package revision this dialect targets.
	PackagePin = "@cloudflare/ci@0.2.0"
	// EventArtifactsPushed is the Wrangler trigger type.
	EventArtifactsPushed = "cf.artifacts.repo.pushed"
	// Dialect is the JobControl dialect id.
	Dialect = "cloudflare.ci/0.2"
)

// ArtifactsPushEvent matches the SDK CloudflareArtifactsPushEvent shape.
type ArtifactsPushEvent struct {
	ID     string `json:"id,omitempty"`
	Type   string `json:"type"`
	Source struct {
		Namespace string `json:"namespace"`
		RepoName  string `json:"repoName"`
	} `json:"source"`
	Payload struct {
		Ref     string `json:"ref"`
		Before  string `json:"before"`
		After   string `json:"after"`
		Commits []struct {
			ID      string `json:"id"`
			Message string `json:"message"`
			Author  struct {
				Name  string `json:"name"`
				Email string `json:"email"`
			} `json:"author"`
		} `json:"commits"`
	} `json:"payload"`
}

// CiParams is the provider-neutral run admission shape from the SDK.
type CiParams struct {
	Provider     string `json:"provider"`
	Owner        string `json:"owner"`
	Repo         string `json:"repo"`
	SHA          string `json:"sha"`
	Trigger      string `json:"trigger"`
	Ref          string `json:"ref"`
	Branch       string `json:"branch,omitempty"`
	Tag          string `json:"tag,omitempty"`
	BeforeSHA    string `json:"beforeSha,omitempty"`
	HeadMessage  string `json:"headCommitMessage,omitempty"`
	Actor        string `json:"actor,omitempty"`
	Remote       string `json:"remote,omitempty"`
	ProviderData struct {
		Namespace string `json:"namespace"`
	} `json:"providerData"`
}

// RunnerOptions matches SDK RunnerOptions used by ci.runner / deps.runner.
type RunnerOptions struct {
	Name    string            `json:"name"`
	Command string            `json:"command"`
	Cwd     string            `json:"cwd,omitempty"`
	Env     map[string]string `json:"env,omitempty"`
	Cache   *struct {
		Inputs []string `json:"inputs"`
	} `json:"cache,omitempty"`
}

// Pipeline is a fixture-authored linearization of a CIWorkflow.pipeline graph.
// Roots run first; Children run only after their parent succeeds (SDK
// deps.runner chaining). Sibling children of one parent may be listed in any
// order; this local profile runs them sequentially.
type Pipeline struct {
	Roots []RunnerNode `json:"roots"`
}

// RunnerNode is one runner plus optional chained children / barrier.
type RunnerNode struct {
	RunnerOptions
	// Children are deps.runner chains (SDK may await Promise.all over them).
	Children []RunnerNode `json:"children,omitempty"`
	// Then runs only after this node and every Child succeeded — models
	// deploy-after-checks in the documented CIWorkflow example.
	Then []RunnerNode `json:"then,omitempty"`
}

// StepResult is one executed (or cache-hit) runner observation.
type StepResult struct {
	Name       string `json:"name"`
	ExitCode   int    `json:"exitCode"`
	CacheHit   bool   `json:"cacheHit"`
	CacheKey   string `json:"cacheKey,omitempty"`
	Logs       string `json:"logs"`
	Skipped    bool   `json:"skipped,omitempty"`
	SkipReason string `json:"skipReason,omitempty"`
}

// RunResult is the terminal pipeline observation.
type RunResult struct {
	Params    CiParams     `json:"params"`
	Status    string       `json:"status"` // succeeded | errored
	Steps     []StepResult `json:"steps"`
	Package   string       `json:"package"`
	StartedAt string       `json:"startedAt"`
	EndedAt   string       `json:"endedAt"`
}

// ParseArtifactsPushEvent parses cf.artifacts.repo.pushed JSON.
// Wrong type → (nil, nil). Malformed → error.
func ParseArtifactsPushEvent(body []byte) (*ArtifactsPushEvent, error) {
	var peek struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(body, &peek); err != nil {
		return nil, err
	}
	if peek.Type != "" && peek.Type != EventArtifactsPushed {
		return nil, nil
	}
	var ev ArtifactsPushEvent
	if err := json.Unmarshal(body, &ev); err != nil {
		return nil, err
	}
	if ev.Type != EventArtifactsPushed {
		return nil, fmt.Errorf("cloudflareci: want type %s", EventArtifactsPushed)
	}
	if ev.Source.Namespace == "" || ev.Source.RepoName == "" {
		return nil, fmt.Errorf("cloudflareci: missing source.namespace/repoName")
	}
	if ev.Payload.After == "" {
		return nil, fmt.Errorf("cloudflareci: missing payload.after")
	}
	return &ev, nil
}

const zeroSHA = "0000000000000000000000000000000000000000"

// MapPushEventToCiParams mirrors mapArtifactsPushEventToCiParams.
func MapPushEventToCiParams(ev *ArtifactsPushEvent) (*CiParams, error) {
	if ev == nil {
		return nil, fmt.Errorf("cloudflareci: nil event")
	}
	if ev.Payload.After == zeroSHA {
		return nil, nil
	}
	ref := ev.Payload.Ref
	isBranch := strings.HasPrefix(ref, "refs/heads/")
	isTag := strings.HasPrefix(ref, "refs/tags/")
	if !isBranch && !isTag {
		return nil, nil
	}
	p := &CiParams{
		Provider:  "cloudflare-artifacts",
		Owner:     ev.Source.Namespace,
		Repo:      ev.Source.RepoName,
		SHA:       ev.Payload.After,
		Trigger:   "push",
		Ref:       ref,
		Remote:    "cloudflare",
		BeforeSHA: ev.Payload.Before,
	}
	p.ProviderData.Namespace = ev.Source.Namespace
	if isTag {
		p.Trigger = "tag"
		p.Tag = strings.TrimPrefix(ref, "refs/tags/")
	} else {
		p.Branch = strings.TrimPrefix(ref, "refs/heads/")
	}
	if n := len(ev.Payload.Commits); n > 0 {
		c := ev.Payload.Commits[n-1]
		p.HeadMessage = c.Message
		p.Actor = c.Author.Name
	}
	return p, nil
}

// Config confines a local pipeline run.
type Config struct {
	// WorkDir is the checked-out source root (fixture-provisioned).
	WorkDir string
	// CacheDir stores content-addressed cache markers (stand-in for R2 pointer).
	CacheDir string
	Output   io.Writer
	// Clock optional; nil uses unix epoch (determinism gate).
	Clock spi.Clock
}

func cfgNow(cfg Config) time.Time {
	if cfg.Clock != nil {
		return cfg.Clock.Now().UTC()
	}
	return time.Unix(0, 0).UTC()
}

// Run executes pipeline against WorkDir. Failed step marks status errored and
// skips dependents (SDK: Workflow Errored; dependents do not start).
func Run(ctx context.Context, params CiParams, pipe Pipeline, cfg Config) (*RunResult, error) {
	if cfg.WorkDir == "" {
		return nil, fmt.Errorf("cloudflareci: empty WorkDir")
	}
	if cfg.CacheDir == "" {
		cfg.CacheDir = filepath.Join(cfg.WorkDir, ".mirror-ci-cache")
	}
	if err := os.MkdirAll(cfg.CacheDir, 0o755); err != nil {
		return nil, err
	}
	start := cfgNow(cfg)
	out := &RunResult{
		Params:    params,
		Status:    "succeeded",
		Package:   PackagePin,
		StartedAt: start.Format(time.RFC3339Nano),
	}
	for _, root := range pipe.Roots {
		if err := runNode(ctx, root, cfg, out, true); err != nil {
			return nil, err
		}
	}
	out.EndedAt = cfgNow(cfg).Format(time.RFC3339Nano)
	return out, nil
}

func runNode(ctx context.Context, node RunnerNode, cfg Config, out *RunResult, parentOK bool) error {
	if !parentOK {
		out.Steps = append(out.Steps, StepResult{
			Name: node.Name, Skipped: true, SkipReason: "parent failed",
		})
		for _, ch := range node.Children {
			_ = runNode(ctx, ch, cfg, out, false)
		}
		for _, th := range node.Then {
			_ = runNode(ctx, th, cfg, out, false)
		}
		return nil
	}
	sr, err := runOne(ctx, node.RunnerOptions, cfg)
	if err != nil {
		return err
	}
	out.Steps = append(out.Steps, sr)
	ok := sr.ExitCode == 0
	if !ok {
		out.Status = "errored"
	}
	kidsOK := ok
	for _, ch := range node.Children {
		before := len(out.Steps)
		if err := runNode(ctx, ch, cfg, out, ok); err != nil {
			return err
		}
		for _, s := range out.Steps[before:] {
			if s.Name == ch.Name && (s.Skipped || s.ExitCode != 0) {
				kidsOK = false
			}
		}
	}
	if !kidsOK {
		out.Status = "errored"
	}
	for _, th := range node.Then {
		if err := runNode(ctx, th, cfg, out, kidsOK); err != nil {
			return err
		}
	}
	return nil
}

func runOne(ctx context.Context, opt RunnerOptions, cfg Config) (StepResult, error) {
	if opt.Name == "" || opt.Command == "" {
		return StepResult{}, fmt.Errorf("cloudflareci: runner needs name and command")
	}
	sr := StepResult{Name: opt.Name}
	dir := cfg.WorkDir
	if opt.Cwd != "" {
		dir = filepath.Join(cfg.WorkDir, opt.Cwd)
	}

	var cacheKey string
	if opt.Cache != nil && len(opt.Cache.Inputs) > 0 {
		key, err := cacheKeyFor(cfg.WorkDir, opt.Name, opt.Cache.Inputs)
		if err != nil {
			return sr, err
		}
		cacheKey = key
		sr.CacheKey = key
		marker := filepath.Join(cfg.CacheDir, key+".ok")
		if _, err := os.Stat(marker); err == nil {
			sr.CacheHit = true
			sr.ExitCode = 0
			sr.Logs = "cache hit: skipped command"
			return sr, nil
		}
	}

	var buf strings.Builder
	w := io.MultiWriter(&buf, discardOr(cfg.Output))
	env := mergeEnv(opt.Env)
	code, err := process.Run(ctx, process.Config{
		Path: "/bin/sh",
		Args: []string{"-c", opt.Command},
		Dir:  dir,
		Env:  env,
	}, w)
	if err != nil {
		return sr, err
	}
	sr.ExitCode = code
	sr.Logs = buf.String()
	if code == 0 && cacheKey != "" {
		_ = os.WriteFile(filepath.Join(cfg.CacheDir, cacheKey+".ok"), []byte(cfgNow(cfg).Format(time.RFC3339Nano)), 0o644)
	}
	return sr, nil
}

func discardOr(w io.Writer) io.Writer {
	if w == nil {
		return io.Discard
	}
	return w
}

func mergeEnv(overlay map[string]string) []string {
	base := process.ScrubEnv(os.Environ())
	if len(overlay) == 0 {
		return base
	}
	drop := map[string]struct{}{}
	for k := range overlay {
		drop[k] = struct{}{}
	}
	out := make([]string, 0, len(base)+len(overlay))
	for _, e := range base {
		k, _, _ := strings.Cut(e, "=")
		if _, ok := drop[k]; ok {
			continue
		}
		out = append(out, e)
	}
	for k, v := range overlay {
		out = append(out, k+"="+v)
	}
	return process.ScrubEnv(out)
}

func cacheKeyFor(root, name string, inputs []string) (string, error) {
	h := sha256.New()
	_, _ = io.WriteString(h, name)
	_, _ = io.WriteString(h, "\n")
	for _, pat := range inputs {
		// Bounded profile: literal relative paths only (no glob expansion yet).
		if filepath.IsAbs(pat) || strings.Contains(pat, "..") {
			return "", fmt.Errorf("cloudflareci: cache input escapes workdir: %s", pat)
		}
		p := filepath.Join(root, pat)
		b, err := os.ReadFile(p)
		if err != nil {
			// Missing input → unique miss key so command runs (and can fail).
			_, _ = io.WriteString(h, "missing:"+pat+"\n")
			continue
		}
		sum := sha256.Sum256(b)
		_, _ = io.WriteString(h, pat)
		_, _ = io.WriteString(h, ":")
		_, _ = io.WriteString(h, hex.EncodeToString(sum[:]))
		_, _ = io.WriteString(h, "\n")
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}
