// Package buildspec runs a CodeBuild buildspec 0.2 command subset locally (no Docker/network).
package buildspec

import (
	"context"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"gopkg.in/yaml.v3"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/execution/process"
)

// Spec is buildspec version 0.2 with phase commands and optional finally.
type Spec struct {
	Version            float64               `yaml:"version"`
	Phases             Phases                `yaml:"phases"`
	Finally            []string              `yaml:"finally"`
	Env                *EnvBlock             `yaml:"env"`
	Artifacts          *Artifacts            `yaml:"artifacts"`
	SecondaryArtifacts map[string]*Artifacts `yaml:"secondary-artifacts"`
}

// EnvBlock is buildspec env (variables + exported-variables).
type EnvBlock struct {
	Variables         map[string]string `yaml:"variables"`
	ExportedVariables []string          `yaml:"exported-variables"`
}

// Artifacts names files that must exist after a successful run.
type Artifacts struct {
	Files []string `yaml:"files"`
}

// Phases are run in install → pre_build → build → post_build order.
type Phases struct {
	Install   *Phase `yaml:"install"`
	PreBuild  *Phase `yaml:"pre_build"`
	Build     *Phase `yaml:"build"`
	PostBuild *Phase `yaml:"post_build"`
}

// Phase holds shell command strings (each run via /bin/sh -c).
type Phase struct {
	Commands []string `yaml:"commands"`
}

// Config confines the run to Dir and overlays Env onto a scrubbed process env.
type Config struct {
	Dir    string            // required working directory
	Env    map[string]string // optional overlay (scrubbed with base)
	Output io.Writer         // combined stdout/stderr; optional
}

// Result is the exit code of the first failing command (0 if all succeed).
// finally always runs; its failures only matter if phases already succeeded.
type Result struct {
	ExitCode int
}

// Parse unmarshals YAML and requires version 0.2.
func Parse(data []byte) (*Spec, error) {
	var s Spec
	if err := yaml.Unmarshal(data, &s); err != nil {
		return nil, fmt.Errorf("buildspec: parse: %w", err)
	}
	if s.Version != 0.2 {
		return nil, fmt.Errorf("buildspec: unsupported version %v (want 0.2)", s.Version)
	}
	return &s, nil
}

// Run executes phase commands then finally. Stops phases on first nonzero exit;
// finally always runs. Returns that first failure code (or finally's if phases ok).
func Run(ctx context.Context, spec *Spec, cfg Config) (*Result, error) {
	if spec == nil {
		return nil, fmt.Errorf("buildspec: nil spec")
	}
	if cfg.Dir == "" {
		return nil, fmt.Errorf("buildspec: empty Dir")
	}
	dir, err := filepath.Abs(cfg.Dir)
	if err != nil {
		return nil, fmt.Errorf("buildspec: Dir: %w", err)
	}
	fi, err := os.Stat(dir)
	if err != nil {
		return nil, fmt.Errorf("buildspec: Dir: %w", err)
	}
	if !fi.IsDir() {
		return nil, fmt.Errorf("buildspec: Dir not a directory: %s", dir)
	}

	overlay := map[string]string{}
	for k, v := range cfg.Env {
		overlay[k] = v
	}
	if spec.Env != nil {
		for k, v := range spec.Env.Variables {
			if _, ok := overlay[k]; !ok {
				overlay[k] = v
			}
		}
	}
	env := mergeEnv(os.Environ(), overlay)
	exit := 0

	phases := []*Phase{spec.Phases.Install, spec.Phases.PreBuild, spec.Phases.Build, spec.Phases.PostBuild}
	for _, ph := range phases {
		if ph == nil {
			continue
		}
		for _, cmd := range ph.Commands {
			code, err := runShell(ctx, dir, env, cmd, cfg.Output)
			if err != nil {
				return nil, err
			}
			if code != 0 {
				exit = code
				goto finally
			}
		}
	}

finally:
	for _, cmd := range spec.Finally {
		code, err := runShell(ctx, dir, env, cmd, cfg.Output)
		if err != nil {
			return nil, err
		}
		if exit == 0 && code != 0 {
			exit = code
		}
	}
	if exit == 0 && spec.Env != nil && len(spec.Env.ExportedVariables) > 0 {
		var b strings.Builder
		envMap := envMapFrom(env)
		for _, name := range spec.Env.ExportedVariables {
			fmt.Fprintf(&b, "%s=%s\n", name, envMap[name])
		}
		_ = os.WriteFile(filepath.Join(dir, "exported-variables"), []byte(b.String()), 0o644)
	}
	if exit == 0 {
		if err := checkArtifactFiles(dir, "artifacts", spec.Artifacts); err != nil {
			return nil, err
		}
		for name, art := range spec.SecondaryArtifacts {
			if err := checkArtifactFiles(dir, "secondary-artifacts."+name, art); err != nil {
				return nil, err
			}
		}
	}
	return &Result{ExitCode: exit}, nil
}

func checkArtifactFiles(dir, label string, art *Artifacts) error {
	if art == nil {
		return nil
	}
	for _, f := range art.Files {
		if strings.ContainsAny(f, "*?[") {
			ok, err := artifactGlobMatches(dir, f)
			if err != nil {
				return fmt.Errorf("buildspec: %s %q: %w", label, f, err)
			}
			if !ok {
				return fmt.Errorf("buildspec: %s %q matched no files", label, f)
			}
			continue
		}
		target, err := artifactPathInDir(dir, f)
		if err != nil {
			return fmt.Errorf("buildspec: %s %q: %w", label, f, err)
		}
		if _, err := os.Stat(target); err != nil {
			return fmt.Errorf("buildspec: %s %q missing: %w", label, f, err)
		}
	}
	return nil
}

// artifactPathInDir joins dir+rel and refuses .. / absolute escape.
func artifactPathInDir(dir, rel string) (string, error) {
	if filepath.IsAbs(rel) {
		return "", fmt.Errorf("path escapes build dir")
	}
	clean := filepath.Clean(rel)
	if clean == ".." || strings.HasPrefix(clean, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("path escapes build dir")
	}
	full := filepath.Join(dir, clean)
	got, err := filepath.Rel(dir, full)
	if err != nil || got == ".." || strings.HasPrefix(got, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("path escapes build dir")
	}
	return full, nil
}

// artifactGlobMatches reports whether pattern matches ≥1 regular file under dir.
// Supports CodeBuild-style ** segments plus filepath.Match on each path segment.
func artifactGlobMatches(dir, pattern string) (bool, error) {
	pattern = filepath.ToSlash(strings.TrimSpace(pattern))
	found := false
	err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(dir, path)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)
		if matchArtifactPattern(pattern, rel) {
			found = true
			return fs.SkipAll
		}
		return nil
	})
	if err != nil {
		return false, err
	}
	return found, nil
}

func matchArtifactPattern(pattern, rel string) bool {
	return matchGlobSegments(strings.Split(pattern, "/"), strings.Split(rel, "/"))
}

func matchGlobSegments(pat, name []string) bool {
	for len(pat) > 0 {
		if pat[0] == "**" {
			pat = pat[1:]
			if len(pat) == 0 {
				return true
			}
			for i := 0; i <= len(name); i++ {
				if matchGlobSegments(pat, name[i:]) {
					return true
				}
			}
			return false
		}
		if len(name) == 0 {
			return false
		}
		ok, err := filepath.Match(pat[0], name[0])
		if err != nil || !ok {
			return false
		}
		pat = pat[1:]
		name = name[1:]
	}
	return len(name) == 0
}

func envMapFrom(env []string) map[string]string {
	m := map[string]string{}
	for _, e := range env {
		k, v, ok := strings.Cut(e, "=")
		if ok {
			m[k] = v
		}
	}
	return m
}

func runShell(ctx context.Context, dir string, env []string, command string, w io.Writer) (int, error) {
	return process.Run(ctx, process.Config{
		Path: "/bin/sh",
		Args: []string{"-c", command},
		Dir:  dir,
		Env:  env,
	}, w)
}

// mergeEnv copies base, drops keys present in overlay, appends overlay, then scrub.
func mergeEnv(base []string, overlay map[string]string) []string {
	if len(overlay) == 0 {
		return process.ScrubEnv(base)
	}
	drop := make(map[string]struct{}, len(overlay))
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
