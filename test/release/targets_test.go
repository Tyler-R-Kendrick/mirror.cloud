package release

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// releaseTargets reads the build matrix from release.yml as YAML, so every
// entry counts however it is written.
func releaseTargets(t *testing.T, root string) [][2]string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(root, ".github", "workflows", "release.yml"))
	if err != nil {
		t.Fatal(err)
	}
	var wf struct {
		Jobs map[string]struct {
			Strategy struct {
				Matrix struct {
					Include []struct {
						GOOS   string `yaml:"goos"`
						GOARCH string `yaml:"goarch"`
					} `yaml:"include"`
				} `yaml:"matrix"`
			} `yaml:"strategy"`
		} `yaml:"jobs"`
	}
	if err := yaml.Unmarshal(b, &wf); err != nil {
		t.Fatal(err)
	}
	var out [][2]string
	for _, e := range wf.Jobs["build"].Strategy.Matrix.Include {
		if e.GOOS == "" || e.GOARCH == "" {
			t.Fatalf("release.yml has a build matrix entry without goos and goarch: %+v", e)
		}
		out = append(out, [2]string{e.GOOS, e.GOARCH})
	}
	if len(out) == 0 {
		t.Fatal("found no jobs.build.strategy.matrix.include entries in release.yml")
	}
	return out
}

// TestEveryReleaseTargetCompiles type-checks ./cmd/mirror and everything it
// imports for each platform in release.yml's matrix, so a platform-specific
// break is found on a pull request, not on a tag. The targets run in parallel
// against the one build cache the test inherits, which concurrent go commands
// share safely (each target's objects are keyed by GOOS and GOARCH). It takes
// minutes on a cold cache, so -short skips it; CI runs it without -short.
func TestEveryReleaseTargetCompiles(t *testing.T) {
	if testing.Short() {
		t.Skip("compiles ./cmd/mirror for every release target; run without -short, as CI does")
	}
	root := repoRoot(t)
	for _, tg := range releaseTargets(t, root) {
		goos, goarch := tg[0], tg[1]
		t.Run(goos+"/"+goarch, func(t *testing.T) {
			t.Parallel()
			cmd := exec.Command("go", "vet", "./cmd/mirror")
			cmd.Dir = root
			cmd.Env = append(os.Environ(), "GOOS="+goos, "GOARCH="+goarch, "CGO_ENABLED=0", "GOFLAGS=")
			if out, err := cmd.CombinedOutput(); err != nil {
				t.Fatalf("%s/%s does not compile: %v\n%s", goos, goarch, err, strings.TrimSpace(string(out)))
			}
		})
	}
}
