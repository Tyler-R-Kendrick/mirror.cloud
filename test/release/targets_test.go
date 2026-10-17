package release

import (
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

var matrixEntry = regexp.MustCompile(`\{ goos: ([a-z0-9]+), goarch: ([a-z0-9]+) \}`)

// TestEveryReleaseTargetCompiles type-checks ./cmd/mirror and everything it
// imports for each platform in release.yml's matrix, so a platform-specific
// break is found on a pull request, not on a tag. The targets are read from the
// workflow, so the two cannot drift apart.
func TestEveryReleaseTargetCompiles(t *testing.T) {
	root := repoRoot(t)
	wf, err := os.ReadFile(filepath.Join(root, ".github", "workflows", "release.yml"))
	if err != nil {
		t.Fatal(err)
	}
	targets := matrixEntry.FindAllStringSubmatch(string(wf), -1)
	if len(targets) < 4 {
		t.Fatalf("found %d targets in release.yml's matrix, want at least 4", len(targets))
	}
	for _, m := range targets {
		goos, goarch := m[1], m[2]
		t.Run(goos+"/"+goarch, func(t *testing.T) {
			cmd := exec.Command("go", "vet", "./cmd/mirror")
			cmd.Dir = root
			cmd.Env = append(os.Environ(), "GOOS="+goos, "GOARCH="+goarch, "CGO_ENABLED=0", "GOFLAGS=")
			if out, err := cmd.CombinedOutput(); err != nil {
				t.Fatalf("%s/%s does not compile: %v\n%s", goos, goarch, err, strings.TrimSpace(string(out)))
			}
		})
	}
}
