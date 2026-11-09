package ci

import (
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// main used to run the unit job with -count=1 after clearing the test cache,
// and the unit job ran with -coverprofile; any one of the three leaves pull
// requests nothing to read from the cache main saves. This pins the repair.
func TestUnitResultsStayCacheable(t *testing.T) {
	root := repo(t)
	read := func(rel string) string {
		b, err := os.ReadFile(filepath.Join(root, rel))
		if err != nil {
			t.Fatal(err)
		}
		return string(b)
	}
	ci := read(".github/workflows/ci.yml")
	if strings.Contains(ci, "go clean -testcache") {
		t.Error("ci.yml clears the test cache, so the cache main saves holds no results")
	}
	if regexp.MustCompile(`(?m)^\s*COUNT:.*count=1`).MatchString(ci) {
		t.Error("ci.yml sets COUNT=-count=1, which stops go test caching results")
	}
	recipe := regexp.MustCompile(`(?ms)^test-unit:\n(.*?)\n\n`).FindStringSubmatch(read("Makefile"))
	if recipe == nil {
		t.Fatal("no test-unit target in the Makefile")
	}
	for _, flag := range []string{"-count", "-cover"} {
		if strings.Contains(recipe[1], flag) {
			t.Errorf("test-unit passes %s, which stops go test caching its results:\n%s", flag, recipe[1])
		}
	}
	if !strings.Contains(ci, "make test-unit GOTESTFLAGS=-json") {
		t.Error("the unit job no longer runs make test-unit with -json; main and pull requests must pass the same flags")
	}
}

// The argument that a cached unit result is safe rests on three things a later
// edit could quietly undo: the coverage job, which go test never caches, runs
// every package the unit job runs; the test job fails unless coverage passed;
// and nothing puts -count=1 or a -cover flag back on the unit job by another
// road than the Makefile.
func TestCoverageIsTheUncachedRunOfEveryUnitPackage(t *testing.T) {
	root, _ := filepath.Abs("../..")
	read := func(rel string) []byte {
		b, err := os.ReadFile(filepath.Join(root, rel))
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	var wf struct {
		Env  map[string]string
		Jobs map[string]struct {
			Needs any
			Env   map[string]string
			Steps []struct {
				Run string
				Env map[string]string
			}
		}
	}
	if err := yaml.Unmarshal(read(".github/workflows/ci.yml"), &wf); err != nil {
		t.Fatal(err)
	}
	needs, _ := wf.Jobs["test"].Needs.([]any)
	if !slices.Contains(needs, any("coverage")) || !slices.Contains(needs, any("unit")) {
		t.Errorf("the test job needs %v; it must need both unit and coverage", wf.Jobs["test"].Needs)
	}
	unit, ok := wf.Jobs["unit"]
	if !ok {
		t.Fatal("no unit job")
	}
	envs := []map[string]string{wf.Env, unit.Env}
	for _, s := range unit.Steps {
		envs = append(envs, s.Env)
		if strings.Contains(s.Run, "-cover") || strings.Contains(s.Run, "-count=1") {
			t.Errorf("a unit step passes -cover or -count=1, which stops go test caching: %s", s.Run)
		}
	}
	for _, env := range envs {
		for _, k := range []string{"GOFLAGS", "COUNT", "GOTESTFLAGS"} {
			if v := env[k]; strings.Contains(v, "-count") || strings.Contains(v, "-cover") {
				t.Errorf("%s=%q reaches the unit job, which stops go test caching", k, v)
			}
		}
	}

	// The same `go list` line in both targets, so coverage runs everything unit
	// runs; the mutation needle check is the one thing unit adds, and it reads
	// only source files, which go test does track.
	mk := string(read("Makefile"))
	list := regexp.MustCompile(`\$\$\(\$\(GO\) list \./\.\.\.[^)]*\)`)
	recipe := func(target string) string {
		m := regexp.MustCompile(`(?ms)^` + target + `:\n(.*?)\n\n`).FindStringSubmatch(mk)
		if m == nil {
			t.Fatalf("no %s target in the Makefile", target)
		}
		return m[1]
	}
	u, c := list.FindAllString(recipe("test-unit"), -1), list.FindAllString(recipe("test-coverage"), -1)
	if len(u) != 1 || len(c) != 1 || u[0] != c[0] {
		t.Errorf("test-unit lists %q and test-coverage %q; coverage must list exactly the packages unit does", u, c)
	}
}
