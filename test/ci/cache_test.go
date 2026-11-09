package ci

import (
	"os"
	"os/exec"
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
	root, _ := filepath.Abs("../..")
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

// The argument that a cached unit result is safe rests on things a later edit
// could quietly undo: the coverage job, which go test never caches, runs every
// test of every package the unit job runs; the test job fails unless coverage
// passed; and nothing puts -count or a -cover flag on the unit run by another
// road than the Makefile. The Makefile is read through `make -n`, so a shared
// variable or a continued line reads the same as the text it expands to.
func TestCoverageIsTheUncachedRunOfEveryUnitPackage(t *testing.T) {
	root, _ := filepath.Abs("../..")
	read := func(rel string) []byte {
		b, err := os.ReadFile(filepath.Join(root, rel))
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	type step struct {
		If  string
		Run string
		Env map[string]string
	}
	var wf struct {
		Env  map[string]string
		Jobs map[string]struct {
			Needs any
			If    string
			Env   map[string]string
			Steps []step
		}
	}
	if err := yaml.Unmarshal(read(".github/workflows/ci.yml"), &wf); err != nil {
		t.Fatal(err)
	}

	// The test job: it needs coverage, runs whatever happened, and fails on
	// any of its needs not passing -- not on a list that could leave one out.
	gate := wf.Jobs["test"]
	needs, _ := gate.Needs.([]any)
	if !slices.Contains(needs, any("coverage")) || !slices.Contains(needs, any("unit")) {
		t.Errorf("the test job needs %v; it must need both unit and coverage", gate.Needs)
	}
	if !strings.Contains(gate.If, "always()") {
		t.Errorf("the test job runs if %q; it must run always(), or a failed need skips it and a skip reads as a pass", gate.If)
	}
	checked := false
	for _, s := range gate.Steps {
		all := true
		for _, r := range []string{"failure", "cancelled", "skipped"} {
			all = all && strings.Contains(s.If, "contains(needs.*.result, '"+r+"')")
		}
		if all && strings.Contains(s.Run, "exit 1") {
			checked = true
		}
	}
	if !checked {
		t.Error("no step of the test job fails on contains(needs.*.result, ...) for failure, cancelled and skipped")
	}

	// The coverage job runs the coverage target.
	ran := false
	for _, s := range wf.Jobs["coverage"].Steps {
		ran = ran || regexp.MustCompile(`(^|\s)make test-coverage(\s|$)`).MatchString(s.Run)
	}
	if !ran {
		t.Error("no step of the coverage job runs make test-coverage")
	}

	// The unit job: no -count or -cover in any env it gets, in any env file a
	// step writes, or on the command line of the steps whose results it caches.
	unit, ok := wf.Jobs["unit"]
	if !ok {
		t.Fatal("no unit job")
	}
	bad := regexp.MustCompile(`-count\b|-cover`)
	envs := []map[string]string{wf.Env, unit.Env}
	for _, s := range unit.Steps {
		envs = append(envs, s.Env)
		cached := strings.Contains(s.Run, "make test-unit") || strings.Contains(s.Run, "make equivalence")
		if (cached || strings.Contains(s.Run, "GITHUB_ENV")) && bad.MatchString(s.Run) {
			t.Errorf("a unit step passes -count or -cover, which stops go test caching: %s", s.Run)
		}
	}
	for _, env := range envs {
		for _, k := range []string{"GOFLAGS", "COUNT", "GOTESTFLAGS"} {
			if v := env[k]; bad.MatchString(v) {
				t.Errorf("%s=%q reaches the unit job, which stops go test caching", k, v)
			}
		}
	}

	// The Makefile: both targets list the same packages, coverage selects no
	// subset of their tests, and test-unit caches.
	readScript(t, filepath.Join(root, "Makefile"))
	dry := func(target string) []string {
		t.Helper()
		cmd := exec.Command("make", "-n", "-s", target)
		cmd.Dir = root
		cmd.Env = append(os.Environ(), "GOTESTFLAGS=", "COUNT=", "MAKEFLAGS=")
		out, err := cmd.Output()
		if err != nil {
			t.Fatalf("make -n %s: %v", target, err)
		}
		var tests []string
		for _, line := range strings.Split(strings.ReplaceAll(string(out), "\\\n", " "), "\n") {
			if f := strings.Fields(line); len(f) > 1 && f[0] == "go" && f[1] == "test" {
				tests = append(tests, strings.Join(f, " "))
			}
		}
		return tests
	}
	list := regexp.MustCompile(`\$\(go list \./\.\.\.[^)]*\)`)
	u, c := dry("test-unit"), dry("test-coverage")
	if len(u) == 0 || len(c) != 1 {
		t.Fatalf("test-unit runs %q and test-coverage %q; expected go test lines in both and one in coverage", u, c)
	}
	if lu, lc := list.FindString(u[0]), list.FindString(c[0]); lu == "" || lu != lc {
		t.Errorf("test-unit lists %q and test-coverage %q; coverage must list exactly the packages unit does", lu, lc)
	}
	if m := regexp.MustCompile(`\s-(run|skip|short|list)\b`).FindString(c[0]); m != "" {
		t.Errorf("test-coverage passes%s, so some unit tests never run uncached: %s", m, c[0])
	}
	for _, line := range u {
		if bad.MatchString(line) {
			t.Errorf("test-unit passes -count or -cover, which stops go test caching: %s", line)
		}
	}
}
