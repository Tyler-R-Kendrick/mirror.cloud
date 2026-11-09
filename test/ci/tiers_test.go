package ci

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/testinputs"
)

const mod = "github.com/tyler-r-kendrick/mirror.cloud/"

// test-tiers.py replaces the plain log of the run it reads, so it must keep
// what known-red.py and a person read in that log -- the package lines and the
// whole output of a failing test -- and put each test in the tier whose suite
// used to run it.
func TestTierSummaryKeepsTheLogAndSortsTestsIntoTiers(t *testing.T) {
	root := repo(t)
	readScript(t, filepath.Join(root, "scripts", "test-tiers.py"))
	readScript(t, filepath.Join(root, "scripts", "known-red.py"))
	events := strings.Join([]string{
		`go test -json ./...`,
		`{"Action":"start","Package":"` + mod + `internal/chaos"}`,
		`{"Action":"run","Package":"` + mod + `internal/chaos","Test":"TestClockJump"}`,
		`{"Action":"output","Package":"` + mod + `internal/chaos","Test":"TestClockJump","Output":"=== RUN   TestClockJump\n"}`,
		`{"Action":"output","Package":"` + mod + `internal/chaos","Test":"TestClockJump","Output":"    chaos_test.go:9: went backwards\n"}`,
		`{"Action":"output","Package":"` + mod + `internal/chaos","Test":"TestClockJump","Output":"--- FAIL: TestClockJump (0.00s)\n"}`,
		`{"Action":"fail","Package":"` + mod + `internal/chaos","Test":"TestClockJump"}`,
		`{"Action":"output","Package":"` + mod + `internal/chaos","Output":"FAIL\n"}`,
		`{"Action":"output","Package":"` + mod + `internal/chaos","Output":"FAIL\t` + mod + `internal/chaos\t0.1s\n"}`,
		`{"Action":"fail","Package":"` + mod + `internal/chaos","Elapsed":0.1}`,
		`{"Action":"run","Package":"` + mod + `internal/services/aws/s3","Test":"FuzzKey"}`,
		`{"Action":"output","Package":"` + mod + `internal/services/aws/s3","Test":"FuzzKey","Output":"=== RUN   FuzzKey\n"}`,
		`{"Action":"pass","Package":"` + mod + `internal/services/aws/s3","Test":"FuzzKey"}`,
		`{"Action":"pass","Package":"` + mod + `internal/services/aws/s3","Test":"TestObjectCharacterization"}`,
		`{"Action":"pass","Package":"` + mod + `internal/services/aws/s3","Test":"TestPut"}`,
		`{"Action":"skip","Package":"` + mod + `internal/services/aws/s3","Test":"TestSlow"}`,
		`{"Action":"output","Package":"` + mod + `internal/services/aws/s3","Output":"ok  \t` + mod + `internal/services/aws/s3\t(cached)\n"}`,
		`{"Action":"pass","Package":"` + mod + `internal/services/aws/s3","Elapsed":0}`,
		`{"Action":"pass","Package":"` + mod + `test/terraform","Test":"TestRead"}`,
		`{"Action":"pass","Package":"` + mod + `test/terraform","Elapsed":2}`,
	}, "\n") + "\n"
	cmd := exec.Command("python3", "-I", filepath.Join(root, "scripts", "test-tiers.py"))
	cmd.Stdin = strings.NewReader(events)
	cmd.Env = append(os.Environ(), "GITHUB_STEP_SUMMARY=")
	raw, err := cmd.CombinedOutput()
	out := string(raw)
	if code := cmd.ProcessState.ExitCode(); code != 1 {
		t.Fatalf("exit %d (%v), want 1 for a failing test:\n%s", code, err, out)
	}
	for _, want := range []string{
		"go test -json ./...\n",
		"    chaos_test.go:9: went backwards\n--- FAIL: TestClockJump (0.00s)\n",
		"FAIL\t" + mod + "internal/chaos\t0.1s\n",
		"ok  \t" + mod + "internal/services/aws/s3\t(cached)\n",
		"| chaos | 1 | 0 | 1 | 0 | 0 |",
		"| fuzz seeds | 1 | 1 | 0 | 0 | 1 |",
		"| snapshot | 1 | 1 | 0 | 0 | 1 |",
		"| bdd | 1 | 1 | 0 | 0 | 0 |",
		"| unit | 1 | 1 | 0 | 1 | 2 |",
		"3 packages with tests, 1 from cache; 2.1s in the 2 that ran.",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("output lacks %q:\n%s", want, out)
		}
	}
	// A passing test's own output is noise in a log of hundreds of packages.
	if strings.Contains(out, "=== RUN   FuzzKey") {
		t.Errorf("printed a passing test's output:\n%s", out)
	}
	known := exec.Command("python3", "-I", filepath.Join(root, "scripts", "known-red.py"))
	known.Stdin = strings.NewReader(out)
	known.Env = append(os.Environ(), "KNOWN_RED_LIST="+filepath.Join(t.TempDir(), "absent.json"))
	verdict, _ := known.CombinedOutput()
	if !strings.Contains(string(verdict), "TestClockJump") {
		t.Errorf("known-red.py did not name the failure in this log:\n%s", verdict)
	}
}

type tiersFile struct {
	Tiers []struct {
		Name     string `json:"name"`
		Packages string `json:"packages"`
		Tests    string `json:"tests"`
	} `json:"tiers"`
	Stages map[string]map[string]string `json:"stages"`
}

func readTiers(t *testing.T, root string) tiersFile {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(root, "test", "tiers.json"))
	if err != nil {
		t.Fatal(err)
	}
	var f tiersFile
	if err := json.Unmarshal(raw, &f); err != nil {
		t.Fatal(err)
	}
	return f
}

// Every package lands in exactly one tier by its package rules: the rules other
// than the last match disjoint sets of packages, each matches at least one, and
// the last matches everything. Rules with a test-name pattern (fuzz seeds,
// characterizations, internal/bundled's equivalence gates) cut individual tests
// out of whichever tier their package is in.
func TestTiersPartitionPackages(t *testing.T) {
	root := repo(t)
	f := readTiers(t, root)
	out, err := testinputs.Command(root, "go", "list", "./...").Output()
	if err != nil {
		t.Fatal(err)
	}
	pkgs := []string{"test/sdk/go"} // its own module, run by make test-sdk
	for _, p := range strings.Fields(string(out)) {
		pkgs = append(pkgs, strings.TrimPrefix(strings.TrimPrefix(p, strings.TrimSuffix(mod, "/")), "/"))
	}
	type rule struct {
		name string
		re   *regexp.Regexp
	}
	var rules []rule
	for _, tier := range f.Tiers {
		if tier.Packages == "" && tier.Tests == "" {
			t.Errorf("tier %s names neither packages nor tests", tier.Name)
		}
		if tier.Tests != "" {
			regexp.MustCompile(tier.Tests) // a cut of tests, not a package tier
			if tier.Packages != "" {
				regexp.MustCompile(tier.Packages)
			}
			continue
		}
		rules = append(rules, rule{tier.Name, regexp.MustCompile(tier.Packages)})
	}
	if len(rules) == 0 || rules[len(rules)-1].re.String() != ".*" {
		t.Fatal("the last package rule must be .*, the tier of every package no other rule names")
	}
	rest, last := rules[:len(rules)-1], rules[len(rules)-1]
	used := make([]int, len(rest))
	for _, p := range pkgs {
		var in []string
		for i, r := range rest {
			if r.re.MatchString(p) {
				in = append(in, r.name)
				used[i]++
			}
		}
		if len(in) > 1 {
			t.Errorf("%s is in %d tiers: %v", p, len(in), in)
		}
		if len(in) == 0 && !last.re.MatchString(p) {
			t.Errorf("%s is in no tier", p)
		}
	}
	for i, n := range used {
		if n == 0 {
			t.Errorf("tier %s (%s) holds no package", rest[i].name, rest[i].re)
		}
	}
}

// The stages in test/tiers.json are the jobs the workflows run: every job of
// ci.yml says what it runs on a pull request and on main, every job of
// nightly.yml what it runs at night, and no stage names a job that is gone.
func TestEveryCIJobHasAStage(t *testing.T) {
	root := repo(t)
	f := readTiers(t, root)
	jobs := func(name string) map[string]bool {
		raw, err := os.ReadFile(filepath.Join(root, ".github", "workflows", name))
		if err != nil {
			t.Fatal(err)
		}
		var wf struct {
			Jobs map[string]any `yaml:"jobs"`
		}
		if err := yaml.Unmarshal(raw, &wf); err != nil {
			t.Fatal(err)
		}
		out := map[string]bool{}
		for j := range wf.Jobs {
			out[j] = true
		}
		return out
	}
	for stage, workflow := range map[string]string{"pull_request": "ci.yml", "main": "ci.yml", "nightly": "nightly.yml"} {
		want := jobs(workflow)
		for j := range want {
			if f.Stages[stage][j] == "" {
				t.Errorf("%s job %s has no %s stage in test/tiers.json", workflow, j, stage)
			}
		}
		for j := range f.Stages[stage] {
			if !want[j] {
				t.Errorf("test/tiers.json's %s stage names %s, which %s has no job called", stage, j, workflow)
			}
		}
	}
}

// tiers runs test-tiers.py over events and returns what it printed and its exit
// status.
func tiers(t *testing.T, events ...string) (string, int) {
	t.Helper()
	root := repo(t)
	script := filepath.Join(root, "scripts", "test-tiers.py")
	readScript(t, script)
	cmd := exec.Command("python3", "-I", script)
	cmd.Stdin = strings.NewReader(strings.Join(events, "\n") + "\n")
	cmd.Env = append(os.Environ(), "GITHUB_STEP_SUMMARY=")
	out, _ := cmd.CombinedOutput()
	return string(out), cmd.ProcessState.ExitCode()
}

func ev(pkg, action, test, output string) string {
	s := `{"Action":"` + action + `","Package":"` + mod + pkg + `"`
	if test != "" {
		s += `,"Test":"` + test + `"`
	}
	if output != "" {
		s += `,"Output":` + strconv.Quote(output)
	}
	return s + "}"
}

// The cases a run can end in besides one failing test, each of which must reach
// the log and the exit status.
func TestTierSummaryReportsEveryWayARunEnds(t *testing.T) {
	t.Run("everything passes", func(t *testing.T) {
		out, code := tiers(t,
			ev("internal/clock", "pass", "TestTick", ""),
			ev("internal/clock", "output", "", "ok  \t"+mod+"internal/clock\t0.1s\n"),
			ev("internal/clock", "pass", "", ""),
			ev("internal/blobs", "output", "", "?   \t"+mod+"internal/blobs\t[no test files]\n"),
			ev("internal/blobs", "skip", "", ""))
		if code != 0 {
			t.Fatalf("exit %d for a green run:\n%s", code, out)
		}
		for _, want := range []string{"ok  \t" + mod + "internal/clock", "?   \t" + mod + "internal/blobs", "| unit | 1 | 1 | 0 | 0 | 0 |", "1 packages with tests, 0 from cache"} {
			if !strings.Contains(out, want) {
				t.Errorf("output lacks %q:\n%s", want, out)
			}
		}
	})
	t.Run("a package does not build", func(t *testing.T) {
		out, code := tiers(t,
			`{"ImportPath":"`+mod+`internal/clock [`+mod+`internal/clock.test]","Action":"build-output","Output":"internal/clock/clock.go:3:1: syntax error\n"}`,
			`{"ImportPath":"`+mod+`internal/clock [`+mod+`internal/clock.test]","Action":"build-fail"}`,
			ev("internal/clock", "output", "", "FAIL\t"+mod+"internal/clock [build failed]\n"),
			ev("internal/clock", "fail", "", ""))
		if code != 1 {
			t.Fatalf("exit %d for a build failure:\n%s", code, out)
		}
		for _, want := range []string{"clock.go:3:1: syntax error", "[build failed]", "| unit | 1 | 0 | 1 |"} {
			if !strings.Contains(out, want) {
				t.Errorf("output lacks %q:\n%s", want, out)
			}
		}
	})
	t.Run("a subtest fails", func(t *testing.T) {
		out, code := tiers(t,
			ev("internal/chaos", "output", "TestWrites/two", "    w_test.go:5: lost a write\n"),
			ev("internal/chaos", "output", "TestWrites/two", "    --- FAIL: TestWrites/two (0.00s)\n"),
			ev("internal/chaos", "fail", "TestWrites/two", ""),
			ev("internal/chaos", "output", "TestWrites/one", "    --- PASS: TestWrites/one (0.00s)\n"),
			ev("internal/chaos", "pass", "TestWrites/one", ""),
			ev("internal/chaos", "output", "TestWrites", "--- FAIL: TestWrites (0.00s)\n"),
			ev("internal/chaos", "fail", "TestWrites", ""),
			ev("internal/chaos", "output", "", "FAIL\t"+mod+"internal/chaos\t0.1s\n"),
			ev("internal/chaos", "fail", "", ""))
		if code != 1 {
			t.Fatalf("exit %d:\n%s", code, out)
		}
		for _, want := range []string{"lost a write", "--- FAIL: TestWrites/two", "--- FAIL: TestWrites (", "| chaos | 1 | 0 | 1 | 0 | 0 |"} {
			if !strings.Contains(out, want) {
				t.Errorf("output lacks %q:\n%s", want, out)
			}
		}
		if strings.Contains(out, "TestWrites/one") {
			t.Errorf("printed a passing subtest:\n%s", out)
		}
	})
	t.Run("a package panics outside any test", func(t *testing.T) {
		out, code := tiers(t,
			ev("internal/store", "output", "", "panic: init failed\n"),
			ev("internal/store", "output", "", "FAIL\t"+mod+"internal/store\t0.0s\n"),
			ev("internal/store", "fail", "", ""))
		if code != 1 {
			t.Fatalf("exit %d:\n%s", code, out)
		}
		for _, want := range []string{"panic: init failed", "| unit | 1 | 0 | 1 |"} {
			if !strings.Contains(out, want) {
				t.Errorf("output lacks %q:\n%s", want, out)
			}
		}
	})
	t.Run("equivalence counts only its gate tests", func(t *testing.T) {
		out, _ := tiers(t,
			ev("internal/bundled", "pass", "TestEveryBundleBuilds", ""),
			ev("internal/bundled", "pass", "TestStripeBundleBehaves", ""),
			ev("internal/bundled", "pass", "", ""),
			ev("internal/equivalence", "pass", "TestBundlesMatchRecordedPacks", ""),
			ev("internal/equivalence", "pass", "", ""))
		for _, want := range []string{"| equivalence | 2 | 2 | 0 | 0 | 0 |", "| unit | 1 | 1 | 0 | 0 | 0 |"} {
			if !strings.Contains(out, want) {
				t.Errorf("output lacks %q:\n%s", want, out)
			}
		}
	})
}
