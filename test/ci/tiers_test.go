package ci

import (
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

const mod = "github.com/tyler-r-kendrick/mirror.cloud/"

// test-tiers.py replaces the plain log of the run it reads, so it must keep
// what known-red.py and a person read in that log -- the package lines and the
// whole output of a failing test -- and put each test in the tier whose suite
// used to run it.
func TestTierSummaryKeepsTheLogAndSortsTestsIntoTiers(t *testing.T) {
	root, _ := filepath.Abs("../..")
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

// tiers runs test-tiers.py over events and returns what it printed and its exit
// status.
func tiers(t *testing.T, events ...string) (string, int) {
	t.Helper()
	root, _ := filepath.Abs("../..")
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
