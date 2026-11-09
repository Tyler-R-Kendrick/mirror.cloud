package ci

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

const mod = "github.com/tyler-r-kendrick/mirror.cloud/"

// test-tiers.py replaces the plain log of the run it reads, so it must keep
// what known-red.py and a person read in that log -- the package lines and the
// whole output of a failing test -- and put each test in the tier whose suite
// used to run it.
func TestTierSummaryKeepsTheLogAndSortsTestsIntoTiers(t *testing.T) {
	root := repo(t)
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
