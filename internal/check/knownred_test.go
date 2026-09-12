package check

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

type knownRed struct {
	Expected []struct {
		Test    string `json:"test"`
		Package string `json:"package"`
		Why     string `json:"why"`
	} `json:"expected"`
}

// TestKnownRedIsDeclaredHonestly keeps known-red.json from becoming the thing
// it exists to prevent.
//
// The file lists the failures this repository expects, so that a step which is
// already red says which red it is. That only works while every entry is true.
// An entry naming a test that no longer exists excuses nothing and reads as a
// documented failure; an entry with no reason is an assertion that something
// should keep failing, with nothing to check it against; an entry naming a
// package that is gone survives the extraction that removed it.
//
// So this is the same shape as TestMutantNeedlesExist and
// TestMakefileNamesRealPackages, and exists for the third time for the same
// reason: when a slow suite is the only thing that can see a class of
// staleness, extract the cheap check that sees it in a second.
//
// What it deliberately does NOT check is that each listed test currently
// fails. Running them to find out would cost the suite this is meant to make
// legible, and the script reports an entry it did not see in a run, which is
// where that belongs.
func TestKnownRedIsDeclaredHonestly(t *testing.T) {
	root := findMod(t)
	body, err := os.ReadFile(filepath.Join(root, "known-red.json"))
	if err != nil {
		t.Fatalf("read known-red.json: %v", err)
	}
	var list knownRed
	if err := json.Unmarshal(body, &list); err != nil {
		t.Fatalf("parse known-red.json: %v", err)
	}
	if len(list.Expected) == 0 {
		t.Fatal("known-red.json declares nothing; delete it rather than leaving an empty list that reads as `nothing is expected to fail`")
	}

	seen := map[string]bool{}
	for _, e := range list.Expected {
		if strings.TrimSpace(e.Why) == "" {
			t.Errorf("%s has no reason. An entry is a claim that a failure is expected; "+
				"without a reason a reader cannot tell it from one nobody got to.", e.Test)
		}
		if seen[e.Test] {
			t.Errorf("%s is listed twice", e.Test)
		}
		seen[e.Test] = true

		dir := filepath.Join(root, filepath.FromSlash(strings.TrimPrefix(e.Package, "./")))
		if info, err := os.Stat(dir); err != nil || !info.IsDir() {
			t.Errorf("%s names package %s, which is not a directory in this tree. "+
				"If the package was deleted, the entry goes with it.", e.Test, e.Package)
			continue
		}
		// The test name may be a subtest -- TestMutantsAreKilled/<mutant> --
		// and only the parent is a function. A subtest's own existence is what
		// TestMutantNeedlesExist covers for the mutants, which is every
		// subtest on this list today.
		fn := e.Test
		if i := strings.Index(fn, "/"); i >= 0 {
			fn = fn[:i]
		}
		if !declaresFunc(t, dir, fn) {
			t.Errorf("%s names no test function %s in %s. The entry excuses nothing "+
				"and reads as a documented failure; delete it.", e.Test, fn, e.Package)
		}
	}
}

// declaresFunc reports whether any _test.go file in dir declares `func name(`.
func declaresFunc(t *testing.T, dir, name string) bool {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		return false
	}
	needle := []byte("func " + name + "(")
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), "_test.go") {
			continue
		}
		body, err := os.ReadFile(filepath.Join(dir, entry.Name()))
		if err == nil && strings.Contains(string(body), string(needle)) {
			return true
		}
	}
	return false
}

// TestKnownRedScriptNamesANewFailure drives the script itself, because a
// reporting tool that reports nothing fails silently by construction: it is
// only ever read when something is already wrong.
func TestKnownRedScriptNamesANewFailure(t *testing.T) {
	root := findMod(t)
	script := filepath.Join(root, "scripts", "known-red.py")
	if _, err := os.Stat(script); err != nil {
		t.Fatalf("scripts/known-red.py: %v", err)
	}
	run := func(output string) (string, bool) {
		t.Helper()
		cmd := exec.Command("python3", script)
		cmd.Stdin = strings.NewReader(output)
		out, err := cmd.CombinedOutput()
		return string(out), err == nil
	}

	// A run with only expected failures: named, with the reason, exit 0.
	got, ok := run("ok  \tpkg\n--- FAIL: TestRatchetNotExceeded (1.00s)\n")
	if !ok {
		t.Errorf("a run with only expected failures exited non-zero:\n%s", got)
	}
	if !strings.Contains(got, "no new failures") || !strings.Contains(got, "TestRatchetNotExceeded") {
		t.Errorf("expected failure not reported as expected:\n%s", got)
	}

	// The case this exists for: one new failure beside the expected ones.
	got, ok = run("--- FAIL: TestRatchetNotExceeded (1.00s)\n--- FAIL: TestGenerateCatalogIdempotent (2.00s)\n")
	if ok {
		t.Errorf("a run with a new failure exited zero:\n%s", got)
	}
	if !strings.Contains(got, "NEW FAILURES") || !strings.Contains(got, "TestGenerateCatalogIdempotent") {
		t.Errorf("the new failure was not named:\n%s", got)
	}

	// A parent that fails only because a declared subtest did is not new.
	got, ok = run("--- FAIL: TestMutantsAreKilled (0.01s)\n    --- FAIL: TestMutantsAreKilled/s3-list-uploads-accept-zero-limit (3.00s)\n")
	if !ok || strings.Contains(got, "NEW FAILURES") {
		t.Errorf("a parent of a declared subtest was reported as new:\n%s", got)
	}

	// ...but a parent that fails for a mutant NOT on the list still is.
	got, ok = run("--- FAIL: TestMutantsAreKilled (0.01s)\n    --- FAIL: TestMutantsAreKilled/some-other-mutant (3.00s)\n")
	if ok || !strings.Contains(got, "some-other-mutant") {
		t.Errorf("an undeclared mutant was absorbed by its parent:\n%s", got)
	}

	// A clean run says so rather than saying nothing.
	got, ok = run("ok  \tpkg\t1.000s\n")
	if !ok || !strings.Contains(got, "no new failures") {
		t.Errorf("a clean run was not reported:\n%s", got)
	}

	// A log downloaded from GitHub Actions carries a timestamp on every line.
	// The first version of this matched only at the start of a line, so run
	// against a real CI log it found none of its twenty-four failures and
	// reported "no new failures" -- the tool's own defect, in the tool's own
	// failure mode.
	got, ok = run("2026-09-11T23:25:48.4922863Z --- FAIL: TestRatchetNotExceeded (1.73s)\n" +
		"2026-09-11T23:42:08.2247033Z --- FAIL: TestFirehoseSnowflakeSecretAndPersistentBuffer (0.00s)\n")
	if ok || !strings.Contains(got, "TestFirehoseSnowflakeSecretAndPersistentBuffer") {
		t.Errorf("a timestamped CI log was not parsed:\n%s", got)
	}

	// And silence is the one answer it must never give by accident. An empty
	// input, a file that is not a test run, a pipe that carried stderr
	// elsewhere -- each produces no failures, and "no new failures" about any
	// of them is a lie in the direction that costs the most.
	for _, input := range []string{"", "make: *** [Makefile:1: x] Error 1\n"} {
		got, ok = run(input)
		if ok || !strings.Contains(got, "no `go test` output") {
			t.Errorf("input %q was treated as a clean run:\n%s", input, got)
		}
	}
}
