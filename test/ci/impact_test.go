package ci

import (
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

// shard.py must put every package in exactly one shard, whatever the costs say,
// and refuse a shard that is not one; --count must stay within its bounds.
func TestShardScriptPartitionsPackages(t *testing.T) {
	root := repo(t)
	script := filepath.Join(root, "scripts", "shard.py")
	costs := filepath.Join(root, "test", "race-costs.json")
	pkgs := "github.com/tyler-r-kendrick/mirror.cloud/internal/check\n./internal/clock\ninternal/engine\n./a/new/package\ngithub.com/tyler-r-kendrick/mirror.cloud\n"
	in := func(args ...string) (string, error) {
		cmd := runIn(root, "python3", append([]string{"-I", script, costs}, args...)...)
		cmd.Stdin = strings.NewReader(pkgs)
		out, err := cmd.CombinedOutput()
		return string(out), err
	}
	for shards := 1; shards <= 6; shards++ {
		seen := map[string]int{}
		for s := 0; s < shards; s++ {
			out, err := in(strconv.Itoa(s), strconv.Itoa(shards))
			if err != nil {
				t.Fatalf("%v\n%s", err, out)
			}
			for _, p := range strings.Fields(out) {
				seen[p]++
			}
		}
		for _, p := range []string{"./internal/check", "./internal/clock", "./internal/engine", "./a/new/package", "."} {
			if seen[p] != 1 {
				t.Errorf("%d shards: %s in %d", shards, p, seen[p])
			}
		}
	}
	if _, err := in("4", "4"); err == nil {
		t.Error("shard 4 of 4 was accepted")
	}
	if out, err := in("--count", "1", "3"); err != nil || strings.TrimSpace(out) != "3" {
		t.Errorf("--count with a tiny budget: %q %v", out, err)
	}
	if out, err := in("--count", "1e9", "3"); err != nil || strings.TrimSpace(out) != "1" {
		t.Errorf("--count with a huge budget: %q %v", out, err)
	}
}

// With FUZZ_PKGS, fuzz.sh fuzzes only the targets in those packages, and a set
// that holds none passes saying so; without it, an empty shard still fails.
func TestFuzzScriptFuzzesOnlyTheAffectedPackages(t *testing.T) {
	root := repo(t)
	dir := t.TempDir()
	targets := filepath.Join(dir, "targets")
	os.WriteFile(targets, []byte("./internal/idgen FuzzNoSuchTarget\n./internal/clock FuzzNoSuchTarget\n"), 0o644)
	pkgs := filepath.Join(dir, "pkgs")
	os.WriteFile(pkgs, []byte("./internal/engine\n"), 0o644)
	run1 := func(env ...string) (string, error) {
		return run(t, root, append([]string{"FUZZ_TARGETS=" + targets}, env...), "bash", filepath.Join(root, "scripts", "fuzz.sh"))
	}
	if out, err := run1("FUZZ_PKGS=" + pkgs); err != nil || !strings.Contains(out, "hold no fuzz target") {
		t.Errorf("no target in the affected packages: err=%v\n%s", err, out)
	}
	// Selecting one of the two: only that one is tried, and its misspelling fails.
	os.WriteFile(pkgs, []byte("./internal/clock\n"), 0o644)
	out, err := run1("FUZZ_PKGS=" + pkgs)
	if err == nil || !strings.Contains(out, "./internal/clock has no fuzz target") || strings.Contains(out, "idgen") {
		t.Errorf("the filter did not narrow the run: err=%v\n%s", err, out)
	}
}

// ci-plan.py: everything gets the full matrices; a selection gets what its
// cost needs; nothing gets one shard that reports a skip.
func TestCIPlanSizesTheMatrices(t *testing.T) {
	root := repo(t)
	plan := func(affected string) map[string]string {
		t.Helper()
		path := filepath.Join(t.TempDir(), "a.json")
		os.WriteFile(path, []byte(affected), 0o644)
		out, err := run(t, root, nil, "python3", "-I", filepath.Join(root, "scripts", "ci-plan.py"), path)
		if err != nil {
			t.Fatalf("%v\n%s", err, out)
		}
		got := map[string]string{}
		lines := strings.Split(out, "\n")
		for i := 0; i < len(lines); i++ {
			if k, v, ok := strings.Cut(lines[i], "="); ok {
				got[k] = v
			} else if k, ok := strings.CutSuffix(lines[i], "<<__AFFECTED__"); ok {
				var body []string
				for i++; i < len(lines) && lines[i] != "__AFFECTED__"; i++ {
					body = append(body, lines[i])
				}
				got[k] = strings.Join(body, "\n")
			}
		}
		return got
	}
	type matrix struct {
		Shards   []int `json:"shards"`
		Count    int   `json:"count"`
		Packages int   `json:"packages"`
		Targets  int   `json:"targets"`
	}
	read := func(s string) matrix {
		var m matrix
		if err := json.Unmarshal([]byte(s), &m); err != nil {
			t.Fatalf("%q: %v", s, err)
		}
		return m
	}
	jobs := `"jobs": {"image": false, "generated-models": true, "cloudflare-miniflare": false}`
	all := plan(`{"all": true, "reason": "-all", "packages": ["./a", "./b"], ` + jobs + `}`)
	if r, f := read(all["race"]), read(all["fuzz"]); r.Count != 4 || len(r.Shards) != 4 || f.Count != 6 || f.Targets < 100 {
		t.Errorf("everything: race %+v fuzz %+v", r, f)
	}
	if all["image"] != "true" || all["packages"] != "./a\n./b" {
		t.Errorf("everything: %v", all)
	}
	none := plan(`{"all": false, "reason": "", "packages": [], ` + jobs + `}`)
	if r, f := read(none["race"]), read(none["fuzz"]); r.Count != 1 || r.Packages != 0 || f.Count != 1 || f.Targets != 0 {
		t.Errorf("nothing: race %+v fuzz %+v", r, f)
	}
	if none["image"] != "false" || none["generated-models"] != "true" {
		t.Errorf("gates: %v", none)
	}
	some := plan(`{"all": false, "reason": "", "packages": ["./internal/services/aws/s3", "./internal/clock", "./internal/mutation"], ` + jobs + `}`)
	if r, f := read(some["race"]), read(some["fuzz"]); r.Packages != 2 || r.Count != 1 || f.Targets < 60 || f.Count != 4 {
		t.Errorf("s3: race %+v fuzz %+v", r, f)
	}
}

// A pull request's race run is scoped and -short; main's is neither. A
// -short that reached main would skip under race there what racegate only
// lets pull requests skip, and a scope that reached main would leave packages
// out of the strict run.
func TestRaceIsStrictOnMain(t *testing.T) {
	root := repo(t)
	ci, err := os.ReadFile(filepath.Join(root, ".github", "workflows", "ci.yml"))
	if err != nil {
		t.Fatal(err)
	}
	step := regexp.MustCompile(`(?s)- name: race detector.*?\n      - name:`).Find(ci)
	if step == nil {
		t.Fatal("no race detector step in ci.yml")
	}
	guard := regexp.MustCompile(`(?s)if \[ "\$\{\{ github.event_name \}\}" = pull_request \]; then\n\s*printf[^\n]*\n\s*scope=\("RACE_PKGS=[^"]*" RACE_FLAGS=-short\)\n\s*fi`)
	if !guard.Match(step) || strings.Count(string(step), "-short") != 1 {
		t.Errorf("the race step must set RACE_PKGS and -short only on pull requests:\n%s", step)
	}
	nightly, err := os.ReadFile(filepath.Join(root, ".github", "workflows", "nightly.yml"))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(nightly), "-short") || strings.Contains(string(nightly), "RACE_PKGS") {
		t.Error("nightly.yml scopes or shortens the race run")
	}
	makefile, _ := os.ReadFile(filepath.Join(root, "Makefile"))
	if !regexp.MustCompile(`(?m)^RACE_FLAGS \?=\s*$`).Match(makefile) || !regexp.MustCompile(`(?m)^RACE_PKGS \?=\s*$`).Match(makefile) {
		t.Error("the Makefile's race defaults are not empty: make test-race must be strict unless told otherwise")
	}
}
