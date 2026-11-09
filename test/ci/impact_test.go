package ci

import (
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
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
	if all["image"] != "true" || all["packages"] != "./a\n./b" || all["all"] != "true" {
		t.Errorf("everything: %v", all)
	}
	none := plan(`{"all": false, "reason": "", "packages": [], ` + jobs + `}`)
	if r, f := read(none["race"]), read(none["fuzz"]); r.Count != 1 || r.Packages != 0 || f.Count != 1 || f.Targets != 0 {
		t.Errorf("nothing: race %+v fuzz %+v", r, f)
	}
	if none["image"] != "false" || none["generated-models"] != "true" || none["all"] != "false" {
		t.Errorf("gates: %v", none)
	}
	some := plan(`{"all": false, "reason": "", "packages": ["./internal/services/aws/s3", "./internal/clock", "./internal/mutation", "./internal/check"], "linked": ["./internal/services/aws/s3", "./internal/clock", "./internal/mutation"], ` + jobs + `}`)
	if r, f := read(some["race"]), read(some["fuzz"]); r.Packages != 2 || r.Count != 1 || f.Targets < 60 || f.Count != 4 {
		t.Errorf("s3: race %+v fuzz %+v", r, f)
	}
	if strings.Contains(some["packages"], "internal/check") {
		t.Errorf("a package that only reads a changed file reached race and fuzz: %q", some["packages"])
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

// Every plan output a job reads is one the plan job declares, and every value
// the plan job declares comes from a key its steps write: cmd/affected's gates
// through scripts/ci-plan.py, and the mutation plan's shards and count. A
// misspelt name reads as empty, and an empty gate must not read as unaffected.
func TestPlanOutputsAreWhatThePlanEmits(t *testing.T) {
	root := repo(t)
	raw, err := os.ReadFile(filepath.Join(root, ".github", "workflows", "ci.yml"))
	if err != nil {
		t.Fatal(err)
	}
	var wf struct {
		Jobs map[string]struct {
			Outputs map[string]string `yaml:"outputs"`
			Env     map[string]string `yaml:"env"`
		} `yaml:"jobs"`
	}
	if err := yaml.Unmarshal(raw, &wf); err != nil {
		t.Fatal(err)
	}
	declared := wf.Jobs["plan"].Outputs
	for _, m := range regexp.MustCompile(`needs\.plan\.outputs\.([A-Za-z0-9_-]+)`).FindAllStringSubmatch(string(raw), -1) {
		if _, ok := declared[m[1]]; !ok {
			t.Errorf("ci.yml reads needs.plan.outputs.%s, which the plan job does not declare", m[1])
		}
	}

	out, err := runIn(root, "go", "run", "./cmd/affected", "-json", "-files", "/dev/null").Output()
	if err != nil {
		t.Fatal(err)
	}
	var affected struct {
		Jobs map[string]bool `json:"jobs"`
	}
	if err := json.Unmarshal(out, &affected); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "a.json")
	os.WriteFile(path, out, 0o644)
	plan, err := run(t, root, nil, "python3", "-I", filepath.Join(root, "scripts", "ci-plan.py"), path)
	if err != nil {
		t.Fatalf("%v\n%s", err, plan)
	}
	emitted := map[string]map[string]bool{"affected": {}, "mutation": {"shards": true, "count": true}}
	for _, line := range strings.Split(plan, "\n") {
		if k, _, ok := strings.Cut(line, "="); ok {
			emitted["affected"][k] = true
		} else if k, ok := strings.CutSuffix(line, "<<__AFFECTED__"); ok {
			emitted["affected"][k] = true
		}
	}
	for name, expr := range declared {
		m := regexp.MustCompile(`^\$\{\{ steps\.(affected|mutation)\.outputs\.([A-Za-z0-9_-]+) \}\}$`).FindStringSubmatch(expr)
		if m == nil || !emitted[m[1]][m[2]] {
			t.Errorf("plan output %s is %q, which no step writes", name, expr)
		}
	}
	// Each gate cmd/affected computes is a job that reads it as its GATE.
	if len(affected.Jobs) == 0 {
		t.Fatal("cmd/affected computes no gate")
	}
	for gate := range affected.Jobs {
		if got := wf.Jobs[gate].Env["GATE"]; got != "${{ needs.plan.outputs."+gate+" }}" {
			t.Errorf("job %s reads GATE=%q", gate, got)
		}
	}
	for job, j := range wf.Jobs {
		if _, gated := j.Env["GATE"]; gated {
			if _, known := affected.Jobs[job]; !known {
				t.Errorf("job %s has a GATE cmd/affected does not compute", job)
			}
		}
	}
	// Skipping is only ever on an explicit false or 0.
	for _, bad := range []string{"env.GATE != 'true'\n        run: echo", "env.GATE == 'true'"} {
		if strings.Contains(string(raw), bad) {
			t.Errorf("ci.yml gates on %q, which reads a missing answer as unaffected", bad)
		}
	}
}

// Every line of scripts/fuzz-targets.txt names a package of the module and a
// Fuzz function in it. fuzz.sh fails on either at run time; this says so on
// every pull request that touches the list or a fuzz test, in seconds.
func TestFuzzTargetsNameRealTargets(t *testing.T) {
	root := repo(t)
	raw, err := os.ReadFile(filepath.Join(root, "scripts", "fuzz-targets.txt"))
	if err != nil {
		t.Fatal(err)
	}
	bodies := map[string]string{}
	for _, line := range strings.Split(string(raw), "\n") {
		f := strings.Fields(line)
		if len(f) == 0 || strings.HasPrefix(f[0], "#") {
			continue
		}
		if len(f) != 2 {
			t.Errorf("%q is not a package and a function", line)
			continue
		}
		pkg, name := f[0], f[1]
		body, ok := bodies[pkg]
		if !ok {
			files, _ := filepath.Glob(filepath.Join(root, filepath.FromSlash(pkg), "*_test.go"))
			if len(files) == 0 {
				t.Errorf("%s: no such package with tests", pkg)
			}
			for _, file := range files {
				b, _ := os.ReadFile(file)
				body += string(b)
			}
			bodies[pkg] = body
		}
		if !regexp.MustCompile(`(?m)^func ` + regexp.QuoteMeta(name) + `\(\w+ \*testing\.F\)`).MatchString(body) {
			t.Errorf("%s has no func %s(*testing.F)", pkg, name)
		}
	}
}

// fuzz.sh refuses a list naming a package the module does not have, even when
// FUZZ_PKGS would have filtered it out.
func TestFuzzScriptRefusesAStalePackage(t *testing.T) {
	root := repo(t)
	dir := t.TempDir()
	targets := filepath.Join(dir, "targets")
	os.WriteFile(targets, []byte("./internal/no/such/package FuzzX\n./internal/clock FuzzNoSuchTarget\n"), 0o644)
	pkgs := filepath.Join(dir, "pkgs")
	os.WriteFile(pkgs, []byte("./internal/engine\n"), 0o644)
	out, err := run(t, root, []string{"FUZZ_TARGETS=" + targets, "FUZZ_PKGS=" + pkgs}, "bash", filepath.Join(root, "scripts", "fuzz.sh"))
	if err == nil || !strings.Contains(out, "does not have") {
		t.Errorf("a stale package passed: err=%v\n%s", err, out)
	}
}
