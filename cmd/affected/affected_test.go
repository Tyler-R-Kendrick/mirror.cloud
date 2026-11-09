package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

var update = flag.Bool("update", false, "rewrite testdata/golden.json from the current tree")

var (
	graphOnce sync.Once
	theGraph  *graph
	graphErr  error
)

func moduleGraph(t *testing.T) (*graph, string) {
	t.Helper()
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	graphOnce.Do(func() { theGraph, graphErr = load(root) })
	if graphErr != nil {
		t.Fatal(graphErr)
	}
	return theGraph, root
}

// Every tracked file is placed: owned by a package, or named by a rule. A file
// that is neither still selects everything (fail closed), but a new file that
// lands there makes every pull request touching it run the whole module, which
// is the cost a rule exists to name.
func TestEveryTrackedFileMapsSomewhere(t *testing.T) {
	g, root := moduleGraph(t)
	cmd := exec.Command("git", "ls-files")
	cmd.Dir = root
	out, err := cmd.Output()
	if err != nil {
		t.Skipf("git ls-files: %v (not a git checkout)", err)
	}
	for _, p := range strings.Fields(string(out)) {
		c := g.classify(p)
		if c.all && c.why == "no rule and no package owns it" {
			t.Errorf("%s: no package owns it and no rule in affected.go names it, so a change to it runs everything; add a rule", p)
		}
	}
}

// Fail closed: a path nothing accounts for selects every package, and so do
// the module's requirements. A path that is gone and that no package holds
// selects nothing: nothing can compile it.
func TestUnknownPathsSelectEverything(t *testing.T) {
	shared, _ := moduleGraph(t)
	// The same graph, looking for unplaced paths in a tree where these exist.
	g := *shared
	g.root = t.TempDir()
	for _, p := range []string{"nowhere/new.bin", "a-new-root-file", ".github/new-thing.yml"} {
		os.MkdirAll(filepath.Dir(filepath.Join(g.root, p)), 0o755)
		if err := os.WriteFile(filepath.Join(g.root, p), nil, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if r := g.selectFor([]string{"internal/services/aws/gone/gone.go", "RELEASE"}); r.all || len(r.pkgs) != 0 {
		t.Errorf("deleted paths no package holds selected %v (all=%v)", r.pkgs, r.all)
	}
	for _, p := range []string{"nowhere/new.bin", "a-new-root-file", "go.mod", "go.sum", ".github/new-thing.yml"} {
		r := g.selectFor([]string{"docs/TESTING.md", p})
		if !r.all || len(r.pkgs) != len(g.pkgs) {
			t.Errorf("%s selected %d of %d packages (all=%v); want every one", p, len(r.pkgs), len(g.pkgs), r.all)
		}
	}
}

// A package a rule names that no longer exists would quietly select nothing.
func TestRulesNameRealPackages(t *testing.T) {
	g, _ := moduleGraph(t)
	known := map[string]bool{}
	for _, p := range g.pkgs {
		known[p] = true
	}
	for _, r := range append(rules, rule{pattern: "readers", pkgs: readers}) {
		for _, p := range r.pkgs {
			if !known[p] {
				t.Errorf("rule %s names %s, which is not a package", r.pattern, p)
			}
		}
	}
}

// A change reaches the package that holds it and every package whose test
// binary links that package, and nothing else but the tree readers.
func TestAChangeSelectsItsReverseTestDependencies(t *testing.T) {
	g, _ := moduleGraph(t)
	r := g.selectFor([]string{"internal/clock/clock.go"})
	has := map[string]bool{}
	for _, p := range r.pkgs {
		has[p] = true
	}
	if r.all || !has["./internal/clock"] || !has["./internal/check"] {
		t.Fatalf("internal/clock selected %v", r.pkgs)
	}
	for p, deps := range g.deps {
		links := false
		for _, d := range deps {
			links = links || d == "./internal/clock"
		}
		if links != has[p] && p != "./internal/clock" && p != "./internal/check" && p != "./internal/mutation" {
			t.Errorf("%s: links internal/clock=%v, selected=%v", p, links, has[p])
		}
	}
	// docs/** names the four packages that read it, one of them a reader, and
	// selects nothing that merely links one of them.
	if docs := g.selectFor([]string{"README.md", "docs/TESTING.md", ".cursor/install.sh"}); docs.all || len(docs.pkgs) != 4+len(readers)-1 {
		t.Errorf("a documentation change selected %v", docs.pkgs)
	}
	if none := g.selectFor([]string{"README.md", "CONTRIBUTING.md"}); none.all || len(none.pkgs) != 0 {
		t.Errorf("root Markdown selected %v", none.pkgs)
	}
}

// The selections for a few representative changes, recorded. A diff here is a
// change in what pull requests run; rerun with -update and review it.
func TestGoldenSelections(t *testing.T) {
	g, _ := moduleGraph(t)
	cases := map[string][]string{
		"root markdown":         {"README.md", "AGENTS.md"},
		"testing docs":          {"docs/TESTING.md"},
		"one leaf package":      {"internal/services/aws/scheduleexpr/scheduleexpr.go"},
		"a test file":           {"test/ci/cache_test.go"},
		"the ci workflow":       {".github/workflows/ci.yml"},
		"a pinned spec":         {"specs/mirror.lock"},
		"the sdk module":        {"test/sdk/go/go.mod"},
		"the module file":       {"go.mod"},
		"a deleted root file":   {"RELEASE"},
		"a fuzz target's tests": {"internal/proto/aws/httpuri/httpuri_test.go"},
		"the dockerfile":        {"Dockerfile"},
		"the miniflare runtime": {"tools/cloudflare-runtime/package-lock.json"},
	}
	got := map[string]any{}
	for name, files := range cases {
		r := g.selectFor(files)
		if r.all {
			got[name] = map[string]any{"files": files, "all": r.reason, "jobs": r.gates(files)}
		} else {
			got[name] = map[string]any{"files": files, "packages": nonNil(r.pkgs), "jobs": r.gates(files)}
		}
	}
	enc, _ := json.MarshalIndent(got, "", "  ")
	enc = append(enc, '\n')
	path := filepath.Join("testdata", "golden.json")
	if *update {
		if err := os.WriteFile(path, enc, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("%v (run go test ./cmd/affected -update)", err)
	}
	if !bytes.Equal(want, enc) {
		t.Errorf("selections differ from %s; review and rerun with -update:\n%s", path, enc)
	}
}

func TestRunPrintsPackagesAndJSON(t *testing.T) {
	_, root := moduleGraph(t)
	var out, errb bytes.Buffer
	if err := run([]string{"-root", root}, strings.NewReader("internal/clock/clock.go\n\n"), &out, &errb); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "./internal/clock\n") || !strings.Contains(errb.String(), "packages") {
		t.Errorf("stdout %q stderr %q", out.String(), errb.String())
	}
	out.Reset()
	errb.Reset()
	if err := run([]string{"-root", root}, strings.NewReader("go.mod\n"), &out, &errb); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(errb.String(), "every package") {
		t.Errorf("stderr %q", errb.String())
	}
	out.Reset()
	list := filepath.Join(t.TempDir(), "changed")
	os.WriteFile(list, []byte("README.md\n"), 0o644)
	if err := run([]string{"-root", root, "-json", "-files", list}, nil, &out, &errb); err != nil {
		t.Fatal(err)
	}
	var r struct {
		All      bool     `json:"all"`
		Packages []string `json:"packages"`
	}
	if err := json.Unmarshal(out.Bytes(), &r); err != nil || r.All || r.Packages == nil || len(r.Packages) != 0 {
		t.Errorf("json %q: %v", out.String(), err)
	}
	if err := run([]string{"-files", filepath.Join(t.TempDir(), "absent")}, nil, &out, &errb); err == nil {
		t.Error("a missing -files was accepted")
	}
	if err := run([]string{"-nosuchflag"}, nil, &out, &errb); err == nil {
		t.Error("an unknown flag was accepted")
	}
	if err := run([]string{"-root", t.TempDir()}, strings.NewReader(""), &out, &errb); err == nil {
		t.Error("a directory with no module was accepted")
	}
}

func TestParseRejectsMalformedListing(t *testing.T) {
	if _, err := parse("m", "/r", []byte("one\ttwo\n")); err == nil {
		t.Error("a short line was accepted")
	}
	if _, err := parse("m", "/r", []byte("")); err == nil {
		t.Error("an empty listing was accepted")
	}
	g, err := parse("m", "/r", []byte("m\t/r\tm\t\nm/a\t/r/a\tm\tm other\nm/a.test\t/r/a\tm\tm/a [m/a.test] m other m/a.test\n"))
	if err != nil {
		t.Fatal(err)
	}
	if g.dirs[""] != "." || g.dirs["a"] != "./a" || strings.Join(g.deps["./a"], " ") != "./a ." {
		t.Errorf("%+v", g)
	}
}

// Every gate opens for a selection of everything and for its own inputs, and a
// documentation change opens none.
func TestGatesOpenForTheirInputs(t *testing.T) {
	g, _ := moduleGraph(t)
	for name, files := range map[string][]string{
		"generated-models":     {"internal/generated/aws/s3/model.go"},
		"image":                {"profiles/core.txt"},
		"cloudflare-miniflare": {"tools/cloudflare-runtime/package.json"},
	} {
		if !g.selectFor(files).gates(files)[name] {
			t.Errorf("%s stays shut for %v", name, files)
		}
	}
	for _, files := range [][]string{{"README.md"}, {"docs/TESTING.md"}} {
		for name, open := range g.selectFor(files).gates(files) {
			if open {
				t.Errorf("%v opens %s", files, name)
			}
		}
	}
	for name, open := range g.selectFor([]string{"go.mod"}).gates([]string{"go.mod"}) {
		if !open {
			t.Errorf("go.mod leaves %s shut", name)
		}
	}
	for name, open := range g.selectFor([]string{"Makefile"}).gates([]string{"Makefile"}) {
		if !open {
			t.Errorf("the Makefile leaves %s shut", name)
		}
	}
}
