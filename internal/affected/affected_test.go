package affected

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

var (
	graphOnce sync.Once
	theGraph  *Graph
	graphErr  error
)

func moduleGraph(t *testing.T) (*Graph, string) {
	t.Helper()
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	graphOnce.Do(func() { theGraph, graphErr = Load(root) })
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
	cmd := exec.Command("git", "ls-files", "-z")
	cmd.Dir = root
	out, err := cmd.Output()
	if err != nil {
		t.Skipf("git ls-files: %v (not a git checkout)", err)
	}
	for _, p := range Split(out, true) {
		c := g.classify(p)
		if c.all && c.why == "no rule and no package owns it" {
			t.Errorf("%s: no package owns it and no rule in affected.go names it, so a change to it runs everything; add a rule", p)
		}
		// A file that selects nothing has to say so: a rule marked none.
		if !c.all && len(c.code)+len(c.self)+len(c.direct) == 0 {
			marked := false
			for _, r := range rules {
				if r.matches(p) {
					marked = r.none
					break
				}
			}
			if !marked {
				t.Errorf("%s selects nothing, and no rule marked none says that is intended", p)
			}
		}
	}
	for _, r := range rules {
		if !r.all && len(r.pkgs) == 0 && !r.none {
			t.Errorf("rule %s selects nothing without saying so (none: true)", r.pattern)
		}
	}
}

// Fail closed: a path nothing accounts for selects every package, and so do
// the module's requirements. A path that is gone and that no package holds
// selects nothing: nothing can compile it.
func TestUnknownPathsSelectEverything(t *testing.T) {
	shared, _ := moduleGraph(t)
	// The same Graph, looking for unplaced paths in a tree where these exist.
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
	// A file tests only read links into nothing, and code links into what
	// the selection holds.
	if docs := g.selectFor([]string{"docs/TESTING.md"}); len(docs.linkedList()) != 0 {
		t.Errorf("a documentation change links %v", docs.linkedList())
	}
	linked := map[string]bool{}
	for _, p := range r.linkedList() {
		linked[p] = true
	}
	if !linked["./internal/clock"] || len(linked) > len(r.pkgs) {
		t.Errorf("internal/clock links %v", r.linkedList())
	}
	for p, deps := range g.deps {
		links := p == "./internal/clock"
		for _, d := range deps {
			links = links || d == "./internal/clock"
		}
		if links != linked[p] {
			t.Errorf("%s: links internal/clock=%v, in linked=%v", p, links, linked[p])
		}
	}
	if every := g.selectFor([]string{"go.mod"}).linkedList(); len(every) != len(g.pkgs) {
		t.Errorf("go.mod links %d of %d", len(every), len(g.pkgs))
	}
	if none := g.selectFor([]string{"README.md", "CONTRIBUTING.md"}); none.all || len(none.pkgs) != 0 {
		t.Errorf("root Markdown selected %v", none.pkgs)
	}
}

// What representative changes select, by what must be in the selection rather
// than the whole list: the reverse dependencies of a leaf change as packages
// are added, and a recorded list would fail for that alone. Every case also
// says which gated jobs it opens.
func TestRepresentativeSelections(t *testing.T) {
	g, _ := moduleGraph(t)
	for name, c := range map[string]struct {
		files  []string
		all    bool
		linked []string // must be in Linked
		only   []string // when set, Pkgs is exactly this
		jobs   []string // the gates it opens
	}{
		"root markdown":         {files: []string{"README.md", "AGENTS.md"}, only: []string{}},
		"testing docs":          {files: []string{"docs/TESTING.md"}, only: []string{"./internal/check", "./internal/mutation", "./internal/runtime", "./test/behavior/cloudflare", "./test/behavior/vercel"}, jobs: []string{"cloudflare-miniflare"}},
		"one leaf package":      {files: []string{"internal/scheduleexpr/expression.go"}, linked: []string{"./internal/scheduleexpr", "./internal/services/aws/scheduler", "./cmd/mirror", "./internal/allservices"}, jobs: []string{"cloudflare-miniflare", "image", "sparse"}},
		"a test file":           {files: []string{"test/ci/cache_test.go"}, linked: []string{"./test/ci"}, only: []string{"./internal/check", "./internal/mutation", "./test/ci"}},
		"a fuzz target's tests": {files: []string{"internal/proto/aws/httpuri/httpuri_fuzz_test.go"}, linked: []string{"./internal/proto/aws/httpuri"}, only: []string{"./internal/check", "./internal/mutation", "./internal/proto/aws/httpuri"}},
		"a pinned spec":         {files: []string{"specs/mirror.lock"}, only: []string{"./cmd/mirror", "./cmd/mirrorgen", "./internal/check", "./internal/mutation", "./internal/specboot", "./test/behavior/vercel"}, jobs: []string{"generated-models", "image", "sparse"}},
		"the sdk module":        {files: []string{"test/sdk/go/go.mod"}, only: []string{}},
		"a deleted root file":   {files: []string{"RELEASE"}, only: []string{}},
		"the dockerfile":        {files: []string{"Dockerfile"}, only: []string{}, jobs: []string{"image"}},
		"the miniflare runtime": {files: []string{"tools/cloudflare-runtime/package-lock.json"}, jobs: []string{"cloudflare-miniflare"}},
		"the ci workflow":       {files: []string{".github/workflows/ci.yml"}, all: true},
		"the module file":       {files: []string{"go.mod"}, all: true},
	} {
		r := g.Select(c.files)
		if r.All != c.all {
			t.Errorf("%s: all=%v (%s)", name, r.All, r.Reason)
			continue
		}
		if c.all {
			continue
		}
		linked := map[string]bool{}
		for _, p := range r.Linked {
			linked[p] = true
		}
		for _, p := range c.linked {
			if !linked[p] {
				t.Errorf("%s: %s is not linked; linked %v", name, p, r.Linked)
			}
		}
		if c.only != nil && strings.Join(r.Pkgs, " ") != strings.Join(c.only, " ") {
			t.Errorf("%s: selected %v, want %v", name, r.Pkgs, c.only)
		}
		want := map[string]bool{}
		for _, j := range c.jobs {
			want[j] = true
		}
		for j, open := range r.Jobs {
			if open != want[j] {
				t.Errorf("%s: %s open=%v", name, j, open)
			}
		}
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
		if !func() map[string]bool { r := g.selectFor(files); return g.gates(r, files) }()[name] {
			t.Errorf("%s stays shut for %v", name, files)
		}
	}
	open := func(files ...string) map[string]bool { r := g.selectFor(files); return g.gates(r, files) }
	for name, isOpen := range open("README.md") {
		if isOpen {
			t.Errorf("README.md opens %s", name)
		}
	}
	// test/behavior/cloudflare reads docs/ at run time, so a docs change opens
	// the gate of the suite that holds it, and only that one.
	if jobs := open("docs/TESTING.md"); !jobs["cloudflare-miniflare"] || jobs["image"] || jobs["generated-models"] {
		t.Errorf("docs/TESTING.md opens %v", jobs)
	}
	for name, open := range g.gates(g.selectFor([]string{"go.mod"}), []string{"go.mod"}) {
		if !open {
			t.Errorf("go.mod leaves %s shut", name)
		}
	}
	for name, open := range g.gates(g.selectFor([]string{"Makefile"}), []string{"Makefile"}) {
		if !open {
			t.Errorf("the Makefile leaves %s shut", name)
		}
	}
}

// The reviewer's three scenarios: a change only to a package's tests must still
// reach race, fuzz and the gates that compile it.
func TestATestChangeReachesItsOwnPackage(t *testing.T) {
	g, _ := moduleGraph(t)
	linked := func(files ...string) (map[string]bool, map[string]bool) {
		r := g.selectFor(files)
		got := map[string]bool{}
		for _, p := range r.linkedList() {
			got[p] = true
		}
		return got, g.gates(r, files)
	}
	// A fuzz body edit: the fuzz target's package runs, and nothing that only
	// imports it.
	got, _ := linked("internal/proto/aws/httpuri/httpuri_fuzz_test.go")
	if !got["./internal/proto/aws/httpuri"] || len(got) != 1 {
		t.Errorf("a fuzz test edit links %v", got)
	}
	// A racy t.Parallel test added to a package: that package runs under race.
	got, _ = linked("internal/engine/race_new_test.go")
	if !got["./internal/engine"] {
		t.Errorf("a new test in internal/engine links %v", got)
	}
	// The live miniflare test is built only with -tags miniflare.
	got, jobs := linked("internal/execution/miniflare_live_test.go")
	if !got["./internal/execution"] || !jobs["cloudflare-miniflare"] {
		t.Errorf("miniflare_live_test.go links %v, gates %v", got, jobs)
	}
	if _, jobs := linked("test/behavior/cloudflare/testdata/x.json"); !jobs["cloudflare-miniflare"] {
		t.Errorf("a cloudflare suite input left the miniflare gate shut")
	}
	// The gate compiles what -tags miniflare links, so it holds packages the
	// plain Graph links only through the tagged files too.
	if !g.gate["cloudflare-miniflare"]["./internal/execution"] || len(g.gate["cloudflare-miniflare"]) < 5 {
		t.Errorf("the miniflare gate compiles %d packages", len(g.gate["cloudflare-miniflare"]))
	}
}

// The CI machinery selects everything, so race, fuzz and the gates run in
// full on a pull request that changes how they are selected or run.
func TestMachinerySelectsEverything(t *testing.T) {
	// The whole list, pinned: dropping an entry is a change to what pull
	// requests run, and has to be made here as well.
	want := []string{
		"Makefile",
		".github/workflows/ci.yml",
		".github/actions/**",
		"scripts/fuzz.sh",
		"scripts/fuzz-targets.txt",
		"scripts/shard.py",
		"scripts/ci-plan.py",
		"test/race-costs.json",
		"cmd/affected/**",
		"internal/affected/**",
	}
	if strings.Join(Machinery, "\n") != strings.Join(want, "\n") {
		t.Errorf("Machinery is\n%q\nwant\n%q", Machinery, want)
	}
	g, _ := moduleGraph(t)
	for _, p := range []string{
		"Makefile", ".github/workflows/ci.yml", ".github/actions/go/action.yml",
		"scripts/fuzz.sh", "scripts/fuzz-targets.txt", "scripts/shard.py", "scripts/ci-plan.py",
		"test/race-costs.json", "cmd/affected/main.go", "internal/affected/affected.go",
	} {
		r := g.selectFor([]string{p})
		if !r.all || len(r.linkedList()) != len(g.pkgs) {
			t.Errorf("%s selected %d of %d (all=%v)", p, len(r.linkedList()), len(g.pkgs), r.all)
		}
		for name, open := range g.gates(r, []string{p}) {
			if !open {
				t.Errorf("%s leaves %s shut", p, name)
			}
		}
	}
}

// A name git would quote is read exactly with -z, and a quoted one, from a
// list without -z, cannot be placed, so it selects everything.
func TestNamesAreReadExactly(t *testing.T) {
	g, _ := moduleGraph(t)
	in := "internal/clock/caf\u00e9 notes.txt\x00docs/TESTING.md\x00"
	got := Split([]byte(in), true)
	if len(got) != 2 || got[0] != "internal/clock/caf\u00e9 notes.txt" {
		t.Fatalf("-z read %q", got)
	}
	if r := g.selectFor(got); r.all || len(r.linkedList()) == 0 || r.linkedList()[0] == "" {
		t.Errorf("a non-ASCII name in internal/clock selected %v (all=%v)", r.linkedList(), r.all)
	}
	if r := g.selectFor([]string{`"internal/clock/caf\303\251.txt"`}); !r.all {
		t.Errorf("a quoted name selected %v", r.pkgs)
	}
}

// internal/bundled replays internal/equivalence's traces.
func TestTracesSelectBundled(t *testing.T) {
	g, _ := moduleGraph(t)
	r := g.selectFor([]string{"internal/equivalence/traces/aws.sqs.json"})
	has := map[string]bool{}
	for _, p := range r.pkgs {
		has[p] = true
	}
	if !has["./internal/bundled"] || !has["./internal/equivalence"] {
		t.Errorf("a trace selected %v", r.pkgs)
	}
}

// A directory rule must not hide a package: a Go file in a package is code of
// that package whatever rule covers its directory, and a package directory a
// non-all rule covers has to be listed here, so its other files' rule is a
// choice someone made.
var packagesUnderARule = map[string]bool{}

func TestRulesDoNotShadowPackages(t *testing.T) {
	g, _ := moduleGraph(t)
	for dir, pkg := range g.dirs {
		probe := dir + "/probe.go"
		if dir == "" {
			probe = "probe.go"
		}
		for _, r := range rules {
			if r.all || !r.matches(dir+"/probe.txt") && !r.matches(probe) {
				continue
			}
			if !packagesUnderARule[pkg] {
				t.Errorf("rule %s covers package %s; list it in packagesUnderARule if that is meant", r.pattern, pkg)
			}
		}
		c := g.classify(probe)
		if !c.all && (len(c.code) != 1 || c.code[0] != pkg) {
			t.Errorf("%s is not code of %s: %+v", probe, pkg, c)
		}
	}
}

func TestAGoFileUnderARuleIsCodeOfItsPackage(t *testing.T) {
	g, err := parse("m", "/r", []byte("m/tools/x\t/r/tools/x\tm\t\nm/tools/x.test\t/r/tools/x\tm\tm/tools/x\n"))
	if err != nil {
		t.Fatal(err)
	}
	if c := g.classify("tools/x/main.go"); len(c.code) != 1 || c.code[0] != "./tools/x" {
		t.Errorf("a Go file under tools/ classed as %+v", c)
	}
	if c := g.classify("tools/x/package.json"); len(c.code) != 0 || len(c.direct) == 0 {
		t.Errorf("a non-Go file under tools/ classed as %+v, want the tools/** rule", c)
	}
}
