package mutation

import (
	"bufio"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
)

// Which mutants a run selects, and how a selection is split across shards.
//
// With no filter set, every mutant is selected: that is what main runs. A pull
// request passes the files it changed (MUTATION_DIFF, from `git diff -U0`, or
// MUTATION_FILES), and a mutant is selected when
//
//   - its file changed, so no mutant on a changed line is ever skipped;
//   - any file of the package it runs changed (its source, tests, embedded
//     files or testdata), or a test helper its tests import (a package only
//     test files import, such as internal/spitest);
//   - its own entry in this table changed (needs MUTATION_DIFF for the lines;
//     with MUTATION_FILES alone a change to this package selects everything);
//   - or the diff touches what every mutant depends on: go.mod, go.sum, the CI
//     machinery (machinery, below), a name the diff quotes or a header it
//     cannot read, or a
//     declaration of this package other than the table, an init that appends
//     to it (which selects the entries it appends) and the imports: the
//     harness.
//
// MUTATION_PKGS and MUTATION_NAMES select by package and by name, for running
// part of the table locally. Filters add up; an empty selection under a filter
// is a pass, because a pull request that changes no mutated file has nothing
// for this suite to say. Without a filter, an empty shard is still a failure.

// selection is what the environment asks for.
type selection struct {
	all     bool
	reason  string // why everything, for the log
	files   map[string]bool
	lines   map[string][][2]int // changed new-side lines of this package's files
	pkgs    map[string]bool
	names   map[string]bool
	filters bool
}

func splitList(s string) []string {
	return strings.FieldsFunc(s, func(r rune) bool { return r == ',' || r == ' ' || r == '\n' || r == '\t' })
}

func readSelection(t *testing.T) selection {
	t.Helper()
	sel := selection{files: map[string]bool{}, lines: map[string][][2]int{}, pkgs: map[string]bool{}, names: map[string]bool{}}
	if path := os.Getenv("MUTATION_DIFF"); path != "" {
		sel.filters = true
		f, err := os.Open(path)
		if err != nil {
			t.Fatalf("MUTATION_DIFF: %v", err)
		}
		files, lines, err := parseDiff(bufio.NewScanner(f))
		f.Close()
		if err != nil {
			// A diff this cannot read, or a name it cannot place, could hold
			// any change: run everything rather than guess.
			sel.all, sel.reason = true, fmt.Sprintf("MUTATION_DIFF: %v", err)
		}
		for _, name := range files {
			sel.files[name] = true
		}
		for name, ranges := range lines {
			if isHarnessFile(name) {
				sel.lines[name] = ranges
			}
		}
	}
	if v, has := os.LookupEnv("MUTATION_FILES"); has {
		sel.filters = true
		for _, name := range splitList(v) {
			sel.files[filepath.ToSlash(filepath.Clean(name))] = true
		}
	}
	if v, has := os.LookupEnv("MUTATION_PKGS"); has {
		sel.filters = true
		for _, p := range splitList(v) {
			sel.pkgs["./"+strings.TrimPrefix(filepath.ToSlash(filepath.Clean(p)), "./")] = true
		}
	}
	if v, has := os.LookupEnv("MUTATION_NAMES"); has {
		sel.filters = true
		for _, n := range splitList(v) {
			sel.names[n] = true
		}
	}
	if !sel.filters {
		sel.all, sel.reason = true, "no filter set"
	}
	return sel
}

func isHarnessFile(name string) bool {
	return strings.HasPrefix(name, "internal/mutation/")
}

// parseDiff reads `git diff --unified=0` output: the files it touches (both
// sides of a rename, a deleted file by its old name, and a binary file, which
// has no ---/+++ lines) and, per file, the new-side lines each hunk changed. A
// hunk that only deletes is recorded as the line it sits after, as an empty
// range, so it is kept apart from lines that were written.
func parseDiff(sc *bufio.Scanner) ([]string, map[string][][2]int, error) {
	sc.Buffer(make([]byte, 1<<20), 1<<26)
	seen := map[string]bool{}
	var files []string
	add := func(name string) {
		if name != "" && name != "/dev/null" && !seen[name] {
			seen[name] = true
			files = append(files, name)
		}
	}
	count := func(spec string) (int, int, error) {
		start, n := spec, "1"
		if i := strings.IndexByte(spec, ','); i >= 0 {
			start, n = spec[:i], spec[i+1:]
		}
		s, err1 := strconv.Atoi(start)
		c, err2 := strconv.Atoi(n)
		if err1 != nil || err2 != nil {
			return 0, 0, fmt.Errorf("unreadable range %q", spec)
		}
		return s, c, nil
	}
	lines := map[string][][2]int{}
	current := ""
	oldLeft, newLeft := 0, 0
	for sc.Scan() {
		line := sc.Text()
		// Inside a hunk every line is content, whatever it starts with: a
		// deleted Markdown rule reads "----".
		if oldLeft > 0 || newLeft > 0 {
			switch {
			case strings.HasPrefix(line, "-"):
				oldLeft--
			case strings.HasPrefix(line, "+"):
				newLeft--
			case strings.HasPrefix(line, " "):
				oldLeft--
				newLeft--
			}
			continue
		}
		switch {
		case strings.HasPrefix(line, "diff --git "):
			current = ""
			// git quotes a name holding a byte it will not print, even with
			// core.quotePath=false (a tab, a newline, a quote). Such a name, or
			// a header that does not split into a/X b/X, cannot be placed.
			rest := strings.TrimPrefix(line, "diff --git ")
			i := strings.Index(rest, " b/")
			if strings.Contains(rest, `"`) || !strings.HasPrefix(rest, "a/") || i < 0 || strings.Count(rest, " b/") != 1 {
				return nil, nil, fmt.Errorf("a file header this cannot place: %q", line)
			}
			add(rest[2:i])
			add(rest[i+3:])
		case strings.HasPrefix(line, "--- "), strings.HasPrefix(line, "+++ "):
			// git ends a name holding a space with a tab here.
			name := strings.TrimSuffix(line[4:], "\t")
			if strings.HasPrefix(name, `"`) {
				return nil, nil, fmt.Errorf("a quoted name this cannot place: %q", line)
			}
			if strings.HasPrefix(line, "--- ") {
				add(strings.TrimPrefix(name, "a/"))
				continue
			}
			current = ""
			if name != "/dev/null" {
				current = strings.TrimPrefix(name, "b/")
				add(current)
			}
		case strings.HasPrefix(line, "@@ "):
			fields := strings.Fields(line)
			if len(fields) < 3 || !strings.HasPrefix(fields[1], "-") || !strings.HasPrefix(fields[2], "+") {
				return nil, nil, fmt.Errorf("unreadable hunk header %q", line)
			}
			_, o, err := count(fields[1][1:])
			if err != nil {
				return nil, nil, err
			}
			s, n, err := count(fields[2][1:])
			if err != nil {
				return nil, nil, err
			}
			oldLeft, newLeft = o, n
			if current == "" {
				continue
			}
			if n == 0 {
				lines[current] = append(lines[current], [2]int{s, s - 1})
			} else {
				lines[current] = append(lines[current], [2]int{s, s + n - 1})
			}
		}
	}
	return files, lines, sc.Err()
}

// entry is where one mutant's literal sits in this package's source.
type entry struct {
	name       string
	start, end int
}

// region is a top-level declaration of this package's source that is not the
// table itself. A change inside an init function that appends to the table
// (group) can change every entry it appends, through the locals they share; a
// change in any other declaration (harness) can change every mutant.
type region struct {
	start, end int
	harness    bool
	group      []string // the names of the entries an init appends
}

var (
	entriesOnce sync.Once
	entriesErr  error
	entries     map[string][]entry  // file (slash, from the root) -> literals, in order
	regions     map[string][]region // file -> declarations other than the table and imports
)

// tableEntries finds every composite literal in this package that has a
// `name:` string field, which is what a mutant entry is, and the declarations
// around them.
func tableEntries(root string) (map[string][]entry, error) {
	entriesOnce.Do(func() {
		entries, regions = map[string][]entry{}, map[string][]region{}
		names, err := filepath.Glob(filepath.Join(root, "internal", "mutation", "*.go"))
		if err != nil {
			entriesErr = err
			return
		}
		fset := token.NewFileSet()
		for _, path := range names {
			f, err := parser.ParseFile(fset, path, nil, 0)
			if err != nil {
				entriesErr = err
				return
			}
			rel := "internal/mutation/" + filepath.Base(path)
			find := func(root ast.Node) []entry {
				var found []entry
				ast.Inspect(root, func(n ast.Node) bool {
					lit, ok := n.(*ast.CompositeLit)
					if !ok {
						return true
					}
					for _, el := range lit.Elts {
						kv, ok := el.(*ast.KeyValueExpr)
						if !ok {
							continue
						}
						if key, ok := kv.Key.(*ast.Ident); !ok || key.Name != "name" {
							continue
						}
						if v, ok := kv.Value.(*ast.BasicLit); ok && v.Kind == token.STRING {
							if name, err := strconv.Unquote(v.Value); err == nil {
								found = append(found, entry{name, fset.Position(lit.Lbrace).Line, fset.Position(lit.Rbrace).Line})
							}
						}
						return false
					}
					return true
				})
				return found
			}
			for _, decl := range f.Decls {
				start, end := fset.Position(decl.Pos()).Line, fset.Position(decl.End()).Line
				found := find(decl)
				entries[rel] = append(entries[rel], found...)
				switch d := decl.(type) {
				case *ast.GenDecl:
					if d.Tok == token.IMPORT {
						continue // an import changes nothing until code uses it
					}
					if isTable(d) {
						continue // its lines outside every entry are braces
					}
				case *ast.FuncDecl:
					if d.Name.Name == "init" && d.Recv == nil && len(found) > 0 {
						var group []string
						for _, e := range found {
							group = append(group, e.name)
						}
						regions[rel] = append(regions[rel], region{start: start, end: end, group: group})
						continue
					}
				}
				regions[rel] = append(regions[rel], region{start: start, end: end, harness: true})
			}
		}
	})
	return entries, entriesErr
}

// isTable reports whether d is `var mutants = []mutant{...}`.
func isTable(d *ast.GenDecl) bool {
	if d.Tok != token.VAR || len(d.Specs) != 1 {
		return false
	}
	v, ok := d.Specs[0].(*ast.ValueSpec)
	return ok && len(v.Names) == 1 && v.Names[0].Name == "mutants"
}

// entriesFor says which mutants a change to the table's own source selects,
// and whether it touched the harness instead (which selects all of them). A
// line inside an entry selects that entry; inside an init that appends
// entries, all of those; inside any other declaration but an import, every
// mutant. A line outside every declaration (a comment, a blank) and the
// braces of the table select nothing, and so does deleting lines that held
// only entries: a deleted mutant cannot run.
func entriesFor(root string, sel selection) (map[string]bool, string, error) {
	table, err := tableEntries(root)
	if err != nil {
		return nil, "", err
	}
	picked := map[string]bool{}
	for file, ranges := range sel.lines {
		for _, r := range ranges {
			lines := [][2]int{{r[0], r[1]}}
			if r[1] < r[0] { // only deleted lines: the place they were
				lines = [][2]int{{r[0], r[0]}}
			}
			for line := lines[0][0]; line <= lines[0][1]; line++ {
				inEntry := false
				for _, e := range table[file] {
					if line >= e.start && line <= e.end {
						inEntry = true
						if r[1] >= r[0] {
							picked[e.name] = true
						}
					}
				}
				if inEntry {
					continue
				}
				for _, g := range regions[file] {
					if line < g.start || line > g.end {
						continue
					}
					if g.harness {
						return nil, fmt.Sprintf("%s:%d is harness code, not a mutant entry", file, line), nil
					}
					for _, name := range g.group {
						picked[name] = true
					}
				}
			}
		}
	}
	return picked, "", nil
}

// selectMutants applies sel to the table and returns the indices it keeps.
func selectMutants(root string, sel selection) ([]int, string, error) {
	all := func(reason string) ([]int, string, error) {
		out := make([]int, len(mutants))
		for i := range mutants {
			out[i] = i
		}
		return out, reason, nil
	}
	if sel.all {
		return all(sel.reason)
	}
	for f := range sel.files {
		if isMachinery(f) {
			return all(f + " changed")
		}
	}
	byEntry := map[string]bool{}
	harnessFiles := false
	for f := range sel.files {
		if isHarnessFile(f) {
			harnessFiles = true
		}
	}
	if harnessFiles {
		if len(sel.lines) == 0 {
			return all("internal/mutation changed and no diff says which lines")
		}
		picked, reason, err := entriesFor(root, sel)
		if err != nil || reason != "" {
			if err != nil {
				return nil, "", err
			}
			return all(reason)
		}
		byEntry = picked
	}
	// A change anywhere in a package (its source, its tests, what it embeds,
	// its testdata) can move whether that package's tests still kill each of
	// its mutants, so it selects all of them.
	dirs := map[string]bool{}
	for f := range sel.files {
		dirs[pkgDir(f)] = true
	}
	// A test helper's change reaches the mutants of every package whose tests
	// import it.
	helped := map[string]bool{}
	if len(dirs) > 0 {
		helpers, err := testHelpers(root)
		if err != nil {
			return nil, "", err
		}
		for dir := range dirs {
			for _, pkg := range helpers[dir] {
				helped[pkg] = true
			}
		}
	}
	var out []int
	for i, m := range mutants {
		file := filepath.ToSlash(m.file)
		dir := strings.TrimPrefix(m.pkg, "./")
		if sel.files[file] || dirs[dir] || helped[m.pkg] || sel.pkgs[m.pkg] || sel.names[m.name] || byEntry[m.name] {
			out = append(out, i)
		}
	}
	return out, "", nil
}

// machinery is what every mutant depends on beyond its package: the module's
// requirements, and how CI selects and runs the scoped jobs (cmd/affected's
// list of the same name). A change to any of it runs the whole table.
var machinery = []string{
	"go.mod",
	"go.sum",
	"Makefile",
	".github/workflows/ci.yml",
	".github/actions/**",
	"scripts/fuzz.sh",
	"scripts/fuzz-targets.txt",
	"scripts/shard.py",
	"scripts/ci-plan.py",
	"test/race-costs.json",
	"cmd/affected/**",
}

func isMachinery(f string) bool {
	for _, m := range machinery {
		if prefix, ok := strings.CutSuffix(m, "**"); ok && strings.HasPrefix(f, prefix) || f == m {
			return true
		}
	}
	return false
}

// pkgDir is the directory of the package a changed file belongs to: its own,
// or for anything under testdata, the one that holds testdata.
func pkgDir(f string) string {
	if i := strings.Index("/"+f, "/testdata/"); i >= 0 {
		if i == 0 {
			return "."
		}
		return f[:i-1]
	}
	return path.Dir(f)
}

var (
	helpersOnce sync.Once
	helpersMap  map[string][]string
	helpersErr  error
)

// testHelpers maps the directory of each test helper (a module package that
// only test files import, such as internal/spitest) to the packages, as ./dir,
// whose tests import it.
func testHelpers(root string) (map[string][]string, error) {
	helpersOnce.Do(func() {
		cmd := exec.Command("go", "list", "-e", "-f", "{{.ImportPath}}\t{{.Dir}}\t{{join .Imports \" \"}}\t{{join .TestImports \" \"}} {{join .XTestImports \" \"}}", "./...")
		cmd.Dir = root
		out, err := cmd.Output()
		if err != nil {
			helpersErr = fmt.Errorf("go list: %v", err)
			return
		}
		modCmd := exec.Command("go", "list", "-m")
		modCmd.Dir = root
		mod, err := modCmd.Output()
		if err != nil {
			helpersErr = fmt.Errorf("go list -m: %v", err)
			return
		}
		module := strings.TrimSpace(string(mod)) + "/"
		type pkg struct{ dir, rel string }
		byPath := map[string]pkg{}
		imported := map[string]bool{}
		testImports := map[string][]string{} // import path -> packages whose tests import it
		for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
			f := strings.Split(line, "\t")
			if len(f) != 4 || !strings.HasPrefix(f[0], module) {
				continue
			}
			rel := "./" + strings.TrimPrefix(f[0], module)
			d, _ := filepath.Rel(root, f[1])
			byPath[f[0]] = pkg{filepath.ToSlash(d), rel}
			for _, ip := range strings.Fields(f[2]) {
				imported[ip] = true
			}
			seen := map[string]bool{}
			for _, ip := range strings.Fields(f[3]) {
				if !seen[ip] { // a test and an external test can both import it
					seen[ip] = true
					testImports[ip] = append(testImports[ip], rel)
				}
			}
		}
		helpersMap = map[string][]string{}
		for ip, users := range testImports {
			if p, ok := byPath[ip]; ok && !imported[ip] {
				helpersMap[p.dir] = users
			}
		}
	})
	return helpersMap, helpersErr
}

// cost is the measured CPU seconds one mutant of pkg takes: the compile and
// link of the mutated package's test binary plus the test it runs. See
// mutantCost in costs_test.go.
func cost(pkg string) float64 {
	if c, ok := mutantCost[pkg]; ok {
		return c
	}
	return defaultMutantCost
}

// assign splits the selected mutants into count shards by cost: the most
// expensive first, each to the shard with the least so far, ties to the lower
// index. It is deterministic, so every job of a matrix computes the same split.
func assign(selected []int, count int) [][]int {
	order := append([]int(nil), selected...)
	sort.SliceStable(order, func(a, b int) bool {
		ca, cb := cost(mutants[order[a]].pkg), cost(mutants[order[b]].pkg)
		if ca != cb {
			return ca > cb
		}
		return order[a] < order[b]
	})
	shards := make([][]int, count)
	load := make([]float64, count)
	for _, i := range order {
		best := 0
		for s := 1; s < count; s++ {
			if load[s] < load[best] {
				best = s
			}
		}
		shards[best] = append(shards[best], i)
		load[best] += cost(mutants[i].pkg)
	}
	for _, s := range shards {
		sort.Ints(s)
	}
	return shards
}

// TestMutationPlan writes what a pull request's diff selects, so CI can size
// the mutation matrix before it starts one: MUTATION_PLAN names the file, and
// the filters are the same ones TestMutantsAreKilled reads. Without
// MUTATION_PLAN it only checks the table parses into entries.
func TestMutationPlan(t *testing.T) {
	root := repoRoot(t)
	table, err := tableEntries(root)
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for _, list := range table {
		n += len(list)
	}
	if n != len(mutants) {
		t.Fatalf("found %d mutant entries in the source and %d in the table; a mutant whose name is not a string literal cannot be selected by the lines that declare it", n, len(mutants))
	}
	out := os.Getenv("MUTATION_PLAN")
	if out == "" {
		return
	}
	selected, reason, err := selectMutants(root, readSelection(t))
	if err != nil {
		t.Fatal(err)
	}
	total := 0.0
	for _, i := range selected {
		total += cost(mutants[i].pkg)
	}
	// One shard per shardBudget seconds of CPU, at most maxShards: the most a
	// full run uses.
	shards := int(total/shardBudget) + 1
	if shards > maxShards || !readSelection(t).filters {
		shards = maxShards // the full table: what main runs
	}
	if len(selected) == 0 {
		shards = 0
	}
	matrix := make([]int, shards)
	for i := range matrix {
		matrix[i] = i
	}
	plan, _ := json.Marshal(map[string]any{"selected": len(selected), "of": len(mutants), "cpu_seconds": int(total), "all_because": reason, "shards": matrix})
	if err := os.WriteFile(out, append(plan, '\n'), 0o644); err != nil {
		t.Fatal(err)
	}
	t.Logf("%s", plan)
}

const (
	maxShards   = 12
	shardBudget = 2400.0 // CPU seconds: about ten minutes on a four-core runner
)

func repoRoot(t *testing.T) string {
	t.Helper()
	wd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	return filepath.Clean(filepath.Join(wd, "..", ".."))
}

func selectDiff(t *testing.T, root, diff string) ([]int, string) {
	t.Helper()
	files, lines, err := parseDiff(bufio.NewScanner(strings.NewReader(diff)))
	if err != nil {
		t.Fatal(err)
	}
	sel := selection{filters: true, files: map[string]bool{}, lines: map[string][][2]int{}}
	for _, f := range files {
		sel.files[f] = true
	}
	for f, r := range lines {
		if isHarnessFile(f) {
			sel.lines[f] = r
		}
	}
	got, reason, err := selectMutants(root, sel)
	if err != nil {
		t.Fatal(err)
	}
	return got, reason
}

func hunk(file string, line int) string {
	return fmt.Sprintf("diff --git a/%s b/%s\n--- a/%s\n+++ b/%s\n@@ -%d +%d @@\n-old\n+new\n", file, file, file, file, line, line)
}

// The promise a pull request relies on: a diff that changes the line a mutant
// rewrites selects that mutant. Every mutant is checked against a one-line
// diff at its own needle.
func TestNoMutantOnAChangedLineIsSkipped(t *testing.T) {
	root := repoRoot(t)
	bodies := map[string]string{}
	for i, m := range mutants {
		body, ok := bodies[m.file]
		if !ok {
			raw, err := os.ReadFile(filepath.Join(root, m.file))
			if err != nil {
				t.Fatal(err)
			}
			body = string(raw)
			bodies[m.file] = body
		}
		at := strings.Index(body, m.old)
		if at < 0 {
			continue // TestMutantNeedlesExist reports it
		}
		line := strings.Count(body[:at], "\n") + 1
		got, _ := selectDiff(t, root, hunk(filepath.ToSlash(m.file), line))
		found := false
		for _, j := range got {
			found = found || j == i
		}
		if !found {
			t.Errorf("%s: a change to %s:%d does not select it", m.name, m.file, line)
		}
	}
}

func TestADiffSelectsWhatItTouches(t *testing.T) {
	root := repoRoot(t)
	if got, _ := selectDiff(t, root, hunk("docs/TESTING.md", 3)+hunk("README.md", 1)); len(got) != 0 {
		t.Errorf("a documentation diff selected %d mutants", len(got))
	}
	// A deleted Markdown rule inside a hunk is content, not a file header.
	if got, _ := selectDiff(t, root, "diff --git a/docs/X.md b/docs/X.md\n--- a/docs/X.md\n+++ b/docs/X.md\n@@ -3,2 +3,0 @@\n---- a/internal/engine/page.go\n-x\n"); len(got) != 0 {
		t.Errorf("a deleted line read as a header selected %d mutants", len(got))
	}
	for _, f := range []string{"go.mod", "go.sum"} {
		if got, reason := selectDiff(t, root, hunk(f, 1)); len(got) != len(mutants) || reason == "" {
			t.Errorf("%s selected %d of %d", f, len(got), len(mutants))
		}
	}
	// A binary file has no ---/+++ lines; the diff --git line still names it.
	if got, _ := selectDiff(t, root, "diff --git a/go.sum b/go.sum\nBinary files a/go.sum and b/go.sum differ\n"); len(got) != len(mutants) {
		t.Errorf("a binary change to go.sum selected %d", len(got))
	}
	// A test file of a mutated package selects that package's mutants alone.
	got, _ := selectDiff(t, root, hunk("internal/engine/page_test.go", 1))
	if len(got) == 0 {
		t.Fatal("a test of internal/engine selected nothing")
	}
	for _, i := range got {
		if mutants[i].pkg != "./internal/engine" {
			t.Errorf("a test of internal/engine selected %s (%s)", mutants[i].name, mutants[i].pkg)
		}
	}

	table, err := tableEntries(root)
	if err != nil {
		t.Fatal(err)
	}
	e := table["internal/mutation/cursor_mutants_test.go"][0]
	got, _ = selectDiff(t, root, hunk("internal/mutation/cursor_mutants_test.go", e.start+1))
	if len(got) != 1 || mutants[got[0]].name != e.name {
		t.Errorf("an edit inside %s's entry selected %d mutants", e.name, len(got))
	}
	// Deleting an entry leaves nothing to run.
	del := "diff --git a/internal/mutation/cursor_mutants_test.go b/internal/mutation/cursor_mutants_test.go\n--- a/internal/mutation/cursor_mutants_test.go\n+++ b/internal/mutation/cursor_mutants_test.go\n" +
		fmt.Sprintf("@@ -%d,8 +%d,0 @@\n", e.end+1, e.end) + strings.Repeat("-gone\n", 8)
	if got, reason := selectDiff(t, root, del); len(got) != 0 {
		t.Errorf("deleting an entry selected %d (%s)", len(got), reason)
	}
	// The harness is every mutant's; an import alone is nobody's.
	raw, _ := os.ReadFile(filepath.Join(root, "internal", "mutation", "select_test.go"))
	at := strings.Count(string(raw[:strings.Index(string(raw), "\nfunc assign(")]), "\n") + 3
	if got, reason := selectDiff(t, root, hunk("internal/mutation/select_test.go", at)); len(got) != len(mutants) || reason == "" {
		t.Errorf("a harness change at line %d selected %d of %d", at, len(got), len(mutants))
	}
	if got, _ := selectDiff(t, root, hunk("internal/mutation/select_test.go", 4)); len(got) != 0 {
		t.Errorf("an import selected %d", len(got))
	}
	// A local of the init that appends the cursor mutants is every one of them.
	cursor, _ := os.ReadFile(filepath.Join(root, "internal", "mutation", "cursor_mutants_test.go"))
	at = strings.Count(string(cursor[:strings.Index(string(cursor), "\tengine := ")]), "\n") + 1
	got, _ = selectDiff(t, root, hunk("internal/mutation/cursor_mutants_test.go", at))
	if want := len(table["internal/mutation/cursor_mutants_test.go"]); len(got) != want {
		t.Errorf("a change to the init's locals selected %d, want its %d entries", len(got), want)
	}
	if _, _, err := parseDiff(bufio.NewScanner(strings.NewReader("diff --git a/x b/x\n+++ b/x\n@@ nonsense\n"))); err == nil {
		t.Error("an unreadable hunk header was accepted")
	}
}

func TestEnvironmentFilters(t *testing.T) {
	root := repoRoot(t)
	// A pull request's shard runs this package with its own MUTATION_DIFF set.
	t.Setenv("MUTATION_DIFF", "")
	t.Setenv("MUTATION_PKGS", "internal/engine")
	t.Setenv("MUTATION_NAMES", "s3-skip-write-preconditions")
	t.Setenv("MUTATION_FILES", "internal/mutation/mutation_test.go")
	sel := readSelection(t)
	if !sel.filters || sel.all || !sel.pkgs["./internal/engine"] {
		t.Fatalf("%+v", sel)
	}
	// MUTATION_FILES naming this package with no diff cannot say which lines.
	if got, reason := mustSelect(t, root, sel); len(got) != len(mutants) || reason == "" {
		t.Errorf("selected %d of %d (%s)", len(got), len(mutants), reason)
	}
	t.Setenv("MUTATION_FILES", "")
	got, _ := mustSelect(t, root, readSelection(t))
	for _, i := range got {
		if m := mutants[i]; m.pkg != "./internal/engine" && m.name != "s3-skip-write-preconditions" {
			t.Errorf("selected %s", m.name)
		}
	}
	diff := filepath.Join(t.TempDir(), "d")
	os.WriteFile(diff, []byte(hunk("internal/engine/page.go", 1)), 0o644)
	t.Setenv("MUTATION_DIFF", diff)
	if sel := readSelection(t); !sel.files["internal/engine/page.go"] {
		t.Errorf("MUTATION_DIFF was not read: %+v", sel)
	}
}

func mustSelect(t *testing.T, root string, sel selection) ([]int, string) {
	t.Helper()
	got, reason, err := selectMutants(root, sel)
	if err != nil {
		t.Fatal(err)
	}
	return got, reason
}

// A change anywhere in a mutated package selects every mutant of it, and a
// test helper selects the mutants of every package whose tests import it.
func TestAPackageChangeSelectsItsMutants(t *testing.T) {
	root := repoRoot(t)
	of := func(pkg string) int {
		n := 0
		for _, m := range mutants {
			if m.pkg == pkg {
				n++
			}
		}
		return n
	}
	for _, f := range []string{"internal/engine/a_new_file.go", "internal/engine/testdata/x.json", "internal/engine/page_test.go"} {
		got, _ := selectDiff(t, root, hunk(f, 1))
		if len(got) != of("./internal/engine") {
			t.Errorf("%s selected %d, want all %d of internal/engine's", f, len(got), of("./internal/engine"))
		}
	}
	helpers, err := testHelpers(root)
	if err != nil {
		t.Fatal(err)
	}
	users := helpers["internal/spitest"]
	if len(users) == 0 {
		t.Fatal("internal/spitest is not found as a test helper")
	}
	got, _ := selectDiff(t, root, hunk("internal/spitest/spitest.go", 1))
	want := 0
	for _, u := range users {
		want += of(u)
	}
	if want == 0 || len(got) != want {
		t.Errorf("internal/spitest selected %d mutants, want the %d of the packages whose tests import it", len(got), want)
	}
	// A production package imported by code is no helper.
	if _, ok := helpers["internal/engine"]; ok {
		t.Error("internal/engine counted as a test helper")
	}
}

// The machinery every mutant depends on selects them all, as cmd/affected
// selects every package for the same list.
func TestMachinerySelectsEveryMutant(t *testing.T) {
	root := repoRoot(t)
	for _, f := range []string{
		"go.mod", "go.sum", "Makefile", ".github/workflows/ci.yml", ".github/actions/go/action.yml",
		"scripts/fuzz.sh", "scripts/fuzz-targets.txt", "scripts/shard.py", "scripts/ci-plan.py",
		"test/race-costs.json", "cmd/affected/affected.go",
	} {
		if got, reason := selectDiff(t, root, hunk(f, 1)); len(got) != len(mutants) || reason == "" {
			t.Errorf("%s selected %d of %d", f, len(got), len(mutants))
		}
	}
}

// The same promises, read from what git itself writes, the way CI asks for it
// (core.quotePath=false, --no-renames): an edit at needles, a move, a name git
// still quotes, and a non-ASCII name.
func TestRealDiffsSelectWhatTheyTouch(t *testing.T) {
	root := repoRoot(t)
	repo := t.TempDir()
	git := func(args ...string) string {
		t.Helper()
		cmd := exec.Command("git", append([]string{"-c", "core.quotePath=false", "-c", "user.name=t", "-c", "user.email=t@t", "-c", "commit.gpgsign=false"}, args...)...)
		cmd.Dir = repo
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("git %v: %v\n%s", args, err, out)
		}
		return string(out)
	}
	files := map[string]bool{}
	for _, m := range mutants {
		files[filepath.ToSlash(m.file)] = true
	}
	var names []string
	for f := range files {
		names = append(names, f)
	}
	sort.Strings(names)
	for _, f := range names {
		body, err := os.ReadFile(filepath.Join(root, f))
		if err != nil {
			t.Fatal(err)
		}
		os.MkdirAll(filepath.Join(repo, filepath.Dir(f)), 0o755)
		os.WriteFile(filepath.Join(repo, f), body, 0o644)
	}
	git("init", "-q")
	git("add", ".")
	git("commit", "-qm", "base")
	diff := func() string { return git("diff", "--unified=0", "--no-color", "--no-renames", "HEAD~1", "HEAD") }

	// Every other mutated file edited at its first mutant's needle.
	edited := map[string]bool{}
	for i, f := range names {
		if i%2 != 0 {
			continue
		}
		p := filepath.Join(repo, f)
		body, _ := os.ReadFile(p)
		for _, m := range mutants {
			if filepath.ToSlash(m.file) == f {
				first, _, _ := strings.Cut(m.old, "\n")
				os.WriteFile(p, []byte(strings.Replace(string(body), first, first+" // edited", 1)), 0o644)
				break
			}
		}
		edited[f] = true
	}
	git("commit", "-qam", "edit needles")
	got, reason := selectDiff(t, root, diff())
	if reason != "" {
		t.Fatalf("an edit at needles selected everything: %s", reason)
	}
	picked := map[int]bool{}
	for _, i := range got {
		picked[i] = true
	}
	for i, m := range mutants {
		if edited[filepath.ToSlash(m.file)] && !picked[i] {
			t.Errorf("%s: its file was edited, and it was not selected", m.name)
		}
	}

	// A move: both sides are named, so the mutants of the old name run.
	moved := names[1]
	git("mv", moved, moved+".moved")
	git("commit", "-qm", "move")
	got, _ = selectDiff(t, root, diff())
	found := false
	for _, i := range got {
		found = found || filepath.ToSlash(mutants[i].file) == moved
	}
	if !found {
		t.Errorf("moving %s selected none of its mutants", moved)
	}

	// A non-ASCII name is printed as it is, and places in its package.
	os.WriteFile(filepath.Join(repo, "internal", "engine", "café.go"), []byte("package engine\n"), 0o644)
	git("add", ".")
	git("commit", "-qm", "non-ascii")
	if got, reason := selectDiff(t, root, diff()); reason != "" || len(got) == 0 || mutants[got[0]].pkg != "./internal/engine" {
		t.Errorf("a non-ASCII name in internal/engine selected %d (%s)", len(got), reason)
	}

	// A tab in a name: git quotes it whatever core.quotePath says, and a
	// quoted name selects everything.
	os.WriteFile(filepath.Join(repo, "internal", "engine", "tab\tname.go"), []byte("package engine\n"), 0o644)
	git("add", ".")
	git("commit", "-qm", "quoted")
	files2, _, err := parseDiff(bufio.NewScanner(strings.NewReader(diff())))
	if err == nil {
		t.Errorf("a quoted name was read as %v", files2)
	}
	path := filepath.Join(t.TempDir(), "pr.diff")
	os.WriteFile(path, []byte(diff()), 0o644)
	t.Setenv("MUTATION_DIFF", path)
	if sel := readSelection(t); !sel.all || sel.reason == "" {
		t.Errorf("MUTATION_DIFF with a quoted name did not select everything: %+v", sel.reason)
	}
}
