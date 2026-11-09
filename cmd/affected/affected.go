package main

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"sort"
	"strings"
)

// A rule says what a changed path outside every package's own directory
// selects. Rules are checked in order, before package ownership, and the first
// that matches wins. A path no rule matches and no package owns selects
// everything: an input this table does not know about is an input it cannot
// rule out.
type rule struct {
	pattern string   // a path, a "dir/**" prefix, a "*.ext" at the root, or a path.Match pattern
	all     bool     // selects every package
	pkgs    []string // selects these (and the readers of every change)
	none    bool     // selects nothing, on purpose: no test reads it, or a job of its own does
	why     string
}

// machinery is how CI selects and runs the scoped jobs: the selector, the plan,
// the shard tables and the targets list. A change to one of them can change what
// every pull request runs, so it selects everything, and race, fuzz and the
// mutation suite (internal/mutation select_test.go keeps the same list) run in
// full on that pull request.
var machinery = []string{
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

// alsoRead adds to what a path's own package selects: packages whose tests read
// a file inside another package's directory at run time. They are selected
// alone, like any reader.
var alsoRead = []rule{
	{pattern: "internal/equivalence/traces/**", pkgs: []string{"./internal/bundled"}, why: "internal/bundled replays the recorded traces"},
}

// readers are the packages whose tests read the tree itself at run time
// (every .go file, every mutated file), so any change that is not a pure
// documentation or editor file selects them.
var readers = []string{"./internal/check", "./internal/mutation"}

var rules = append(machineryRules(), []rule{
	{pattern: "go.mod", all: true, why: "the module's requirements"},
	{pattern: "go.sum", all: true, why: "the module's requirements"},

	// Read at run time by tests outside their own directory. go list cannot
	// see these; each list names the packages that open the path.
	{pattern: "docs/**", pkgs: []string{"./internal/check", "./internal/runtime", "./test/behavior/cloudflare", "./test/behavior/vercel"}, why: "read by tests"},
	{pattern: "specs/**", pkgs: []string{"./cmd/mirror", "./cmd/mirrorgen", "./internal/check", "./internal/specboot", "./test/behavior/vercel"}, why: "read by tests and by mirrorgen"},
	{pattern: "scripts/**", pkgs: []string{"./internal/check", "./test/behavior/vercel", "./test/ci", "./test/release"}, why: "scripts the tests run"},
	{pattern: ".github/workflows/**", pkgs: []string{"./internal/execution", "./test/ci", "./test/release"}, why: "read by tests"},
	{pattern: "test/tiers.json", pkgs: []string{"./test/ci"}, why: "read by test/ci"},
	{pattern: "profiles/**", pkgs: []string{"./test/ci"}, why: "read by test/ci"},
	{pattern: "ratchet.json", pkgs: []string{"./internal/check"}, why: "the ratchet baseline"},
	{pattern: "known-red.json", pkgs: []string{"./internal/check"}, why: "the known-red declaration"},
	{pattern: "LICENSE", pkgs: []string{"./test/release"}, why: "packed into the release archive"},
	{pattern: "tools/**", pkgs: []string{"./internal/execution", "./internal/runtime", "./test/behavior/cloudflare"}, why: "pinned runtimes the tests drive"},
	{pattern: "test/vercel-clients/**", pkgs: []string{"./test/behavior/vercel"}, why: "clients the Vercel suite runs"},

	// Built and tested by jobs of their own, which the CI plan path-gates:
	// test/sdk/go is a separate module (the unit job's sdk step), and the
	// Dockerfiles are the image job's.
	{pattern: "test/sdk/**", none: true, why: "its own module, run whole by the unit job"},
	{pattern: "Dockerfile", none: true, why: "the image job's (its gate)"},
	{pattern: "Dockerfile.release", none: true, why: "the image job's (its gate)"},
	{pattern: ".dockerignore", none: true, why: "the image job's (its gate)"},

	// Read by no test.
	{pattern: "*.md", none: true, why: "documentation at the root"},
	{pattern: ".cursor/**", none: true, why: "editor configuration"},
	{pattern: ".github/ISSUE_TEMPLATE/**", none: true, why: "GitHub metadata"},
	{pattern: ".github/pull_request_template.md", none: true, why: "GitHub metadata"},
	{pattern: ".github/CODEOWNERS", none: true, why: "GitHub metadata"},
	{pattern: ".gitignore", none: true, why: "git metadata"},
	{pattern: "oxlint.config.ts", none: true, why: "editor lint configuration"},
}...)

func machineryRules() []rule {
	out := make([]rule, len(machinery))
	for i, m := range machinery {
		out[i] = rule{pattern: m, all: true, why: "how CI selects and runs the scoped jobs"}
	}
	return out
}

func (r rule) matches(p string) bool {
	switch {
	case strings.HasSuffix(r.pattern, "/**"):
		return strings.HasPrefix(p, strings.TrimSuffix(r.pattern, "**"))
	case strings.HasPrefix(r.pattern, "*"):
		return !strings.Contains(p, "/") && strings.HasSuffix(p, r.pattern[1:])
	case strings.Contains(r.pattern, "*"):
		ok, _ := path.Match(r.pattern, p)
		return ok
	default:
		return p == r.pattern
	}
}

// graph is the module as go list sees it with its tests.
type graph struct {
	pkgs []string                   // every package, as "./dir", sorted
	dirs map[string]string          // "dir" (slash, from the root; "" for the root) -> "./dir"
	deps map[string][]string        // "./dir" -> every module package its test binaries link
	root string                     // where to look for a path no package or rule places
	gate map[string]map[string]bool // gate name -> every module package its suite compiles, with its tags
}

// load runs go list in root. -test lists each package's test binary (p.test)
// with everything it links, which is the set of packages whose change can
// change that package's test result.
func load(root string) (*graph, error) {
	// go list prints absolute, symlink-free directories; root has to be the
	// same kind of path for a file to find the package that holds it.
	abs, err := filepath.Abs(root)
	if err != nil {
		return nil, err
	}
	if root, err = filepath.EvalSymlinks(abs); err != nil {
		return nil, err
	}
	cmd := exec.Command("go", "list", "-e", "-test", "-f", "{{.ImportPath}}\t{{.Dir}}\t{{.Module}}\t{{join .Deps \" \"}}", "./...")
	cmd.Dir = root
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("go list: %v\n%s", err, stderr.String())
	}
	modCmd := exec.Command("go", "list", "-m")
	modCmd.Dir = root
	mod, err := modCmd.Output()
	if err != nil {
		return nil, fmt.Errorf("go list -m: %v", err)
	}
	module := strings.TrimSpace(string(mod))
	g, err := parse(module, root, out)
	if err != nil {
		return nil, err
	}
	// What each gated job compiles, with the build tags it uses: a file built
	// only with -tags miniflare can import a package the plain graph never
	// links.
	g.gate = map[string]map[string]bool{}
	for name, gt := range gates {
		args := []string{"list", "-e", "-deps", "-test", "-tags", gt.tags, "-f", "{{.ImportPath}}"}
		cmd := exec.Command("go", append(args, gt.pkgs...)...)
		cmd.Dir = root
		cmd.Stderr = &stderr
		out, err := cmd.Output()
		if err != nil {
			return nil, fmt.Errorf("go list for %s: %v\n%s", name, err, stderr.String())
		}
		set := map[string]bool{}
		for _, ip := range strings.Fields(string(out)) {
			if strings.HasSuffix(ip, ".test") || strings.HasSuffix(ip, "]") {
				continue
			}
			if ip == module {
				set["."] = true
			} else if strings.HasPrefix(ip, module+"/") {
				set["./"+strings.TrimPrefix(ip, module+"/")] = true
			}
		}
		g.gate[name] = set
	}
	return g, nil
}

func parse(module, root string, listing []byte) (*graph, error) {
	g := &graph{dirs: map[string]string{}, deps: map[string][]string{}, root: root}
	rel := func(importPath string) (string, bool) {
		importPath, _, _ = strings.Cut(importPath, " ")
		if importPath == module {
			return ".", true
		}
		if !strings.HasPrefix(importPath, module+"/") {
			return "", false
		}
		return "./" + strings.TrimPrefix(importPath, module+"/"), true
	}
	sc := bufio.NewScanner(bytes.NewReader(listing))
	sc.Buffer(make([]byte, 1<<20), 1<<26)
	for sc.Scan() {
		f := strings.Split(sc.Text(), "\t")
		if len(f) != 4 {
			return nil, fmt.Errorf("unexpected go list line %q", sc.Text())
		}
		ip, dir, deps := f[0], f[1], f[3]
		switch {
		case strings.HasSuffix(ip, ".test"):
			p, ok := rel(strings.TrimSuffix(ip, ".test"))
			if !ok {
				continue
			}
			for _, d := range strings.Fields(deps) {
				if r, ok := rel(d); ok && !strings.HasSuffix(d, ".test") {
					g.deps[p] = append(g.deps[p], r)
				}
			}
		case !strings.Contains(ip, " "):
			p, ok := rel(ip)
			if !ok {
				continue
			}
			d := strings.TrimPrefix(strings.TrimPrefix(dir, root), "/")
			g.dirs[d] = p
			g.pkgs = append(g.pkgs, p)
		}
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	sort.Strings(g.pkgs)
	if len(g.pkgs) == 0 {
		return nil, fmt.Errorf("go list found no packages")
	}
	return g, nil
}

// owner is the package whose directory holds p: its source, its embedded
// files and its testdata all sit at or below it. The nearest such directory
// wins, so a file of a nested package belongs to that package.
func (g *graph) owner(p string) (string, bool) {
	for dir := path.Dir(p); ; dir = path.Dir(dir) {
		key := dir
		if key == "." {
			key = ""
		}
		if pkg, ok := g.dirs[key]; ok {
			return pkg, true
		}
		if dir == "." || dir == "/" {
			return "", false
		}
	}
}

// classification is what one changed path selects, and why. code is a
// package the path is compiled into (its own files and embedded files), so
// every package whose tests link it is selected too; direct is a package whose
// tests read the path at run time, which selects that package alone.
//
// self is a package whose own test binary alone the path is in (a _test.go
// file, testdata): its tests changed, so race and fuzz run it, but no other
// package links it.
type classification struct {
	all    bool
	code   []string
	self   []string
	direct []string
	why    string
}

func (g *graph) classify(p string) classification {
	// git quotes a name holding a byte it will not print; such a name is not
	// a path here, so nothing can place it.
	if strings.HasPrefix(p, "\"") {
		return classification{all: true, why: "a quoted name, which cannot be placed"}
	}
	for _, r := range rules {
		if r.matches(p) {
			if r.all {
				return classification{all: true, why: r.why}
			}
			c := classification{why: r.why}
			if len(r.pkgs) > 0 {
				c.direct = append(append([]string(nil), r.pkgs...), readers...)
			}
			return c
		}
	}
	var also []string
	for _, r := range alsoRead {
		if r.matches(p) {
			also = append(also, r.pkgs...)
		}
	}
	if pkg, ok := g.owner(p); ok {
		// A test file, and anything under testdata, is in that package's test
		// binary alone: no other package can link it.
		if strings.HasSuffix(p, "_test.go") || strings.Contains("/"+p, "/testdata/") {
			return classification{self: []string{pkg}, direct: append(also, readers...), why: "a test input of " + pkg}
		}
		return classification{code: []string{pkg}, direct: append(also, readers...), why: "in " + pkg}
	}
	// A file that is gone and that no package holds now is not compiled into
	// anything: a package that imported the package it was in has to change
	// its own imports, and is selected by that change. A path that still
	// exists here, or a root we cannot read, stays unknown.
	if g.root != "" {
		if _, err := os.Lstat(filepath.Join(g.root, filepath.FromSlash(p))); errors.Is(err, fs.ErrNotExist) {
			return classification{why: "deleted, and no package holds it"}
		}
	}
	return classification{all: true, why: "no rule and no package owns it"}
}

// result is a selection: every package whose tests a change can reach.
type result struct {
	all     bool
	reason  string
	pkgs    []string
	linked  map[string]bool // selected because their tests link changed code, or are it
	changed map[string]bool // packages whose own files changed: code and tests
}

func (g *graph) selectFor(changed []string) result {
	code, self, direct := map[string]bool{}, map[string]bool{}, map[string]bool{}
	for _, p := range changed {
		p = strings.TrimPrefix(path.Clean(p), "./")
		if p == "" || p == "." {
			continue
		}
		c := g.classify(p)
		if c.all {
			return result{all: true, reason: p + ": " + c.why, pkgs: g.pkgs}
		}
		for _, pkg := range c.code {
			code[pkg] = true
		}
		for _, pkg := range c.self {
			self[pkg] = true
		}
		for _, pkg := range c.direct {
			direct[pkg] = true
		}
	}
	known := map[string]bool{}
	for _, p := range g.pkgs {
		known[p] = true
	}
	selected := map[string]bool{}
	for _, set := range []map[string]bool{code, self, direct} {
		for p := range set {
			if known[p] {
				selected[p] = true
			}
		}
	}
	linked, own := map[string]bool{}, map[string]bool{}
	for _, set := range []map[string]bool{code, self} {
		for p := range set {
			if known[p] {
				linked[p], own[p] = true, true
			}
		}
	}
	for p, deps := range g.deps {
		for _, d := range deps {
			if code[d] {
				selected[p], linked[p] = true, true
				break
			}
		}
	}
	out := make([]string, 0, len(selected))
	for p := range selected {
		out = append(out, p)
	}
	sort.Strings(out)
	return result{pkgs: out, linked: linked, changed: own}
}

// A gate is a CI job that runs only when its inputs change: paths it reads
// itself; any package its suite compiles (go list -deps -test of pkgs, with
// tags) whose own files changed; and pkgs themselves when anything selects
// them, a test that reads a changed file included. A selection of everything,
// and the CI machinery, open every gate.
type gate struct {
	paths []string
	pkgs  []string
	tags  string
}

var gates = map[string]gate{
	// Regenerates internal/generated from the pinned specs and diffs it.
	"generated-models": {
		paths: []string{"specs/**", "internal/generated/**", "cmd/mirrorgen/**", "scripts/specs-sync.sh", "scripts/resolve_specs.py"},
		pkgs:  []string{"./cmd/mirrorgen"},
	},
	// Builds the Dockerfile and Dockerfile.release around ./cmd/mirror.
	"image": {
		paths: []string{"Dockerfile", "Dockerfile.release", ".dockerignore", "profiles/**", "scripts/slim-overlay.py"},
		pkgs:  []string{"./cmd/mirror"},
	},
	// The live miniflare suite: internal/execution and test/behavior/cloudflare
	// built with -tags miniflare, against the pinned runtime under tools/.
	"cloudflare-miniflare": {
		paths: []string{"tools/cloudflare-runtime/**", "internal/execution/*miniflare*", "test/behavior/cloudflare/**"},
		pkgs:  []string{"./internal/execution", "./test/behavior/cloudflare"},
		tags:  "miniflare",
	},
}

// linkedList is the selected packages whose test binaries link changed code:
// every package when everything is selected.
func (r result) linkedList() []string {
	out := []string{}
	for _, p := range r.pkgs {
		if r.all || r.linked[p] {
			out = append(out, p)
		}
	}
	return out
}

func (g *graph) gates(r result, changed []string) map[string]bool {
	selected := map[string]bool{}
	for _, p := range r.pkgs {
		selected[p] = true
	}
	out := map[string]bool{}
	for name, gt := range gates {
		open := r.all
		for _, p := range gt.pkgs {
			open = open || selected[p]
		}
		for p := range r.changed {
			open = open || g.gate[name][p]
		}
		for _, c := range changed {
			for _, pattern := range gt.paths {
				open = open || rule{pattern: pattern}.matches(strings.TrimPrefix(path.Clean(c), "./"))
			}
		}
		out[name] = open
	}
	return out
}
