package main

import (
	"bufio"
	"bytes"
	"fmt"
	"os/exec"
	"path"
	"sort"
	"strings"
)

// A rule says what a changed path outside every package's own directory
// selects. Rules are checked in order, before package ownership, and the first
// that matches wins. A path no rule matches and no package owns selects
// everything: an input this table does not know about is an input it cannot
// rule out.
type rule struct {
	pattern string   // a path, a "dir/**" prefix, or a "*.ext" at the root
	all     bool     // selects every package
	pkgs    []string // selects these (and the readers of every change)
	why     string
}

// readers are the packages whose tests read the tree itself at run time
// (every .go file, every mutated file), so any change that is not a pure
// documentation or editor file selects them.
var readers = []string{"./internal/check", "./internal/mutation"}

var rules = []rule{
	{pattern: "go.mod", all: true, why: "the module's requirements"},
	{pattern: "go.sum", all: true, why: "the module's requirements"},

	// Read at run time by tests outside their own directory. go list cannot
	// see these; each list names the packages that open the path.
	{pattern: "docs/**", pkgs: []string{"./internal/check", "./internal/runtime", "./test/behavior/cloudflare", "./test/behavior/vercel"}, why: "read by tests"},
	{pattern: "specs/**", pkgs: []string{"./cmd/mirror", "./cmd/mirrorgen", "./internal/check", "./internal/specboot", "./test/behavior/vercel"}, why: "read by tests and by mirrorgen"},
	{pattern: "scripts/**", pkgs: []string{"./internal/check", "./test/behavior/vercel", "./test/ci", "./test/release"}, why: "scripts the tests run"},
	{pattern: "Makefile", pkgs: []string{"./internal/check", "./test/ci"}, why: "read by tests"},
	{pattern: ".github/workflows/**", pkgs: []string{"./internal/execution", "./test/ci", "./test/release"}, why: "read by tests"},
	{pattern: ".github/actions/**", pkgs: []string{"./test/ci"}, why: "the CI action"},
	{pattern: "test/tiers.json", pkgs: []string{"./test/ci"}, why: "read by test/ci"},
	{pattern: "test/race-costs.json", pkgs: []string{"./test/ci"}, why: "read by test/ci"},
	{pattern: "profiles/**", pkgs: []string{"./test/ci"}, why: "read by test/ci"},
	{pattern: "ratchet.json", pkgs: []string{"./internal/check"}, why: "the ratchet baseline"},
	{pattern: "known-red.json", pkgs: []string{"./internal/check"}, why: "the known-red declaration"},
	{pattern: "LICENSE", pkgs: []string{"./test/release"}, why: "packed into the release archive"},
	{pattern: "tools/**", pkgs: []string{"./internal/execution", "./internal/runtime", "./test/behavior/cloudflare"}, why: "pinned runtimes the tests drive"},
	{pattern: "test/vercel-clients/**", pkgs: []string{"./test/behavior/vercel"}, why: "clients the Vercel suite runs"},

	// Built and tested by jobs of their own, which the CI plan path-gates:
	// test/sdk/go is a separate module (the unit job's sdk step), and the
	// Dockerfiles are the image job's.
	{pattern: "test/sdk/**", why: "its own module, run whole by the unit job"},
	{pattern: "Dockerfile", why: "the image job's"},
	{pattern: "Dockerfile.release", why: "the image job's"},
	{pattern: ".dockerignore", why: "the image job's"},

	// Read by no test.
	{pattern: "*.md", why: "documentation at the root"},
	{pattern: ".cursor/**", why: "editor configuration"},
	{pattern: ".github/ISSUE_TEMPLATE/**", why: "GitHub metadata"},
	{pattern: ".github/pull_request_template.md", why: "GitHub metadata"},
	{pattern: ".github/CODEOWNERS", why: "GitHub metadata"},
	{pattern: ".gitignore", why: "git metadata"},
	{pattern: "oxlint.config.ts", why: "editor lint configuration"},
}

func (r rule) matches(p string) bool {
	switch {
	case strings.HasSuffix(r.pattern, "/**"):
		return strings.HasPrefix(p, strings.TrimSuffix(r.pattern, "**"))
	case strings.HasPrefix(r.pattern, "*"):
		return !strings.Contains(p, "/") && strings.HasSuffix(p, r.pattern[1:])
	default:
		return p == r.pattern
	}
}

// graph is the module as go list sees it with its tests.
type graph struct {
	pkgs []string            // every package, as "./dir", sorted
	dirs map[string]string   // "dir" (slash, from the root; "" for the root) -> "./dir"
	deps map[string][]string // "./dir" -> every module package its test binaries link
}

// load runs go list in root. -test lists each package's test binary (p.test)
// with everything it links, which is the set of packages whose change can
// change that package's test result.
func load(root string) (*graph, error) {
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
	return parse(module, root, out)
}

func parse(module, root string, listing []byte) (*graph, error) {
	g := &graph{dirs: map[string]string{}, deps: map[string][]string{}}
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
type classification struct {
	all    bool
	code   []string
	direct []string
	why    string
}

func (g *graph) classify(p string) classification {
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
	if pkg, ok := g.owner(p); ok {
		// A test file, and anything under testdata, is in that package's test
		// binary alone: no other package can link it.
		if strings.HasSuffix(p, "_test.go") || strings.Contains("/"+p, "/testdata/") {
			return classification{direct: append([]string{pkg}, readers...), why: "a test input of " + pkg}
		}
		return classification{code: []string{pkg}, direct: readers, why: "in " + pkg}
	}
	return classification{all: true, why: "no rule and no package owns it"}
}

// result is a selection: every package whose tests a change can reach.
type result struct {
	all    bool
	reason string
	pkgs   []string
}

func (g *graph) selectFor(changed []string) result {
	code, direct := map[string]bool{}, map[string]bool{}
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
		for _, pkg := range c.direct {
			direct[pkg] = true
		}
	}
	known := map[string]bool{}
	for _, p := range g.pkgs {
		known[p] = true
	}
	selected := map[string]bool{}
	for _, set := range []map[string]bool{code, direct} {
		for p := range set {
			if known[p] {
				selected[p] = true
			}
		}
	}
	for p, deps := range g.deps {
		for _, d := range deps {
			if code[d] {
				selected[p] = true
				break
			}
		}
	}
	out := make([]string, 0, len(selected))
	for p := range selected {
		out = append(out, p)
	}
	sort.Strings(out)
	return result{pkgs: out}
}
