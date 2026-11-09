// Package ci checks the two scripts CI leans on for speed and for not going
// quietly green.
package ci

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func run(t *testing.T, dir string, env []string, name string, args ...string) (string, error) {
	t.Helper()
	cmd := exec.Command(name, args...)
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), env...)
	out, err := cmd.CombinedOutput()
	return string(out), err
}

// restore-mtimes must give each file the time of the last commit that touched
// it (not the newest commit's, and not a constant: either would let an edit that
// keeps a file's size hit a stale go test result), and a directory the newest
// time beneath it.
func TestRestoreMtimesUsesTheLastCommitThatTouchedEachFile(t *testing.T) {
	root, _ := filepath.Abs("../..")
	repo := t.TempDir()
	git := func(date string, args ...string) {
		t.Helper()
		env := []string{"GIT_AUTHOR_DATE=" + date, "GIT_COMMITTER_DATE=" + date,
			"GIT_AUTHOR_NAME=t", "GIT_AUTHOR_EMAIL=t@t", "GIT_COMMITTER_NAME=t", "GIT_COMMITTER_EMAIL=t@t"}
		if out, err := run(t, repo, env, "git", args...); err != nil {
			t.Fatalf("git %v: %v\n%s", args, err, out)
		}
	}
	write := func(name, body string) {
		p := filepath.Join(repo, name)
		os.MkdirAll(filepath.Dir(p), 0o755)
		os.WriteFile(p, []byte(body), 0o644)
	}
	git("1000000000 +0000", "init", "-q")
	write("a.txt", "a")
	write("d/b.txt", "b")
	git("1000000000 +0000", "add", ".")
	git("1000000000 +0000", "commit", "-qm", "one")
	write("d/b.txt", "bb")
	git("1100000000 +0000", "commit", "-qam", "two")
	// A fresh checkout: everything stamped with one time that is no commit's.
	now := time.Unix(1700000000, 0)
	for _, p := range []string{"a.txt", "d/b.txt", "d", "."} {
		os.Chtimes(filepath.Join(repo, p), now, now)
	}
	if out, err := run(t, repo, nil, "python3", "-I", filepath.Join(root, "scripts", "restore-mtimes.py")); err != nil {
		t.Fatalf("%v\n%s", err, out)
	}
	for p, want := range map[string]int64{"a.txt": 1000000000, "d/b.txt": 1100000000, "d": 1100000000, ".": 1100000000} {
		info, err := os.Stat(filepath.Join(repo, p))
		if err != nil || info.ModTime().Unix() != want {
			t.Errorf("%s: mtime %v (%v), want %d", p, info.ModTime().Unix(), err, want)
		}
	}
}

// go test -fuzz exits 0 for a name that matches nothing; fuzz.sh must not.
func TestFuzzScriptFailsWhereGoTestStaysGreen(t *testing.T) {
	root, _ := filepath.Abs("../..")
	script := filepath.Join(root, "scripts", "fuzz.sh")
	dir := t.TempDir()
	list := func(body string) []string {
		p := filepath.Join(dir, "targets")
		os.WriteFile(p, []byte(body), 0o644)
		return []string{"FUZZ_TARGETS=" + p}
	}
	// One package with tests and no such target, one with no tests at all.
	for _, pkg := range []string{"./internal/clock", "./internal/idgen"} {
		out, err := run(t, root, list(pkg+" FuzzNoSuchTarget\n"), "bash", script)
		if err == nil || !strings.Contains(out, "no fuzz target named FuzzNoSuchTarget") {
			t.Errorf("%s: a misspelled target passed: err=%v\n%s", pkg, err, out)
		}
	}
	out, err := run(t, root, list("./internal/idgen FuzzNoSuchTarget\n"), "bash", script, "1", "2")
	if err == nil || !strings.Contains(out, "selected no target") {
		t.Errorf("a shard with no target passed: err=%v\n%s", err, out)
	}
}

// A build of some of the services must list exactly those and refuse a name that
// is not one: a profile that quietly built a different set is the failure that
// matters here, since nothing else would notice.
func TestSlimBuildServesExactlyTheServicesItWasGiven(t *testing.T) {
	root, _ := filepath.Abs("../..")
	needEveryService(t, root)
	script := filepath.Join(root, "scripts", "slim-overlay.py")
	dir := t.TempDir()

	if out, err := run(t, root, nil, "python3", "-I", script, filepath.Join(dir, "o.json"), "aws.nosuchservice"); err == nil || !strings.Contains(out, "no service") {
		t.Errorf("an unknown service was accepted: err=%v\n%s", err, out)
	}

	if out, err := run(t, root, nil, "python3", "-I", script, filepath.Join(dir, "o.json"), "aws.s3", "aws.sqs"); err != nil {
		t.Fatalf("%v\n%s", err, out)
	}
	// make build-slim writes the overlay inside the module, where a .go file
	// beside it would become a package of `go vet ./...`.
	entries, _ := os.ReadDir(filepath.Join(dir, "o.json.d"))
	if len(entries) == 0 {
		t.Fatal("the overlay wrote no files beside itself")
	}
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".go") {
			t.Errorf("%s is a .go file inside the overlay directory", e.Name())
		}
	}
	bin := filepath.Join(dir, "mirror")
	env := []string{"CGO_ENABLED=0", "GOFLAGS=-buildvcs=false"}
	if out, err := run(t, root, env, "go", "build", "-overlay", filepath.Join(dir, "o.json"), "-o", bin, "./cmd/mirror"); err != nil {
		t.Fatalf("go build: %v\n%s", err, out)
	}
	out, err := run(t, dir, nil, bin, "services")
	if err != nil {
		t.Fatalf("mirror services: %v\n%s", err, out)
	}
	var got []string
	for _, line := range strings.Split(strings.TrimSpace(out), "\n") {
		got = append(got, strings.Fields(line)[0])
	}
	if strings.Join(got, " ") != "aws.s3 aws.sqs" {
		t.Errorf("the slim build lists %v; want exactly aws.s3 aws.sqs", got)
	}
}

// cut-release.sh is the one script that creates a tag, so what makes it decline
// matters as much as what makes it act. Each case is a real origin and checkout
// holding the given commit messages (oldest first), with the given tags on the
// commits they name, and a stand-in gh that records what it is asked to create.
func TestCutReleaseDerivesTheNextVersionFromTheCommitsSinceTheLastTag(t *testing.T) {
	root, _ := filepath.Abs("../..")
	script := filepath.Join(root, "scripts", "cut-release.sh")

	type result struct {
		out, tag, ghCalls string
		err               error
	}
	// tags maps a tag to the number of commits that precede it, so
	// {"v1.0.0": 1} tags the first. `advance` leaves main one commit ahead of
	// the checkout the script runs in.
	run1 := func(t *testing.T, commits []string, tags map[string]int, advance bool) result {
		t.Helper()
		dir := t.TempDir()
		origin, work := filepath.Join(dir, "origin.git"), filepath.Join(dir, "work")
		ident := []string{"GIT_AUTHOR_NAME=t", "GIT_AUTHOR_EMAIL=t@t", "GIT_COMMITTER_NAME=t", "GIT_COMMITTER_EMAIL=t@t"}
		must := func(d string, args ...string) {
			t.Helper()
			if out, err := run(t, d, ident, "git", args...); err != nil {
				t.Fatalf("git %v: %v\n%s", args, err, out)
			}
		}
		must(dir, "init", "-q", "--bare", "-b", "main", origin)
		must(dir, "clone", "-q", origin, work)
		for i, msg := range commits {
			must(work, "commit", "-q", "--allow-empty", "-m", msg)
			for tag, n := range tags {
				if n == i+1 {
					must(work, "tag", "-a", "-m", tag, tag)
				}
			}
		}
		must(work, "push", "-q", "origin", "HEAD:main", "--tags")
		if advance {
			other := filepath.Join(dir, "other")
			must(dir, "clone", "-q", origin, other)
			must(other, "commit", "-q", "--allow-empty", "-m", "fix: later")
			must(other, "push", "-q", "origin", "HEAD:main")
		}
		bin := filepath.Join(dir, "bin")
		os.MkdirAll(bin, 0o755)
		log := filepath.Join(dir, "gh.log")
		os.WriteFile(filepath.Join(bin, "gh"), []byte("#!/bin/sh\necho \"$@\" >> "+log+"\necho deadbeef\n"), 0o755)
		outFile := filepath.Join(dir, "output")
		out, err := run(t, work, []string{"PATH=" + bin + ":" + os.Getenv("PATH"), "GH_REPO=o/r", "GITHUB_OUTPUT=" + outFile}, "bash", script)
		got, _ := os.ReadFile(outFile)
		calls, _ := os.ReadFile(log)
		return result{out: out, tag: strings.TrimSpace(strings.TrimPrefix(string(got), "tag=")), ghCalls: string(calls), err: err}
	}

	for name, c := range map[string]struct {
		commits []string
		tags    map[string]int
		want    string // "" is no release
	}{
		"nothing since the tag":               {[]string{"fix: a"}, map[string]int{"v0.0.1-alpha.1": 1}, ""},
		"fix after an alpha":                  {[]string{"chore: base", "fix: a"}, map[string]int{"v0.0.1-alpha.1": 1}, "v0.0.1-alpha.2"},
		"alpha.9 goes to alpha.10, not alpha": {[]string{"chore: base", "feat: a"}, map[string]int{"v0.0.1-alpha.9": 1}, "v0.0.1-alpha.10"},
		"rc1 goes to rc2":                     {[]string{"chore: base", "perf: a"}, map[string]int{"v1.0.0-rc1": 1}, "v1.0.0-rc2"},
		"fix after a stable 1.x":              {[]string{"chore: base", "fix(api): a"}, map[string]int{"v1.2.3": 1}, "v1.2.4"},
		"feat after a stable 1.x":             {[]string{"chore: base", "fix: a", "feat: b"}, map[string]int{"v1.2.3": 1}, "v1.3.0"},
		"breaking after a stable 1.x":         {[]string{"chore: base", "feat!: a", "fix: b"}, map[string]int{"v1.2.3": 1}, "v2.0.0"},
		"breaking in the body":                {[]string{"chore: base", "fix: a\n\nBREAKING CHANGE: gone"}, map[string]int{"v1.2.3": 1}, "v2.0.0"},
		"breaking in 0.x is a minor":          {[]string{"chore: base", "refactor(x)!: a"}, map[string]int{"v0.4.2": 1}, "v0.5.0"},
		"feat in 0.x is a patch":              {[]string{"chore: base", "feat: a"}, map[string]int{"v0.4.2": 1}, "v0.4.3"},
		"Release-As leaves a prerelease":      {[]string{"chore: base", "fix: a", "docs: b\n\nRelease-As: 0.1.0"}, map[string]int{"v0.0.1-alpha.1": 1}, "v0.1.0"},
		"the newest Release-As wins":          {[]string{"chore: base", "docs: a\n\nRelease-As: 1.0.0", "docs: b\n\nRelease-As: v1.0.1"}, map[string]int{"v0.9.0": 1}, "v1.0.1"},
		"no tag yet":                          {[]string{"chore: a", "fix: b"}, nil, "v0.0.1"},
		"docs, ci and chore release nothing":  {[]string{"chore: base", "docs: a", "ci: b", "chore(deps): c", "test: d"}, map[string]int{"v1.2.3": 1}, ""},
		"a Release-As that already exists":    {[]string{"chore: base", "docs: a\n\nRelease-As: 1.0.0"}, map[string]int{"v1.0.0": 1, "v0.9.0": 0}, ""},
	} {
		t.Run(name, func(t *testing.T) {
			r := run1(t, c.commits, c.tags, false)
			if r.err != nil || r.tag != c.want {
				t.Fatalf("tag %q, want %q, err %v\n%s", r.tag, c.want, r.err, r.out)
			}
			if c.want == "" && r.ghCalls != "" {
				t.Errorf("no release, but gh was called:\n%s", r.ghCalls)
			}
			if c.want != "" && (strings.Count(r.ghCalls, "\n") != 2 || !strings.Contains(r.ghCalls, "refs/tags/"+c.want)) {
				t.Errorf("expected a tag object and a ref for %s, got:\n%s", c.want, r.ghCalls)
			}
		})
	}

	t.Run("no tag when main has moved on past this commit", func(t *testing.T) {
		r := run1(t, []string{"chore: base", "fix: a"}, map[string]int{"v1.2.3": 1}, true)
		if r.err != nil || r.tag != "" || r.ghCalls != "" {
			t.Errorf("tag %q, gh called with %q, err %v\n%s", r.tag, r.ghCalls, r.err, r.out)
		}
	})
	for name, commits := range map[string][]string{
		"a name with a plus": {"chore: base", "docs: a\n\nRelease-As: 1.0.0+build"},
	} {
		t.Run("an error when Release-As is "+name, func(t *testing.T) {
			r := run1(t, commits, map[string]int{"v0.9.0": 1}, false)
			if r.err == nil || r.ghCalls != "" {
				t.Errorf("accepted %q: err %v, gh %q\n%s", commits, r.err, r.ghCalls, r.out)
			}
		})
	}
}

// image-check.sh is what stands between a pushed image and a release, so each way
// an image can be wrong must stop it. docker is a stand-in that answers for one
// image: its version, its services and the binary it holds.
func TestImageCheckStopsAnImageThatIsNotWhatTheReleaseSays(t *testing.T) {
	root, _ := filepath.Abs("../..")
	script := filepath.Join(root, "scripts", "image-check.sh")
	profile, err := os.ReadFile(filepath.Join(root, "profiles", "core.txt"))
	if err != nil {
		t.Fatal(err)
	}
	var core []string
	for _, l := range strings.Split(string(profile), "\n") {
		if l = strings.TrimSpace(l); l != "" && !strings.HasPrefix(l, "#") {
			core = append(core, l)
		}
	}
	var full []string
	for i := 0; i < 101; i++ {
		full = append(full, "aws.svc"+strings.Repeat("x", i))
	}

	check := func(t *testing.T, variant, version, imageVersion string, services []string, binary string) error {
		t.Helper()
		dir := t.TempDir()
		bin := filepath.Join(dir, "bin")
		os.MkdirAll(bin, 0o755)
		os.WriteFile(filepath.Join(dir, "mirror"), []byte("the binary"), 0o755)
		archive := filepath.Join(dir, "a.tar.gz")
		if out, err := run(t, dir, nil, "tar", "-czf", archive, "mirror"); err != nil {
			t.Fatalf("%v\n%s", err, out)
		}
		imageDir := filepath.Join(dir, "image")
		os.MkdirAll(imageDir, 0o755)
		os.WriteFile(filepath.Join(imageDir, "mirror"), []byte(binary), 0o755)
		os.WriteFile(filepath.Join(bin, "docker"), []byte(`#!/bin/sh
case "$1" in
  run) case "$*" in
         *version) echo "$IMAGE_VERSION" ;;
         *services) for s in $IMAGE_SERVICES; do printf '%s\ttier\n' "$s"; done ;;
       esac ;;
  create) echo cid ;;
  cp) tar -cf - -C "$IMAGE_DIR" mirror ;;
esac
`), 0o755)
		_, err := run(t, root, []string{
			"PATH=" + bin + ":" + os.Getenv("PATH"), "IMAGE_VERSION=" + imageVersion,
			"IMAGE_SERVICES=" + strings.Join(services, " "), "IMAGE_DIR=" + imageDir,
		}, "bash", script, "img", variant, version, archive)
		return err
	}

	if err := check(t, "core", "1.2.3", "1.2.3", core, "the binary"); err != nil {
		t.Errorf("a correct core image was refused: %v", err)
	}
	if err := check(t, "full", "1.2.3", "1.2.3", full, "the binary"); err != nil {
		t.Errorf("a correct full image was refused: %v", err)
	}
	for name, c := range map[string]struct {
		variant, imageVersion string
		services              []string
		binary                string
	}{
		"the wrong version":                  {"core", "1.2.2", core, "the binary"},
		"a core image missing a service":     {"core", "1.2.3", core[1:], "the binary"},
		"a core image with an extra one":     {"core", "1.2.3", append([]string{"aws.extra"}, core...), "the binary"},
		"a full image with too few":          {"full", "1.2.3", full[:100], "the binary"},
		"a binary that is not the archive's": {"core", "1.2.3", core, "another binary"},
	} {
		if err := check(t, c.variant, "1.2.3", c.imageVersion, c.services, c.binary); err == nil {
			t.Errorf("accepted %s", name)
		}
	}
}

// A service pack is a directory a developer can leave out of a sparse checkout
// (scripts/sparse.sh), so nothing the binary is built from may import one except
// the list that names them, allservices, and other packs, which sparse.sh follows.
// A core package that reached into a pack would stop building the moment the
// pack was absent.
func TestNoCorePackageImportsAServicePack(t *testing.T) {
	root, _ := filepath.Abs("../..")
	out, err := run(t, root, nil, "go", "list", "-f", "{{.ImportPath}} {{join .Imports \" \"}}", "./...")
	if err != nil {
		t.Skipf("go list: %v", err) // a sparse tree may not build everything
	}
	const mod = "github.com/tyler-r-kendrick/mirror.cloud/"
	for _, line := range strings.Split(strings.TrimSpace(out), "\n") {
		f := strings.Fields(line)
		pkg := strings.TrimPrefix(f[0], mod)
		if strings.HasPrefix(pkg, "internal/services/") || pkg == "internal/allservices" {
			continue
		}
		for _, imp := range f[1:] {
			if strings.HasPrefix(imp, mod+"internal/services/") {
				t.Errorf("%s imports %s: core must not import a service pack (use a hook in internal/spi)", pkg, strings.TrimPrefix(imp, mod))
			}
		}
	}
}

// scripts/sparse.py decides what a sparse checkout leaves out from
// profiles/graph.txt alone, so a stale graph is a checkout that does not build.
// The file is rebuilt from the tree and compared; a sparse tree has not got the
// whole tree to rebuild it from.
func TestServiceGraphMatchesTheTree(t *testing.T) {
	root, _ := filepath.Abs("../..")
	needEveryService(t, root)
	got, err := run(t, root, nil, "python3", "scripts/service-graph.py")
	if err != nil {
		t.Fatalf("service-graph.py: %v\n%s", err, got)
	}
	want, err := os.ReadFile(filepath.Join(root, "profiles", "graph.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if got != string(want) {
		t.Fatal("profiles/graph.txt is stale; run `make graph` and commit it")
	}
}

// What a sparse checkout brings for a service is what the graph says it needs,
// all the way down, and no more. Read from the committed tree, so it checks the
// graph and the script together without cloning.
func TestSparseBringsWhatAServiceNeedsAndNothingElse(t *testing.T) {
	root, _ := filepath.Abs("../..")
	if out, _ := run(t, root, nil, "git", "rev-parse", "--verify", "-q", "HEAD:profiles/graph.txt"); out == "" {
		t.Skip("profiles/graph.txt is not committed yet")
	}
	list := func(args ...string) string {
		out, err := run(t, root, nil, "python3", append([]string{"scripts/sparse.py", "--list"}, args...)...)
		if err != nil {
			t.Fatalf("sparse.py %v: %v\n%s", args, err, out)
		}
		return out
	}
	s3 := list("aws.s3")
	for _, want := range []string{
		"/internal/services/aws/s3/", "/behavior/aws/s3/", "/internal/generated/aws/s3/", "/internal/allservices/svc_aws_s3.go",
		// what S3 imports, what it names ("aws.sqs" is reached through the registry), and what its tests use
		"/internal/services/aws/sns/", "/internal/generated/aws/sqs/", "/internal/services/aws/events/", "/internal/generated/aws/kms/",
		// what those need in turn
		"/internal/services/aws/eventhttp/", "/internal/services/aws/states/",
		// the placeholder that keeps behavior's embed pattern matching
		"/behavior/none/placeholder/",
		// a pack that is only here to be built is not tested
		"!/internal/services/aws/sns/**/*_test.go",
		// the tree is not complete
		"!/internal/specboot/complete.go",
	} {
		if !strings.Contains(s3+"\n", "\n"+want+"\n") {
			t.Errorf("aws.s3 does not bring %s", want)
		}
	}
	for _, unwanted := range []string{"/internal/services/aws/dynamodb/", "/internal/generated/aws/ec2/", "/internal/services/gcp/", "/behavior/vercel/"} {
		if strings.Contains(s3+"\n", "\n"+unwanted+"\n") {
			t.Errorf("aws.s3 brings %s, which nothing it uses needs", unwanted)
		}
	}
	if !strings.Contains(s3, "!/test/behavior/aws/") {
		t.Error("the AWS behavior suite imports packs aws.s3 does not bring, so it must be left out")
	}
	// A pack that serves its service under another name is found by the service's.
	if !strings.Contains("\n"+list("gcp.storage"), "\n/internal/services/gcp/gcs/\n") {
		t.Error("gcp.storage does not bring the gcp/gcs pack that serves it")
	}
	// A whole provider brings its specs and its behavior suite, which also names
	// aws.s3, so that suite waits until S3 is asked for too.
	if strings.Contains("\n"+list("vercel.*"), "\n/test/behavior/vercel/\n") {
		t.Error("the Vercel suite names aws.s3 and must wait for it")
	}
	v := list("vercel.*", "aws.s3")
	for _, want := range []string{"/specs/vercel/", "/test/behavior/vercel/"} {
		if !strings.Contains(v+"\n", "\n"+want+"\n") {
			t.Errorf("vercel.* does not bring %s", want)
		}
	}
	if out, err := run(t, root, nil, "python3", "scripts/sparse.py", "--list", "aws.nonesuch"); err == nil {
		t.Errorf("a service that does not exist was accepted:\n%s", out)
	}
}

// needEveryService skips a test that uses services as examples, or rebuilds
// something from the whole tree, in a sparse checkout (scripts/sparse.py).
func needEveryService(t *testing.T, root string) {
	t.Helper()
	if out, _ := run(t, root, nil, "git", "config", "core.sparseCheckout"); strings.TrimSpace(out) == "true" {
		t.Skip("this is a sparse checkout; the full tree is CI's")
	}
}
