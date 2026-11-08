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
	script := filepath.Join(root, "scripts", "slim-overlay.py")
	dir := t.TempDir()

	if out, err := run(t, root, nil, "python3", "-I", script, filepath.Join(dir, "o.json"), "aws.nosuchservice"); err == nil || !strings.Contains(out, "no service") {
		t.Errorf("an unknown service was accepted: err=%v\n%s", err, out)
	}

	if out, err := run(t, root, nil, "python3", "-I", script, filepath.Join(dir, "o.json"), "aws.s3", "aws.sqs"); err != nil {
		t.Fatalf("%v\n%s", err, out)
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
