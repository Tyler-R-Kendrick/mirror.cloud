// Package testinputs tells go test's result cache what a test's subprocesses
// read.
//
// go test keys a cached result on the test binary and on the files and
// directories the test process itself opened or stat'd. A test that runs a
// script or a go command reads files go test never sees, so its cached result
// can outlive a change to them. Tests in test/ci and test/release used to be
// saved from that by accident: setting exec.Cmd.Dir to the repository root makes
// os.StartProcess stat the root, whose mtime (scripts/restore-mtimes.py) moves
// with every commit, so they missed the cache on every pull request, a
// documentation change included.
//
// Record and Packages open exactly what such a test depends on, and Command
// runs a subprocess in the root without that stat.
package testinputs

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
)

// Record opens each path under root, so go test keys the result on it: a file
// by its size and mtime, a directory by the names, sizes and mtimes of its
// entries.
func Record(t testing.TB, root string, paths ...string) {
	t.Helper()
	for _, p := range paths {
		full := filepath.Join(root, filepath.FromSlash(p))
		info, err := os.Stat(full)
		if err != nil {
			t.Fatalf("testinputs: %v", err)
		}
		if info.IsDir() {
			if _, err := os.ReadDir(full); err != nil {
				t.Fatalf("testinputs: %v", err)
			}
			continue
		}
		f, err := os.Open(full)
		if err != nil {
			t.Fatalf("testinputs: %v", err)
		}
		f.Close()
	}
}

// Packages records go.mod, go.sum and every directory of this module that a
// build of patterns, with their tests, compiles: the source, embedded files
// and testdata of each package beneath it all sit in that directory. It is
// what a go build, go vet or go test of patterns run by the test reads.
func Packages(t testing.TB, root string, patterns ...string) {
	t.Helper()
	args := append([]string{"list", "-deps", "-test", "-f", "{{if .Module}}{{if .Module.Main}}{{.Dir}}{{end}}{{end}}"}, patterns...)
	cmd := Command(root, "go", args...)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("testinputs: go list: %v\n%s", err, stderr.String())
	}
	paths := []string{"go.mod", "go.sum"}
	seen := map[string]bool{}
	for _, dir := range strings.Fields(string(out)) {
		rel, err := filepath.Rel(root, dir)
		if err != nil || seen[rel] {
			continue
		}
		seen[rel] = true
		paths = append(paths, filepath.ToSlash(rel))
	}
	Record(t, root, paths...)
}

// PackageSet returns the module's packages, as go list ./... prints them, and
// records what that list is read from: every package directory, so a package
// that goes away is noticed, and the directories above them up to the root
// (not the root, whose listing changes with every commit), so a package that
// appears beside them is. A new top-level directory is the one addition this
// does not see.
func PackageSet(t testing.TB, root string) []string {
	t.Helper()
	cmd := Command(root, "go", "list", "-f", "{{.ImportPath}}\t{{.Dir}}", "./...")
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("testinputs: go list: %v\n%s", err, stderr.String())
	}
	var pkgs, dirs []string
	seen := map[string]bool{}
	for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		ip, dir, _ := strings.Cut(line, "\t")
		pkgs = append(pkgs, ip)
		for rel, err := filepath.Rel(root, dir); err == nil && rel != "." && !seen[rel]; rel = filepath.Dir(rel) {
			seen[rel] = true
			dirs = append(dirs, filepath.ToSlash(rel))
		}
	}
	Record(t, root, append(dirs, "go.mod")...)
	return pkgs
}

// Command is exec.Command run in dir. Setting SysProcAttr keeps
// os.StartProcess from stat'ing dir first, which go test would record: for the
// repository root that stat changes with every commit. Whatever the command
// reads in dir has to be recorded with Record or Packages instead.
func Command(dir, name string, args ...string) *exec.Cmd {
	cmd := exec.Command(name, args...)
	cmd.Dir = dir
	cmd.SysProcAttr = &syscall.SysProcAttr{}
	return cmd
}
