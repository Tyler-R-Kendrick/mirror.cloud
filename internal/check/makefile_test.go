package check

import (
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
)

// pkgPath matches a package path as the Makefile writes one: ./ followed by
// path segments, optionally ending in the /... wildcard.
var pkgPath = regexp.MustCompile(`\./[A-Za-z0-9_./-]+`)

// TestMakefileNamesRealPackages checks that every package path the Makefile
// hands to `go test` resolves to a directory that exists.
//
// This guards a blind spot that only opens when a package is deleted, which is
// the central operation of the whole extraction programme. `test-unit` derives
// its own inputs -- `go list ./...` enumerates what is there -- so it cannot
// fail on a path that is gone. `test-snapshot`, `test-fuzz-seeds` and
// `test-fuzz` hard-code a couple of hundred paths instead, because they select
// individual tests and fuzz targets, and a stale one is a hard error.
//
// Deleting internal/services/digitalocean/v2 broke three CI steps that way at
// once, after a full local `go test $(go list ./...)` had reported every
// package passing. Both statements were true: the suite that runs everything
// that exists cannot see a reference to something that does not.
//
// So this is the same shape as TestMutantNeedlesExist, and exists for the same
// reason: a fast check for a stale reference that the slow suite reports half
// an hour later, from CI, on a step whose name does not say which path is
// wrong. It runs in milliseconds and names the line.
func TestMakefileNamesRealPackages(t *testing.T) {
	root := findMod(t)
	body, err := os.ReadFile(filepath.Join(root, "Makefile"))
	if err != nil {
		t.Fatalf("read Makefile: %v", err)
	}

	// Deduplicated so a path repeated across targets is reported once, and
	// sorted so the report is stable rather than following map order.
	missing := map[string][]int{}
	for i, line := range strings.Split(string(body), "\n") {
		if !strings.Contains(line, "$(GO) test") {
			continue
		}
		for _, path := range pkgPath.FindAllString(line, -1) {
			// The wildcard form names a tree, not a package: ./test/... is
			// satisfied by the directory above it.
			dir := strings.TrimSuffix(strings.TrimSuffix(path, "/..."), "/")
			info, err := os.Stat(filepath.Join(root, dir))
			if err == nil && info.IsDir() {
				continue
			}
			missing[path] = append(missing[path], i+1)
		}
	}

	for _, path := range sortedPaths(missing) {
		t.Errorf("Makefile line(s) %v: %s is not a directory in this tree. "+
			"A `go test` against a path that does not exist fails the step outright; "+
			"if the package was deleted, delete the line too.",
			missing[path], path)
	}
}

func sortedPaths(m map[string][]int) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
