package ci

import (
	"path/filepath"
	"sync"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/testinputs"
)

var packagesOnce sync.Once

// repo returns the repository root and records, for go test's result cache,
// everything this package's scripts and builds read there: the scripts, what
// they are given, the CI files the tests parse, and the packages the fuzz and
// slim-build tests compile. Nothing else in the tree can change a result here,
// so a pull request that changes nothing of these reads the cached one.
func repo(t *testing.T) string {
	t.Helper()
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	testinputs.Record(t, root, "scripts", "profiles", "Makefile", ".github/workflows", ".github/actions/go")
	packagesOnce.Do(func() { testinputs.Packages(t, root, "./cmd/mirror", "./internal/clock", "./internal/idgen") })
	return root
}
