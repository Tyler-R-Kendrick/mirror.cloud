package ci

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// main used to run the unit job with -count=1 after clearing the test cache,
// and the unit job ran with -coverprofile; any one of the three leaves pull
// requests nothing to read from the cache main saves. This pins the repair.
func TestUnitResultsStayCacheable(t *testing.T) {
	root := repo(t)
	read := func(rel string) string {
		b, err := os.ReadFile(filepath.Join(root, rel))
		if err != nil {
			t.Fatal(err)
		}
		return string(b)
	}
	ci := read(".github/workflows/ci.yml")
	if strings.Contains(ci, "go clean -testcache") {
		t.Error("ci.yml clears the test cache, so the cache main saves holds no results")
	}
	if regexp.MustCompile(`(?m)^\s*COUNT:.*count=1`).MatchString(ci) {
		t.Error("ci.yml sets COUNT=-count=1, which stops go test caching results")
	}
	recipe := regexp.MustCompile(`(?ms)^test-unit:\n(.*?)\n\n`).FindStringSubmatch(read("Makefile"))
	if recipe == nil {
		t.Fatal("no test-unit target in the Makefile")
	}
	for _, flag := range []string{"-count", "-cover"} {
		if strings.Contains(recipe[1], flag) {
			t.Errorf("test-unit passes %s, which stops go test caching its results:\n%s", flag, recipe[1])
		}
	}
	if !strings.Contains(ci, "make test-unit GOTESTFLAGS=-json") {
		t.Error("the unit job no longer runs make test-unit with -json; main and pull requests must pass the same flags")
	}
}
