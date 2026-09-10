package runtime

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spitest"

	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/allservices"
)

func TestSupportMatrixMatchesDocs(t *testing.T) {
	got := SupportMatrix()
	root := findGoMod(t)
	want, err := os.ReadFile(filepath.Join(root, "docs", "SUPPORT.md"))
	if err != nil {
		t.Fatal(err)
	}
	if string(want) != got {
		t.Fatalf("docs/SUPPORT.md stale; run `mirror support-matrix`\n--- want ---\n%s\n--- got ---\n%s", want, got)
	}
}

func TestSupportMatrixEmulateCountsMatchPacks(t *testing.T) {
	want := packEmulateCounts(t)
	if len(want) == 0 {
		t.Fatal("no emulate packs registered")
	}
	for _, r := range SupportRows() {
		n := want[r.ID]
		if r.Emulate != n {
			t.Errorf("%s: SupportRows emulate=%d pack Operations=%d", r.ID, r.Emulate, n)
		}
		if r.Emulate > n {
			t.Errorf("%s: emulate count %d exceeds pack Operations() %d", r.ID, r.Emulate, n)
		}
	}
	root := findGoMod(t)
	docs, err := os.ReadFile(filepath.Join(root, "docs", "SUPPORT.md"))
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range strings.Split(string(docs), "\n") {
		if !strings.HasPrefix(line, "| `") {
			continue
		}
		cols := strings.Split(line, "|")
		if len(cols) < 5 {
			continue
		}
		id := strings.Trim(strings.TrimSpace(cols[1]), "`")
		emu, err := strconv.Atoi(strings.TrimSpace(cols[3]))
		if err != nil {
			continue
		}
		n := want[id]
		if emu > n {
			t.Errorf("docs/SUPPORT.md %s emulate %d > pack Operations() %d", id, emu, n)
		}
	}
}

func packEmulateCounts(t *testing.T) map[string]int {
	t.Helper()
	out := map[string]int{}
	for _, f := range registry.Factories() {
		if f.Tier != model.TierEmulate {
			continue
		}
		p, err := f.New(spitest.Deps(t))
		if err != nil || p == nil {
			t.Fatalf("%s: %v", f.ServiceID, err)
		}
		out[f.ServiceID] = len(p.Operations())
	}
	return out
}

func findGoMod(t *testing.T) string {
	t.Helper()
	wd, _ := os.Getwd()
	for p := wd; p != "/"; p = filepath.Dir(p) {
		if _, err := os.Stat(filepath.Join(p, "go.mod")); err == nil {
			return p
		}
	}
	t.Fatal("go.mod")
	return ""
}
