package runtime

import (
	"bytes"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/rand"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spitest"
)

// TestASnapshotCarriesWhereTheIdentifiersStood is the bug that made persisting a
// Vercel project unsafe: the store came back and the stream of generated ids did
// not, so the first project created after a restart drew the id the first
// project of the last run had and wrote over it. The position is in the archive
// now, and a restored process draws on from it.
func TestASnapshotCarriesWhereTheIdentifiersStood(t *testing.T) {
	deps := spitest.Deps(t)
	deps.Rand = rand.New("persist-seed")
	handed := map[string]bool{}
	for i := 0; i < 6; i++ {
		handed[deps.Rand.Hex(12)] = true
	}
	var archive bytes.Buffer
	if err := Snapshot(deps, "", &archive); err != nil {
		t.Fatal(err)
	}

	restarted := spitest.Deps(t)
	restarted.Rand = rand.New("persist-seed")
	if err := Restore(restarted, "", bytes.NewReader(archive.Bytes())); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 6; i++ {
		if id := restarted.Rand.Hex(12); handed[id] {
			t.Fatalf("a restored process drew %q again, an id the last run handed out", id)
		}
	}
}
