package engine

import (
	"fmt"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/rand"
)

// What a walk does past the ceiling, stated so the docs can state it: the items of
// one listing made in one millisecond span more sequence numbers than there are
// places, so two of them share a place and the cursor of the newer is read as the
// older. The older bound is a whole slot count away, so the walk jumps over the
// items between, silently, once per wrap, and never serves one twice.

func walkIDs(t *testing.T, ls []listed, limit int) []string {
	t.Helper()
	return walkPages(t, recsOf(ls, rand.New("ceiling")), limit)
}

func TestPastTheCeilingAWalkSkipsAWholeWrapAndNeverRepeats(t *testing.T) {
	n := int(slotCount(today))
	for _, tc := range []struct {
		items, limit, served int
	}{
		{n, 50, n}, {n + 1, 50, n + 1}, // at the ceiling, and one past it that no boundary falls on
		{6000, 50, 1905}, {9000, 50, 810}, // one wrap skipped, two
	} {
		ls := run(today, 0, tc.items)
		got := walkIDs(t, ls, tc.limit)
		want := wantIDs(ls)
		seen := map[string]bool{}
		next := 0 // got must be a subsequence of want, in order, with no repeats
		for _, id := range got {
			if seen[id] {
				t.Fatalf("%d items by %d: %s was served twice", tc.items, tc.limit, id)
			}
			seen[id] = true
			for next < len(want) && want[next] != id {
				next++
			}
			if next == len(want) {
				t.Fatalf("%d items by %d: %s is out of order", tc.items, tc.limit, id)
			}
			next++
		}
		if len(got) != tc.served {
			t.Errorf("%d items by %d: served %d, measured %d", tc.items, tc.limit, len(got), tc.served)
		}
	}
}

// TestAFewItemsReachTheCeilingOnAClockThatSteppedBack is how it is reached without
// thousands of items: three items of an instant, more than a slot count of other
// creations from the same counter, and three more at the instant again.
func TestAFewItemsReachTheCeilingOnAClockThatSteppedBack(t *testing.T) {
	n := slotCount(today)
	ls := []listed{{today, 0}, {today, 1}, {today, 2}, {today, n + 5}, {today, n + 6}, {today, n + 7}}
	got := walkIDs(t, ls, 1)
	want := []string{idOf(ls[5]), idOf(ls[2]), idOf(ls[1]), idOf(ls[0])}
	sameIDs(t, "six items of an instant that span more than the places", got, want)
}

// TestTheCursorIsNotMonotoneAtTheWrapOfThePlaces: the cursors of items 4096, 4095
// and 4094 of an instant, the order they are served in, are 2/4096, 1/4096 and
// 4095/4096 past the millisecond, because a place is a sequence modulo the count.
// The listing resolves them; a client that insists on a decreasing cursor would see
// one step that is not, once per wrap.
func TestTheCursorIsNotMonotoneAtTheWrapOfThePlaces(t *testing.T) {
	n := slotCount(today)
	step := func(seq int64) string { return fmt.Sprint(pageCursor(today, seq) - float64(today)) }
	if a, b, c := pageCursor(today, n+1), pageCursor(today, n), pageCursor(today, n-1); !(a > b && b < c) {
		t.Fatalf("cursors of sequences %d, %d, %d are %v, %v, %v; want them to fall and then rise at the wrap", n+1, n, n-1, a, b, c)
	}
	if step(n+1) != "0.00048828125" || step(n) != "0.000244140625" || step(n-1) != "0.999755859375" {
		t.Fatalf("fractions %s %s %s", step(n+1), step(n), step(n-1))
	}
}
