package engine

import (
	"fmt"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/rand"
)

// A cursor names a position between items: what is older than the item it was made
// from stays, what is newer goes. The item itself may be gone by the time the next
// page is asked for (a client that deletes what it lists is the usual way), and the
// position is still there. These tests delete the item a cursor names, and whatever
// else, between pages.

func idOf(l listed) string { return fmt.Sprintf("%d/%d", l.ms, l.seq) }

// olderThanItem is what a page after the cursor of x is meant to hold: the items of
// alive that sort after x, newest first, limit of them.
func olderThanItem(alive []listed, x listed, limit int) []string {
	var ids []string
	for _, l := range served(alive) {
		if l.ms < x.ms || (l.ms == x.ms && l.seq < x.seq) {
			ids = append(ids, idOf(l))
		}
	}
	return ids[:min(limit, len(ids))]
}

// pageAfter asks for the page after the cursor made from x.
func pageAfter(t *testing.T, alive []listed, x listed, limit int, rnd *rand.Seeded) []string {
	t.Helper()
	c, err := parseCursor(wire(pageCursor(x.ms, x.seq)))
	if err != nil {
		t.Fatal(err)
	}
	var ids []string
	for _, r := range pageOf(recsOf(alive, rnd), "createdAt", nil, []cursor{c}, false, limit)["page"].([]any) {
		ids = append(ids, r.(map[string]any)["id"].(string))
	}
	return ids
}

func without(ls []listed, drop func(listed) bool) []listed {
	var out []listed
	for _, l := range ls {
		if !drop(l) {
			out = append(out, l)
		}
	}
	return out
}

// TestAPageAfterADeletedItemKeepsWhatWasOlderThanIt is the review's repro and its
// neighbours: two items at one instant, three at the next, the page of three ends
// at the third newest, which is deleted; the next page is the two older ones, not
// the two newer that are left in its millisecond.
func TestAPageAfterADeletedItemKeepsWhatWasOlderThanIt(t *testing.T) {
	repro := []listed{{today, 0}, {today, 1}, {today + 1, 2}, {today + 1, 3}, {today + 1, 4}}
	got := pageAfter(t, without(repro, func(l listed) bool { return l.seq == 2 }), repro[2], 3, rand.New("repro"))
	sameIDs(t, "the page after the deleted p2", got, []string{idOf(repro[1]), idOf(repro[0])})

	n := slotCount(today)
	for _, first := range []int64{0, 5, n - 6, n - 3, n - 1, n, 3*n - 2} {
		older := []listed{{today, first}, {today, first + 1}, {today, first + 2}}
		var run []listed
		for i := int64(0); i < 7; i++ {
			run = append(run, listed{today + 1, first + 3 + i})
		}
		all := append(append([]listed(nil), older...), run...)
		for _, x := range run {
			for name, drop := range map[string]func(listed) bool{
				"only it":                  func(l listed) bool { return l == x },
				"it and what is older":     func(l listed) bool { return l.ms == x.ms && l.seq <= x.seq },
				"it and what is newer":     func(l listed) bool { return l.ms == x.ms && l.seq >= x.seq },
				"it and the older instant": func(l listed) bool { return l == x || l.ms < x.ms },
				"every other of the run":   func(l listed) bool { return l.ms == x.ms && (l == x || l.seq%2 == 0) },
			} {
				alive := without(all, drop)
				for _, limit := range []int{1, 3, 20} {
					sameIDs(t, fmt.Sprintf("first %d, after %s with %s deleted, by %d", first, idOf(x), name, limit),
						pageAfter(t, alive, x, limit, rand.New("variants")), olderThanItem(alive, x, limit))
				}
			}
		}
	}
}

// TestPagesSurviveDeletesOfTheItemACursorNames pages random listings against a
// brute-force model. After each page the model deletes some of what is left, the
// item the cursor names included in half the steps, and the next page must be the
// items older than that item, whatever is gone. Runs span at most half the slot
// count, which is what a deleted cursor item can be placed within.
func TestPagesSurviveDeletesOfTheItemACursorNames(t *testing.T) {
	rnd := rand.New("deletes")
	walks, deletedNamed := 0, 0
	for walks < 1500 {
		era := []int64{0, 1, 1 << 20, today, 1 << 41, 1 << 43, 1 << 49, -1, -(1 << 20), -(1 << 41)}[rnd.Intn(10)]
		n := slotCount(era)
		if n < 3 {
			continue
		}
		half := int(n / 2)
		var ls []listed
		seq := int64(rnd.Intn(int(3*n))) + int64(rnd.Intn(2))*(1<<33)
		for run := 0; run < 1+rnd.Intn(4); run++ {
			size := 1 + rnd.Intn(min(half, 40))
			ms := era + int64(rnd.Intn(4))
			for i := 0; i < size; i++ {
				ls = append(ls, listed{ms, seq})
				seq++
			}
		}
		// A run (of one instant) must span at most half the slot count.
		low, high := map[int64]int64{}, map[int64]int64{}
		for _, l := range ls {
			if lo, ok := low[l.ms]; !ok || l.seq < lo {
				low[l.ms] = l.seq
			}
			high[l.ms] = max(high[l.ms], l.seq)
		}
		span := false
		for ms := range low {
			span = span || int(high[ms]-low[ms]+1) > half
		}
		if span {
			continue
		}
		walks++
		limit := 1 + rnd.Intn(7)
		alive := append([]listed(nil), ls...)
		var x *listed // the item the last page ended at: the one its cursor was made from
		for step := 0; step <= len(ls)+1; step++ {
			var until []cursor
			var want []string
			if x == nil {
				for _, l := range served(alive)[:min(limit, len(alive))] {
					want = append(want, idOf(l))
				}
			} else {
				// The client holds the cursor the previous page answered, made before
				// anything was deleted; the listing resolves it against what is left.
				c, err := parseCursor(wire(pageCursor(x.ms, x.seq)))
				if err != nil {
					t.Fatal(err)
				}
				until = []cursor{c}
				want = olderThanItem(alive, *x, limit)
			}
			out := pageOf(recsOf(alive, rnd), "createdAt", nil, until, false, limit)
			var got []string
			var last listed
			for _, r := range out["page"].([]any) {
				m := r.(map[string]any)
				got = append(got, m["id"].(string))
				last = listed{ms: m["createdAt"].(int64), seq: m["seq"].(int64)}
			}
			sameIDs(t, fmt.Sprintf("walk %d (era %d, %d items, limit %d), step %d", walks, era, len(ls), limit, step), got, want)
			if len(got) == 0 || out["pagination"].(map[string]any)["next"] == nil {
				break
			}
			x = &last
			// Delete between pages: the item the cursor names in half the steps, and a few others.
			if rnd.Intn(2) == 0 {
				deletedNamed++
				alive = without(alive, func(l listed) bool { return l == last })
			}
			for k := rnd.Intn(4); k > 0 && len(alive) > 0; k-- {
				d := alive[rnd.Intn(len(alive))]
				alive = without(alive, func(l listed) bool { return l == d })
			}
		}
	}
	if deletedNamed < 500 {
		t.Fatalf("only %d of the steps deleted the item the cursor names", deletedNamed)
	}
}
