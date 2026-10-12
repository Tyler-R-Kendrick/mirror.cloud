package engine

import (
	"math"
	"testing"
)

// TestPageCursorIsAnExactNumberThatStillReadsAsTheTimestamp holds what the
// document and the SDK need of a cursor (a number) and what a client that
// floors it, or sends it back as text, gets.
func TestPageCursorIsAnExactNumberThatStillReadsAsTheTimestamp(t *testing.T) {
	const ms = int64(1791836623140)
	seen := map[float64]int64{}
	for seq := int64(0); seq < 2*cursorSlots; seq++ {
		c := pageCursor(ms, seq)
		if int64(math.Floor(c)) != ms {
			t.Fatalf("seq %d: cursor %v does not floor to the timestamp", seq, c)
		}
		if c == float64(ms) {
			t.Fatalf("seq %d: cursor %v is indistinguishable from a bare timestamp", seq, c)
		}
		if prev, dup := seen[c]; dup && (seq-prev)%cursorSlots != 0 {
			t.Fatalf("seq %d and %d, not a whole wrap apart, share cursor %v", prev, seq, c)
		}
		seen[c] = seq
		// What goes on the wire and comes back as the text of the number.
		got, err := parseCursor(trimFloat(c))
		if err != nil || !got.composite || got.ms != ms || got.slot != cursorSlot(seq) {
			t.Fatalf("seq %d: %v read back as %+v (%v)", seq, c, got, err)
		}
	}
}

func TestACursorWithNoFractionIsTheOldBareTimestamp(t *testing.T) {
	for _, in := range []any{"1791836623140", float64(1791836623140), int64(1791836623140), "1791836623140.0"} {
		c, err := parseCursor(in)
		if err != nil || !c.set || c.composite || c.ms != 1791836623140 {
			t.Errorf("parseCursor(%v) = %+v, %v; want the bare timestamp", in, c, err)
		}
	}
	bare, _ := parseCursor("100")
	for _, tc := range []struct {
		ms          int64
		older, newr bool
	}{{99, true, false}, {100, false, false}, {101, false, true}} {
		if got := olderThanCursor(tc.ms, 7, bare); got != tc.older {
			t.Errorf("olderThanCursor(%d, bare 100) = %v", tc.ms, got)
		}
		if got := newerThanCursor(tc.ms, 7, bare); got != tc.newr {
			t.Errorf("newerThanCursor(%d, bare 100) = %v", tc.ms, got)
		}
	}
}

func TestNoCursorCutsNothingAndABadOneIsAnError(t *testing.T) {
	for _, in := range []any{nil, "", "  "} {
		c, err := parseCursor(in)
		if err != nil || c.set || !olderThanCursor(1, 1, c) || !newerThanCursor(1, 1, c) {
			t.Errorf("parseCursor(%#v) = %+v, %v; want no cursor that cuts nothing", in, c, err)
		}
	}
	for _, in := range []any{"later", "1e999x", true} {
		if _, err := parseCursor(in); err == nil {
			t.Errorf("parseCursor(%#v) read a cursor out of something that is not a number", in)
		}
	}
}

// TestACompositeCursorCutsBetweenItemsOfOneMillisecond is the case the cursor
// exists for: fifty items on one millisecond, paged by seven, lose none.
func TestACompositeCursorCutsBetweenItemsOfOneMillisecond(t *testing.T) {
	const ms = int64(1791836623140)
	type item struct{ ms, seq int64 }
	var all []item
	for seq := int64(1); seq <= 50; seq++ {
		all = append(all, item{ms, seq})
	}
	// Newest first, as the listings run.
	for i, j := 0, len(all)-1; i < j; i, j = i+1, j-1 {
		all[i], all[j] = all[j], all[i]
	}
	var got []int64
	cur := cursor{}
	for pages := 0; pages < 20; pages++ {
		var page []item
		for _, it := range all {
			if olderThanCursor(it.ms, it.seq, cur) {
				page = append(page, it)
			}
		}
		if len(page) == 0 {
			break
		}
		if len(page) > 7 {
			page = page[:7]
		}
		for _, it := range page {
			got = append(got, it.seq)
		}
		last := page[len(page)-1]
		var err error
		cur, err = parseCursor(trimFloat(pageCursor(last.ms, last.seq)))
		if err != nil {
			t.Fatal(err)
		}
	}
	if len(got) != 50 {
		t.Fatalf("paged %d of 50: %v", len(got), got)
	}
	for i, seq := range got {
		if seq != int64(50-i) {
			t.Fatalf("page order broke at %d: %v", i, got)
		}
	}
	// And going the other way from the middle: since the 20th item newest-first.
	mid, _ := parseCursor(trimFloat(pageCursor(ms, 31)))
	var newer []int64
	for _, it := range all {
		if newerThanCursor(it.ms, it.seq, mid) {
			newer = append(newer, it.seq)
		}
	}
	if len(newer) != 19 || newer[0] != 50 || newer[18] != 32 {
		t.Fatalf("items newer than seq 31 = %v", newer)
	}
}

func TestTheCursorFunctionsAreInTheLanguage(t *testing.T) {
	items := []any{
		map[string]any{"createdAt": int64(1791836623140), "seq": int64(7)},
		map[string]any{"createdAt": int64(1791836623140), "seq": int64(6)},
	}
	vars := map[string]any{"items": items}
	if got := evalCEL(t, `cursorAt(items, 'createdAt', 1)`, vars); got != 1791836623140+7.0/1024 {
		t.Errorf("cursorAt from CEL = %v", got)
	}
	if got := evalCEL(t, `cursorAt(items, 'createdAt', 2)`, vars); got != nil {
		t.Errorf("cursorAt past the end = %v, want null", got)
	}
	vars = map[string]any{"c": "1791836623140.005859375"} // seq 5
	if got := evalCEL(t, `olderThanCursor(1791836623140, 5, c)`, vars); got != false {
		t.Errorf("the item the cursor names is not older than it: %v", got)
	}
	if got := evalCEL(t, `olderThanCursor(1791836623140, 4, c)`, vars); got != true {
		t.Errorf("the item before it is older: %v", got)
	}
	if got := evalCEL(t, `newerThanCursor(1791836623140, 6, c)`, vars); got != true {
		t.Errorf("the item after it is newer: %v", got)
	}
	if got := evalCEL(t, `olderThanCursor(1, 1, '')`, nil); got != true {
		t.Errorf("an empty cursor cuts nothing: %v", got)
	}
	if got := evalCEL(t, `validCursors(['', '1791836623140', '1791836623140.005859375'])`, nil); got != true {
		t.Errorf("validCursors of cursors = %v", got)
	}
	if got := evalCEL(t, `validCursors(['1791836623140', 'yesterday'])`, nil); got != false {
		t.Errorf("validCursors of a word = %v", got)
	}
}
