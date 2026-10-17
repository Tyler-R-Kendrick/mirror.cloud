package engine

import "sort"

// The page a listing answers.
//
// A listing runs newest first: the later instant first, and of two made at the
// same instant the one made later. That is one order, and the cut a cursor makes
// (cursor.go) is a cut of that order and no other, so a clock that steps back, and
// puts the creation order and the instant order at odds, cannot put a boundary
// where the items on either side of it are not.

// entry is a listed record with the two numbers it is ordered by.
type entry struct {
	rec     any
	ms, seq int64
}

func entriesOf(items []any, msMember string) []entry {
	out := make([]entry, 0, len(items))
	for _, it := range items {
		rec, _ := it.(map[string]any)
		ms, _ := toFloat(rec[msMember])
		seq, _ := toFloat(rec["seq"])
		out = append(out, entry{rec: it, ms: int64(ms), seq: int64(seq)})
	}
	return out
}

// bound is a cursor with the item it names found among the listing's items.
type bound struct {
	cursor
	seq int64 // the sequence of the item named; meaningful when slot != 0
}

// boundOf finds the item a cursor names. A bare timestamp names no item. A slot
// is a sequence modulo the slot count, so of the sequences that share it the one
// named is the first at or after the oldest made in the cursor's millisecond:
// every item of the run is within a slot count of that one, and the item the
// cursor was made from is one of them (cursor.go says what happens when more than
// a slot count were made in one millisecond).
//
// The item may be gone, deleted since the cursor was made, and the cursor still
// names the position it held between its neighbours. When the first sequence at or
// after the oldest lies beyond the newest left in the run, the named item was
// below the run or above it, and which is told by which end it is nearer (the
// two candidates are a slot count apart, and a run is shorter than that): below
// the run, everything left in the millisecond is newer than it; above, older.
// That is exact for a run of up to half the slot count; a longer one whose named
// item and every neighbour on one side are gone can be placed on the wrong side.
func boundOf(c cursor, es []entry) bound {
	b := bound{cursor: c}
	if c.slot == 0 {
		return b
	}
	n := slotCount(c.ms)
	lo, hi, found := int64(0), int64(0), false
	for _, e := range es {
		if e.ms != c.ms {
			continue
		}
		if !found {
			lo, hi, found = e.seq, e.seq, true
		}
		lo, hi = min(lo, e.seq), max(hi, e.seq)
	}
	b.seq = lo + (((c.slot-1-lo)%n)+n)%n
	if found && b.seq > hi && lo-(b.seq-n) < b.seq-hi {
		b.seq -= n
	}
	return b
}

// olderThan is whether the item sorts strictly after the bound in a listing that
// runs newest first: what `until` and `from` keep.
func (b bound) olderThan(e entry) bool {
	if !b.set {
		return true
	}
	return e.ms < b.ms || (b.slot != 0 && e.ms == b.ms && e.seq < b.seq)
}

// newerThan is the other direction, for `since`: what sorts strictly before it.
func (b bound) newerThan(e entry) bool {
	if !b.set {
		return true
	}
	return e.ms > b.ms || (b.slot != 0 && e.ms == b.ms && e.seq > b.seq)
}

// pageOf is the answer of a listing: the items in order, cut to those newer than
// every cursor of newer and older than every cursor of older, and its first limit
// as the page, with the cursor to ask for the next one. ascending reverses the
// order and not the cuts.
//
// The members it answers are page (the items), pagination ({count, next, prev};
// prev is always null) and first (the cursor of the first item of the cut, null
// when it is empty, for the listing that answers it as prev).
func pageOf(items []any, msMember string, newer, older []cursor, ascending bool, limit int) map[string]any {
	es := entriesOf(items, msMember)
	var keep []entry
	newerB, olderB := make([]bound, len(newer)), make([]bound, len(older))
	for i, c := range newer {
		newerB[i] = boundOf(c, es)
	}
	for i, c := range older {
		olderB[i] = boundOf(c, es)
	}
	for _, e := range es {
		in := true
		for _, b := range newerB {
			in = in && b.newerThan(e)
		}
		for _, b := range olderB {
			in = in && b.olderThan(e)
		}
		if in {
			keep = append(keep, e)
		}
	}
	sort.SliceStable(keep, func(i, j int) bool {
		a, b := keep[i], keep[j]
		if ascending {
			a, b = b, a
		}
		if a.ms != b.ms {
			return a.ms > b.ms
		}
		return a.seq > b.seq
	})

	limit = max(limit, 0)
	page := make([]any, 0, min(limit, len(keep)))
	for _, e := range keep[:min(limit, len(keep))] {
		page = append(page, e.rec)
	}
	var next, first any
	if limit > 0 && len(keep) > limit {
		next = pageCursor(keep[limit-1].ms, keep[limit-1].seq)
	}
	if len(keep) > 0 {
		first = pageCursor(keep[0].ms, keep[0].seq)
	}
	return map[string]any{
		"page":       page,
		"pagination": map[string]any{"count": int64(len(page)), "next": next, "prev": nil},
		"first":      first,
	}
}
