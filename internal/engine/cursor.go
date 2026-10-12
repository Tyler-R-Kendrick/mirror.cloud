package engine

import (
	"fmt"
	"math"
	"strconv"
	"strings"
)

// A listing cursor that does not skip.
//
// Vercel pages by timestamp: a page answers `pagination.next`, the millisecond
// the last item on it was created, and the client sends it back as `until` (or
// `from`, or, going the other way, `since`) and gets the items created before it.
// Two items created in one millisecond have one timestamp, so a page boundary that
// falls between them loses the second: `until=<t>` means "before t", and the item
// the page did not reach is not before t, nor is it on the page. On a clock that
// does not move (every request of a test on a controllable clock) that is every
// item after the first page.
//
// The document types the cursor as a number, and the SDK validates it as one, so
// the cursor stays a number. It keeps the timestamp in its integer part and puts
// the item's place among those created in the same millisecond in the fraction:
// slot/1024, the slot being 1 + the record's creation sequence modulo 1023. Both
// parts are exact in a float64 (a millisecond timestamp needs 41 bits, the
// fraction 10), the cursor still reads as the timestamp to anything that floors
// it, and a slot is never 0, so a cursor with a fraction is never mistaken for a
// bare timestamp. Records created consecutively get consecutive slots, so up to
// 1023 created in one millisecond are told apart; beyond that the boundary is
// ambiguous again, which is the ceiling.
//
// A cursor with no fraction is the old form, a bare millisecond timestamp, and
// means what it always meant: strictly before (or after) that millisecond. That
// is what a client that computes its own timestamp sends, and what a client of an
// earlier version of the emulator echoed.
const cursorSlots = 1023

// cursorSlot is the item's place among those created in its millisecond.
func cursorSlot(seq int64) int64 {
	return 1 + ((seq%cursorSlots)+cursorSlots)%cursorSlots
}

// pageCursor is what `pagination.next` answers for the last item of a page.
func pageCursor(ms, seq int64) float64 {
	return float64(ms) + float64(cursorSlot(seq))/1024
}

// cursor is a client's cursor, read.
type cursor struct {
	set       bool  // false: the client sent none, and nothing is cut
	ms        int64 // the millisecond
	slot      int64 // the place among that millisecond's items; 0 for a bare timestamp
	composite bool
}

// parseCursor reads what a request carried as `until`, `since` or `from`: a
// number, or the text of one. Nothing, or the empty string, is no cursor. Text
// that is not a number is an error rather than no cursor: a typo that quietly
// returned the first page again would loop a client for ever.
func parseCursor(v any) (cursor, error) {
	var f float64
	switch t := v.(type) {
	case nil:
		return cursor{}, nil
	case string:
		s := strings.TrimSpace(t)
		if s == "" {
			return cursor{}, nil
		}
		n, err := strconv.ParseFloat(s, 64)
		if err != nil || math.IsNaN(n) || math.IsInf(n, 0) {
			return cursor{}, fmt.Errorf("the cursor %q is not a number", t)
		}
		f = n
	default:
		n, ok := toFloat(v)
		if !ok {
			return cursor{}, fmt.Errorf("the cursor %v is not a number", v)
		}
		f = n
	}
	ms := math.Floor(f)
	frac := f - ms
	if frac == 0 {
		return cursor{set: true, ms: int64(ms)}, nil
	}
	slot := int64(math.Round(frac * 1024))
	if slot < 1 || slot > cursorSlots {
		// A fraction that is no slot (a timestamp with a stray decimal) is
		// the millisecond it falls in, read as a bare timestamp.
		return cursor{set: true, ms: int64(ms)}, nil
	}
	return cursor{set: true, ms: int64(ms), slot: slot, composite: true}, nil
}

// olderThanCursor reports whether the item sorts strictly after the cursor in a
// listing that runs newest first: the items the next page is made of.
func olderThanCursor(ms, seq int64, c cursor) bool {
	switch {
	case !c.set:
		return true
	case !c.composite:
		return ms < c.ms
	}
	return ms < c.ms || (ms == c.ms && cursorSlot(seq) < c.slot)
}

// newerThanCursor is the other direction, for `since`: the items strictly before
// the cursor in a newest-first listing.
func newerThanCursor(ms, seq int64, c cursor) bool {
	switch {
	case !c.set:
		return true
	case !c.composite:
		return ms > c.ms
	}
	return ms > c.ms || (ms == c.ms && cursorSlot(seq) > c.slot)
}

// trimFloat prints a cursor the way it goes on the wire: the shortest text that
// reads back as the same number, which is what a JSON encoder writes.
func trimFloat(f float64) string {
	return strconv.FormatFloat(f, 'f', -1, 64)
}
