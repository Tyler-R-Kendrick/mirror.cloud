package engine

import (
	"fmt"
	"math"
	"math/bits"
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
// slot / 2^k, where the slot is 1 + the record's creation sequence modulo the
// number of slots, 2^k - 1, and k is as many fraction bits as the timestamp leaves
// a float64 (53 less the bits of the millisecond, and at most 12). A millisecond
// of today has 41 bits, so k is 12 and there are 4095 slots; they halve each time
// the timestamp gains a bit (2039, 2109, ...), and at 2^52 milliseconds none are
// left and the cursor is a whole timestamp. Both parts are exact in a float64, the
// cursor still reads as the timestamp to anything that floors it, and a slot is
// never 0, so a cursor with a fraction is never mistaken for a bare timestamp.
//
// A slot is the sequence modulo the slot count, so it wraps, and a cursor alone
// cannot say which of the items that share its slot it named. A listing says: it
// has the items, and the oldest of those made in the cursor's millisecond bounds
// the sequences that can have been meant, since the slots of a run of fewer items
// than there are slots are all different (see pageOf). That is the ceiling: while
// the items made in one millisecond, in one listing, span fewer sequence numbers
// than there are slots (4095 today), a cursor names exactly the item it was made
// from, wherever the run falls in the count. Past it a slot is shared by two
// items of the run and the boundary is ambiguous again; the listing still answers,
// its page boundary inside that run may repeat or skip an item.
//
// A cursor with no fraction is the old form, a bare millisecond timestamp, and
// means what it always meant: strictly before (or after) that millisecond. That
// is what a client that computes its own timestamp sends, and what a client of an
// earlier version of the emulator echoed.

const (
	floatBits   = 53 // bits a float64 holds of an integer and its fraction together
	maxSlotBits = 12 // fraction bits a cursor uses at most
	maxCursorMS = 1 << floatBits
	cursorForm  = "a cursor is a decimal number, as pagination.next of an earlier page is"
)

// slotBits is how many bits of fraction a cursor made at this millisecond has.
func slotBits(ms int64) int {
	if ms < 0 {
		return 0
	}
	return min(max(floatBits-bits.Len64(uint64(ms)), 0), maxSlotBits)
}

// slotCount is how many places a cursor tells apart within this millisecond.
func slotCount(ms int64) int64 { return int64(1)<<slotBits(ms) - 1 }

// slotOf is the item's place among those created in its millisecond: 1 to
// slotCount, 0 where the millisecond has no room for a fraction.
func slotOf(ms, seq int64) int64 {
	n := slotCount(ms)
	if n == 0 {
		return 0
	}
	return 1 + ((seq%n)+n)%n
}

// pageCursor is what `pagination.next` answers for an item.
func pageCursor(ms, seq int64) float64 {
	slot := slotOf(ms, seq)
	if slot == 0 {
		return float64(ms)
	}
	return float64(ms) + float64(slot)/float64(int64(1)<<slotBits(ms))
}

// cursor is a client's cursor, read.
type cursor struct {
	set  bool  // false: the client sent none, and nothing is cut
	ms   int64 // the millisecond
	slot int64 // the place among that millisecond's items; 0 for a bare timestamp
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
		if err != nil {
			return cursor{}, fmt.Errorf("the cursor %q is not a number: %s", t, cursorForm)
		}
		f = n
	default:
		n, ok := toFloat(v)
		if !ok {
			return cursor{}, fmt.Errorf("the cursor %v is not a number: %s", v, cursorForm)
		}
		f = n
	}
	return cursorOf(f)
}

// cursorOf reads a number as a cursor. A number past the range an integer and a
// float64 share is the bound it is: every item is before 1e19 and after -1e19, and
// converting it to a millisecond count would overflow into some other bound.
func cursorOf(f float64) (cursor, error) {
	if math.IsNaN(f) || math.IsInf(f, 0) {
		return cursor{}, fmt.Errorf("the cursor %v is not a number: %s", f, cursorForm)
	}
	f = min(max(f, -maxCursorMS), maxCursorMS)
	ms := math.Floor(f)
	frac := f - ms
	if frac == 0 {
		return cursor{set: true, ms: int64(ms)}, nil
	}
	places := frac * float64(int64(1)<<slotBits(int64(ms)))
	if places != math.Trunc(places) {
		return cursor{}, fmt.Errorf("the cursor %v has a fraction that is no place among the items of a millisecond: %s", f, cursorForm)
	}
	return cursor{set: true, ms: int64(ms), slot: int64(places)}, nil
}
