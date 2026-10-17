package engine

import (
	"fmt"
	"math"
	"math/bits"
	"regexp"
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
// a float64 (53 less the bits of the magnitude of the millisecond, and at most 12:
// before 1970 as after it). A millisecond of today has 41 bits, so k is 12 and
// there are 4095 slots; they halve each time the timestamp gains a bit (2039,
// 2109, ...), and at 2^52 milliseconds from the epoch, either way, none are left
// and the cursor is a whole timestamp. Both parts are exact in a float64, the
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
// items of the run and the cursor of the newer is read as the older: the listing
// still answers, and a walk silently skips what lies between, up to one slot count
// of items per wrap, and never serves one twice (page_ceiling_test.go measures it).
// For the same reason a cursor is not monotone at the wrap: sequences 4096, 4095
// and 4094 answer 2/4096, 1/4096 and 4095/4096.
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

// slotBits is how many bits of fraction a cursor made at this millisecond has. They
// are sized by the magnitude, so a millisecond before 1970 has as many as the same
// distance after it: the double ms + slot/2^k lies between ms and ms+1, so it is no
// larger in magnitude than ms (or ms+1 where ms is -1) and no less exact.
func slotBits(ms int64) int {
	magnitude := uint64(ms)
	if ms < 0 {
		magnitude = -magnitude // two's complement, so MinInt64 is 2^63 and has no bits left
	}
	return min(max(floatBits-bits.Len64(magnitude), 0), maxSlotBits)
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

// cursorGrammar is the text of a cursor: what a JavaScript client prints for a
// number (a minus sign, digits with no leading zero, a fraction, an exponent) and
// nothing else a number parser would take.
var cursorGrammar = regexp.MustCompile(`^-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?(?:[eE][+-]?[0-9]+)?$`)

// parseCursor reads what a request carried as `until`, `since` or `from`: a
// number, or the text of one. Nothing, or text of nothing but spaces, is no
// cursor. Anything else that is not a decimal number is an error rather than no
// cursor: a typo that quietly returned the first page again would loop a client
// for ever, and ParseFloat alone takes `.5`, `1_000`, `0x1p4`, `+5` and `00012`.
func parseCursor(v any) (cursor, error) {
	var f float64
	switch t := v.(type) {
	case nil:
		return cursor{}, nil
	case string:
		if strings.TrimSpace(t) == "" {
			return cursor{}, nil
		}
		if !cursorGrammar.MatchString(t) {
			return cursor{}, fmt.Errorf("the cursor %q is not a number: %s", t, cursorForm)
		}
		n, err := strconv.ParseFloat(t, 64)
		if err != nil {
			return cursor{}, fmt.Errorf("the cursor %q is not a number in range: %s", t, cursorForm)
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
