package engine

import (
	"math"
	"strconv"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/rand"
)

const today = int64(1791836623140) // a millisecond of this era: 41 bits

// wire is a cursor as a client gets it back: the shortest text of the number,
// which is what a JSON encoder writes and a JavaScript String(number) prints.
func wire(f float64) string { return strconv.FormatFloat(f, 'f', -1, 64) }

// TestTheSlotsAHundredYearsLeaveAreAsManyAsTheTimestampLetsInAFloat holds the
// ceiling the cursor documents: as many places as the bits a timestamp leaves in a
// float64's 53, at most 12 of them, none from 2^52.
func TestTheSlotsAHundredYearsLeaveAreAsManyAsTheTimestampLetsInAFloat(t *testing.T) {
	for _, tc := range []struct {
		ms    int64
		slots int64
	}{
		{0, 4095}, {1, 4095}, {1 << 20, 4095}, {today, 4095}, {1<<41 - 1, 4095},
		{1 << 41, 2047}, {1<<42 - 1, 2047}, {1 << 42, 1023}, {1 << 43, 511},
		{1 << 44, 255}, {1 << 50, 3}, {1 << 51, 1}, {1<<52 - 1, 1}, {1 << 52, 0},
		{1<<53 - 1, 0}, {-1, 0},
	} {
		if got := slotCount(tc.ms); got != tc.slots {
			t.Errorf("slotCount(%d) = %d, want %d", tc.ms, got, tc.slots)
		}
	}
}

// TestPageCursorIsAnExactNumberAtEveryMagnitude holds what the document and the
// SDK need of a cursor (a number) and what a client that floors it, or sends it
// back as text, gets, for a timestamp of every length from 0 to 2^52.
func TestPageCursorIsAnExactNumberAtEveryMagnitude(t *testing.T) {
	check := func(ms, seq int64) {
		t.Helper()
		c := pageCursor(ms, seq)
		got, err := parseCursor(wire(c))
		if err != nil {
			t.Fatalf("ms %d seq %d: %v does not read back: %v", ms, seq, c, err)
		}
		if n := slotCount(ms); n == 0 {
			if c != float64(ms) || got.slot != 0 || got.ms != ms {
				t.Fatalf("ms %d has no room for a slot and answered %v read as %+v", ms, c, got)
			}
			return
		}
		if int64(math.Floor(c)) != ms {
			t.Fatalf("ms %d seq %d: cursor %v does not floor to the timestamp", ms, seq, c)
		}
		if c == float64(ms) {
			t.Fatalf("ms %d seq %d: cursor %v is indistinguishable from a bare timestamp", ms, seq, c)
		}
		if got.ms != ms || got.slot != slotOf(ms, seq) {
			t.Fatalf("ms %d seq %d: %v read back as %+v, want slot %d", ms, seq, c, got, slotOf(ms, seq))
		}
	}
	rnd := rand.New("cursor")
	for bit := 0; bit <= 53; bit++ {
		for _, ms := range []int64{1<<bit - 1, 1 << bit, 1<<bit + 1} {
			if ms < 0 || ms > 1<<53 {
				continue
			}
			n := slotCount(ms)
			for _, seq := range []int64{0, 1, n - 1, n, n + 1, 2 * n, 1<<40 + 3, -1} {
				check(ms, seq)
			}
		}
	}
	for i := 0; i < 200000; i++ {
		check(int64(rnd.Intn(1<<52)), int64(rnd.Intn(1<<24)))
	}
}

// TestACursorWithNoFractionIsTheBareTimestamp keeps the old form: a whole number
// is a millisecond, and means strictly before it (until) or after it (since).
func TestACursorWithNoFractionIsTheBareTimestamp(t *testing.T) {
	for _, in := range []any{"1791836623140", float64(1791836623140), int64(1791836623140), "1791836623140.0"} {
		c, err := parseCursor(in)
		if err != nil || !c.set || c.slot != 0 || c.ms != today {
			t.Errorf("parseCursor(%v) = %+v, %v; want the bare timestamp", in, c, err)
		}
	}
	bare := bound{cursor: cursor{set: true, ms: 100}}
	for _, tc := range []struct {
		ms          int64
		older, newr bool
	}{{99, true, false}, {100, false, false}, {101, false, true}} {
		e := entry{ms: tc.ms, seq: 7}
		if got := bare.olderThan(e); got != tc.older {
			t.Errorf("olderThan(%d, bare 100) = %v", tc.ms, got)
		}
		if got := bare.newerThan(e); got != tc.newr {
			t.Errorf("newerThan(%d, bare 100) = %v", tc.ms, got)
		}
	}
}

func TestNoCursorCutsNothingAndABadOneIsAnError(t *testing.T) {
	for _, in := range []any{nil, "", "  "} {
		c, err := parseCursor(in)
		if err != nil || c.set || !(bound{cursor: c}).olderThan(entry{ms: 1}) || !(bound{cursor: c}).newerThan(entry{ms: 1}) {
			t.Errorf("parseCursor(%#v) = %+v, %v; want no cursor that cuts nothing", in, c, err)
		}
	}
	for _, in := range []any{"later", "1e999x", true, math.NaN(), math.Inf(1)} {
		if _, err := parseCursor(in); err == nil {
			t.Errorf("parseCursor(%#v) read a cursor out of something that is not a number", in)
		}
	}
}
