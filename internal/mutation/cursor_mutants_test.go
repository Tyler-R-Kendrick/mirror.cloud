package mutation

import "path/filepath"

// The mutants of the listing cursor (internal/engine/cursor.go) and the page it
// cuts (page.go): the order a listing serves, the item a cursor names where the
// places wrap, the bits a cursor has at each magnitude, and the form it is read in.
// They are appended to the table so the shards and the uniqueness and needle checks
// treat them like the rest.
func init() {
	cursor := filepath.Join("internal", "engine", "cursor.go")
	page := filepath.Join("internal", "engine", "page.go")
	engine := "./internal/engine"
	mutants = append(mutants, []mutant{
		{
			name: "page-order-ties-oldest-first",
			file: page,
			old:  "\t\treturn a.seq > b.seq",
			new:  "\t\treturn a.seq < b.seq",
			pkg:  engine,
			run:  "TestPagesFollowTheInstantNotTheSequenceWhenTheClockStepsBack",
		},
		{
			name: "page-order-oldest-instant-first",
			file: page,
			old:  "\t\t\treturn a.ms > b.ms",
			new:  "\t\t\treturn a.ms < b.ms",
			pkg:  engine,
			run:  "TestPagesFollowTheInstantNotTheSequenceWhenTheClockStepsBack",
		},
		{
			name: "page-cursor-names-the-slot-not-the-sequence",
			file: page,
			old:  "b.seq = lo + (((c.slot-1-lo)%n)+n)%n",
			new:  "b.seq = c.slot - 1 + n*0",
			pkg:  engine,
			run:  "TestPagesCutBetweenItemsOfOneMillisecondWhereverTheSlotsWrap",
		},
		{
			name: "page-until-keeps-the-item-the-cursor-names",
			file: page,
			old:  "e.ms == b.ms && e.seq < b.seq)",
			new:  "e.ms == b.ms && e.seq <= b.seq)",
			pkg:  engine,
			run:  "TestPagesNeverLoseOrRepeatAnItemOnAWanderingClock|TestPagesServeAnyNumberOfItems",
		},
		{
			name: "page-since-keeps-the-item-the-cursor-names",
			file: page,
			old:  "e.ms == b.ms && e.seq > b.seq)",
			new:  "e.ms == b.ms && e.seq >= b.seq)",
			pkg:  engine,
			run:  "TestSinceKeepsTheItemsNewerThanACursorWhereverTheSlotsWrap",
		},
		{
			name: "cursor-fraction-capped-at-eleven-bits",
			file: cursor,
			old:  "maxSlotBits = 12",
			new:  "maxSlotBits = 11",
			pkg:  engine,
			run:  "TestTheSlotsAHundredYearsLeaveAreAsManyAsTheTimestampLetsInAFloat",
		},
		{
			name: "cursor-slot-can-be-zero",
			file: cursor,
			old:  "return 1 + ((seq%n)+n)%n",
			new:  "return ((seq % n) + n) % n",
			pkg:  engine,
			run:  "TestPageCursorIsAnExactNumberAtEveryMagnitude",
		},
		{
			name: "cursor-fraction-over-a-fixed-denominator",
			file: cursor,
			old:  "float64(slot)/float64(int64(1)<<slotBits(ms))",
			new:  "float64(slot)/1024",
			pkg:  engine,
			run:  "TestPageCursorIsAnExactNumberAtEveryMagnitude",
		},
		{
			name: "cursor-beyond-the-range-not-clamped-above",
			file: cursor,
			old:  "f = min(max(f, -maxCursorMS), maxCursorMS)",
			new:  "f = max(f, -maxCursorMS)",
			pkg:  engine,
			run:  "TestACursorBeyondTheRangeIsTheBoundItIs",
		},
		{
			name: "cursor-grammar-takes-leading-zeros",
			file: cursor,
			old:  "(?:0|[1-9][0-9]*)",
			new:  "(?:[0-9]+)",
			pkg:  engine,
			run:  "TestACursorIsADecimalNumberAndNothingAParserWouldTake",
		},
		{
			name: "cursor-fraction-that-is-no-place-is-rounded",
			file: cursor,
			old:  "if places != math.Trunc(places) {",
			new:  "if false {",
			pkg:  engine,
			run:  "TestACursorIsADecimalNumberAndNothingAParserWouldTake",
		},
		{
			name: "page-deleted-cursor-item-always-placed-above-the-run",
			file: page,
			old:  "if found && b.seq > hi && lo-(b.seq-n) < b.seq-hi {",
			new:  "if false {",
			pkg:  engine,
			run:  "TestAPageAfterADeletedItemKeepsWhatWasOlderThanIt|TestPagesSurviveDeletesOfTheItemACursorNames",
		},
		{
			name: "page-deleted-cursor-item-placed-at-the-farther-end",
			file: page,
			old:  "lo-(b.seq-n) < b.seq-hi {",
			new:  "lo-(b.seq-n) > b.seq-hi {",
			pkg:  engine,
			run:  "TestAPageAfterADeletedItemKeepsWhatWasOlderThanIt|TestPagesSurviveDeletesOfTheItemACursorNames",
		},
		{
			name: "cursor-places-sized-by-the-signed-millisecond",
			file: cursor,
			old:  "magnitude = -magnitude // two's complement, so MinInt64 is 2^63 and has no bits left",
			new:  "magnitude = magnitude",
			pkg:  engine,
			run:  "TestTheSlotsAHundredYearsLeaveAreAsManyAsTheTimestampLetsInAFloat|TestPageCursorIsAnExactNumberAtEveryMagnitude",
		},
	}...)
}
