package engine

import (
	"fmt"
	"sort"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/rand"
)

type listed struct{ ms, seq int64 }

func (l listed) rec() map[string]any {
	return map[string]any{"createdAt": l.ms, "seq": l.seq, "id": fmt.Sprintf("%d/%d", l.ms, l.seq)}
}

func recsOf(ls []listed, rnd *rand.Seeded) []any {
	out := make([]any, len(ls))
	for i, l := range ls {
		out[i] = l.rec()
	}
	// The store lists by key, which is not any order of these.
	for i := len(out) - 1; i > 0; i-- {
		j := rnd.Intn(i + 1)
		out[i], out[j] = out[j], out[i]
	}
	return out
}

// served is what a listing is meant to serve: the later instant first, and of one
// instant the later made.
func served(ls []listed) []listed {
	out := append([]listed(nil), ls...)
	sort.Slice(out, func(i, j int) bool {
		if out[i].ms != out[j].ms {
			return out[i].ms > out[j].ms
		}
		return out[i].seq > out[j].seq
	})
	return out
}

// walkPages pages a listing the way a client does: the next of each page, as
// text, is the `until` of the one after.
func walkPages(t *testing.T, items []any, limit int) []string {
	t.Helper()
	var got []string
	var until []cursor
	for page := 0; page <= len(items)+1; page++ {
		out := pageOf(items, "createdAt", nil, until, false, limit)
		recs := out["page"].([]any)
		for _, r := range recs {
			got = append(got, r.(map[string]any)["id"].(string))
		}
		next := out["pagination"].(map[string]any)["next"]
		if next == nil {
			return got
		}
		if len(recs) != limit {
			t.Fatalf("a page of %d says there is more, the limit is %d", len(recs), limit)
		}
		c, err := parseCursor(wire(next.(float64)))
		if err != nil {
			t.Fatal(err)
		}
		until = []cursor{c}
	}
	t.Fatalf("still paging after %d pages", len(items)+1)
	return nil
}

func wantIDs(ls []listed) []string {
	var ids []string
	for _, l := range served(ls) {
		ids = append(ids, fmt.Sprintf("%d/%d", l.ms, l.seq))
	}
	return ids
}

func sameIDs(t *testing.T, what string, got, want []string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s: served %d of %d: %v", what, len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("%s: item %d is %s, want %s", what, i, got[i], want[i])
		}
	}
}

// run is count items made in the millisecond ms, in creation order from seq first.
func run(ms, first int64, count int) []listed {
	out := make([]listed, count)
	for i := range out {
		out[i] = listed{ms, first + int64(i)}
	}
	return out
}

// TestPagesCutBetweenItemsOfOneMillisecondWhereverTheSlotsWrap is the case the
// cursor exists for, at the place it broke: two items of one millisecond whose
// sequences are either side of a multiple of the slot count, and so whose slots are
// in the opposite order.
func TestPagesCutBetweenItemsOfOneMillisecondWhereverTheSlotsWrap(t *testing.T) {
	n := slotCount(today)
	for _, first := range []int64{0, 1, n - 2, n - 1, n, n + 1, 2*n - 1, 7*n - 3, 1 << 30} {
		for _, count := range []int{2, 3, 50, int(n)} {
			ls := run(today, first, count)
			limits := []int{1, 2, 7, count - 1, count}
			if count == int(n) {
				limits = []int{100, 1000, count - 1}
			}
			for _, limit := range limits {
				if limit < 1 {
					continue
				}
				sameIDs(t, fmt.Sprintf("%d items from seq %d by %d", count, first, limit),
					walkPages(t, recsOf(ls, rand.New(fmt.Sprint("first", first))), limit), wantIDs(ls))
			}
		}
	}
}

// TestPagesServeAnyNumberOfItemsUpToTheSlotCountOfOneMillisecond holds the ceiling
// from both sides of a thousand: 1024, 1100 and 3000 items of one instant, and as
// many as there are slots, each paged by a limit that puts boundaries all over.
func TestPagesServeAnyNumberOfItemsUpToTheSlotCountOfOneMillisecond(t *testing.T) {
	n := int(slotCount(today))
	for _, count := range []int{1023, 1024, 1100, 3000, n} {
		for _, limit := range []int{7, 100} {
			ls := run(today, 5, count)
			sameIDs(t, fmt.Sprintf("%d items by %d", count, limit),
				walkPages(t, recsOf(ls, rand.New(fmt.Sprint("count", count))), limit), wantIDs(ls))
		}
	}
}

func TestPagesOfAThousandAndTwentyFourByTwo(t *testing.T) {
	ls := run(today, 0, 1024)
	sameIDs(t, "1024 items by 2", walkPages(t, recsOf(ls, rand.New("two")), 2), wantIDs(ls))
}

// TestPagesFollowTheInstantNotTheSequenceWhenTheClockStepsBack makes the creation
// order and the instant order differ and pages one at a time.
func TestPagesFollowTheInstantNotTheSequenceWhenTheClockStepsBack(t *testing.T) {
	ls := []listed{{today + 1000000, 1}, {today + 900000, 2}, {today + 1000000, 3}}
	got := walkPages(t, recsOf(ls, rand.New("one")), 1)
	sameIDs(t, "a clock that stepped back", got, wantIDs(ls))
	if want := fmt.Sprintf("%d/3", today+1000000); got[0] != want {
		t.Errorf("the first is %s, want the later made of the later instant, %s", got[0], want)
	}
}

// TestPagesNeverLoseOrRepeatAnItemOnAWanderingClock pages random listings: runs of
// several sizes at instants in no order, sequences that start anywhere, a limit at
// random, the store's order shuffled.
func TestPagesNeverLoseOrRepeatAnItemOnAWanderingClock(t *testing.T) {
	rnd := rand.New("pages")
	for trial := 0; trial < 400; trial++ {
		era := []int64{0, 1 << 20, today, 1 << 41, 1 << 43, 1 << 49}[rnd.Intn(6)]
		n := slotCount(era)
		var ls []listed
		seq := int64(rnd.Intn(int(3*max(n, 1)))) + int64(rnd.Intn(2))*(1<<33)
		for run := 0; run < 1+rnd.Intn(5); run++ {
			size := 1 + rnd.Intn(int(min(max(n, 1), 60)))
			if rnd.Intn(8) == 0 {
				size = int(max(n, 1)) // a run as long as there are slots
			}
			ms := era + int64(rnd.Intn(5))
			for i := 0; i < size; i++ {
				ls = append(ls, listed{ms, seq})
				seq++
				if rnd.Intn(6) == 0 { // another instant's item is made in between
					ls = append(ls, listed{era + int64(rnd.Intn(5)), seq})
					seq++
				}
			}
		}
		// The sequences of the items of one millisecond must lie within the slot
		// count of each other: that is the ceiling.
		low, high := map[int64]int64{}, map[int64]int64{}
		for _, l := range ls {
			if lo, ok := low[l.ms]; !ok || l.seq < lo {
				low[l.ms] = l.seq
			}
			if l.seq > high[l.ms] {
				high[l.ms] = l.seq
			}
		}
		skip := n == 0
		for ms := range low {
			skip = skip || high[ms]-low[ms]+1 > n
		}
		if skip {
			continue
		}
		limit := 1 + rnd.Intn(11)
		sameIDs(t, fmt.Sprintf("trial %d (era %d, %d items, limit %d)", trial, era, len(ls), limit),
			walkPages(t, recsOf(ls, rnd), limit), wantIDs(ls))
	}
}

func TestSinceKeepsTheItemsNewerThanACursorWhereverTheSlotsWrap(t *testing.T) {
	n := slotCount(today)
	ls := run(today, n-3, 8) // the slots wrap after the third
	boundary := ls[3]
	c, err := parseCursor(wire(pageCursor(boundary.ms, boundary.seq)))
	if err != nil {
		t.Fatal(err)
	}
	out := pageOf(recsOf(ls, rand.New("one")), "createdAt", []cursor{c}, nil, false, 100)
	var got []string
	for _, r := range out["page"].([]any) {
		got = append(got, r.(map[string]any)["id"].(string))
	}
	var want []string
	for _, l := range served(ls) {
		if l.seq > boundary.seq {
			want = append(want, fmt.Sprintf("%d/%d", l.ms, l.seq))
		}
	}
	sameIDs(t, "since the fourth", got, want)
	if len(got) != 4 {
		t.Fatalf("kept %d, want the 4 made after it", len(got))
	}
}

func TestTheFirstCursorAndAnAscendingPage(t *testing.T) {
	ls := run(today, 0, 5)
	out := pageOf(recsOf(ls, rand.New("one")), "createdAt", nil, nil, true, 2)
	recs := out["page"].([]any)
	if len(recs) != 2 || recs[0].(map[string]any)["seq"] != int64(0) || recs[1].(map[string]any)["seq"] != int64(1) {
		t.Fatalf("an ascending page of 2 = %v", recs)
	}
	if out["first"] != pageCursor(today, 0) || out["pagination"].(map[string]any)["next"] != pageCursor(today, 1) {
		t.Errorf("first and next = %v, %v", out["first"], out["pagination"])
	}
	empty := pageOf(nil, "createdAt", nil, nil, false, 20)
	if empty["first"] != nil || empty["pagination"].(map[string]any)["next"] != nil || len(empty["page"].([]any)) != 0 {
		t.Errorf("an empty listing = %v", empty)
	}
}

func TestThePageFunctionsAreInTheLanguage(t *testing.T) {
	items := []any{
		map[string]any{"createdAt": today, "seq": int64(6), "id": "a"},
		map[string]any{"createdAt": today, "seq": int64(7), "id": "b"},
		map[string]any{"createdAt": today - 1, "seq": int64(8), "id": "c"},
	}
	vars := map[string]any{"items": items, "until": ""}
	ids := func(src string) []any {
		t.Helper()
		var out []any
		for _, r := range evalCEL(t, src, vars).([]any) {
			out = append(out, r.(map[string]any)["id"])
		}
		return out
	}
	got := ids(`pageOf(items, 'createdAt', [], [until], false, 2).page`)
	if len(got) != 2 || got[0] != "b" || got[1] != "a" {
		t.Errorf("page of 2 = %v", got)
	}
	if next := evalCEL(t, `pageOf(items, 'createdAt', [], [until], false, 2).pagination.next`, vars); next != pageCursor(today, 6) {
		t.Errorf("next = %v, want the cursor of the second item", next)
	}
	if count := evalCEL(t, `pageOf(items, 'createdAt', [], [until], false, 2).pagination.count`, vars); count != int64(2) {
		t.Errorf("count = %v", count)
	}
	vars["until"] = wire(pageCursor(today, 7))
	if got := ids(`pageOf(items, 'createdAt', [], [until], false, 2).page`); len(got) != 2 || got[0] != "a" || got[1] != "c" {
		t.Errorf("the page after b = %v", got)
	}
	if next := evalCEL(t, `pageOf(items, 'createdAt', [], [until], false, 2).pagination.next`, vars); next != nil {
		t.Errorf("the last page has a next: %v", next)
	}
	if ok := evalCEL(t, `validCursors(['', '1791836623140', '1791836623140.5'])`, nil); ok != true {
		t.Errorf("validCursors of cursors = %v", ok)
	}
	if ok := evalCEL(t, `validCursors(['1791836623140', 'yesterday'])`, nil); ok != false {
		t.Errorf("validCursors of a word = %v", ok)
	}
}
