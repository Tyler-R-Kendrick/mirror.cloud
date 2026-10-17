package vercel

import (
	"fmt"
	"net/http"
	"testing"
	"time"
)

// A cursor names an item by its millisecond and its place among the items made in
// that millisecond. These tests make that place run past every size it was once
// given: more items in one millisecond than a slot count, and a run of items whose
// places wrap in the middle of it.

func makeTeams(t *testing.T, v *vx, from, to int) []pagedItem {
	t.Helper()
	return stamped(t, v, to-from, nil, func(i int) (string, reply) {
		r := v.do(http.MethodPost, "/v2/teams", map[string]any{"slug": fmt.Sprintf("run-team-%05d", from+i)})
		return str(r.body, "id"), r
	})
}

// TestVercelTeamsPageThroughManyItemsOfOneMillisecond makes the teams of one
// instant (the clock does not move) as many as 1024, 1100 and 3000 and pages them,
// each by a limit that puts the page boundaries on different items.
func TestVercelTeamsPageThroughManyItemsOfOneMillisecond(t *testing.T) {
	for _, tc := range []struct{ teams, limit int }{{1024, 2}, {1100, 100}, {3000, 7}} {
		tc := tc
		t.Run(fmt.Sprintf("Given %d teams made at one instant When paged by %d Then each comes once, newest first", tc.teams, tc.limit), func(t *testing.T) {
			v := newVX(t, fmt.Sprintf("vercel-paging-runs-%d", tc.teams))
			made := makeTeams(t, v, 0, tc.teams)
			teams := pagedListing{name: "teams", older: "until", items: "teams", id: "id"}
			got := walkListing(t, v, teams, "/v2/teams", tc.limit, tc.teams/tc.limit+2)
			holdsExactly(t, "teams", got, newestFirst(made))
		})
	}
}

// TestVercelTeamsPageAcrossAWrapOfTheSlots makes a run of teams that begins just
// before the count the slots wrap at and ends just after it, in a millisecond of
// its own, below a much longer run of another. A page boundary between two teams
// whose slots wrap must be as exact as one that is not.
func TestVercelTeamsPageAcrossAWrapOfTheSlots(t *testing.T) {
	v := newVX(t, "vercel-paging-wrap")
	const below, above = 4090, 110 // the slots wrap at 4095 teams
	made := makeTeams(t, v, 0, below)
	v.advance(time.Millisecond)
	made = append(made, makeTeams(t, v, below, below+above)...)
	teams := pagedListing{name: "teams", older: "until", items: "teams", id: "id"}
	want := newestFirst(made)
	for _, limit := range []int{1, 5, 7, 100} {
		// A walk by 1 would be four thousand requests; the boundaries that matter
		// are the ones inside the second run, so walk it and stop a page past it.
		got := walkLimited(t, v, teams, "/v2/teams", limit, above+limit+1)
		holdsExactly(t, fmt.Sprintf("the newest %d teams, by %d", len(got), limit), got, want[:len(got)])
	}
	holdsExactly(t, "every team, by 100", walkListing(t, v, teams, "/v2/teams", 100, 60), want)
}

// walkLimited is walkListing that stops after the first n items.
func walkLimited(t *testing.T, v *vx, l pagedListing, path string, limit, n int) []string {
	t.Helper()
	var got []string
	ask := fmt.Sprintf("%s?limit=%d", path, limit)
	for len(got) < n {
		r := v.do(http.MethodGet, ask, nil)
		if r.status != 200 {
			t.Fatalf("GET %s = %d %s", ask, r.status, r.raw)
		}
		items, _ := r.body[l.items].([]any)
		for _, it := range items {
			got = append(got, str(it.(map[string]any), l.id))
		}
		next, more := cursorText(t, r)
		if !more {
			break
		}
		ask = fmt.Sprintf("%s?limit=%d&%s=%s", path, limit, l.older, next)
	}
	return got
}

// TestVercelListingsPageOnAClockBeyondTheYear2248 winds the controllable clock past
// 2^43 milliseconds, where a cursor with a ten-bit fraction stopped being exact in a
// float64 (about half of the places it handed out read back as another) and where
// the places a cursor has begin to shrink: 511 now, halving with each bit. The
// teams of one instant, as many as there are places, page without a skip or a
// repeat there, and so do those of a moving clock.
func TestVercelListingsPageOnAClockBeyondTheYear2248(t *testing.T) {
	v := newVX(t, "vercel-paging-far-future")
	for i := 0; i < 2; i++ { // two advances: a duration holds 292 years
		v.advance(145 * 365 * 24 * time.Hour)
	}
	if ms := v.rt.Deps.Clock.Now().UnixMilli(); ms < 1<<43 || ms >= 1<<44 {
		t.Fatalf("the clock reads %d, want it between 2^43 and 2^44", ms)
	}
	teams := pagedListing{name: "teams", older: "until", items: "teams", id: "id"}
	made := makeTeams(t, v, 0, 511) // every place a cursor has at this magnitude
	for _, limit := range []int{1, 7, 100, 510} {
		holdsExactly(t, "511 teams of one instant on a far clock", walkLimited(t, v, teams, "/v2/teams", limit, 600), newestFirst(made))
	}
	for i := 0; i < 40; i++ { // and a clock that moves
		v.advance(time.Duration(i%3) * time.Millisecond)
		made = append(made, makeTeams(t, v, 1000+i, 1001+i)...)
	}
	holdsExactly(t, "teams on a far clock that moves", walkLimited(t, v, teams, "/v2/teams", 3, 600), newestFirst(made))
}
