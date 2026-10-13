package vercel

import (
	"testing"
	"time"
)

// The controllable clock can be wound to before 1970 (the harness starts it in
// 2020 and an advance may be negative). A millisecond count there is negative and a
// cursor has places in it as after: six teams made at one instant before the epoch
// page by 2 as six teams of today do, and so do teams made across the epoch itself.

const sinceTheEpoch = 1577836800 * time.Second // where the harness's clock starts

func TestVercelListingsPageBeforeTheEpoch(t *testing.T) {
	teams := pagedListing{name: "teams", older: "until", items: "teams", id: "id"}
	t.Run("Given six teams made at one instant 60 years before 1970 When paged by 2 Then each comes once, newest first", func(t *testing.T) {
		v := newVX(t, "vercel-paging-before-the-epoch")
		v.advance(-(sinceTheEpoch + 60*365*24*time.Hour))
		if ms := v.rt.Deps.Clock.Now().UnixMilli(); ms >= 0 {
			t.Fatalf("the clock reads %d, want it before 1970", ms)
		}
		made := makeTeams(t, v, 0, 6)
		for _, limit := range []int{1, 2, 5} {
			holdsExactly(t, "six teams before 1970", walkLimited(t, v, teams, "/v2/teams", limit, 30), newestFirst(made))
		}
	})
	t.Run("Given teams made on a clock that wanders across the epoch When paged Then each comes once", func(t *testing.T) {
		v := newVX(t, "vercel-paging-across-the-epoch")
		v.advance(-(sinceTheEpoch + 3*time.Millisecond)) // the instant -3
		var made []pagedItem
		for i := 0; i < 24; i++ {
			v.advance(time.Duration(i%3) * time.Millisecond)
			made = append(made, makeTeams(t, v, i, i+1)...)
		}
		first := v.rt.Deps.Clock.Now().UnixMilli()
		if first < 0 || made[0].at >= 0 {
			t.Fatalf("the teams run from %d to %d, want them to cross 0", made[0].at, first)
		}
		for _, limit := range []int{1, 2, 7} {
			holdsExactly(t, "teams across the epoch", walkLimited(t, v, teams, "/v2/teams", limit, 60), newestFirst(made))
		}
	})
}
