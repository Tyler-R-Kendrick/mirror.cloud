package vercel

import (
	"fmt"
	"net/http"
	"testing"
	"time"
)

// Vercel pages by timestamp: a page answers pagination.next, a number, and the
// client sends it back as `until` (`from` for projects) to get the items before
// it. Items created in one millisecond share a timestamp, so a boundary between
// two of them used to lose the second, and on a clock that does not move that was
// every item after the first page. The cursor still reads as the timestamp, a
// number, and carries the item's place among those created in the same
// millisecond in its fraction.

func TestVercelListingsPageWithoutSkippingItemsOfOneMillisecond(t *testing.T) {
	const total, pageSize = 50, 7
	for _, listing := range pagedListings() {
		listing := listing
		t.Run(listing.name, func(t *testing.T) {
			// The clock is frozen: every item below is created at one instant.
			v := newVX(t, "vercel-paging-"+listing.name)
			path, made := listing.make(t, v, total, nil)
			if len(made) != total {
				t.Fatalf("made %d of %d", len(made), total)
			}
			// Newest first: the reverse of the order they were made in.
			holdsExactly(t, listing.name, walkListing(t, v, listing, path, pageSize, 20), newestFirst(made))
		})
	}
}

// TestVercelListingsStillTakeABareTimestamp holds the old form: a client that
// computes its own millisecond, or echoes the integer an earlier emulator
// answered, gets the items strictly before (until) or after (since) that
// millisecond, as it always did.
func TestVercelListingsStillTakeABareTimestamp(t *testing.T) {
	for _, tc := range []struct{ name, list, items, id string }{
		{"deployments", "/v7/deployments", "deployments", "uid"},
		{"teams", "/v2/teams", "teams", "id"},
		{"account domains", "/v5/domains", "domains", "name"},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			v := newVX(t, "vercel-bare-"+tc.name)
			makeOne := func(i int) {
				switch tc.name {
				case "deployments":
					if i == 0 {
						newProjectNamed(t, v, "bare-app")
					}
					v.do(http.MethodPost, "/v13/deployments", map[string]any{"name": "bare-app", "project": "bare-app", "files": []map[string]any{{"file": "index.html", "data": fmt.Sprint(i)}}})
				case "teams":
					v.do(http.MethodPost, "/v2/teams", map[string]any{"slug": fmt.Sprintf("bare-%d", i)})
				default:
					v.do(http.MethodPost, "/v7/domains", map[string]any{"name": fmt.Sprintf("bare%d.example", i)})
				}
			}
			for i := 0; i < 3; i++ {
				makeOne(i)
			}
			v.advance(time.Second)
			for i := 3; i < 6; i++ {
				makeOne(i)
			}
			all := v.do(http.MethodGet, tc.list+"?limit=100", nil)
			items, _ := all.body[tc.items].([]any)
			if len(items) != 6 {
				t.Fatalf("listed %d of 6: %s", len(items), all.raw)
			}
			at := func(i int) int64 {
				m := items[i].(map[string]any)
				for _, k := range []string{"createdAt", "created"} {
					if f, ok := m[k].(float64); ok {
						return int64(f)
					}
				}
				t.Fatalf("no creation instant on %v", m)
				return 0
			}
			late, early := at(0), at(5)
			if late <= early {
				t.Fatalf("instants %d, %d: the clock did not move", early, late)
			}
			count := func(query string) int {
				r := v.do(http.MethodGet, tc.list+"?limit=100&"+query, nil)
				if r.status != 200 {
					t.Fatalf("%s = %d %s", query, r.status, r.raw)
				}
				l, _ := r.body[tc.items].([]any)
				return len(l)
			}
			if n := count(fmt.Sprintf("until=%d", late)); n != 3 {
				t.Errorf("until=%d (a bare timestamp) = %d items, want the 3 made before it", late, n)
			}
			if n := count(fmt.Sprintf("since=%d", early)); n != 3 {
				t.Errorf("since=%d (a bare timestamp) = %d items, want the 3 made after it", early, n)
			}
			if n := count(fmt.Sprintf("until=%d&since=%d", late+1, early-1)); n != 6 {
				t.Errorf("a window around both = %d items, want 6", n)
			}
		})
	}
}

func TestVercelListingsRefuseACursorThatIsNotANumber(t *testing.T) {
	v := newVX(t, "vercel-paging-bad")
	if r := v.do(http.MethodGet, "/v7/deployments?until=yesterday", nil); r.status < 400 {
		t.Fatalf("until=yesterday = %d %s: a cursor that is not a number must not quietly return the first page", r.status, r.raw)
	}
}
