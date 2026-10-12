package vercel

import (
	"fmt"
	"testing"
	"time"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/rand"
)

// A listing is ordered by the instant its items were made, and of two made at one
// instant, the later made first; a page boundary cuts that order and no other. A
// clock that steps back (an NTP correction, a controllable clock a test winds)
// makes the creation order and the instant order differ, and a cut that used one
// to place the boundary and the other to order the items drops whatever lies
// between them.

// TestVercelListingsOrderByInstantWhenTheClockGoesBack makes three items with the
// clock stepped +1000 s, -100 s, +100 s, so the first and third share an instant
// and the second is a hundred seconds before both, and pages one at a time.
func TestVercelListingsOrderByInstantWhenTheClockGoesBack(t *testing.T) {
	steps := []time.Duration{1000 * time.Second, -100 * time.Second, 100 * time.Second}
	for _, listing := range pagedListings() {
		listing := listing
		t.Run(listing.name, func(t *testing.T) {
			v := newVX(t, "vercel-paging-clock-"+listing.name)
			path, made := listing.make(t, v, len(steps), func(i int) { v.advance(steps[i]) })
			for _, limit := range []int{1, 2} {
				got := walkListing(t, v, listing, path, limit, 10)
				holdsExactly(t, fmt.Sprintf("%s by %d", listing.name, limit), got, newestFirst(made))
			}
		})
	}
}

// TestVercelListingsKeepEveryItemWhenTheClockJitters makes items on a clock that
// wanders two milliseconds either way between them, which puts items at the same
// instant, an instant out of order, and several of each in a row, and pages by a
// limit that lands boundaries on all of them.
func TestVercelListingsKeepEveryItemWhenTheClockJitters(t *testing.T) {
	const items = 40
	for _, listing := range pagedListings() {
		listing := listing
		t.Run(listing.name, func(t *testing.T) {
			v := newVX(t, "vercel-paging-jitter-"+listing.name)
			jitter := rand.New("jitter")
			path, made := listing.make(t, v, items, func(int) {
				v.advance(time.Duration(jitter.Intn(5)-2) * time.Millisecond)
			})
			for _, limit := range []int{1, 3, 7} {
				got := walkListing(t, v, listing, path, limit, items+2)
				holdsExactly(t, fmt.Sprintf("%s by %d", listing.name, limit), got, newestFirst(made))
			}
		})
	}
}
