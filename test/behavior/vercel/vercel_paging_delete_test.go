package vercel

import (
	"net/http"
	"strconv"
	"testing"
	"time"
)

// A client that deletes what it lists asks for the next page with the cursor of an
// item that is no longer there. The cursor names a position, and the position
// holds: what was older than the deleted item is the next page, and the newer items
// that share its millisecond do not come back.

func askAfter(t *testing.T, v *vx, l pagedListing, path string, limit int, next string) []string {
	t.Helper()
	r := v.do(http.MethodGet, path+"?limit="+strconv.Itoa(limit)+"&"+l.older+"="+next, nil)
	if r.status != 200 {
		t.Fatalf("GET %s = %d %s", path, r.status, r.raw)
	}
	items, _ := r.body[l.items].([]any)
	var ids []string
	for _, it := range items {
		ids = append(ids, str(it.(map[string]any), l.id))
	}
	return ids
}

// TestVercelListingsKeepWhatIsOlderWhenTheItemACursorNamesIsDeleted is the review's
// repro on every listing that has a delete: two items at one instant, the clock a
// millisecond on, three more; the page of three ends at the third newest; that
// item is deleted; the next page is the two at the earlier instant.
func TestVercelListingsKeepWhatIsOlderWhenTheItemACursorNamesIsDeleted(t *testing.T) {
	for _, listing := range pagedListings() {
		listing := listing
		if listing.remove == nil {
			continue
		}
		t.Run(listing.name, func(t *testing.T) {
			v := newVX(t, "vercel-paging-delete-"+listing.name)
			path, made := listing.make(t, v, 5, func(i int) {
				if i == 2 {
					v.advance(time.Millisecond)
				}
			})
			ask := path + "?limit=3"
			first := v.do(http.MethodGet, ask, nil)
			if first.status != 200 {
				t.Fatalf("GET %s = %d %s", ask, first.status, first.raw)
			}
			holdsExactly(t, "the first page", idsOf(listing, first), []string{made[4].id, made[3].id, made[2].id})
			next, more := cursorText(t, first)
			if !more {
				t.Fatalf("no next after a page of 3 of 5: %s", first.raw)
			}
			listing.remove(t, v, path, made[2].id)
			holdsExactly(t, "the page after the deleted item", askAfter(t, v, listing, path, 3, next), []string{made[1].id, made[0].id})
		})
	}
}

func idsOf(l pagedListing, r reply) []string {
	var ids []string
	items, _ := r.body[l.items].([]any)
	for _, it := range items {
		ids = append(ids, str(it.(map[string]any), l.id))
	}
	return ids
}
