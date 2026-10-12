package vercel

import (
	"fmt"
	"net/http"
	"strconv"
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

// pagedListing is a listing that pages: how to make n items in it, where to ask,
// and what the answer calls its items.
type pagedListing struct {
	name   string
	make   func(t *testing.T, v *vx, n int) (path string, created []string) // oldest first
	cursor string                                                           // the query parameter that carries the page boundary
	items  string                                                           // the member holding the page
	id     string                                                           // the member that names an item
}

func makeEach(t *testing.T, v *vx, n int, one func(i int) reply, id string) []string {
	t.Helper()
	var ids []string
	for i := 0; i < n; i++ {
		r := one(i)
		if r.status != 200 && r.status != 201 {
			t.Fatalf("item %d = %d %s", i, r.status, r.raw)
		}
		ids = append(ids, str(r.body, id))
	}
	return ids
}

func pagedListings() []pagedListing {
	return []pagedListing{
		{name: "projects", cursor: "from", items: "projects", id: "id",
			make: func(t *testing.T, v *vx, n int) (string, []string) {
				return "/v10/projects?limit=7", makeEach(t, v, n, func(i int) reply {
					return v.do(http.MethodPost, "/v11/projects", map[string]any{"name": fmt.Sprintf("paged-%02d", i)})
				}, "id")
			}},
		{name: "deployments", cursor: "until", items: "deployments", id: "uid",
			make: func(t *testing.T, v *vx, n int) (string, []string) {
				newProjectNamed(t, v, "paged-app")
				return "/v7/deployments?limit=7", makeEach(t, v, n, func(i int) reply {
					return v.do(http.MethodPost, "/v13/deployments", map[string]any{"name": "paged-app", "project": "paged-app", "files": []map[string]any{{"file": "index.html", "data": fmt.Sprint(i)}}})
				}, "id")
			}},
		{name: "aliases", cursor: "until", items: "aliases", id: "uid",
			make: func(t *testing.T, v *vx, n int) (string, []string) {
				v.do(http.MethodPost, "/v7/domains", map[string]any{"name": "paged.test"})
				newProjectNamed(t, v, "paged-alias")
				dep := deploy(t, v, "paged-alias", true)
				// Every deployment is an alias of its own URL, made when it was and so the oldest.
				own, _ := v.do(http.MethodGet, "/v2/deployments/"+dep+"/aliases", nil).body["aliases"].([]any)
				if len(own) != 1 {
					t.Fatalf("a new deployment has %d aliases, want its own URL", len(own))
				}
				ids := []string{str(own[0].(map[string]any), "uid")}
				return "/v4/aliases?limit=7", append(ids, makeEach(t, v, n-1, func(i int) reply {
					return aliasAt(v, "v2", dep, fmt.Sprintf("a%02d.paged.test", i))
				}, "uid")...)
			}},
		{name: "account domains", cursor: "until", items: "domains", id: "name",
			make: func(t *testing.T, v *vx, n int) (string, []string) {
				var names []string
				for i := 0; i < n; i++ {
					name := fmt.Sprintf("d%02d.example", i)
					if r := v.do(http.MethodPost, "/v7/domains", map[string]any{"name": name}); r.status != 200 {
						t.Fatalf("domain %d = %d %s", i, r.status, r.raw)
					}
					names = append(names, name)
				}
				return "/v5/domains?limit=7", names
			}},
		{name: "project domains", cursor: "until", items: "domains", id: "name",
			make: func(t *testing.T, v *vx, n int) (string, []string) {
				pid := newProjectNamed(t, v, "paged-domains")
				var names []string
				for i := 0; i < n; i++ {
					name := fmt.Sprintf("h%02d.paged.test", i)
					if r := v.do(http.MethodPost, "/v10/projects/"+pid+"/domains", map[string]any{"name": name}); r.status != 200 {
						t.Fatalf("project domain %d = %d %s", i, r.status, r.raw)
					}
					names = append(names, name)
				}
				return "/v9/projects/" + pid + "/domains?limit=7", names
			}},
		{name: "the project domains of an apex", cursor: "until", items: "projectDomains", id: "name",
			make: func(t *testing.T, v *vx, n int) (string, []string) {
				v.do(http.MethodPost, "/v7/domains", map[string]any{"name": "apex.test"})
				pid := newProjectNamed(t, v, "paged-apex")
				var names []string
				for i := 0; i < n; i++ {
					name := fmt.Sprintf("h%02d.apex.test", i)
					if r := v.do(http.MethodPost, "/v10/projects/"+pid+"/domains", map[string]any{"name": name}); r.status != 200 {
						t.Fatalf("project domain %d = %d %s", i, r.status, r.raw)
					}
					names = append(names, name)
				}
				return "/v1/domains/apex.test/project-domains?limit=7", names
			}},
		{name: "teams", cursor: "until", items: "teams", id: "id",
			make: func(t *testing.T, v *vx, n int) (string, []string) {
				return "/v2/teams?limit=7", makeEach(t, v, n, func(i int) reply {
					return v.do(http.MethodPost, "/v2/teams", map[string]any{"slug": fmt.Sprintf("paged-team-%02d", i)})
				}, "id")
			}},
	}
}

// cursorText is the number as a client sends it back.
func cursorText(t *testing.T, r reply) (string, bool) {
	t.Helper()
	pg, _ := r.body["pagination"].(map[string]any)
	if pg == nil {
		t.Fatalf("no pagination in %s", r.raw)
	}
	if pg["next"] == nil {
		return "", false
	}
	f, ok := pg["next"].(float64)
	if !ok {
		t.Fatalf("pagination.next is %T %v, the document types it as a number: %s", pg["next"], pg["next"], r.raw)
	}
	return strconv.FormatFloat(f, 'f', -1, 64), true
}

func TestVercelListingsPageWithoutSkippingItemsOfOneMillisecond(t *testing.T) {
	const total, pageSize = 50, 7
	for _, listing := range pagedListings() {
		listing := listing
		t.Run(listing.name, func(t *testing.T) {
			// The clock is frozen: every item below is created at one instant.
			v := newVX(t, "vercel-paging-"+listing.name)
			path, created := listing.make(t, v, total)
			if len(created) != total {
				t.Fatalf("made %d of %d", len(created), total)
			}

			var got []string
			ask := path
			for page := 0; page < 20; page++ {
				r := v.do(http.MethodGet, ask, nil)
				if r.status != 200 {
					t.Fatalf("GET %s = %d %s", ask, r.status, r.raw)
				}
				items, _ := r.body[listing.items].([]any)
				if len(items) > pageSize {
					t.Fatalf("page %d holds %d, the limit is %d", page, len(items), pageSize)
				}
				for _, it := range items {
					got = append(got, str(it.(map[string]any), listing.id))
				}
				next, more := cursorText(t, r)
				if !more {
					break
				}
				if len(items) != pageSize {
					t.Fatalf("page %d holds %d and says there is more: %s", page, len(items), r.raw)
				}
				ask = path + "&" + listing.cursor + "=" + next
			}

			seen := map[string]bool{}
			for _, id := range got {
				if seen[id] {
					t.Errorf("%s came on two pages", id)
				}
				seen[id] = true
			}
			for _, id := range created {
				if !seen[id] {
					t.Errorf("%s was skipped between pages", id)
				}
			}
			if len(got) != total {
				t.Fatalf("paged %d items, made %d", len(got), total)
			}
			// Newest first: the reverse of the order they were made in.
			for i, id := range got {
				if want := created[total-1-i]; id != want {
					t.Fatalf("item %d of the listing is %s, want %s (newest first)", i, id, want)
				}
			}
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
