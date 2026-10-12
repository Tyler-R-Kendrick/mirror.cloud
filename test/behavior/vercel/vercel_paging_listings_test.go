package vercel

import (
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"testing"
)

// The listings that page, as one table: how to make n items in each, where to ask,
// which query parameters cut it, and what the answer calls its items. Every paging
// test runs over it, so a listing added to one is held to all of them.

// pagedItem is one thing a listing was made to hold: its id, and the instant the
// clock read when it was made, which is the instant the listing orders it by.
type pagedItem struct {
	id string
	at int64
}

type pagedListing struct {
	name string
	// make creates n items oldest first and answers the listing's address (no
	// query) and what it made. pace, when it is not nil, is called before each
	// item is made, and may move the clock.
	make   func(t *testing.T, v *vx, n int, pace func(i int)) (path string, made []pagedItem)
	older  string   // the parameter a walk sends pagination.next back as
	before []string // parameters that keep the items older than their cursor
	after  []string // parameters that keep the items newer than their cursor
	items  string   // the member holding the page
	id     string   // the member that names an item
}

// stamped makes n items with one, noting the instant each was made at.
func stamped(t *testing.T, v *vx, n int, pace func(i int), one func(i int) (id string, r reply)) []pagedItem {
	t.Helper()
	var made []pagedItem
	for i := 0; i < n; i++ {
		if pace != nil {
			pace(i)
		}
		id, r := one(i)
		if r.status != 200 && r.status != 201 {
			t.Fatalf("item %d = %d %s", i, r.status, r.raw)
		}
		made = append(made, pagedItem{id: id, at: v.rt.Deps.Clock.Now().UnixMilli()})
	}
	return made
}

func deploymentsListing(name, path string) pagedListing {
	return pagedListing{name: name, older: "until", before: []string{"until"}, after: []string{"since"}, items: "deployments", id: "uid",
		make: func(t *testing.T, v *vx, n int, pace func(i int)) (string, []pagedItem) {
			newProjectNamed(t, v, "paged-app")
			return path, stamped(t, v, n, pace, func(i int) (string, reply) {
				r := v.do(http.MethodPost, "/v13/deployments", map[string]any{"name": "paged-app", "project": "paged-app", "files": []map[string]any{{"file": "index.html", "data": fmt.Sprint(i)}}})
				return str(r.body, "id"), r
			})
		}}
}

func pagedListings() []pagedListing {
	return []pagedListing{
		{name: "projects", older: "from", before: []string{"from"}, items: "projects", id: "id",
			make: func(t *testing.T, v *vx, n int, pace func(i int)) (string, []pagedItem) {
				return "/v10/projects", stamped(t, v, n, pace, func(i int) (string, reply) {
					r := v.do(http.MethodPost, "/v11/projects", map[string]any{"name": fmt.Sprintf("paged-%02d", i)})
					return str(r.body, "id"), r
				})
			}},
		deploymentsListing("deployments", "/v7/deployments"),
		deploymentsListing("deployments (the older address)", "/v6/deployments"),
		{name: "aliases", older: "until", before: []string{"until"}, after: []string{"since", "from"}, items: "aliases", id: "uid",
			make: func(t *testing.T, v *vx, n int, pace func(i int)) (string, []pagedItem) {
				v.do(http.MethodPost, "/v7/domains", map[string]any{"name": "paged.test"})
				newProjectNamed(t, v, "paged-alias")
				if pace != nil {
					pace(0)
				}
				dep := deploy(t, v, "paged-alias", true)
				// Every deployment is an alias of its own URL, made when it was and so the oldest.
				own, _ := v.do(http.MethodGet, "/v2/deployments/"+dep+"/aliases", nil).body["aliases"].([]any)
				if len(own) != 1 {
					t.Fatalf("a new deployment has %d aliases, want its own URL", len(own))
				}
				// The deployment's own listing says when it was made as text; the
				// account's listing, the one paged, as the instant it orders by.
				ownID := str(own[0].(map[string]any), "uid")
				var at float64
				for _, a := range v.do(http.MethodGet, "/v4/aliases?limit=100", nil).body["aliases"].([]any) {
					if m := a.(map[string]any); str(m, "uid") == ownID {
						at, _ = m["createdAt"].(float64)
					}
				}
				made := []pagedItem{{id: ownID, at: int64(at)}}
				return "/v4/aliases", append(made, stamped(t, v, n-1, func(i int) {
					if pace != nil {
						pace(i + 1)
					}
				}, func(i int) (string, reply) {
					r := aliasAt(v, "v2", dep, fmt.Sprintf("a%02d.paged.test", i))
					return str(r.body, "uid"), r
				})...)
			}},
		{name: "account domains", older: "until", before: []string{"until"}, after: []string{"since"}, items: "domains", id: "name",
			make: func(t *testing.T, v *vx, n int, pace func(i int)) (string, []pagedItem) {
				return "/v5/domains", stamped(t, v, n, pace, func(i int) (string, reply) {
					name := fmt.Sprintf("d%02d.example", i)
					return name, v.do(http.MethodPost, "/v7/domains", map[string]any{"name": name})
				})
			}},
		{name: "project domains", older: "until", before: []string{"until"}, after: []string{"since"}, items: "domains", id: "name",
			make: func(t *testing.T, v *vx, n int, pace func(i int)) (string, []pagedItem) {
				pid := newProjectNamed(t, v, "paged-domains")
				return "/v9/projects/" + pid + "/domains", stamped(t, v, n, pace, func(i int) (string, reply) {
					name := fmt.Sprintf("h%02d.paged.test", i)
					return name, v.do(http.MethodPost, "/v10/projects/"+pid+"/domains", map[string]any{"name": name})
				})
			}},
		{name: "the project domains of an apex", older: "until", before: []string{"until"}, after: []string{"since"}, items: "projectDomains", id: "name",
			make: func(t *testing.T, v *vx, n int, pace func(i int)) (string, []pagedItem) {
				v.do(http.MethodPost, "/v7/domains", map[string]any{"name": "apex.test"})
				pid := newProjectNamed(t, v, "paged-apex")
				return "/v1/domains/apex.test/project-domains", stamped(t, v, n, pace, func(i int) (string, reply) {
					name := fmt.Sprintf("h%02d.apex.test", i)
					return name, v.do(http.MethodPost, "/v10/projects/"+pid+"/domains", map[string]any{"name": name})
				})
			}},
		{name: "teams", older: "until", before: []string{"until"}, after: []string{"since"}, items: "teams", id: "id",
			make: func(t *testing.T, v *vx, n int, pace func(i int)) (string, []pagedItem) {
				return "/v2/teams", stamped(t, v, n, pace, func(i int) (string, reply) {
					r := v.do(http.MethodPost, "/v2/teams", map[string]any{"slug": fmt.Sprintf("paged-team-%04d", i)})
					return str(r.body, "id"), r
				})
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

// walkListing pages a listing from its first page to its last the way a client
// does, sending each page's pagination.next back as the listing's cursor, and
// answers the ids in the order they were served.
func walkListing(t *testing.T, v *vx, l pagedListing, path string, limit, most int) []string {
	t.Helper()
	var got []string
	ask := fmt.Sprintf("%s?limit=%d", path, limit)
	for page := 0; ; page++ {
		if page > most {
			t.Fatalf("%s: still paging after %d pages (%d items so far)", l.name, page, len(got))
		}
		r := v.do(http.MethodGet, ask, nil)
		if r.status != 200 {
			t.Fatalf("GET %s = %d %s", ask, r.status, r.raw)
		}
		items, _ := r.body[l.items].([]any)
		if len(items) > limit {
			t.Fatalf("page %d holds %d, the limit is %d", page, len(items), limit)
		}
		for _, it := range items {
			got = append(got, str(it.(map[string]any), l.id))
		}
		next, more := cursorText(t, r)
		if !more {
			return got
		}
		if len(items) != limit {
			t.Fatalf("page %d holds %d and says there is more: %s", page, len(items), r.raw)
		}
		ask = fmt.Sprintf("%s?limit=%d&%s=%s", path, limit, l.older, next)
	}
}

// newestFirst is the order a listing serves what it holds: the later instant
// first, and of two made at one instant the later made.
func newestFirst(made []pagedItem) []string {
	idx := make([]int, len(made))
	for i := range idx {
		idx[i] = i
	}
	sort.SliceStable(idx, func(a, b int) bool {
		x, y := made[idx[a]], made[idx[b]]
		if x.at != y.at {
			return x.at > y.at
		}
		return idx[a] > idx[b]
	})
	ids := make([]string, len(made))
	for i, j := range idx {
		ids[i] = made[j].id
	}
	return ids
}

// holdsExactly fails unless got is want, with the first difference named.
func holdsExactly(t *testing.T, what string, got, want []string) {
	t.Helper()
	seen := map[string]int{}
	for _, id := range got {
		seen[id]++
	}
	for id, n := range seen {
		if n > 1 {
			t.Errorf("%s: %s came %d times", what, id, n)
		}
	}
	for _, id := range want {
		if seen[id] == 0 {
			t.Errorf("%s: %s was skipped", what, id)
		}
	}
	if len(got) != len(want) {
		t.Fatalf("%s: %d items served, %d made", what, len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("%s: item %d is %s, want %s", what, i, got[i], want[i])
		}
	}
}
