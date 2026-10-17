package vercel

import (
	"fmt"
	"math"
	"net/http"
	"reflect"
	"strconv"
	"testing"
	"time"
)

// TestVercelDeploymentsListNewestFirstAndPage holds the listing's order, its
// filters and the cursor a client pages with.
func TestVercelDeploymentsListNewestFirstAndPage(t *testing.T) {
	v := newVX(t, "vercel-listing")
	var ids []string
	var created []float64
	for i := 0; i < 5; i++ {
		name := "alpha"
		if i%2 == 1 {
			name = "beta"
		}
		r := v.do(http.MethodPost, "/v13/deployments", map[string]any{"name": name})
		if r.status != 200 {
			t.Fatalf("create %d = %d %s", i, r.status, r.raw)
		}
		ids = append(ids, str(r.body, "id"))
		created = append(created, r.body["createdAt"].(float64))
		v.advance(10 * time.Millisecond)
	}
	list := func(query string) (got []string, pagination map[string]any) {
		r := v.do(http.MethodGet, "/v7/deployments"+query, nil)
		if r.status != 200 {
			t.Fatalf("list%s = %d %s", query, r.status, r.raw)
		}
		items, _ := r.body["deployments"].([]any)
		for _, it := range items {
			got = append(got, str(it.(map[string]any), "uid"))
		}
		pagination, _ = r.body["pagination"].(map[string]any)
		return
	}
	rev := func(in []string) []string {
		out := make([]string, len(in))
		for i, s := range in {
			out[len(in)-1-i] = s
		}
		return out
	}

	all, p := list("")
	if !reflect.DeepEqual(all, rev(ids)) || p["next"] != nil || p["prev"] != nil || p["count"] != float64(5) {
		t.Fatalf("unfiltered = %v pagination %v, want newest first %v", all, p, rev(ids))
	}

	// The cursor is a number that still reads as the creation instant of the last
	// item on the page (its integer part), with the item's place among those made
	// in the same millisecond in its fraction. It goes back as it came.
	cursor := func(p map[string]any) string { return strconv.FormatFloat(p["next"].(float64), 'f', -1, 64) }
	first, p := list("?limit=2")
	if !reflect.DeepEqual(first, rev(ids)[:2]) || p["count"] != float64(2) || math.Floor(p["next"].(float64)) != created[3] {
		t.Fatalf("page one = %v %v", first, p)
	}
	// A client that keeps only the integer part sends the old bare timestamp, and
	// gets what it always did: the items made before that millisecond.
	if bare, _ := list(fmt.Sprintf("?limit=2&until=%d", int64(p["next"].(float64)))); !reflect.DeepEqual(bare, rev(ids)[2:4]) {
		t.Fatalf("page two from the bare timestamp = %v", bare)
	}
	second, p := list("?limit=2&until=" + cursor(p))
	if !reflect.DeepEqual(second, rev(ids)[2:4]) || p["prev"] == nil {
		t.Fatalf("page two = %v %v", second, p)
	}
	last, p := list("?limit=2&until=" + cursor(p))
	if !reflect.DeepEqual(last, rev(ids)[4:]) || p["next"] != nil {
		t.Fatalf("last page = %v %v", last, p)
	}
	newer, _ := list(fmt.Sprintf("?since=%d", int64(created[2])))
	if !reflect.DeepEqual(newer, rev(ids)[:2]) {
		t.Fatalf("since = %v", newer)
	}
	if byApp, _ := list("?app=beta"); len(byApp) != 2 {
		t.Fatalf("app filter = %v", byApp)
	}
	if byState, _ := list("?state=READY,ERROR"); len(byState) != 0 {
		t.Fatalf("a queued deployment matched state=READY,ERROR: %v", byState)
	}
	v.advance(5 * time.Second)
	if byState, _ := list("?state=READY"); len(byState) != 5 {
		t.Fatalf("after the builds = %v", byState)
	}
	// The older address answers the same list.
	if r := v.do(http.MethodGet, "/v6/deployments?limit=1", nil); r.status != 200 {
		t.Fatalf("v6 = %d %s", r.status, r.raw)
	}
}
