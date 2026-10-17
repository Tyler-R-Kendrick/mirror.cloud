package vercel

import (
	"net/http"
	"strings"
	"testing"
)

// A cursor is a millisecond timestamp, and a client can send any number it likes:
// a far-future bound to say "everything", a far-past one to say "nothing", the
// largest integer its language holds. The listing reads those as the bound they
// are rather than as a number that does not fit.

const (
	hugeBounds    = "1e19 9223372036854775807 99999999999999999999 1e300 9007199254740993"
	negativeBound = "-1 -1e300 -9223372036854775808 0 -0"
)

func TestVercelListingsReadABoundBeyondTheRangeAsTheBoundItIs(t *testing.T) {
	const items = 5
	for _, listing := range pagedListings() {
		listing := listing
		t.Run(listing.name, func(t *testing.T) {
			v := newVX(t, "vercel-paging-range-"+listing.name)
			path, made := listing.make(t, v, items, nil)
			count := func(param, value string) int {
				t.Helper()
				r := v.do(http.MethodGet, path+"?limit=100&"+param+"="+value, nil)
				if r.status != 200 {
					t.Fatalf("GET %s?%s=%s = %d %s", path, param, value, r.status, r.raw)
				}
				list, _ := r.body[listing.items].([]any)
				return len(list)
			}
			for _, value := range strings.Fields(hugeBounds) {
				for _, param := range listing.before {
					if n := count(param, value); n != len(made) {
						t.Errorf("%s=%s keeps %d of %d: a bound past every item is no bound on the older side", param, value, n, len(made))
					}
				}
				for _, param := range listing.after {
					if n := count(param, value); n != 0 {
						t.Errorf("%s=%s keeps %d: nothing is newer than a bound past every item", param, value, n)
					}
				}
			}
			for _, value := range strings.Fields(negativeBound) {
				for _, param := range listing.before {
					if n := count(param, value); n != 0 {
						t.Errorf("%s=%s keeps %d: nothing is older than a bound before every item", param, value, n)
					}
				}
				for _, param := range listing.after {
					if n := count(param, value); n != len(made) {
						t.Errorf("%s=%s keeps %d of %d: a bound before every item is no bound on the newer side", param, value, n, len(made))
					}
				}
			}
			for _, value := range []string{"1e400", "-1e400", "NaN", "Inf", "-Inf", "0x10"} {
				for _, param := range append(append([]string{}, listing.before...), listing.after...) {
					if r := v.do(http.MethodGet, path+"?"+param+"="+value, nil); r.status != 400 {
						t.Errorf("%s=%s = %d %s, want 400: it is not a timestamp", param, value, r.status, r.raw)
					}
				}
			}
		})
	}
}
