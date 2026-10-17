package vercel

import (
	"net/http"
	"net/url"
	"testing"
)

// A cursor is a decimal number: an optional minus sign, digits with no leading
// zero, an optional fraction of digits, an optional exponent. It is what
// pagination.next prints (a client sends String(next) back) and what a client
// that computes its own timestamp prints. Everything else a number parser would
// take (a bare fraction, a trailing point, digit separators, a hexadecimal float,
// a plus sign, leading zeros, padding, NaN and Inf) is not a timestamp and is a
// 400, not a guess. A fraction is a place among the items of one millisecond, so
// one that is not a place this emulator hands out is refused too.

func TestVercelListingsReadOnlyADecimalNumberAsACursor(t *testing.T) {
	good := []string{
		"0", "-0", "-1", "1577836800000", "1577836800000.5", "1577836800000.4",
		"1577836800000.0002", "0.000244140625", "1e19", "1.5e3", "1E3", "-1e300", "5e+2", "1577836800000.00001",
	}
	bad := []string{
		".5", "5.", "-.5", "1_000", "0x1p4", "0x10", "0b11", "+5", "+1e3", "00012", "01", "-01", "1e", "1e+", "e5", "1e5.5", "--1",
		"- 1", "1 000", " 5", "5 ", "1,5", "1.2.3", "NaN", "Inf", "+Inf", "-Inf", "infinity", "1e400", "-1e400", "true", "null",
		"１２", "١٢", "0.4", "1.3", "1e-7",
	}
	v := newVX(t, "vercel-paging-grammar")
	for _, param := range []string{"until", "since"} {
		for _, in := range good {
			if r := v.do(http.MethodGet, "/v2/teams?"+param+"="+url.QueryEscape(in), nil); r.status != 200 {
				t.Errorf("%s=%q = %d %s, want it read as a cursor", param, in, r.status, r.raw)
			}
		}
		for _, in := range bad {
			if r := v.do(http.MethodGet, "/v2/teams?"+param+"="+url.QueryEscape(in), nil); r.status != 400 {
				t.Errorf("%s=%q = %d %s, want 400: it is not a number a cursor is", param, in, r.status, r.raw)
			}
		}
	}
	// No value at all, and a value of nothing but spaces, ask for no cut.
	for _, in := range []string{"", "%20%20"} {
		if r := v.do(http.MethodGet, "/v2/teams?until="+in+"&since="+in, nil); r.status != 200 {
			t.Errorf("a blank cursor %q = %d %s, want no cursor", in, r.status, r.raw)
		}
	}
}
