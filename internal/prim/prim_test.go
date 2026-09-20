package prim

import (
	"strings"
	"testing"
)

// The registry is the budget: a bundle may only call what is registered,
// and what is registered is this list. An unknown name must fail at lookup
// rather than smuggling a nil through the call.
func TestRegistryLookup(t *testing.T) {
	if _, ok := Lookup("prim.no.such.thing"); ok {
		t.Fatal("unknown primitive looked up")
	}
	f, ok := Lookup("opensearch.match")
	if !ok {
		t.Fatal("opensearch.match is not registered")
	}
	if f.Version != 1 {
		t.Fatalf("opensearch.match version %d, want 1", f.Version)
	}
	if got := strings.Join(Names(), ","); !strings.Contains(got, "opensearch.match") {
		t.Fatalf("Names() = %q", got)
	}
}

// The match half of the OpenSearch data plane, case by case from the pack:
// a nil query matches everything, match_all and the empty query match
// everything, term is exact, match and query_string are case-insensitive
// substrings, and anything else falls through to a case-sensitive substring.
func TestOpensearchMatch(t *testing.T) {
	f, ok := Lookup("opensearch.match")
	if !ok {
		t.Fatal("opensearch.match is not registered")
	}
	call := func(q, src any) bool {
		t.Helper()
		out, err := f.Call([]any{q, src})
		if err != nil {
			t.Fatalf("call(%v, %v): %v", q, src, err)
		}
		b, ok := out.(bool)
		if !ok {
			t.Fatalf("call(%v, %v) = %T, want bool", q, src, out)
		}
		return b
	}
	src := map[string]any{"city": "Austin", "state": "TX"}
	cases := []struct {
		name string
		q    any
		want bool
	}{
		{"nil matches", nil, true},
		{"empty map matches", map[string]any{}, true},
		{"match_all matches", map[string]any{"match_all": map[string]any{}}, true},
		{"term exact", map[string]any{"term": map[string]any{"city": "Austin"}}, true},
		{"term miss", map[string]any{"term": map[string]any{"city": "Boston"}}, false},
		{"term case-sensitive", map[string]any{"term": map[string]any{"city": "austin"}}, false},
		{"match substring", map[string]any{"match": map[string]any{"city": "ust"}}, true},
		{"match case-insensitive", map[string]any{"match": map[string]any{"city": "AUSTIN"}}, true},
		{"match miss", map[string]any{"match": map[string]any{"city": "zzz"}}, false},
		{"query_string substring", map[string]any{"query_string": map[string]any{"query": "tin"}}, true},
		{"query_string case-insensitive", map[string]any{"query_string": map[string]any{"query": "AUSTIN TX"}}, false},
		{"query_string miss", map[string]any{"query_string": map[string]any{"query": "zzz"}}, false},
		{"bare string substring", "ust", true},
		{"bare string case-sensitive", "UST", false},
		{"term on missing field", map[string]any{"term": map[string]any{"zip": "1"}}, false},
		{"term on scalar source", map[string]any{"term": map[string]any{"city": "Austin"}}, false},
	}
	for _, c := range cases {
		var s any = src
		if c.name == "term on scalar source" {
			s = "Austin"
		}
		if got := call(c.q, s); got != c.want {
			t.Errorf("%s: got %v, want %v", c.name, got, c.want)
		}
	}
	if _, err := f.Call([]any{"only-one"}); err == nil {
		t.Error("one argument accepted, want arity error")
	}
}
