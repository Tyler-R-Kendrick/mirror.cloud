package httpuri_test

import (
	"errors"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/generated"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/proto/aws/httpuri"
)

// FuzzParseAndMatch drives the pattern parser and matcher with arbitrary
// patterns and paths. Both come off the wire in effect: the pattern from a
// vendored specification the project does not write, the path from a client.
// Neither may panic, and a match must bind every label the pattern names --
// an operation routed with a member silently missing is worse than one that
// does not route.
func FuzzParseAndMatch(f *testing.F) {
	for _, seed := range []struct{ uri, path, query string }{
		{"/detector/{DetectorId}", "/detector/d-1", ""},
		{"/v2/{Key+}", "/v2/a/b/c", ""},
		{"/x?mode=fast", "/x", "mode=fast"},
		{"/", "/", ""},
		{"", "", ""},
		{"{}", "/", ""},
		{"/{+}", "/a", ""},
		{"//{A}//", "//a//", ""},
		{"/{A}/{A}", "/x/y", ""},
		{"/%2F", "/%2F", ""},
	} {
		f.Add(seed.uri, seed.path, seed.query)
	}
	f.Fuzz(func(t *testing.T, uri, path, rawQuery string) {
		query, err := url.ParseQuery(rawQuery)
		if err != nil {
			t.Skip()
		}
		p := httpuri.Parse(uri)
		bound, ok := p.Match(path, query)
		if !ok {
			if bound != nil {
				t.Fatalf("Parse(%q).Match(%q) refused but bound %v", uri, path, bound)
			}
			return
		}
		// Every label in the pattern must appear in the binding. A pattern
		// naming the same label twice keeps one value, which is why this
		// checks presence rather than count.
		for _, part := range strings.Split(uri, "/") {
			if i := strings.IndexByte(part, '?'); i >= 0 {
				part = part[:i]
			}
			if !strings.HasPrefix(part, "{") || !strings.HasSuffix(part, "}") {
				continue
			}
			label := strings.TrimSuffix(part[1:len(part)-1], "+")
			if _, has := bound[label]; !has {
				t.Fatalf("Parse(%q).Match(%q) matched without binding %q: %v",
					uri, path, label, bound)
			}
		}
	})
}

// FuzzMatchAgainstARealService points the whole matcher at a specification, so
// the corpus explores real patterns rather than only the ones a seed names.
// Routing must be total in the sense that matters: it either names an
// operation the service declares, or it names none.
func FuzzMatchAgainstARealService(f *testing.F) {
	svc, err := generated.Model("aws.guardduty")
	if err != nil {
		f.Fatal(err)
	}
	declared := map[string]bool{}
	for _, op := range svc.Operations {
		declared[op.Name] = true
	}
	for _, seed := range []string{"/detector", "/detector/d-1", "/detector/d-1/filter/f-1", "/", "//", "/detector/"} {
		f.Add("GET", seed)
	}
	f.Fuzz(func(t *testing.T, method, path string) {
		r, err := newRequest(method, path)
		if err != nil {
			t.Skip()
		}
		op, bound, ok := httpuri.Match(svc, r)
		if !ok {
			return
		}
		if op == nil || !declared[op.Name] {
			t.Fatalf("%s %q routed to an operation the service does not declare: %#v", method, path, op)
		}
		for _, seg := range strings.Split(op.HTTP.URI, "/") {
			if !strings.HasPrefix(seg, "{") || !strings.HasSuffix(seg, "}") {
				continue
			}
			label := strings.TrimSuffix(seg[1:len(seg)-1], "+")
			if _, has := bound[label]; !has {
				t.Fatalf("%s %q routed to %s without binding %q", method, path, op.Name, label)
			}
		}
	})
}

// newRequest builds a request without httptest.NewRequest's panic on a target
// it cannot parse; the fuzzer reaches those long before it reaches a path.
func newRequest(method, target string) (*http.Request, error) {
	if method == "" || strings.ContainsAny(method, " \t\r\n") {
		return nil, errBadMethod
	}
	u, err := url.Parse(target)
	if err != nil {
		return nil, err
	}
	return &http.Request{Method: method, URL: u}, nil
}

var errBadMethod = errors.New("httpuri: unusable method")
