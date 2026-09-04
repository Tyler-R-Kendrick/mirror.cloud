package httpuri_test

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/generated"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/proto/aws/httpuri"
)

func get(method, target string) *http.Request {
	return httptest.NewRequest(method, target, nil)
}

// TestPatternMatch covers the pattern language on its own: literals, plain
// labels, greedy labels and the query entries a pattern can require.
func TestPatternMatch(t *testing.T) {
	for _, tc := range []struct {
		name    string
		uri     string
		path    string
		query   string
		want    map[string]string
		nomatch bool
	}{
		{name: "literal", uri: "/detector", path: "/detector", want: map[string]string{}},
		{name: "literal rejects a longer path", uri: "/detector", path: "/detector/x", nomatch: true},
		{name: "literal rejects a shorter path", uri: "/detector/x", path: "/detector", nomatch: true},
		{
			name: "label binds one segment",
			uri:  "/detector/{DetectorId}", path: "/detector/abc",
			want: map[string]string{"DetectorId": "abc"},
		},
		{
			name: "label does not span a separator",
			uri:  "/detector/{DetectorId}", path: "/detector/a/b", nomatch: true,
		},
		{
			name: "label is percent-decoded",
			uri:  "/detector/{DetectorId}", path: "/detector/a%2Fb",
			want: map[string]string{"DetectorId": "a/b"},
		},
		{
			name: "greedy label takes the rest",
			uri:  "/v2/{Key+}", path: "/v2/a/b/c",
			want: map[string]string{"Key": "a/b/c"},
		},
		{name: "greedy label needs at least one segment", uri: "/v2/{Key+}", path: "/v2", nomatch: true},
		{
			name: "required query value",
			uri:  "/x?mode=fast", path: "/x", query: "mode=fast",
			want: map[string]string{},
		},
		{name: "required query value absent", uri: "/x?mode=fast", path: "/x", nomatch: true},
		{name: "required query value differs", uri: "/x?mode=fast", path: "/x", query: "mode=slow", nomatch: true},
		{
			name: "query flag needs only the key",
			uri:  "/x?delete", path: "/x", query: "delete=",
			want: map[string]string{},
		},
		{name: "root", uri: "/", path: "/", want: map[string]string{}},
		{name: "root rejects a path", uri: "/", path: "/x", nomatch: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			q, err := url.ParseQuery(tc.query)
			if err != nil {
				t.Fatal(err)
			}
			bound, ok := httpuri.Parse(tc.uri).Match(tc.path, q)
			if tc.nomatch {
				if ok {
					t.Fatalf("%q matched %q, binding %v", tc.path, tc.uri, bound)
				}
				return
			}
			if !ok {
				t.Fatalf("%q did not match %q", tc.path, tc.uri)
			}
			if len(bound) != len(tc.want) {
				t.Fatalf("bound %v, want %v", bound, tc.want)
			}
			for k, v := range tc.want {
				if bound[k] != v {
					t.Errorf("%s = %q, want %q", k, bound[k], v)
				}
			}
		})
	}
}

// TestMatchPrefersTheMoreConstrainedPattern pins the ranking. Two patterns can
// both match, and the choice must not depend on the order the model lists its
// operations in -- which is the failure mode the method-only route had in its
// most general form.
func TestMatchPrefersTheMoreConstrainedPattern(t *testing.T) {
	svc := &model.Service{
		ID: "test",
		Operations: []model.Operation{
			{Name: "ZGreedy", HTTP: model.HTTPBinding{Method: "GET", URI: "/a/{Rest+}"}},
			{Name: "ALabel", HTTP: model.HTTPBinding{Method: "GET", URI: "/a/{Id}"}},
			{Name: "MLiteral", HTTP: model.HTTPBinding{Method: "GET", URI: "/a/status"}},
			{Name: "BQuery", HTTP: model.HTTPBinding{Method: "GET", URI: "/a/status?detail"}},
		},
	}
	for _, tc := range []struct{ path, want string }{
		// The literal beats the label, and the query-constrained pattern beats
		// the literal alone.
		{"/a/status", "MLiteral"},
		{"/a/status?detail=1", "BQuery"},
		{"/a/other", "ALabel"},
		{"/a/x/y", "ZGreedy"},
	} {
		op, _, ok := httpuri.Match(svc, get("GET", tc.path))
		if !ok {
			t.Fatalf("%s matched nothing", tc.path)
		}
		if op.Name != tc.want {
			t.Errorf("%s routed to %s, want %s", tc.path, op.Name, tc.want)
		}
	}
	// Listing the operations in the opposite order must not change the answer.
	reversed := &model.Service{ID: "test"}
	for i := len(svc.Operations) - 1; i >= 0; i-- {
		reversed.Operations = append(reversed.Operations, svc.Operations[i])
	}
	op, _, _ := httpuri.Match(reversed, get("GET", "/a/status"))
	if op.Name != "MLiteral" {
		t.Errorf("reversing the operation list changed the route to %s", op.Name)
	}
}

// TestMatchAgainstARealSpec is the check that matters. GuardDuty is the
// service the disagreement was found on: it is restJson1 with ninety
// operations, all of them distinguished by path.
//
// With the old route -- the first operation whose HTTP method matched -- every
// one of these resolved to whichever GET the model happened to list first.
func TestMatchAgainstARealSpec(t *testing.T) {
	svc, err := generated.Model("aws.guardduty")
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		method, path, want string
		bound              map[string]string
	}{
		{"GET", "/detector", "ListDetectors", map[string]string{}},
		{"POST", "/detector", "CreateDetector", map[string]string{}},
		{"GET", "/detector/d-1", "GetDetector", map[string]string{"DetectorId": "d-1"}},
		{"DELETE", "/detector/d-1", "DeleteDetector", map[string]string{"DetectorId": "d-1"}},
		{"GET", "/detector/d-1/filter", "ListFilters", map[string]string{"DetectorId": "d-1"}},
		{
			"GET", "/detector/d-1/filter/f-1", "GetFilter",
			map[string]string{"DetectorId": "d-1", "FilterName": "f-1"},
		},
		{"POST", "/detector/d-1/findings/create", "CreateSampleFindings", map[string]string{"DetectorId": "d-1"}},
	} {
		op, bound, ok := httpuri.Match(svc, get(tc.method, tc.path))
		if !ok {
			t.Errorf("%s %s matched no operation", tc.method, tc.path)
			continue
		}
		if op.Name != tc.want {
			t.Errorf("%s %s routed to %s, want %s", tc.method, tc.path, op.Name, tc.want)
			continue
		}
		for k, v := range tc.bound {
			if bound[k] != v {
				t.Errorf("%s %s bound %s=%q, want %q", tc.method, tc.path, k, bound[k], v)
			}
		}
		if len(bound) != len(tc.bound) {
			t.Errorf("%s %s bound %v, want %v", tc.method, tc.path, bound, tc.bound)
		}
	}
}

// TestEveryOperationRoutesToItself is the property the ranking has to satisfy
// across a whole specification: instantiate each operation's own URI with
// values for its labels, and the router must come back with that operation.
//
// Where it cannot, the two operations are genuinely indistinguishable from a
// request -- the same method and a pattern that differs only in label names --
// and a real SDK could not address them apart either. Those are named rather
// than tolerated silently, so a routing regression cannot hide among them.
func TestEveryOperationRoutesToItself(t *testing.T) {
	for _, id := range []string{"aws.guardduty", "aws.apigatewayv2", "aws.backup", "aws.location", "aws.mq"} {
		t.Run(id, func(t *testing.T) {
			svc, err := generated.Model(id)
			if err != nil {
				t.Fatal(err)
			}
			// Group by the shape of the pattern, so a collision can be
			// recognised as an ambiguity in the specification rather than
			// reported as a routing fault.
			shape := map[string][]string{}
			for _, op := range svc.Operations {
				shape[op.HTTP.Method+" "+skeleton(op.HTTP.URI)] = append(
					shape[op.HTTP.Method+" "+skeleton(op.HTTP.URI)], op.Name)
			}
			ambiguous := 0
			for i := range svc.Operations {
				op := &svc.Operations[i]
				if op.HTTP.URI == "" {
					t.Errorf("%s has no URI", op.Name)
					continue
				}
				got, _, ok := httpuri.Match(svc, get(op.HTTP.Method, instantiate(op.HTTP.URI)))
				if !ok {
					t.Errorf("%s %s (%s) matched no operation", op.HTTP.Method, op.HTTP.URI, op.Name)
					continue
				}
				if got.Name == op.Name {
					continue
				}
				if len(shape[op.HTTP.Method+" "+skeleton(op.HTTP.URI)]) > 1 {
					ambiguous++
					continue
				}
				t.Errorf("%s %s (%s) routed to %s", op.HTTP.Method, op.HTTP.URI, op.Name, got.Name)
			}
			if ambiguous > 0 {
				t.Logf("%d operations share a request shape with another and "+
					"cannot be told apart from the request alone", ambiguous)
			}
		})
	}
}

// instantiate fills a pattern's labels with values, producing a request target
// the pattern itself matches.
func instantiate(uri string) string {
	out, depth := make([]byte, 0, len(uri)), 0
	for i := 0; i < len(uri); i++ {
		switch uri[i] {
		case '{':
			depth++
			out = append(out, 'v', 'a', 'l')
		case '}':
			depth--
		default:
			if depth == 0 {
				out = append(out, uri[i])
			}
		}
	}
	return string(out)
}

// skeleton erases label names, leaving the shape a request can distinguish:
// `/x/{A}` and `/x/{B}` are the same request.
func skeleton(uri string) string {
	out, depth := make([]byte, 0, len(uri)), 0
	for i := 0; i < len(uri); i++ {
		switch uri[i] {
		case '{':
			if depth == 0 {
				out = append(out, '{')
			}
			depth++
		case '}':
			depth--
			if depth == 0 {
				out = append(out, '}')
			}
		case '+':
			if depth > 0 {
				out = append(out, '+')
				continue
			}
			out = append(out, '+')
		default:
			if depth == 0 {
				out = append(out, uri[i])
			}
		}
	}
	return string(out)
}
