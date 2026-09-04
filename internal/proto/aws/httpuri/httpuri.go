// Package httpuri matches an HTTP request against the `httpUri` patterns the
// service model carries, and reports the path labels it bound.
//
// It exists because the REST protocols had no way to do this. restjson's
// generic route was "the first operation whose HTTP method equals the
// request's method", which resolves every GET a service serves to the same
// operation -- for GuardDuty, `GET /detector` and `GET /detector/{id}/filter`
// and `GET /findings` all answer as DescribeOrganizationConfiguration. Four
// services worked around it with hand-written routers; the rest could not be
// served over REST at all, which is why they were served as JSON-RPC instead.
//
// The reason the generic route was never wrong in practice is that no service
// in the booted catalog carried a URI to route on: every `HTTP.URI` is empty
// there, so the loop was unreachable. The patterns exist in the generated
// models, which is where this reads them from.
package httpuri

import (
	"net/http"
	"net/url"
	"sort"
	"strings"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
)

// segment is one path component of a pattern.
type segment struct {
	literal string // matched exactly when label is empty
	label   string // the member this component binds
	greedy  bool   // a `{Label+}` that swallows the rest of the path
}

// constraint is one required query-string entry from a pattern's `?a=b` tail.
// A constraint with no value requires only that the key be present, which is
// how a flag such as `?delete` is written.
type constraint struct {
	key      string
	value    string
	hasValue bool
}

// Pattern is a parsed `httpUri`.
type Pattern struct {
	segments []segment
	query    []constraint
	literals int // how many segments are literal, used to rank matches
	greedy   bool
}

// Parse reads one `httpUri` value. A pattern that binds nothing and constrains
// nothing -- "/" or "" -- is returned with no segments, which matches only the
// root path.
func Parse(uri string) Pattern {
	var p Pattern
	path := uri
	if i := strings.IndexByte(uri, '?'); i >= 0 {
		path = uri[:i]
		for _, raw := range strings.Split(uri[i+1:], "&") {
			if raw == "" {
				continue
			}
			c := constraint{key: raw}
			if k, v, ok := strings.Cut(raw, "="); ok {
				c = constraint{key: k, value: v, hasValue: true}
			}
			p.query = append(p.query, c)
		}
	}
	for _, part := range strings.Split(strings.Trim(path, "/"), "/") {
		if part == "" {
			continue
		}
		if strings.HasPrefix(part, "{") && strings.HasSuffix(part, "}") {
			name := part[1 : len(part)-1]
			s := segment{label: strings.TrimSuffix(name, "+")}
			s.greedy = strings.HasSuffix(name, "+")
			p.greedy = p.greedy || s.greedy
			p.segments = append(p.segments, s)
			continue
		}
		p.segments = append(p.segments, segment{literal: part})
		p.literals++
	}
	return p
}

// Match reports whether a request path and query satisfy the pattern, and
// binds the labels it names. A greedy label takes every remaining segment,
// separators included; a plain label takes exactly one and is percent-decoded,
// because a path segment carrying a `/` arrives escaped.
//
// The path must be the escaped one -- net/url decodes `%2F` into a separator
// on the way in, and a decoded path splits a single label into two segments.
func (p Pattern) Match(path string, query url.Values) (map[string]string, bool) {
	for _, c := range p.query {
		vs, ok := query[c.key]
		if !ok {
			return nil, false
		}
		if c.hasValue {
			found := false
			for _, v := range vs {
				if v == c.value {
					found = true
					break
				}
			}
			if !found {
				return nil, false
			}
		}
	}
	var parts []string
	for _, part := range strings.Split(strings.Trim(path, "/"), "/") {
		if part != "" {
			parts = append(parts, part)
		}
	}
	bound := map[string]string{}
	for i, seg := range p.segments {
		if seg.greedy {
			// A greedy label needs at least one segment, and it is the last
			// thing in the pattern by construction.
			if i >= len(parts) {
				return nil, false
			}
			bound[seg.label] = strings.Join(parts[i:], "/")
			return bound, true
		}
		if i >= len(parts) {
			return nil, false
		}
		if seg.label == "" {
			if parts[i] != seg.literal {
				return nil, false
			}
			continue
		}
		v, err := url.PathUnescape(parts[i])
		if err != nil {
			v = parts[i]
		}
		bound[seg.label] = v
	}
	if len(parts) != len(p.segments) {
		return nil, false
	}
	return bound, true
}

// Match finds the operation a request addresses. It returns the operation, the
// path labels it bound, and whether anything matched at all.
//
// More than one pattern can match the same request -- `/detector/{DetectorId}`
// and `/detector/{DetectorId}?foo` differ only in the query, and a literal
// segment can sit where another pattern has a label. The most constrained
// pattern wins: most required query entries first, then most literal segments,
// then a non-greedy pattern over a greedy one. Operation name breaks a
// remaining tie, so the choice does not depend on the order the model happens
// to list operations in.
func Match(svc *model.Service, r *http.Request) (*model.Operation, map[string]string, bool) {
	type candidate struct {
		op    *model.Operation
		bound map[string]string
		p     Pattern
	}
	var found []candidate
	query := r.URL.Query()
	for i := range svc.Operations {
		op := &svc.Operations[i]
		if op.HTTP.URI == "" || !strings.EqualFold(op.HTTP.Method, r.Method) {
			continue
		}
		p := Parse(op.HTTP.URI)
		bound, ok := p.Match(r.URL.EscapedPath(), query)
		if !ok {
			continue
		}
		found = append(found, candidate{op: op, bound: bound, p: p})
	}
	if len(found) == 0 {
		return nil, nil, false
	}
	sort.SliceStable(found, func(i, j int) bool {
		a, b := found[i], found[j]
		if len(a.p.query) != len(b.p.query) {
			return len(a.p.query) > len(b.p.query)
		}
		if a.p.literals != b.p.literals {
			return a.p.literals > b.p.literals
		}
		if a.p.greedy != b.p.greedy {
			return !a.p.greedy
		}
		return a.op.Name < b.op.Name
	})
	return found[0].op, found[0].bound, true
}
