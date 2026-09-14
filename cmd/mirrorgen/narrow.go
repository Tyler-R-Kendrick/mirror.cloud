package main

import (
	"fmt"
	"sort"
	"strings"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
)

// narrow keeps the operations of a service whose URI begins with one of the
// declared prefixes, and drops every shape the survivors cannot reach.
//
// It exists because a vendor may publish one document for its entire platform.
// Cloudflare's is 24 MB: 2,164 paths, 3,462 operations, 56,854 shapes once
// ingested, gzipping to 2.8 MB -- against 5.6 MB for all 152 models the repo
// has today, and 0.3 MB for the largest single one. Six of those operations
// are the KV surface anything here serves; the other 3,456 answer 501 and are
// carried by every build and every binary forever.
//
// The obvious alternative is to trim the document at sync time, and
// scripts/specs-sync.sh refuses it for a reason worth repeating: the lock's
// hash is the pin, and hashing a document we rewrote pins our rewrite rather
// than the vendor's bytes. So the whole document stays vendored and hashed,
// and the narrowing happens here, after ingest, where it is a declared
// property of specs/mirror.set rather than a silent edit to the input.
//
// Nothing declares a selector today except a service that needs one, and a
// service that declares none is untouched -- which is what keeps every
// existing model byte-identical.
//
// A URI prefix is not the only way to say which operations matter, and for one
// protocol it cannot say it at all. Every GraphQL operation is served from one
// endpoint, so `paths=` either keeps all of them or none. `fields=` names them
// instead. The two are alternatives rather than a filter pair: an operation
// survives if it matches ANY criterion the line declares, because a line that
// declares both is asking for the union of two ways of pointing at operations,
// not for operations that are somehow both.
func narrow(svc model.Service, sel selector) (model.Service, error) {
	if sel.empty() {
		return svc, nil
	}

	var kept []model.Operation
	for _, op := range svc.Operations {
		if sel.matches(op) {
			kept = append(kept, op)
		}
	}
	// A selector that matches nothing is the failure this project keeps
	// finding in another costume: a declaration nothing in the tree answers,
	// producing an empty artifact and no complaint. It is fatal here, naming
	// the prefixes, because the alternative is a model with no operations that
	// looks exactly like a service nobody has written yet.
	if len(kept) == 0 {
		return svc, fmt.Errorf("mirrorgen: %s: no operation matched %s; "+
			"the document has %d operations and the selector matched none, so the "+
			"model would be empty", svc.ID, sel, len(svc.Operations))
	}

	reach := map[string]bool{}
	var walk func(id string)
	walk = func(id string) {
		if id == "" || reach[id] {
			return
		}
		shape, ok := svc.Shapes[id]
		if !ok {
			// A dangling reference is the receiver's business, and it already
			// refuses documents that carry one. Stopping here rather than
			// failing keeps this function about narrowing.
			return
		}
		reach[id] = true
		for _, m := range shape.Members {
			walk(m.Shape)
		}
		walk(shape.Member)
		walk(shape.Key)
	}
	for _, op := range kept {
		walk(op.Input)
		walk(op.Output)
		for _, e := range op.Errors {
			walk(e)
		}
	}

	shapes := make(map[string]model.Shape, len(reach))
	for id := range reach {
		shapes[id] = svc.Shapes[id]
	}
	svc.Operations = kept
	svc.Shapes = shapes
	return svc, nil
}

// narrowAll applies each service's declared prefixes. The entries are indexed
// by ID rather than walked per service so a set with hundreds of lines stays
// linear.
func narrowAll(svcs []model.Service, want []setEntry) ([]model.Service, error) {
	sel := map[string]selector{}
	for _, e := range want {
		if !e.Select.empty() {
			sel[e.ID] = e.Select
		}
	}
	if len(sel) == 0 {
		return svcs, nil
	}
	out := make([]model.Service, 0, len(svcs))
	for _, svc := range svcs {
		narrowed, err := narrow(svc, sel[svc.ID])
		if err != nil {
			return nil, err
		}
		out = append(out, narrowed)
	}
	return out, nil
}

// How specs/mirror.set spells a narrowing: trailing fields on the service's
// line, `paths=/a/b,/c/d` or `fields=name,otherName`.
const (
	pathsPrefix  = "paths="
	fieldsPrefix = "fields="
)

// selector is what one set line asks to keep.
type selector struct {
	Paths  []string // operations whose URI begins with one of these
	Fields []string // operations with one of these names
}

func (s selector) empty() bool { return len(s.Paths) == 0 && len(s.Fields) == 0 }

func (s selector) matches(op model.Operation) bool {
	for _, p := range s.Paths {
		if strings.HasPrefix(op.HTTP.URI, p) {
			return true
		}
	}
	for _, f := range s.Fields {
		if op.Name == f {
			return true
		}
	}
	return false
}

// String is what the "matched nothing" error names, so it has to say which kind
// of selector was tried rather than printing a bare list.
func (s selector) String() string {
	var parts []string
	if len(s.Paths) > 0 {
		parts = append(parts, pathsPrefix+strings.Join(s.Paths, ","))
	}
	if len(s.Fields) > 0 {
		parts = append(parts, fieldsPrefix+strings.Join(s.Fields, ","))
	}
	return strings.Join(parts, " ")
}

// parseSelector reads one set line's remaining fields.
func parseSelector(fields []string) (selector, error) {
	var out selector
	for _, f := range fields {
		switch {
		case strings.HasPrefix(f, pathsPrefix):
			for _, p := range split(f, pathsPrefix) {
				// A path selector is a statement about the document's URIs, so
				// a value that is not one is a typo rather than a filter that
				// happens to match nothing.
				if !strings.HasPrefix(p, "/") {
					return selector{}, fmt.Errorf("selector %q is not a URI prefix; it must begin with /", p)
				}
				out.Paths = append(out.Paths, p)
			}
		case strings.HasPrefix(f, fieldsPrefix):
			for _, n := range split(f, fieldsPrefix) {
				// The mirror image of the rule above: an operation name is not
				// a path, and a value beginning with / is a `paths=` entry
				// written under the wrong key.
				if strings.HasPrefix(n, "/") {
					return selector{}, fmt.Errorf("selector %q is a URI prefix, not an operation name; use %s", n, pathsPrefix)
				}
				out.Fields = append(out.Fields, n)
			}
		default:
			return selector{}, fmt.Errorf("unknown field %q; expected %s<prefix>[,...] or %s<name>[,...]", f, pathsPrefix, fieldsPrefix)
		}
	}
	// Deterministic: the same line always narrows the same way.
	sort.Strings(out.Paths)
	sort.Strings(out.Fields)
	return out, nil
}

func split(field, prefix string) []string {
	var out []string
	for _, v := range strings.Split(strings.TrimPrefix(field, prefix), ",") {
		if v = strings.TrimSpace(v); v != "" {
			out = append(out, v)
		}
	}
	return out
}
