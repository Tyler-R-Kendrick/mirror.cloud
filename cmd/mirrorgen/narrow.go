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
// Nothing declares prefixes today except a service that needs them, and a
// service that declares none is untouched -- which is what keeps every
// existing model byte-identical.
func narrow(svc model.Service, prefixes []string) (model.Service, error) {
	if len(prefixes) == 0 {
		return svc, nil
	}

	var kept []model.Operation
	for _, op := range svc.Operations {
		for _, p := range prefixes {
			if strings.HasPrefix(op.HTTP.URI, p) {
				kept = append(kept, op)
				break
			}
		}
	}
	// A selector that matches nothing is the failure this project keeps
	// finding in another costume: a declaration nothing in the tree answers,
	// producing an empty artifact and no complaint. It is fatal here, naming
	// the prefixes, because the alternative is a model with no operations that
	// looks exactly like a service nobody has written yet.
	if len(kept) == 0 {
		return svc, fmt.Errorf("mirrorgen: %s: no operation URI begins with any of %s; "+
			"the document has %d operations and the selector matched none, so the "+
			"model would be empty", svc.ID, strings.Join(prefixes, ", "), len(svc.Operations))
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
	sel := map[string][]string{}
	for _, e := range want {
		if len(e.Paths) > 0 {
			sel[e.ID] = e.Paths
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

// selectorPrefix is how specs/mirror.set spells a narrowing: a third field
// `paths=/a/b,/c/d` on the service's line.
const selectorPrefix = "paths="

// parseSelector reads the prefixes off one set line's remaining fields.
func parseSelector(fields []string) ([]string, error) {
	var out []string
	for _, f := range fields {
		if !strings.HasPrefix(f, selectorPrefix) {
			return nil, fmt.Errorf("unknown field %q; expected %s<prefix>[,<prefix>...]", f, selectorPrefix)
		}
		for _, p := range strings.Split(strings.TrimPrefix(f, selectorPrefix), ",") {
			p = strings.TrimSpace(p)
			if p == "" {
				continue
			}
			if !strings.HasPrefix(p, "/") {
				return nil, fmt.Errorf("selector %q is not a URI prefix; it must begin with /", p)
			}
			out = append(out, p)
		}
	}
	sort.Strings(out) // deterministic: the same line always narrows the same way
	return out, nil
}
