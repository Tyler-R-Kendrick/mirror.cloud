// Package fusion merges receiver fragments into one Bundle with recorded
// precedence. Higher-precedence evidence narrows; lower-precedence evidence
// completes. Fusion never drops provenance.
package fusion

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
)

// Fuse merges service fragments from any number of receivers into one Bundle.
// Precedence when two fragments describe the same cell:
//
//	verified > observed > declared.
//
// Equal confidence: the fragment whose SourceRef sorts first wins, and the
// conflict is recorded. Fusion never drops provenance and never silently
// resolves a conflict without recording it.
func Fuse(ctx context.Context, provider model.Provider, in [][]model.Service) (model.Bundle, []Conflict, error) {
	_ = ctx
	byID := map[string]model.Service{}
	var conflicts []Conflict
	var sources []model.SourceRef
	seenSrc := map[string]bool{}

	rank := func(c model.Confidence) int {
		switch c {
		case model.ConfVerified:
			return 3
		case model.ConfObserved:
			return 2
		default:
			return 1
		}
	}
	srcKey := func(s model.SourceRef) string {
		return s.Repo + "@" + s.Ref + ":" + s.Path + "#" + s.SHA256
	}

	for _, group := range in {
		for _, svc := range group {
			if svc.Source.Path != "" && !seenSrc[srcKey(svc.Source)] {
				sources = append(sources, svc.Source)
				seenSrc[srcKey(svc.Source)] = true
			}
			existing, ok := byID[svc.ID]
			if !ok {
				byID[svc.ID] = svc
				continue
			}
			// Operation-level merge: keep the higher-confidence op, record ties.
			ops := map[string]model.Operation{}
			for _, op := range existing.Operations {
				ops[op.Name] = op
			}
			for _, op := range svc.Operations {
				cur, have := ops[op.Name]
				if !have {
					ops[op.Name] = op
					continue
				}
				if rank(op.Confidence) > rank(cur.Confidence) {
					ops[op.Name] = op
					continue
				}
				if rank(op.Confidence) == rank(cur.Confidence) {
					winner, loser := cur.Source, op.Source
					if srcKey(op.Source) < srcKey(cur.Source) {
						ops[op.Name] = op
						winner, loser = op.Source, cur.Source
					}
					conflicts = append(conflicts, Conflict{
						ServiceID: svc.ID,
						Path:      "operations." + op.Name,
						Winner:    winner,
						Losers:    []model.SourceRef{loser},
						Reason:    "equal confidence; SourceRef sort order",
					})
				}
			}
			merged := existing
			merged.RouteAliases = append(append([]model.RouteAlias(nil), existing.RouteAliases...), svc.RouteAliases...)
			merged.Operations = merged.Operations[:0]
			names := make([]string, 0, len(ops))
			for n := range ops {
				names = append(names, n)
			}
			sort.Strings(names)
			for _, n := range names {
				merged.Operations = append(merged.Operations, ops[n])
			}
			if merged.Shapes == nil {
				merged.Shapes = map[string]model.Shape{}
			}
			for k, v := range svc.Shapes {
				if _, ok := merged.Shapes[k]; !ok {
					merged.Shapes[k] = v
				}
			}
			byID[svc.ID] = merged
		}
	}

	ids := make([]string, 0, len(byID))
	for id := range byID {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	out := model.Bundle{
		SchemaVersion: "1",
		Provider:      provider,
		Sources:       sources,
	}
	for _, id := range ids {
		svc := byID[id]
		if err := foldAliases(&svc); err != nil {
			return model.Bundle{}, nil, err
		}
		out.Services = append(out.Services, svc)
	}
	return out, conflicts, nil
}

// foldAliases moves a service's RouteAliases onto the operations they name and
// clears them, so the model that leaves fusion states each address once, on the
// operation that answers it.
//
// An alias that names no operation fails the build rather than vanishing: it
// would be a route the supplement believes it serves and nothing does. So does
// one that lands on an address another operation already owns with the same
// method -- the router would then pick between two operations by name, which is
// a guess, and which operation answers a request should never be one.
func foldAliases(svc *model.Service) error {
	if len(svc.RouteAliases) == 0 {
		return nil
	}
	owner := map[string]string{} // METHOD shape -> operation name
	for _, op := range svc.Operations {
		owner[strings.ToUpper(op.HTTP.Method)+" "+addressShape(op.HTTP.URI)] = op.Name
		for _, alt := range op.HTTP.AltURIs {
			owner[strings.ToUpper(op.HTTP.Method)+" "+addressShape(alt)] = op.Name
		}
	}
	index := map[string]int{}
	for i, op := range svc.Operations {
		index[op.Name] = i
	}
	for _, a := range svc.RouteAliases {
		i, ok := index[a.Operation]
		if !ok {
			return fmt.Errorf("fusion: %s: alias %s names operation %q, which no fragment declares", svc.ID, a.URI, a.Operation)
		}
		op := &svc.Operations[i]
		key := strings.ToUpper(op.HTTP.Method) + " " + addressShape(a.URI)
		if who, taken := owner[key]; taken {
			if who == op.Name {
				continue // already served there; the supplement repeating the document is harmless
			}
			return fmt.Errorf("fusion: %s: alias %s for %s collides with %s at %s", svc.ID, a.URI, op.Name, who, key)
		}
		owner[key] = op.Name
		op.HTTP.AltURIs = append(op.HTTP.AltURIs, a.URI)
	}
	for i := range svc.Operations {
		sort.Strings(svc.Operations[i].HTTP.AltURIs)
	}
	svc.RouteAliases = nil
	return nil
}

// addressShape reduces an address to what a router tells apart: a label is a
// label whatever it is called, so /v9/projects/{idOrName} and
// /v9/projects/{id} are one address.
func addressShape(uri string) string {
	parts := strings.Split(uri, "/")
	for i, p := range parts {
		if strings.HasPrefix(p, "{") && strings.HasSuffix(p, "}") {
			parts[i] = "{}"
		}
	}
	return strings.Join(parts, "/")
}

// Conflict records a fusion precedence decision.
type Conflict struct {
	ServiceID string
	Path      string // dotted path to the conflicting cell
	Winner    model.SourceRef
	Losers    []model.SourceRef
	Reason    string
}
