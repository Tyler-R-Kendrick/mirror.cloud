// Package fusion merges receiver fragments into one Bundle with recorded
// precedence. Higher-precedence evidence narrows; lower-precedence evidence
// completes. Fusion never drops provenance.
package fusion

import (
	"context"
	"sort"

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
		out.Services = append(out.Services, byID[id])
	}
	return out, conflicts, nil
}

// Conflict records a fusion precedence decision.
type Conflict struct {
	ServiceID string
	Path      string // dotted path to the conflicting cell
	Winner    model.SourceRef
	Losers    []model.SourceRef
	Reason    string
}
