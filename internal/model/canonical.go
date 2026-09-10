package model

import "strings"

// Canonical rewrites a produced value's member names into the names the shape
// declares, leaving everything the shape does not describe untouched.
//
// It exists because two producers of the same answer may name its members
// differently and still be saying the same thing. A hand-written pack answers
// `vpcSet` because someone typed the wire name; a bundle generated from the
// model answers `Vpcs`, because that is what the model calls the member. The
// codec serializes both to the same bytes -- a client cannot tell them apart --
// but a comparison of the two maps sees a difference on every renamed member.
//
// Canonicalizing both sides removes that difference and only that difference.
// Row counts, ordering, values and every member the model does not carry are
// left exactly as they were, so a comparison over canonical forms is as strict
// as one over the raw maps everywhere the names actually agreed.
//
// A wire name is only honoured when exactly one member claims it. Two members
// disagreeing about a name is a fact about the model, not something to resolve
// by picking one, so an ambiguous name is left alone.
func Canonical(svc *Service, shapeID string, v any) any {
	if svc == nil {
		return v
	}
	return canonical(svc, shapeID, v, 0)
}

// canonicalDepth bounds the descent. The walk follows the value, so a cyclic
// shape cannot loop on a finite value -- but a value deep enough to matter is
// already not an answer any service produces, and a bound costs nothing.
const canonicalDepth = 64

func canonical(svc *Service, shapeID string, v any, depth int) any {
	if depth > canonicalDepth {
		return v
	}
	shape, known := svc.Shapes[shapeID]
	switch t := v.(type) {
	case map[string]any:
		if known && shape.Kind == KindMap {
			// A map's keys are data, not member names.
			out := make(map[string]any, len(t))
			for k, item := range t {
				out[k] = canonical(svc, shape.Member, item, depth+1)
			}
			return out
		}
		out := make(map[string]any, len(t))
		for k, item := range t {
			name, child := declared(shape, known, k)
			out[name] = canonical(svc, child, item, depth+1)
		}
		return out
	case []any:
		child := ""
		if known && shape.Kind == KindList {
			child = shape.Member
		}
		out := make([]any, len(t))
		for i, item := range t {
			out[i] = canonical(svc, child, item, depth+1)
		}
		return out
	default:
		return v
	}
}

// declared answers the member name a produced key stands for, and the shape to
// carry into it.
func declared(shape Shape, known bool, key string) (name, child string) {
	if !known || (shape.Kind != KindStructure && shape.Kind != KindUnion) {
		return key, ""
	}
	if m, ok := shape.Members[key]; ok {
		return key, m.Shape
	}
	found, count := "", 0
	for n, m := range shape.Members {
		if m.Binding.Name == key {
			found, count = n, count+1
		}
	}
	if count == 1 {
		return found, shape.Members[found].Shape
	}
	return key, ""
}

// CanonicalPath rewrites a dotted path through a shape into the names the
// model declares, leaving list indices and anything the shape does not carry
// alone.
//
// A recording names a value by where it sat in the answer -- `vpc.VpcId` --
// and that path was written from what the pack produced. The bundle replacing
// it answers `Vpc.VpcId`, so the recorded path finds nothing in the new answer
// and a read-after-create silently becomes a read of the empty string.
func CanonicalPath(svc *Service, shapeID, path string) string {
	if svc == nil || path == "" {
		return path
	}
	segs := strings.Split(path, ".")
	cur := shapeID
	for i, seg := range segs {
		shape, known := svc.Shapes[cur]
		if known && (shape.Kind == KindList || shape.Kind == KindMap) {
			// An index or a map key: data, not a member name.
			cur = shape.Member
			continue
		}
		name, child := declared(shape, known, seg)
		segs[i], cur = name, child
	}
	return strings.Join(segs, ".")
}
