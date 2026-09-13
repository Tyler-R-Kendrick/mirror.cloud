package bir

import (
	"fmt"
	"sort"

	"cel.dev/cel-go/cel"
	celast "cel.dev/cel-go/common/ast"
)

// Why `fx` is checked by key and the other bindings are not.
//
// Every binding a behavior expression may name is declared to CEL, so naming
// one that does not exist is already a load error. `fx` is the exception: it is
// one binding holding a map, so `fx.anything` type-checks whatever the
// operation's effects actually produce, and the mistake surfaces as a nil at
// request time instead.
//
// That is not hypothetical. The Railway bundle projected `fx.project.id` for
// six operations and loaded clean; `fx` is keyed by the effect's own name, so a
// `create:` effect binds `fx.create`, and `fx.project` was a key no effect in
// the tree could ever produce. The equivalence recording caught it. Nothing in
// the loader did, and an extraction without a recording would have shipped it.
//
// The keys are knowable without running anything, which is what makes this a
// load-time check rather than a test: an effect binds its own name, a counter
// binds `counter`, and a generate binds whatever `bind` says.

// fxKeys answers the keys an operation's effects can bind, in the order the
// engine would bind them.
//
// It mirrors runEffects. A delete, a dedup and a send_event bind nothing, which
// is why they are absent rather than forgotten: only the three writes, the
// counter and an explicit generate reach `fx`.
func fxKeys(op Operation) map[string]bool {
	keys := map[string]bool{}
	for _, eff := range op.Effects {
		switch {
		case eff.Create != nil:
			keys["create"] = true
		case eff.Put != nil:
			keys["put"] = true
		case eff.Patch != nil:
			keys["patch"] = true
		case eff.Counter != nil:
			keys["counter"] = true
		case eff.Generate != nil:
			if eff.Generate.Bind != "" {
				keys[eff.Generate.Bind] = true
			}
		}
	}
	return keys
}

// fxSelects answers every key an expression reads out of `fx`.
//
// It walks the compiled syntax rather than the source text, so a `fx.name`
// written inside a string literal is not mistaken for a reference and a
// reference spread across lines is still found. A dynamic index -- `fx[x]` --
// yields no key and is left alone: what it reads cannot be known here, and
// refusing it would be a guess in the opposite direction.
func fxSelects(ast *cel.Ast) []string {
	var found []string
	var walk func(celast.Expr)
	walk = func(e celast.Expr) {
		if e == nil {
			return
		}
		if e.Kind() == celast.SelectKind {
			sel := e.AsSelect()
			if op := sel.Operand(); op != nil && op.Kind() == celast.IdentKind && op.AsIdent() == "fx" {
				found = append(found, sel.FieldName())
			}
		}
		for _, child := range children(e) {
			walk(child)
		}
	}
	walk(ast.NativeRep().Expr())
	sort.Strings(found)
	return found
}

// children answers an expression's sub-expressions, across the kinds a
// behavior expression can take.
func children(e celast.Expr) []celast.Expr {
	switch e.Kind() {
	case celast.SelectKind:
		return []celast.Expr{e.AsSelect().Operand()}
	case celast.CallKind:
		call := e.AsCall()
		out := call.Args()
		if call.IsMemberFunction() {
			out = append(append([]celast.Expr{}, call.Target()), out...)
		}
		return out
	case celast.ListKind:
		return e.AsList().Elements()
	case celast.MapKind:
		var out []celast.Expr
		for _, entry := range e.AsMap().Entries() {
			kv := entry.AsMapEntry()
			out = append(out, kv.Key(), kv.Value())
		}
		return out
	case celast.StructKind:
		var out []celast.Expr
		for _, field := range e.AsStruct().Fields() {
			out = append(out, field.AsStructField().Value())
		}
		return out
	case celast.ComprehensionKind:
		c := e.AsComprehension()
		return []celast.Expr{
			c.IterRange(), c.AccuInit(), c.LoopCondition(), c.LoopStep(), c.Result(),
		}
	default:
		return nil
	}
}

// checkFxRefs reports an expression reading a key no effect of this operation
// binds.
//
// An operation with no effects at all reads no `fx`, and says so plainly rather
// than listing an empty set.
func checkFxRefs(serviceID, where string, ast *cel.Ast, keys map[string]bool, problems *Errors) {
	for _, key := range fxSelects(ast) {
		if keys[key] {
			continue
		}
		if len(keys) == 0 {
			*problems = append(*problems, fmt.Errorf(
				"%s: %s: reads fx.%s but this operation has no effect that binds anything",
				serviceID, where, key))
			continue
		}
		*problems = append(*problems, fmt.Errorf(
			"%s: %s: reads fx.%s but this operation's effects bind %v; an effect binds its own name, so a `create` binds fx.create and the record it wrote is `rec`",
			serviceID, where, key, sortedSet(keys)))
	}
}

func sortedSet(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
