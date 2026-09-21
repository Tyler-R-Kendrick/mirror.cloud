package bir

import (
	"sort"

	"cel.dev/cel-go/cel"
	celast "cel.dev/cel-go/common/ast"
)

// Why prim calls are checked against the bundle's declarations.
//
// `prim` takes the primitive's name as a string, so calling one that does
// not exist type-checks and fails at request time instead. The names are
// knowable without running anything -- they are the string literals in first
// position of a `prim(...)` call -- which is what makes this a load-time
// check rather than a test: a bundle may only call what it declares under
// `primitives:`, and may only declare what the registry carries at the
// pinned version.

// primCalls answers every primitive name an expression calls, in sorted
// order. It walks the compiled syntax rather than the source text, so a
// `prim("x", ...)` written inside a string literal is not mistaken for a
// call. A dynamically computed name yields nothing and is left alone: what
// it names cannot be known here, and the engine fails it loudly at request
// time.
func primCalls(ast *cel.Ast) []string {
	var found []string
	var walk func(celast.Expr)
	walk = func(e celast.Expr) {
		if e == nil {
			return
		}
		if e.Kind() == celast.CallKind {
			call := e.AsCall()
			if call.FunctionName() == "prim" && !call.IsMemberFunction() && len(call.Args()) > 0 {
				if first := call.Args()[0]; first.Kind() == celast.LiteralKind {
					if s, ok := first.AsLiteral().Value().(string); ok {
						found = append(found, s)
					}
				}
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
