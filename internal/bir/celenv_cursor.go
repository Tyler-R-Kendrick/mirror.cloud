package bir

import (
	"cel.dev/cel-go/cel"
	"cel.dev/cel-go/common/types"
	"cel.dev/cel-go/common/types/ref"
)

// cursorFuncs declares the two functions a paging listing calls. The engine
// supplies them (internal/engine/cursor_cel.go says what they answer).
func cursorFuncs() []cel.EnvOption {
	str, num, dyn := cel.StringType, cel.IntType, cel.DynType
	return []cel.EnvOption{
		// pageOf(items, msMember, newer, older, ascending, limit) is the page a
		// listing answers, in the order of the instant its items were made and then
		// the order they were: {page, pagination, first}. A cursor is a timestamp
		// in milliseconds that carries the item's place among those made in the
		// same millisecond, so that a page boundary can fall between them; the
		// cursors that cut the listing are lists of the text a client sent.
		cel.Function("pageOf", cel.Overload("pageOf_6", []*cel.Type{dyn, str, dyn, dyn, cel.BoolType, num}, dyn,
			cel.FunctionBinding(func(...ref.Val) ref.Val { return types.NullValue }))),
		// validCursors is whether every cursor in a list reads.
		cel.Function("validCursors", cel.Overload("validCursors_1", []*cel.Type{dyn}, cel.BoolType,
			cel.UnaryBinding(func(ref.Val) ref.Val { return types.NullValue }))),
	}
}
