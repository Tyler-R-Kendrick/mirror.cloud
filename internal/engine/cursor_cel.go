package engine

import (
	"cel.dev/cel-go/cel"
	"cel.dev/cel-go/common/types"
	"cel.dev/cel-go/common/types/ref"
)

// cursorFuncs are the two functions a paging listing calls. pageOf is the page
// (page.go), and validCursors is whether every cursor in a list reads, so a
// listing can refuse `until=yesterday` with a 400 before it cuts anything.
//
//	pageOf(items, msMember, newer, older, ascending, limit)
//
// answers {page, pagination, first}: `newer` and `older` are lists of the cursors
// as text (`since`; `until`), and msMember names the member that holds the
// instant an item was made.
func cursorFuncs() []cel.EnvOption {
	str, num, dyn := cel.StringType, cel.IntType, cel.DynType
	return []cel.EnvOption{
		cel.Function("pageOf", cel.Overload("pageOf_6", []*cel.Type{dyn, str, dyn, dyn, cel.BoolType, num}, dyn,
			cel.FunctionBinding(func(args ...ref.Val) ref.Val {
				items, _ := fromCEL(args[0]).([]any)
				newer, err := cursorsOf(args[2])
				if err != nil {
					return types.NewErr("%s", err.Error())
				}
				older, err := cursorsOf(args[3])
				if err != nil {
					return types.NewErr("%s", err.Error())
				}
				ascending, _ := args[4].Value().(bool)
				out := pageOf(items, args[1].Value().(string), newer, older, ascending, int(asInt(args[5])))
				return types.DefaultTypeAdapter.NativeToValue(out)
			}))),
		cel.Function("validCursors", cel.Overload("validCursors_1", []*cel.Type{dyn}, cel.BoolType,
			cel.UnaryBinding(func(v ref.Val) ref.Val {
				_, err := cursorsOf(v)
				return types.Bool(err == nil)
			}))),
	}
}

// cursorsOf reads a CEL list of cursors, as text or numbers.
func cursorsOf(v ref.Val) ([]cursor, error) {
	list, _ := fromCEL(v).([]any)
	out := make([]cursor, 0, len(list))
	for _, e := range list {
		c, err := parseCursor(e)
		if err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, nil
}
