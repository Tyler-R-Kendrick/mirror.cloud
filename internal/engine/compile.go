package engine

import (
	"crypto/md5"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"cel.dev/cel-go/cel"
	"cel.dev/cel-go/common/types"
	"cel.dev/cel-go/common/types/ref"
)

// compileAll turns the bundle's expression sources into runnable programs.
//
// bir.Validate already compiled every expression to prove it is well-formed
// and references only bindings in scope. This compiles them again against the
// same declarations plus real function implementations, because validation
// deliberately keeps the runtime out of the loader.
func (e *Engine) compileAll() error {
	if e.ir.Compiled == nil {
		return fmt.Errorf("engine: %s was not loaded through bir.Load", e.ir.ServiceID)
	}
	names := e.bindingNames()
	env, err := runtimeEnv(names)
	if err != nil {
		return err
	}
	paths := make([]string, 0, len(e.ir.Compiled.Programs))
	for p := range e.ir.Compiled.Programs {
		paths = append(paths, p)
	}
	sort.Strings(paths)
	for _, p := range paths {
		src := e.ir.Compiled.Programs[p].Source
		ast, iss := env.Compile(src)
		if iss != nil && iss.Err() != nil {
			return fmt.Errorf("engine: %s: %s: %w", e.ir.ServiceID, p, iss.Err())
		}
		prg, err := env.Program(ast)
		if err != nil {
			return fmt.Errorf("engine: %s: %s: %w", e.ir.ServiceID, p, err)
		}
		e.programs[p] = prg
	}
	return nil
}

// bindingNames is the union of every name any scope in this bundle can bind.
// The loader already rejected out-of-scope references per operation, so a
// single permissive runtime environment cannot loosen that guarantee.
func (e *Engine) bindingNames() []string {
	seen := map[string]bool{"id": true, "rec": true, "event": true, "fx": true, "hit": true}
	for name := range e.ir.Resources {
		seen[name] = true
	}
	for _, op := range e.ir.Operations {
		for b := range op.Reads {
			seen[b] = true
			seen[b+"_found"] = true
		}
		for b := range op.Let {
			seen[b] = true
		}
		if op.Select != nil && op.Select.Binding != "" {
			seen[op.Select.Binding] = true
		}
	}
	out := make([]string, 0, len(seen))
	for n := range seen {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}

func runtimeEnv(names []string) (*cel.Env, error) {
	opts := []cel.EnvOption{
		cel.Variable("input", cel.MapType(cel.StringType, cel.DynType)),
		cel.Variable("identity", cel.MapType(cel.StringType, cel.DynType)),
		cel.Variable("now", cel.TimestampType),
	}
	for _, n := range names {
		opts = append(opts, cel.Variable(n, cel.DynType))
	}
	opts = append(opts, runtimeFuncs()...)
	return cel.NewEnv(opts...)
}

// runtimeFuncs implements the helpers behavior data may call. They are pure by
// construction: string and value manipulation only, no store, clock or
// randomness, so an expression cannot become an effect by accident.
func runtimeFuncs() []cel.EnvOption {
	str := cel.StringType
	num := cel.IntType
	dyn := cel.DynType

	return []cel.EnvOption{
		cel.Function("md5hex", cel.Overload("md5hex_1", []*cel.Type{str}, str,
			cel.UnaryBinding(func(v ref.Val) ref.Val {
				sum := md5.Sum([]byte(fmt.Sprint(v.Value())))
				return types.String(hex.EncodeToString(sum[:]))
			}))),

		cel.Function("sha256hex", cel.Overload("sha256hex_1", []*cel.Type{str}, str,
			cel.UnaryBinding(func(v ref.Val) ref.Val {
				sum := sha256.Sum256([]byte(fmt.Sprint(v.Value())))
				return types.String(hex.EncodeToString(sum[:]))
			}))),

		// coalesce returns the first argument that is neither null nor empty,
		// which is how behavior data states a documented default.
		cel.Function("coalesce", cel.Overload("coalesce_2", []*cel.Type{dyn, dyn}, dyn,
			cel.BinaryBinding(func(a, b ref.Val) ref.Val {
				if !blank(a) {
					return a
				}
				return b
			}))),

		cel.Function("clamp", cel.Overload("clamp_3", []*cel.Type{num, num, num}, num,
			cel.FunctionBinding(func(args ...ref.Val) ref.Val {
				v, lo, hi := asInt(args[0]), asInt(args[1]), asInt(args[2])
				if v < lo {
					v = lo
				}
				if v > hi {
					v = hi
				}
				return types.Int(v)
			}))),

		cel.Function("seconds", cel.Overload("seconds_1", []*cel.Type{dyn}, cel.DurationType,
			cel.UnaryBinding(func(v ref.Val) ref.Val {
				return types.Duration{Duration: time.Duration(asInt(v)) * time.Second}
			}))),

		cel.Function("parseJSON", cel.Overload("parseJSON_1", []*cel.Type{str}, dyn,
			cel.UnaryBinding(func(v ref.Val) ref.Val {
				var out any
				if err := json.Unmarshal([]byte(fmt.Sprint(v.Value())), &out); err != nil {
					return types.NullValue
				}
				return types.DefaultTypeAdapter.NativeToValue(out)
			}))),

		cel.Function("lastSegment", cel.Overload("lastSegment_2", []*cel.Type{str, str}, str,
			cel.BinaryBinding(func(s, sep ref.Val) ref.Val {
				parts := strings.Split(fmt.Sprint(s.Value()), fmt.Sprint(sep.Value()))
				return types.String(parts[len(parts)-1])
			}))),
	}
}

func blank(v ref.Val) bool {
	if v == nil || v == types.NullValue {
		return true
	}
	switch t := v.Value().(type) {
	case nil:
		return true
	case string:
		return t == ""
	}
	return false
}

func asInt(v ref.Val) int64 {
	switch t := v.Value().(type) {
	case int64:
		return t
	case int:
		return int64(t)
	case float64:
		return int64(t)
	case string:
		var n int64
		_, _ = fmt.Sscanf(t, "%d", &n)
		return n
	}
	return 0
}
