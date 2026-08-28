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
	seen := map[string]bool{
		"id": true, "rec": true, "event": true, "fx": true,
		"hit": true, "item": true, "arn": true, "items": true,
	}
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
		if op.List != nil {
			for b := range op.List.Reads {
				seen[b] = true
				seen[b+"_found"] = true
			}
			for b := range op.List.Let {
				seen[b] = true
			}
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
		cel.Variable("endpoint", cel.StringType),
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

		// The inverse of parseJSON, for the services that keep a structured
		// document inside a string-valued attribute -- an SQS queue's access
		// policy, an SNS topic's, an IAM role's trust document. Reading one
		// without being able to write it back means a bundle can inspect such
		// a document but never amend it, which is what AddPermission does.
		//
		// Marshalling sorts map keys, so the same document always serializes
		// to the same bytes and a stored policy does not churn between
		// otherwise identical writes.
		cel.Function("toJSON", cel.Overload("toJSON_1", []*cel.Type{dyn}, str,
			cel.UnaryBinding(func(v ref.Val) ref.Val {
				raw, err := json.Marshal(fromCEL(v))
				if err != nil {
					return types.String("")
				}
				return types.String(string(raw))
			}))),

		// CEL's core has no way to build a map from another map minus some
		// keys: comprehensions over a map yield a list. Every `Untag*`
		// operation in every provider needs exactly that, so it is a function
		// rather than a per-service primitive.
		cel.Function("without", cel.Overload("without_2", []*cel.Type{dyn, dyn}, dyn,
			cel.BinaryBinding(func(m, keys ref.Val) ref.Val {
				src, _ := fromCEL(m).(map[string]any)
				drop := map[string]bool{}
				if list, ok := fromCEL(keys).([]any); ok {
					for _, k := range list {
						drop[fmt.Sprint(k)] = true
					}
				}
				out := map[string]any{}
				for k, v := range src {
					if !drop[k] {
						out[k] = v
					}
				}
				return types.DefaultTypeAdapter.NativeToValue(out)
			}))),

		// merge layers maps left to right, which is how a service states
		// "defaults, then what was stored, then what was set".
		cel.Function("merge", cel.Overload("merge_2", []*cel.Type{dyn, dyn}, dyn,
			cel.BinaryBinding(func(a, b ref.Val) ref.Val {
				out := map[string]any{}
				for _, layer := range []ref.Val{a, b} {
					if m, ok := fromCEL(layer).(map[string]any); ok {
						for k, v := range m {
							out[k] = v
						}
					}
				}
				return types.DefaultTypeAdapter.NativeToValue(out)
			}))),

		cel.Function("lastSegment", cel.Overload("lastSegment_2", []*cel.Type{str, str}, str,
			cel.BinaryBinding(func(s, sep ref.Val) ref.Val {
				parts := strings.Split(fmt.Sprint(s.Value()), fmt.Sprint(sep.Value()))
				return types.String(parts[len(parts)-1])
			}))),

		// arn builds "arn:<partition>:<rest joined by :>" from parts. String
		// assembly, not provider logic: the engine stays free of service names.
		cel.Function("arn", cel.Overload("arn_2", []*cel.Type{str, dyn}, str,
			cel.BinaryBinding(func(partition, parts ref.Val) ref.Val {
				out := []string{"arn", fmt.Sprint(partition.Value())}
				if list, ok := fromCEL(parts).([]any); ok {
					for _, p := range list {
						out = append(out, fmt.Sprint(p))
					}
				}
				return types.String(strings.Join(out, ":"))
			}))),

		// queueFromArn is lastSegment with the ARN separator, named for the
		// thing bundles actually write.
		cel.Function("queueFromArn", cel.Overload("queueFromArn_1", []*cel.Type{str}, str,
			cel.UnaryBinding(func(v ref.Val) ref.Val {
				parts := strings.Split(fmt.Sprint(v.Value()), ":")
				return types.String(parts[len(parts)-1])
			}))),

		// filterAttrs narrows a map to a requested subset. An empty request or
		// one naming "All" (or its "." shorthand) returns everything, which is
		// the convention every provider that has such a parameter follows.
		cel.Function("filterAttrs", cel.Overload("filterAttrs_2", []*cel.Type{dyn, dyn}, dyn,
			cel.BinaryBinding(func(attrs, want ref.Val) ref.Val {
				src, _ := fromCEL(attrs).(map[string]any)
				names, ok := fromCEL(want).([]any)
				if !ok || len(names) == 0 {
					return types.DefaultTypeAdapter.NativeToValue(src)
				}
				out := map[string]any{}
				for _, n := range names {
					name := fmt.Sprint(n)
					if name == "All" || name == "." {
						return types.DefaultTypeAdapter.NativeToValue(src)
					}
					if v, ok := src[name]; ok {
						out[name] = v
					}
				}
				return types.DefaultTypeAdapter.NativeToValue(out)
			}))),

		// prim dispatches to a named pure primitive. None are registered yet,
		// so a bundle that calls one fails loudly rather than returning a
		// plausible-looking value.
		cel.Function("prim", cel.Overload("prim_2", []*cel.Type{str, dyn}, dyn,
			cel.BinaryBinding(func(name, _ ref.Val) ref.Val {
				return types.NewErr("engine: primitive %q is not registered", fmt.Sprint(name.Value()))
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
