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

		// indices answers with 0..n-1 for a list, which is the one thing CEL's
		// comprehensions cannot give a bundle: `map` binds the element and
		// never its position.
		//
		// AWS batch responses are correlated by position rather than by a
		// caller-supplied id -- Comprehend's BatchDetectSentiment answers a
		// ResultList whose rows carry an Index into the TextList that was
		// sent, and its ErrorList carries the same Index for the rows that
		// failed. Without this a bundle can answer the right number of rows
		// and cannot say which input each one is about.
		//
		// It takes the list rather than a count so it cannot be asked for a
		// range larger than something already in memory.
		cel.Function("indices", cel.Overload("indices_1", []*cel.Type{dyn}, dyn,
			cel.UnaryBinding(func(v ref.Val) ref.Val {
				list, _ := fromCEL(v).([]any)
				out := make([]any, len(list))
				for i := range list {
					out[i] = int64(i)
				}
				return types.DefaultTypeAdapter.NativeToValue(out)
			}))),

		// series answers 0..n-1 for a count, where indices takes a list.
		//
		// RunInstances is why: the caller sends MinCount and the service
		// creates that many instances, so the count is the request's to choose
		// and there is no list to take it from. That is exactly the reason
		// indices refuses a count, so the bound it got from the list is
		// replaced by an explicit one -- seriesMax, far above any real launch.
		//
		// Above the bound this refuses rather than truncating. The hand-written
		// pack looped MinCount times with no bound at all, so one request
		// naming a million instances built a million records; answering a
		// truncated list instead would be a wrong answer where an error is an
		// honest one. A count that is absent, unparseable or below one answers
		// an empty range, and the bundle decides what that means.
		cel.Function("series", cel.Overload("series_1", []*cel.Type{dyn}, dyn,
			cel.UnaryBinding(func(v ref.Val) ref.Val {
				n := asInt(v)
				if n > seriesMax {
					return types.NewErr(
						"engine: series(%d) exceeds the %d the engine will build in one call", n, seriesMax)
				}
				out := make([]any, 0, max(n, 0))
				for i := int64(0); i < n; i++ {
					out = append(out, i)
				}
				return types.DefaultTypeAdapter.NativeToValue(out)
			}))),

		cel.Function("lastSegment", cel.Overload("lastSegment_2", []*cel.Type{str, str}, str,
			cel.BinaryBinding(func(s, sep ref.Val) ref.Val {
				parts := strings.Split(fmt.Sprint(s.Value()), fmt.Sprint(sep.Value()))
				return types.String(parts[len(parts)-1])
			}))),

		// lower case-folds ASCII. See the declaration in internal/bir/celenv.go
		// for why the Unicode form is not offered.
		cel.Function("lower", cel.Overload("lower_1", []*cel.Type{str}, str,
			cel.UnaryBinding(func(s ref.Val) ref.Val {
				return types.String(asciiLower(fmt.Sprint(s.Value())))
			}))),

		// upper is the same fold the other way. See the declaration in
		// internal/bir/celenv.go.
		cel.Function("upper", cel.Overload("upper_1", []*cel.Type{str}, str,
			cel.UnaryBinding(func(s ref.Val) ref.Val {
				return types.String(asciiUpper(fmt.Sprint(s.Value())))
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

// seriesMax bounds what one call may build. It is generous next to any real
// launch -- EC2's own RunInstances rejects counts far below it -- and finite,
// which is the property that matters: the count comes from the request.
const seriesMax = 1024

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

// asciiLower folds A-Z and leaves every other byte alone, so a value that is
// already a key stays the same length and the same bytes outside that range.
func asciiUpper(s string) string {
	out := []byte(s)
	for i, c := range out {
		if c >= 'a' && c <= 'z' {
			out[i] = c - ('a' - 'A')
		}
	}
	return string(out)
}

func asciiLower(s string) string {
	out := []byte(s)
	for i, c := range out {
		if c >= 'A' && c <= 'Z' {
			out[i] = c + ('a' - 'A')
		}
	}
	return string(out)
}
