package bir

import (
	"cel.dev/cel-go/cel"
	"cel.dev/cel-go/common/types"
	"cel.dev/cel-go/common/types/ref"
)

// The CEL surface available to behavior data.
//
// Two rules shape it. Expressions are pure: they see a request, resolved
// reads, and an injected clock reading — never the store, randomness or I/O.
// And bindings are declared per scope, so an expression that references a
// binding the operation does not define fails to load rather than at request
// time.
//
// Function bodies live in the engine. Declaring them here with placeholder
// bindings is enough to type-check behavior data without the validator
// depending on the runtime.

// baseVars are visible in every scope.
func baseVars() []cel.EnvOption {
	return []cel.EnvOption{
		cel.Variable("input", cel.MapType(cel.StringType, cel.DynType)),
		cel.Variable("identity", cel.MapType(cel.StringType, cel.DynType)),
		cel.Variable("now", cel.TimestampType),
		cel.Variable("endpoint", cel.StringType),
	}
}

// envFor builds an environment declaring the base variables plus names, each
// as a dynamic value. Anything not named is a load-time error.
func envFor(names ...string) (*cel.Env, error) {
	opts := baseVars()
	for _, n := range names {
		if n == "" {
			continue
		}
		opts = append(opts, cel.Variable(n, cel.DynType))
	}
	opts = append(opts, celFuncs()...)
	return cel.NewEnv(opts...)
}

// celFuncs declares the helper functions behavior data may call. Each has a
// placeholder binding so programs can be constructed during validation; the
// engine supplies the real implementations.
func celFuncs() []cel.EnvOption {
	dyn := cel.DynType
	str := cel.StringType
	num := cel.IntType

	unaryDyn := func(name string, in, out *cel.Type) cel.EnvOption {
		return cel.Function(name,
			cel.Overload(name+"_1", []*cel.Type{in}, out,
				cel.UnaryBinding(func(ref.Val) ref.Val { return types.NullValue })))
	}
	binaryDyn := func(name string, a, b, out *cel.Type) cel.EnvOption {
		return cel.Function(name,
			cel.Overload(name+"_2", []*cel.Type{a, b}, out,
				cel.BinaryBinding(func(ref.Val, ref.Val) ref.Val { return types.NullValue })))
	}
	ternaryDyn := func(name string, a, b, c, out *cel.Type) cel.EnvOption {
		return cel.Function(name,
			cel.Overload(name+"_3", []*cel.Type{a, b, c}, out,
				cel.FunctionBinding(func(...ref.Val) ref.Val { return types.NullValue })))
	}

	return []cel.EnvOption{
		// Digests and identifiers.
		unaryDyn("md5hex", str, str),
		unaryDyn("sha256hex", str, str),

		// Value helpers. coalesce takes the first non-empty argument, which is
		// how behavior data expresses a documented default without branching.
		binaryDyn("coalesce", dyn, dyn, dyn),
		ternaryDyn("clamp", num, num, num, num),
		unaryDyn("seconds", dyn, cel.DurationType),
		unaryDyn("parseJSON", str, dyn),
		binaryDyn("lastSegment", str, str, str),
		// CEL comprehensions over a map yield a list, so there is no core way
		// to build a map minus some keys or to layer two maps. Every provider's
		// Untag* and every "defaults, then stored, then set" attribute merge
		// needs one of these.
		binaryDyn("without", dyn, dyn, dyn),
		binaryDyn("merge", dyn, dyn, dyn),

		// Provider-shaped helpers. These are string manipulation, not provider
		// logic: the engine stays free of service names.
		binaryDyn("arn", str, dyn, str),
		unaryDyn("queueFromArn", str, str),
		binaryDyn("filterAttrs", dyn, dyn, dyn),

		// prim.<alias>(...) dispatch for pure primitives.
		binaryDyn("prim", str, dyn, dyn),
	}
}
