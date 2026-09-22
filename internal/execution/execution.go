// Package execution is the one provider-neutral boundary between a bundle's
// declared resource actions and the backend that authoritatively executes
// them. It exists because some operations cannot be served by the native
// store: a Workers KV value written through the public REST API and the same
// value read through a Worker binding must be one authoritative resource, and
// when the selected execution backend owns that resource, the engine needs a
// way to say "this action, these arguments" without naming a provider, a
// process, or an address.
//
// The contract, in full:
//
//   - Descriptors are data: a finite named action with typed input/output, an
//     allowed resource kind, a body mode, an effect classification and a
//     backend requirement. Validation rejects unknown actions, version
//     mismatches and kind mismatches at registration and composition time;
//     nothing is discovered at request time.
//   - Resource references are trusted values resolved by mirror (environment,
//     account, kind, id). A reference never carries a user-supplied address,
//     method name or filesystem path.
//   - Call receives a context, the reference, validated arguments and an
//     optional bounded body stream. It answers structured output or a stream
//     plus a typed Failure that names its class -- validation, unsupported,
//     unavailable, absent, conflict, or unknown-outcome after a mutation was
//     transmitted.
//   - Nothing here holds a store lock: dispatch is for work that leaves the
//     store, and the caller's locking discipline is its own.
//
// The classes are the honesty of the boundary: an unavailable backend and a
// missing resource are different answers, and a mutation whose reply was lost
// says so rather than guessing.
package execution

import (
	"context"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
)

// Class is the failure taxonomy the boundary promises. Every Failure carries
// one; providers may not invent parallel words for the same meaning.
type Class string

const (
	// ClassValidation: the arguments do not satisfy the descriptor.
	ClassValidation Class = "validation"
	// ClassUnsupported: this backend does not implement that action.
	ClassUnsupported Class = "unsupported"
	// ClassUnavailable: the backend is not reachable or not ready.
	ClassUnavailable Class = "unavailable"
	// ClassAbsent: the referenced resource does not exist.
	ClassAbsent Class = "absent"
	// ClassConflict: the action collided with existing state.
	ClassConflict Class = "conflict"
	// ClassUnknownOutcome: the mutation was transmitted and the reply was
	// lost. Retrying blindly is the caller's decision to make, not ours to
	// pretend away.
	ClassUnknownOutcome Class = "outcome_unknown"
)

// Failure is the typed error of the boundary.
type Failure struct {
	Class  Class
	Action string
	Detail string
}

func (f *Failure) Error() string {
	return fmt.Sprintf("execution: %s: %s: %s", f.Action, f.Class, f.Detail)
}

// BodyMode declares how an action carries its payload.
type BodyMode string

const (
	BodyNone   BodyMode = "none"
	BodyBytes  BodyMode = "bytes"
	BodyJSON   BodyMode = "json"
	BodyStream BodyMode = "stream"
)

// EffectClass declares what an action does to the world. A read may repeat; a
// write may not be retried blindly; an invoke runs application code.
type EffectClass string

const (
	EffectRead   EffectClass = "read"
	EffectWrite  EffectClass = "write"
	EffectInvoke EffectClass = "invoke"
)

// Field is one typed member of an action's input or output.
type Field struct {
	Type     string `yaml:"type" json:"type"` // string, bytes, int, bool, json
	Required bool   `yaml:"required" json:"required,omitempty"`
}

// Descriptor is one finite, named action. A backend implements a closed set
// of these; the set is validated at registration and again when a bundle's
// declared actions are checked against it.
type Descriptor struct {
	Name       string           `yaml:"name" json:"name"`
	Version    int              `yaml:"version" json:"version"`
	Resource   string           `yaml:"resource" json:"resource"` // allowed resource kind
	Body       BodyMode         `yaml:"body" json:"body"`
	Effect     EffectClass      `yaml:"effect" json:"effect"`
	Backend    string           `yaml:"backend" json:"backend"`         // "" = any backend
	MinVersion string           `yaml:"min_version" json:"min_version"` // backend identity requirement, "" = none
	Input      map[string]Field `yaml:"input" json:"input"`
	Output     map[string]Field `yaml:"output" json:"output"`
}

// Ref is a trusted resource reference: mirror resolved every field before the
// caller ever saw a handle. It carries no address and no method.
type Ref struct {
	Environment string
	Account     string
	Kind        string
	ID          string
	Generation  uint64
}

// String is a compact identity for journals and errors -- never a dial target.
func (r Ref) String() string {
	return r.Environment + "/" + r.Account + "/" + r.Kind + "/" + r.ID + "@" + fmt.Sprint(r.Generation)
}

// Request is one validated invocation.
type Request struct {
	Ref    Ref
	Action string
	Args   map[string]any
	// Body accompanies the request when the descriptor's Body mode allows
	// it. It is bounded by the caller before dispatch.
	Body io.ReadCloser
}

// Response is a structured answer. Stream is set only when the descriptor's
// body mode is BodyStream, and the caller owns closing it.
type Response struct {
	Output     map[string]any
	Stream     io.ReadCloser
	Generation uint64
}

// Backend is one authoritative implementation: pinned identity plus the
// descriptors it implements.
type Backend interface {
	// Identity answers the backend's name and version for evidence and for
	// MinVersion checks.
	Identity() (name, version string)
	// Describe answers every descriptor the backend implements.
	Describe() []Descriptor
	// Call executes one action. It must return a *Failure for every
	// expected problem so callers can branch on Class.
	Call(ctx context.Context, req Request) (Response, error)
}

// Registry validates descriptors once, at registration, so a request never
// discovers that an action's shape disagrees with its implementation.
type Registry struct {
	mu      sync.RWMutex
	byName  map[string]Descriptor
	backend Backend
}

// NewRegistry validates every descriptor of the backend and answers a
// registry that serves them. Duplicate names with disagreeing shapes are
// refused; so are empty names, non-positive versions, unknown body/effect
// modes, and a descriptor whose Backend names this backend differently from
// Identity.
func NewRegistry(b Backend) (*Registry, error) {
	name, _ := b.Identity()
	seen := map[string]Descriptor{}
	for _, d := range b.Describe() {
		if err := validateDescriptor(d, name); err != nil {
			return nil, err
		}
		if prev, dup := seen[d.Name]; dup {
			if fmt.Sprint(prev) != fmt.Sprint(d) {
				return nil, fmt.Errorf("execution: %q declared twice with different shapes", d.Name)
			}
			continue
		}
		seen[d.Name] = d
	}
	if len(seen) == 0 {
		return nil, fmt.Errorf("execution: backend %q describes no actions", name)
	}
	return &Registry{byName: seen, backend: b}, nil
}

func validateDescriptor(d Descriptor, backendName string) error {
	switch {
	case strings.TrimSpace(d.Name) == "":
		return fmt.Errorf("execution: descriptor with empty name")
	case d.Version < 1:
		return fmt.Errorf("execution: %q: version must be >= 1", d.Name)
	case strings.TrimSpace(d.Resource) == "":
		return fmt.Errorf("execution: %q: no allowed resource kind", d.Name)
	}
	switch d.Body {
	case BodyNone, BodyBytes, BodyJSON, BodyStream:
	default:
		return fmt.Errorf("execution: %q: unknown body mode %q", d.Name, d.Body)
	}
	switch d.Effect {
	case EffectRead, EffectWrite, EffectInvoke:
	default:
		return fmt.Errorf("execution: %q: unknown effect class %q", d.Name, d.Effect)
	}
	if d.Backend != "" && d.Backend != backendName {
		return fmt.Errorf("execution: %q: requires backend %q, registered under %q", d.Name, d.Backend, backendName)
	}
	for _, side := range []struct {
		what string
		fs   map[string]Field
	}{{"input", d.Input}, {"output", d.Output}} {
		for fname, f := range side.fs {
			switch f.Type {
			case "string", "bytes", "int", "bool", "json":
			default:
				return fmt.Errorf("execution: %q: %s field %q has unknown type %q", d.Name, side.what, fname, f.Type)
			}
		}
	}
	return nil
}

// Lookup answers the descriptor for an action.
func (r *Registry) Lookup(action string) (Descriptor, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	d, ok := r.byName[action]
	return d, ok
}

// Actions lists the registered action names, sorted, for diagnostics.
func (r *Registry) Actions() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]string, 0, len(r.byName))
	for n := range r.byName {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}

// Covers reports whether the registry implements every named action at the
// given version. Composition runs this against a bundle's declared actions
// before serving traffic; a shortfall is a startup failure, never a
// request-time surprise.
func (r *Registry) Covers(declared map[string]int) error {
	for name, version := range declared {
		d, ok := r.Lookup(name)
		if !ok {
			return fmt.Errorf("execution: backend does not implement declared action %q", name)
		}
		if d.Version != version {
			return fmt.Errorf("execution: action %q: bundle declares version %d, backend implements %d", name, version, d.Version)
		}
	}
	return nil
}

// CheckRef rejects a reference whose kind the action does not allow, or that
// is missing its trusted scope. The check is here, once, rather than in each
// backend: a kind mixup is a mirror bug, not a provider behavior.
func CheckRef(d Descriptor, ref Ref) *Failure {
	if ref.Environment == "" || ref.Account == "" || ref.ID == "" {
		return &Failure{Class: ClassValidation, Action: d.Name, Detail: "reference missing trusted scope"}
	}
	if ref.Kind != d.Resource {
		return &Failure{Class: ClassValidation, Action: d.Name,
			Detail: fmt.Sprintf("resource kind %q not allowed (action allows %q)", ref.Kind, d.Resource)}
	}
	return nil
}

// ValidateArgs checks presence and declared type of every required input.
// Types are the descriptor's closed set; anything else is a validation
// failure naming the field, never a backend error.
func ValidateArgs(d Descriptor, args map[string]any) *Failure {
	fail := func(format string, a ...any) *Failure {
		return &Failure{Class: ClassValidation, Action: d.Name, Detail: fmt.Sprintf(format, a...)}
	}
	for name, f := range d.Input {
		v, present := args[name]
		if !present || v == nil {
			if f.Required {
				return fail("missing required input %q", name)
			}
			continue
		}
		if !typeMatches(f.Type, v) {
			return fail("input %q: want %s, got %T", name, f.Type, v)
		}
	}
	for name := range args {
		if _, declared := d.Input[name]; !declared {
			return fail("unknown input %q", name)
		}
	}
	return nil
}

func typeMatches(want string, v any) bool {
	switch want {
	case "string":
		_, ok := v.(string)
		return ok
	case "bytes":
		_, ok := v.([]byte)
		return ok
	case "int":
		switch v.(type) {
		case int, int32, int64:
			return true
		}
		return false
	case "bool":
		_, ok := v.(bool)
		return ok
	case "json":
		return true // any JSON-representable value
	}
	return false
}

// Dispatch validates then executes: descriptor, reference, arguments, then
// the backend. It never falls back to another backend -- an action this
// registry does not hold is unsupported, and saying so is the whole point.
func (r *Registry) Dispatch(ctx context.Context, req Request) (Response, error) {
	d, ok := r.Lookup(req.Action)
	if !ok {
		return Response{}, &Failure{Class: ClassUnsupported, Action: req.Action, Detail: "action not registered"}
	}
	if f := CheckRef(d, req.Ref); f != nil {
		return Response{}, f
	}
	if f := ValidateArgs(d, req.Args); f != nil {
		return Response{}, f
	}
	if d.Body == BodyNone && req.Body != nil {
		return Response{}, &Failure{Class: ClassValidation, Action: d.Name, Detail: "action takes no body"}
	}
	if err := ctx.Err(); err != nil {
		return Response{}, &Failure{Class: ClassUnavailable, Action: d.Name, Detail: "cancelled before dispatch: " + err.Error()}
	}
	return r.backend.Call(ctx, req)
}
