// Package engine serves Behavior IR bundles.
//
// One generic implementation replaces the per-service Go packs: given a
// validated bundle and the generated model for the same service, it is an
// ordinary spi.BehaviorPack. Adding a service becomes a specs/ entry plus
// behavior/ data, with no Go to write.
//
// Evaluation order is fixed, and each step exists for a reason:
//
//	validate   required members and constraints, from the generated model
//	reads      resolve declared store reads before any expression runs
//	let        derived values
//	require    ordered preconditions; the first failure decides the error,
//	           which is what makes error precedence reviewable
//	effects    the closed mutation vocabulary, in order
//	output     projection, type-checked against the operation's output shape
//
// Expressions never touch the store, the wall clock or randomness: reads are
// resolved up front, time is injected, and identifiers come from effects. That
// is what keeps a bundle reproducible from a seed.
package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"

	"cel.dev/cel-go/cel"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/bir"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

// Engine serves one service from its bundle.
type Engine struct {
	deps  spi.Deps
	ir    *bir.Service
	model *model.Service

	ops      []string
	programs map[string]cel.Program
	modelOps map[string]model.Operation
}

var _ spi.BehaviorPack = (*Engine)(nil)

// New builds an engine for one service.
//
// The model is required and must carry shapes: without them there is nothing
// to validate requests against and nothing to check projections against, which
// is the state the bootstrap catalog shipped in. Refusing here means that
// condition can never silently reach a request again.
func New(deps spi.Deps, ir *bir.Service, svc *model.Service) (*Engine, error) {
	if ir == nil {
		return nil, fmt.Errorf("engine: no bundle")
	}
	if svc == nil || len(svc.Shapes) == 0 {
		return nil, fmt.Errorf("engine: %s has no generated model with shapes; "+
			"run `make specs-sync && make generate`", ir.ServiceID)
	}
	if deps.Clock == nil || deps.Rand == nil || deps.Store == nil {
		return nil, fmt.Errorf("engine: %s needs Clock, Rand and Store", ir.ServiceID)
	}

	e := &Engine{
		deps:     deps,
		ir:       ir,
		model:    svc,
		programs: map[string]cel.Program{},
		modelOps: map[string]model.Operation{},
	}
	for _, op := range svc.Operations {
		e.modelOps[op.Name] = op
	}
	for name := range ir.Operations {
		if _, ok := e.modelOps[name]; !ok {
			return nil, fmt.Errorf("engine: %s declares %s, which the model does not have",
				ir.ServiceID, name)
		}
		e.ops = append(e.ops, name)
	}
	sort.Strings(e.ops)

	if err := e.compileAll(); err != nil {
		return nil, err
	}
	return e, nil
}

// ServiceID reports the service this engine serves.
func (e *Engine) ServiceID() string { return e.ir.ServiceID }

// Tier reports emulate: a bundle describes real semantics, not synthesis.
func (e *Engine) Tier() model.Tier { return model.TierEmulate }

// Operations lists the operations the bundle defines.
func (e *Engine) Operations() []string { return append([]string(nil), e.ops...) }

// Invoke serves one request.
func (e *Engine) Invoke(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	op, ok := e.ir.Operations[req.Operation]
	if !ok {
		return nil, spi.NotImplemented(e.ServiceID(), req.Operation, string(model.TierEmulate))
	}
	modelOp := e.modelOps[req.Operation]

	if fault := e.validateInput(modelOp, req); fault != nil {
		return nil, fault
	}

	// A batch operation is its singular sibling, run once per entry.
	if op.Batch != nil {
		return e.runBatch(ctx, req, op)
	}

	ev := e.newEval(req, op)
	if err := ev.resolveReads(ctx, op); err != nil {
		return nil, err
	}
	if err := ev.evalLets(op); err != nil {
		return nil, err
	}
	if fault := ev.checkRequires(op); fault != nil {
		return nil, fault
	}
	// Select is the observation point: expired deadlines fire here, so what an
	// operation acts on is a function of the clock reading rather than of when
	// anyone last looked. Wait re-observes until the bundle's condition holds.
	if err := ev.runSelect(ctx, op); err != nil {
		return nil, err
	}
	if err := ev.runWait(ctx, op); err != nil {
		return nil, err
	}
	if op.Wait != nil && len(op.Wait.OnTimeout) > 0 {
		done, err := ev.evalBool("operations." + req.Operation + ".wait.until")
		if err != nil {
			return nil, err
		}
		if !done {
			out, err := ev.projectMap("operations."+req.Operation+".wait.on_timeout.output",
				op.Wait.OnTimeout["output"])
			if err != nil {
				return nil, err
			}
			return &spi.Response{Output: out}, nil
		}
	}
	if err := ev.runEffects(ctx, op); err != nil {
		if errors.Is(err, errShortCircuit) {
			return &spi.Response{Output: ev.shortOutput}, nil
		}
		return nil, err
	}
	if err := ev.runList(ctx, op, modelOp); err != nil {
		return nil, err
	}
	out, err := ev.project(op, modelOp)
	if err != nil {
		return nil, err
	}
	// An idempotency entry can only be completed once the answer exists; a
	// duplicate inside the window replays exactly this.
	if err := ev.completeDedup(ctx, out); err != nil {
		return nil, err
	}
	return &spi.Response{Output: out}, nil
}

// validateInput enforces the model's required members and constraints. This is
// the check the empty-shape catalog silently disabled; it runs before any
// behavior so a malformed request never reaches an effect.
func (e *Engine) validateInput(op model.Operation, req *spi.Request) *spi.Fault {
	if op.Input == "" {
		return nil
	}
	shape, ok := e.model.Shapes[op.Input]
	if !ok {
		return nil
	}
	names := make([]string, 0, len(shape.Members))
	for name := range shape.Members {
		names = append(names, name)
	}
	sort.Strings(names) // deterministic: the same request always names the same member first
	for _, name := range names {
		m := shape.Members[name]
		v, present := req.Input[name]
		// Required means present, not non-empty. Whether an empty value is
		// acceptable is a length constraint, which the model already carries
		// -- and conflating the two took the decision away from the service:
		// Polly answers an empty Text with InvalidSsmlException, which it
		// could never do if the engine had already rejected it as missing.
		if m.Required && !present {
			return &spi.Fault{
				Code:       "ValidationException",
				Message:    fmt.Sprintf("%s is required", name),
				HTTPStatus: 400,
				Fault:      "client",
			}
		}
		if !present {
			continue
		}
		if fault := checkConstraints(e.model, name, m, v); fault != nil {
			return fault
		}
	}
	return nil
}

// checkConstraints applies the length, range and pattern traits carried by the
// member's shape. These were parsed from the specs all along and consumed by
// nothing.
func checkConstraints(svc *model.Service, name string, m model.Member, v any) *spi.Fault {
	shape, ok := svc.Shapes[m.Shape]
	if !ok {
		return nil
	}
	c := shape.Constraints
	bad := func(msg string) *spi.Fault {
		return &spi.Fault{
			Code:       "ValidationException",
			Message:    fmt.Sprintf("%s %s", name, msg),
			HTTPStatus: 400,
			Fault:      "client",
		}
	}
	if s, isStr := v.(string); isStr {
		if c.MinLength != nil && int64(len(s)) < *c.MinLength {
			return bad(fmt.Sprintf("must be at least %d characters", *c.MinLength))
		}
		if c.MaxLength != nil && int64(len(s)) > *c.MaxLength {
			return bad(fmt.Sprintf("must be at most %d characters", *c.MaxLength))
		}
	}
	if n, isNum := toFloat(v); isNum {
		if c.MinValue != nil && n < *c.MinValue {
			return bad(fmt.Sprintf("must be at least %v", *c.MinValue))
		}
		if c.MaxValue != nil && n > *c.MaxValue {
			return bad(fmt.Sprintf("must be at most %v", *c.MaxValue))
		}
	}
	return nil
}

func toFloat(v any) (float64, bool) {
	switch t := v.(type) {
	case float64:
		return t, true
	case float32:
		return float64(t), true
	case int:
		return float64(t), true
	case int64:
		return float64(t), true
	}
	return 0, false
}

// fault renders one row of the bundle's error table.
func (e *Engine) fault(ref, message string) *spi.Fault {
	def, ok := e.ir.Errors[ref]
	if !ok {
		// Validation rejects unknown references at load time, so reaching here
		// means the bundle was constructed in memory rather than loaded.
		return &spi.Fault{
			Code:       "InternalFailure",
			Message:    fmt.Sprintf("undefined error %q", ref),
			HTTPStatus: 500,
			Fault:      "server",
		}
	}
	msg := message
	if msg == "" {
		msg = def.Message
	}
	return &spi.Fault{
		Code:       def.Code,
		Message:    msg,
		HTTPStatus: def.HTTP,
		Fault:      def.Fault,
	}
}

// scope returns the account+region namespaced store scope for a request. Every
// read and write goes through here, so tenant isolation is structural.
func (e *Engine) scope(req *spi.Request) spi.Scope {
	return e.deps.Store.Scope(req.Identity.Account, req.Identity.Region)
}

// collection resolves a resource's collection name, interpolating {x.y}
// references against the current bindings for parent-scoped resources.
func (ev *eval) collection(res bir.Resource) (spi.Collection, error) {
	name, err := ev.interpolate(res.Collection)
	if err != nil {
		return nil, err
	}
	return ev.e.scope(ev.req).Collection(name), nil
}

// interpolate expands {binding.field} templates in a collection name or ARN.
func (ev *eval) interpolate(s string) (string, error) {
	if !strings.Contains(s, "{") {
		return s, nil
	}
	var out strings.Builder
	for {
		i := strings.Index(s, "{")
		if i < 0 {
			out.WriteString(s)
			return out.String(), nil
		}
		j := strings.Index(s[i:], "}")
		if j < 0 {
			return "", fmt.Errorf("engine: unterminated template in %q", s)
		}
		out.WriteString(s[:i])
		ref := s[i+1 : i+j]
		v, err := ev.lookup(ref)
		if err != nil {
			return "", err
		}
		out.WriteString(fmt.Sprint(v))
		s = s[i+j+1:]
	}
}

// lookup resolves a dotted reference against the current bindings and the
// request identity. Templates are deliberately simpler than CEL: they name a
// value, they do not compute one.
func (ev *eval) lookup(ref string) (any, error) {
	switch ref {
	case "account":
		return ev.req.Identity.Account, nil
	case "region":
		return ev.req.Identity.Region, nil
	case "partition":
		return "aws", nil
	case "id":
		return ev.id, nil
	}
	head, rest, _ := strings.Cut(ref, ".")
	// A resource name resolves to the key currently in play for it, which is
	// how a child's collection names its parent -- "msgs:{queue.id}" -- without
	// depending on what this operation called its binding.
	if rest == "id" {
		if key, ok := ev.resIDs[head]; ok {
			return key, nil
		}
	}
	v, ok := ev.binds[head]
	if !ok {
		return nil, fmt.Errorf("engine: template references unknown binding %q", head)
	}
	if rest == "" {
		return v, nil
	}
	m, ok := v.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("engine: %q is not a record", head)
	}
	return m[rest], nil
}

func marshal(v any) ([]byte, error)   { return json.Marshal(v) }
func unmarshal(b []byte, v any) error { return json.Unmarshal(b, v) }
