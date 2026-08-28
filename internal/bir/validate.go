package bir

import (
	"fmt"
	"sort"
	"strings"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
)

// Validate checks a loaded bundle against the generated model and compiles
// every expression. It reports all problems at once.
//
// The checks exist to make three classes of mistake impossible to merge:
//
//   - describing a response the protocol cannot serialize (an output member
//     that is not in the operation's output shape);
//   - referring to something that does not exist (an unknown resource, error,
//     primitive alias or statechart state);
//   - shipping an expression that only fails at request time.
func Validate(s *Service, svc *model.Service) error {
	var problems Errors

	ops := map[string]model.Operation{}
	for _, op := range svc.Operations {
		ops[op.Name] = op
	}

	compiled := &Compiled{Programs: map[string]Program{}}

	// compilerFor returns a compile function scoped to the bindings that are
	// actually in scope. An expression naming anything else fails to load,
	// which turns a typo in behavior data into a build error rather than a
	// request-time surprise.
	compilerFor := func(names ...string) func(string, string) {
		env, err := envFor(names...)
		if err != nil {
			problems = append(problems, fmt.Errorf("%s: CEL environment: %w", s.ServiceID, err))
			return func(string, string) {}
		}
		return func(pathStr, src string) {
			if strings.TrimSpace(src) == "" {
				return
			}
			ast, iss := env.Compile(src)
			if iss != nil && iss.Err() != nil {
				problems = append(problems, fmt.Errorf("%s: %s: %v", s.ServiceID, pathStr, iss.Err()))
				return
			}
			if _, err := env.Program(ast); err != nil {
				problems = append(problems, fmt.Errorf("%s: %s: %w", s.ServiceID, pathStr, err))
				return
			}
			compiled.Programs[pathStr] = Program{Source: src}
		}
	}

	if s.Provenance != "" && !s.Provenance.Valid() {
		problems = append(problems, fmt.Errorf("%s: unknown provenance %q", s.ServiceID, s.Provenance))
	}

	// Resources.
	for _, name := range sortedKeys(s.Resources) {
		res := s.Resources[name]
		where := "resources." + name
		if res.Collection == "" {
			problems = append(problems, fmt.Errorf("%s: %s: no collection", s.ServiceID, where))
		}
		if res.Parent != "" {
			if _, ok := s.Resources[res.Parent]; !ok {
				problems = append(problems, fmt.Errorf("%s: %s: unknown parent resource %q",
					s.ServiceID, where, res.Parent))
			}
		}
		if g := res.ID.Generate; g != nil {
			switch g.Kind {
			case "hex", "uuid", "int":
			default:
				problems = append(problems, fmt.Errorf("%s: %s.id.generate: unknown kind %q",
					s.ServiceID, where, g.Kind))
			}
		}
		// A resource expression sees its own record, ID and ARN, plus its
		// parent resource when it has one (an SQS message guard reads
		// queue.redrive). `arn` is bound only where the resource declares an
		// arn template, so referencing it without one is an authoring error
		// the engine reports at the reference.
		resScope := []string{"id", "rec", "arn"}
		if res.Parent != "" {
			resScope = append(resScope, res.Parent)
		}
		compile := compilerFor(resScope...)
		compile(where+".id.derive", res.ID.Derive)
		for _, k := range sortedKeys(res.Record) {
			compile(where+".record."+k, res.Record[k])
			if reservedMember(k) {
				problems = append(problems, fmt.Errorf(
					"%s: %s.record.%s: %s is reserved for the record's lifecycle state",
					s.ServiceID, where, k, k))
			}
		}
		for _, k := range sortedKeys(res.Views) {
			compile(where+".views."+k, res.Views[k])
			// Views are merged onto the record when it is read, so a view that
			// shares a name with a stored member would silently shadow it and
			// the two would disagree the moment either changed.
			if _, clash := res.Record[k]; clash {
				problems = append(problems, fmt.Errorf(
					"%s: %s.views.%s: a record member of that name already exists; "+
						"a view may not shadow stored data", s.ServiceID, where, k))
			}
		}
		if res.Key != "" {
			if _, ok := res.Record[res.Key]; !ok {
				problems = append(problems, fmt.Errorf(
					"%s: %s.key: keys on %q, which the record does not set",
					s.ServiceID, where, res.Key))
			}
		}
		if res.Statechart != nil {
			// Transitions additionally see the event that triggered them.
			validateStatechart(s, where, res.Statechart,
				compilerFor(append(append([]string{}, resScope...), "event")...), &problems)
		}
	}

	// Error table.
	for _, name := range sortedKeys(s.Errors) {
		e := s.Errors[name]
		where := "errors." + name
		if e.Code == "" {
			problems = append(problems, fmt.Errorf("%s: %s: no code", s.ServiceID, where))
		}
		if e.HTTP < 100 || e.HTTP > 599 {
			problems = append(problems, fmt.Errorf("%s: %s: http %d out of range", s.ServiceID, where, e.HTTP))
		}
		switch e.Fault {
		case "client", "server":
		default:
			problems = append(problems, fmt.Errorf("%s: %s: fault must be client or server, got %q",
				s.ServiceID, where, e.Fault))
		}
		if e.Provenance != "" && !e.Provenance.Valid() {
			problems = append(problems, fmt.Errorf("%s: %s: unknown provenance %q", s.ServiceID, where, e.Provenance))
		}
	}

	// Operations.
	for _, name := range sortedKeys(s.Operations) {
		op := s.Operations[name]
		where := "operations." + name

		modelOp, known := ops[name]
		if !known {
			problems = append(problems, fmt.Errorf("%s: %s: no such operation in the generated model",
				s.ServiceID, where))
			continue
		}

		// An operation expression sees: its read bindings and their _found
		// companions, its let bindings, its select binding, the results of
		// earlier effects (fx), a dedup hit, the resolved resource ID, and the
		// resource names themselves for collection templates. Nothing else.
		scope := []string{"id", "fx", "hit"}
		for _, rn := range sortedKeys(s.Resources) {
			scope = append(scope, rn)
		}
		for _, b := range sortedKeys(op.Reads) {
			scope = append(scope, b, b+"_found")
		}
		for _, b := range sortedKeys(op.Let) {
			scope = append(scope, b)
		}
		if op.Select != nil && op.Select.Binding != "" {
			scope = append(scope, op.Select.Binding)
		}
		// A write effect binds the record it wrote as `rec`, which is how an
		// operation returns what it just created without restating it. It is
		// in scope only for operations that actually write, so referring to it
		// elsewhere stays a load error rather than a nil at request time. A
		// read binding named rec adds it above by its own name.
		if opWrites(op) {
			scope = append(scope, "rec")
		}
		compile := compilerFor(scope...)

		for _, b := range sortedKeys(op.Reads) {
			r := op.Reads[b]
			if _, ok := s.Resources[r.Resource]; !ok {
				problems = append(problems, fmt.Errorf("%s: %s.reads.%s: unknown resource %q",
					s.ServiceID, where, b, r.Resource))
			}
			compile(where+".reads."+b+".key", r.Key)
		}
		for _, b := range sortedKeys(op.Let) {
			compile(where+".let."+b, op.Let[b])
		}

		for i, req := range op.Require {
			p := fmt.Sprintf("%s.require[%d]", where, i)
			compile(p+".cond", req.Cond)
			if req.Error == "" {
				problems = append(problems, fmt.Errorf("%s: %s: no error reference", s.ServiceID, p))
			} else if _, ok := s.Errors[req.Error]; !ok {
				problems = append(problems, fmt.Errorf("%s: %s: unknown error %q; add it to the error table",
					s.ServiceID, p, req.Error))
			}
		}

		if sel := op.Select; sel != nil {
			if _, ok := s.Resources[sel.Resource]; !ok {
				problems = append(problems, fmt.Errorf("%s: %s.select: unknown resource %q",
					s.ServiceID, where, sel.Resource))
			}
			if sel.Binding == "" {
				problems = append(problems, fmt.Errorf("%s: %s.select: no binding", s.ServiceID, where))
			}
			compile(where+".select.limit", sel.Limit)
			// order_by and filter are evaluated once per candidate, which is
			// bound as `item`.
			perItem := compilerFor(append(append([]string{}, scope...), "item")...)
			perItem(where+".select.filter", sel.Filter)
			perItem(where+".select.order_by", sel.OrderBy)
			if sel.Group != nil {
				compile(where+".select.group.when", sel.Group.When)
				if sel.Group.By == "" {
					problems = append(problems, fmt.Errorf("%s: %s.select.group: no `by` member",
						s.ServiceID, where))
				}
			}
		}
		if w := op.Wait; w != nil {
			if op.Select == nil {
				problems = append(problems, fmt.Errorf("%s: %s.wait: wait needs a select to observe",
					s.ServiceID, where))
			}
			compile(where+".wait.until", w.Until)
			compile(where+".wait.timeout", w.Timeout)
			compileAny(where+".wait.on_timeout.output", asMapAny(w.OnTimeout["output"]), compile)
		}

		for i, eff := range op.Effects {
			validateEffect(s, fmt.Sprintf("%s.effects[%d]", where, i), eff, compile, &problems)
		}

		if l := op.List; l != nil {
			if _, ok := s.Resources[l.Resource]; !ok {
				problems = append(problems, fmt.Errorf("%s: %s.list: unknown resource %q",
					s.ServiceID, where, l.Resource))
			}
			if l.Member == "" {
				problems = append(problems, fmt.Errorf("%s: %s.list: no output member", s.ServiceID, where))
			} else {
				checkOutputMember(s, svc, modelOp, where+".list.member", l.Member, &problems)
			}
			compile(where+".list.key", l.Key)
			// The filter sees one candidate record as `item`, and nothing the
			// rest of the operation cannot see.
			compilerFor(append(append([]string{}, scope...), "item")...)(where+".list.filter", l.Filter)
		}

		// The central check: a bundle may not describe a response the wire
		// protocol cannot carry.
		for _, m := range sortedKeys(op.Output) {
			compile(where+".output."+m, op.Output[m])
			checkOutputMember(s, svc, modelOp, where+".output", m, &problems)
		}

		if op.Provenance != "" && !op.Provenance.Valid() {
			problems = append(problems, fmt.Errorf("%s: %s: unknown provenance %q",
				s.ServiceID, where, op.Provenance))
		}
	}

	// Primitive aliases must be declared before use; the budget is enforced
	// separately, but an undeclared alias is always a mistake.
	for _, alias := range sortedKeys(s.Primitives) {
		if s.Primitives[alias].Name == "" {
			problems = append(problems, fmt.Errorf("%s: primitives.%s: no name", s.ServiceID, alias))
		}
	}

	if len(problems) > 0 {
		return problems
	}
	s.Compiled = compiled
	return nil
}

// checkOutputMember reports an output member that the operation's output shape
// does not declare. This is what stops a bundle inventing a member name that
// no SDK can read.
func checkOutputMember(s *Service, svc *model.Service, op model.Operation, where, member string, problems *Errors) {
	if op.Output == "" {
		*problems = append(*problems, fmt.Errorf("%s: %s: operation %s declares no output shape, cannot set %q",
			s.ServiceID, where, op.Name, member))
		return
	}
	shape, ok := svc.Shapes[op.Output]
	if !ok {
		*problems = append(*problems, fmt.Errorf("%s: %s: output shape %q missing from the model",
			s.ServiceID, where, op.Output))
		return
	}
	if _, ok := shape.Members[member]; !ok {
		known := sortedKeys(shape.Members)
		*problems = append(*problems, fmt.Errorf("%s: %s: %q is not a member of %s (have: %s)",
			s.ServiceID, where, member, op.Output, strings.Join(known, ", ")))
	}
}

func validateStatechart(s *Service, where string, sc *Statechart, compile func(string, string), problems *Errors) {
	if sc.Initial == "" {
		*problems = append(*problems, fmt.Errorf("%s: %s.statechart: no initial state", s.ServiceID, where))
	} else if _, ok := sc.States[sc.Initial]; !ok {
		*problems = append(*problems, fmt.Errorf("%s: %s.statechart: initial state %q is not defined",
			s.ServiceID, where, sc.Initial))
	}
	for _, name := range sortedKeys(sc.States) {
		st := sc.States[name]
		sw := fmt.Sprintf("%s.statechart.states.%s", where, name)
		for _, evt := range sortedKeys(st.On) {
			for i, tr := range st.On[evt] {
				tw := fmt.Sprintf("%s.on.%s[%d]", sw, evt, i)
				compile(tw+".guard", tr.Guard)
				if _, ok := sc.States[tr.Target]; !ok {
					*problems = append(*problems, fmt.Errorf("%s: %s: target %q is not a defined state",
						s.ServiceID, tw, tr.Target))
				}
				for j, act := range tr.Actions {
					aw := fmt.Sprintf("%s.actions[%d]", tw, j)
					for _, k := range sortedKeys(act.Set) {
						compile(aw+".set."+k, act.Set[k])
					}
					if act.Deadline != nil {
						compile(aw+".deadline.after", act.Deadline.After)
						if act.Deadline.Name == "" {
							*problems = append(*problems, fmt.Errorf("%s: %s.deadline: no name", s.ServiceID, aw))
						}
					}
					if act.Move != nil {
						for _, k := range sortedKeys(act.Move.To) {
							compile(aw+".move.to."+k, act.Move.To[k])
						}
						compileAny(aw+".move.set", act.Move.Set, compile)
					}
					if act.Move != nil && act.Move.State != "" {
						// The move target lives in another collection, so its
						// state belongs to that resource's chart; only require
						// that a state was named.
						if strings.TrimSpace(act.Move.State) == "" {
							*problems = append(*problems, fmt.Errorf("%s: %s.move: empty state", s.ServiceID, aw))
						}
					}
				}
			}
		}
		for i, tm := range st.Timers {
			tw := fmt.Sprintf("%s.timers[%d]", sw, i)
			if tm.Deadline == "" {
				*problems = append(*problems, fmt.Errorf("%s: %s: no deadline name", s.ServiceID, tw))
			}
			if _, ok := sc.States[tm.Target]; !ok {
				*problems = append(*problems, fmt.Errorf("%s: %s: target %q is not a defined state",
					s.ServiceID, tw, tm.Target))
			}
		}
	}
}

func validateEffect(s *Service, where string, eff Effect, compile func(string, string), problems *Errors) {
	set := 0
	res := func(name, field string) {
		set++
		if name == "" {
			return
		}
		if _, ok := s.Resources[name]; !ok {
			*problems = append(*problems, fmt.Errorf("%s: %s.%s: unknown resource %q",
				s.ServiceID, where, field, name))
		}
	}
	if e := eff.Create; e != nil {
		res(e.Resource, "create")
		compile(where+".create.when", e.When)
		compileAny(where+".create.record", e.Record, compile)
	}
	if e := eff.Put; e != nil {
		res(e.Resource, "put")
		compile(where+".put.when", e.When)
		compileAny(where+".put.record", e.Record, compile)
	}
	if e := eff.Patch; e != nil {
		res(e.Resource, "patch")
		compile(where+".patch.when", e.When)
		compileAny(where+".patch.record", e.Record, compile)
	}
	if e := eff.Delete; e != nil {
		res(e.Resource, "delete")
		compile(where+".delete.key", e.Key)
		compile(where+".delete.when", e.When)
	}
	if e := eff.Counter; e != nil {
		set++
		if e.Name == "" {
			*problems = append(*problems, fmt.Errorf("%s: %s.counter: no name", s.ServiceID, where))
		}
	}
	if e := eff.Dedup; e != nil {
		set++
		compile(where+".dedup.key", e.Key)
		compile(where+".dedup.when", e.When)
		for _, k := range sortedKeys(e.Record) {
			compile(where+".dedup.record."+k, e.Record[k])
		}
		compileAny(where+".dedup.on_hit.output", asMapAny(e.OnHit["output"]), compile)
		if e.Table == "" || e.TTL == "" {
			*problems = append(*problems, fmt.Errorf("%s: %s.dedup: table and ttl are required", s.ServiceID, where))
		}
	}
	if e := eff.SendEvent; e != nil {
		set++
		if e.Resource != "" {
			res(e.Resource, "send_event")
			set-- // res already counted it
		}
		if e.Event == "" {
			*problems = append(*problems, fmt.Errorf("%s: %s.send_event: no event", s.ServiceID, where))
		}
		compile(where+".send_event.key", e.Key)
		for _, k := range sortedKeys(e.Context) {
			compile(where+".send_event.context."+k, e.Context[k])
		}
	}
	if e := eff.Emit; e != nil {
		set++
		if e.Target == "" {
			*problems = append(*problems, fmt.Errorf("%s: %s.emit: no target", s.ServiceID, where))
		}
		compile(where+".emit.when", e.When)
		for _, k := range sortedKeys(e.Payload) {
			compile(where+".emit.payload."+k, e.Payload[k])
		}
	}
	if e := eff.Primitive; e != nil {
		set++
		if _, ok := s.Primitives[e.Use]; !ok {
			*problems = append(*problems, fmt.Errorf("%s: %s.primitive: %q is not declared in primitives",
				s.ServiceID, where, e.Use))
		}
		for _, k := range sortedKeys(e.Args) {
			compile(where+".primitive.args."+k, e.Args[k])
		}
	}
	if set == 0 {
		*problems = append(*problems, fmt.Errorf("%s: %s: effect sets no action", s.ServiceID, where))
	}
	if set > 1 {
		*problems = append(*problems, fmt.Errorf("%s: %s: effect sets %d actions; exactly one is allowed",
			s.ServiceID, where, set))
	}
}

// compileAny compiles the string-valued entries of a record literal. Non-string
// values are constants and need no compilation.
func compileAny(where string, rec map[string]any, compile func(string, string)) {
	for _, k := range sortedKeysAny(rec) {
		if s, ok := rec[k].(string); ok {
			compile(where+"."+k, s)
		}
	}
}

func sortedKeys[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func sortedKeysAny(m map[string]any) []string { return sortedKeys(m) }

// opWrites reports whether an operation has an effect that binds `rec`.
func opWrites(op Operation) bool {
	for _, e := range op.Effects {
		if e.Create != nil || e.Put != nil || e.Patch != nil {
			return true
		}
	}
	return false
}

// asMapAny narrows an inline output block to the map form the compiler walks.
func asMapAny(v any) map[string]any {
	m, _ := v.(map[string]any)
	return m
}

// ReservedMembers are the record members the engine owns. A record with a
// lifecycle stores its state and its armed deadlines alongside its data, which
// keeps the two atomic; the cost is that these two names are not a bundle's to
// use, and saying so at load time is cheaper than debugging a record whose
// state a service overwrote.
var ReservedMembers = []string{"__state", "__deadlines"}

func reservedMember(name string) bool {
	for _, r := range ReservedMembers {
		if name == r {
			return true
		}
	}
	return false
}
