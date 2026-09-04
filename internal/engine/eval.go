package engine

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"strings"

	"cel.dev/cel-go/common/types"
	"cel.dev/cel-go/common/types/ref"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/bir"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

// eval is the per-request state: the bindings visible to expressions, plus the
// resolved resource ID. It exists for one Invoke and is never shared.
type eval struct {
	e     *Engine
	req   *spi.Request
	op    bir.Operation
	binds map[string]any
	id    string
	fx    map[string]any
	// selection is what the operation's select gathered, with store keys, so
	// effects can write each chosen record back where it came from.
	selection []selected
	// shortOutput is the answer a short-circuiting effect already decided --
	// a deduplicated send replays the first send's identifiers.
	shortOutput map[string]any
	// dedupPending is the idempotency entry to complete with this operation's
	// output once its effects have produced one.
	dedupPending *pendingDedup
	// resIDs is the key most recently resolved for each resource, by resource
	// name. A child resource's collection template names its parent by
	// resource name -- "msgs:{queue.id}" -- rather than by whichever binding
	// this particular operation happened to call it, so the template stays the
	// same across every operation that touches the resource.
	resIDs map[string]string
}

func (e *Engine) newEval(req *spi.Request, op bir.Operation) *eval {
	return &eval{
		e:      e,
		req:    req,
		op:     op,
		binds:  map[string]any{},
		fx:     map[string]any{},
		resIDs: map[string]string{},
	}
}

// activation builds the CEL variable set. Only declared bindings, the request,
// the identity and an injected clock reading are visible — never the store.
func (ev *eval) activation() map[string]any {
	act := map[string]any{
		"input": ev.req.Input,
		"identity": map[string]any{
			"account": ev.req.Identity.Account,
			"region":  ev.req.Identity.Region,
			"arn":     ev.req.Identity.ARN,
			"project": ev.req.Identity.Project,
		},
		"now": ev.e.deps.Clock.Now(),
		"id":  ev.id,
		"fx":  ev.fx,
		"hit": map[string]any{},
		// The base URL the caller actually reached. Services that hand back
		// URLs to themselves -- an SQS queue URL, a GCS resumable-upload
		// location -- have to echo the endpoint the client used, or the client
		// follows a link to somewhere it cannot reach.
		"endpoint": ev.endpoint(),
	}
	for k, v := range ev.binds {
		act[k] = v
	}
	return act
}

// eval runs one compiled program by its path. A missing program means the
// bundle was not loaded through bir.Load, which validation would have caught.
func (ev *eval) eval(path string) (any, error) {
	prg, ok := ev.e.programs[path]
	if !ok {
		return nil, fmt.Errorf("engine: %s: expression was not compiled", path)
	}
	out, _, err := prg.Eval(ev.activation())
	if err != nil {
		return nil, fmt.Errorf("engine: %s: %w", path, err)
	}
	return fromCEL(out), nil
}

// evalBool evaluates a condition. A non-boolean result is a bundle bug, not a
// request error, so it surfaces as a server fault rather than a silent false.
func (ev *eval) evalBool(path string) (bool, error) {
	v, err := ev.eval(path)
	if err != nil {
		return false, err
	}
	b, ok := v.(bool)
	if !ok {
		return false, fmt.Errorf("engine: %s: expected a boolean, got %T", path, v)
	}
	return b, nil
}

// resolveReads loads every declared read before any expression runs. Each
// binding x also binds x_found, so a bundle can distinguish absent from empty.
//
// Reads run in dependency order, not name order. A parent-scoped resource
// lives in a collection named for its parent -- "ccbr:{repository.id}" -- so
// reading a branch requires the repository's id to be resolved already.
// Ordering by name would make that work or not work according to which letter
// the bundle author reached for, which is not a contract anyone should have to
// keep. So each pass resolves whatever it can and repeats while it makes
// progress, the same way derived bindings are resolved.
//
// Name order still decides between reads that are equally ready, so a run is
// deterministic and a bundle with no parent scoping behaves exactly as before.
func (ev *eval) resolveReads(ctx context.Context, op bir.Operation) error {
	pending := make([]string, 0, len(op.Reads))
	for n := range op.Reads {
		pending = append(pending, n)
	}
	sort.Strings(pending)

	for len(pending) > 0 {
		var blocked []string
		progress := false
		for _, name := range pending {
			ready, err := ev.readIsReady(op.Reads[name])
			if err != nil {
				return err
			}
			if !ready && len(pending) > 1 {
				blocked = append(blocked, name)
				continue
			}
			// The last read standing runs whether or not its scope resolved:
			// a genuine mistake -- a collection naming a resource this
			// operation never reads -- should report itself as that, rather
			// than as a stall with no failing expression to look at.
			if err := ev.resolveRead(ctx, name, op.Reads[name]); err != nil {
				return err
			}
			progress = true
		}
		if !progress {
			// Every remaining read waits on another; running one surfaces the
			// cycle as the unresolved template it actually is.
			if err := ev.resolveRead(ctx, blocked[0], op.Reads[blocked[0]]); err != nil {
				return err
			}
			blocked = blocked[1:]
		}
		pending = blocked
	}
	return nil
}

// readIsReady reports whether a read's collection template can be expanded
// yet: every resource id it names must already have been resolved by an
// earlier read. A template naming the read's own resource is not a dependency
// -- it would be satisfied by this very read -- and is left to fail as itself.
func (ev *eval) readIsReady(read bir.Read) (bool, error) {
	res, ok := ev.e.ir.Resources[read.Resource]
	if !ok {
		return false, fmt.Errorf("engine: unknown resource %q", read.Resource)
	}
	for _, ref := range templateRefs(res.Collection) {
		head, rest, _ := strings.Cut(ref, ".")
		if rest != "id" || head == read.Resource {
			continue
		}
		if _, declared := ev.e.ir.Resources[head]; !declared {
			continue
		}
		if _, resolved := ev.resIDs[head]; !resolved {
			return false, nil
		}
	}
	return true, nil
}

// templateRefs lists the {x.y} references in a template, ignoring malformed
// ones: expanding the template is what reports those, with the whole string
// in the message.
func templateRefs(s string) []string {
	var refs []string
	for {
		i := strings.Index(s, "{")
		if i < 0 {
			return refs
		}
		j := strings.Index(s[i:], "}")
		if j < 0 {
			return refs
		}
		refs = append(refs, s[i+1:i+j])
		s = s[i+j+1:]
	}
}

func (ev *eval) resolveRead(ctx context.Context, name string, read bir.Read) error {
	res, ok := ev.e.ir.Resources[read.Resource]
	if !ok {
		return fmt.Errorf("engine: unknown resource %q", read.Resource)
	}
	key, err := ev.resourceKey(res, read.Key,
		"operations."+ev.req.Operation+".reads."+name+".key")
	if err != nil {
		return err
	}
	ev.id = key
	ev.resIDs[read.Resource] = key

	col, err := ev.collection(res)
	if err != nil {
		return err
	}
	raw, found, err := col.Get(ctx, key)
	if err != nil {
		return err
	}
	rec := map[string]any{}
	if found {
		if err := unmarshal(raw, &rec); err != nil {
			return err
		}
	}
	// Views derive from a record, so there is nothing to derive when there
	// is none. An operation that names a view guards on _found first, and
	// the require rule that does so runs before anything reads it.
	if found {
		if err := ev.applyViews(read.Resource, res, rec); err != nil {
			return err
		}
	}
	ev.binds[name] = rec
	ev.binds[name+"_found"] = found
	return nil
}

// applyViews computes a resource's derived members over a loaded record and
// merges them in, so a bundle names `q.fifo` rather than repeating
// `id.endsWith('.fifo')` at every site that needs it. Views are read-only: they
// are recomputed on load and never persisted, so a record cannot drift from the
// values derived out of it.
//
// The loader rejects a view whose name collides with a record member, so a
// merge cannot silently shadow stored data.
func (ev *eval) applyViews(resource string, res bir.Resource, rec map[string]any) error {
	if len(res.Views) == 0 {
		return nil
	}
	// The view sees the record it is derived from, under the name its
	// definition uses.
	saved, hadRec := ev.binds["rec"]
	ev.binds["rec"] = rec
	defer func() {
		if hadRec {
			ev.binds["rec"] = saved
		} else {
			delete(ev.binds, "rec")
		}
	}()

	for _, name := range sortedKeys(res.Views) {
		v, err := ev.eval("resources." + resource + ".views." + name)
		if err != nil {
			return err
		}
		rec[name] = v
	}
	return nil
}

// resourceKey determines which key a read or write addresses: an explicit key
// expression, a singleton's fixed key, the request members that carry the ID,
// or a derivation.
//
// keyPath is where the key expression was compiled, which the caller knows and
// this cannot infer: a read's key lives under the binding that declared it, an
// effect's under that effect's own index. Deriving it here from a binding name
// worked only for reads, and left every effect asking for a `reads..key` that
// was never compiled -- which surfaced only when the effect also had no id
// already bound, since an id in hand skips this entirely.
// carriedKey is the key an effect inherits when it does not compute one.
//
// Resolving a read binds the key it used, and an effect on the same resource
// should address the record the operation has already been talking about --
// a patch after a read does not repeat the key expression. What it must not
// do is inherit a key resolved for a *different* resource: an operation that
// reads a parent and then writes a child would otherwise address the child by
// the parent's key, which is a silent write to the wrong row rather than an
// error. So only this resource's own resolved id carries over.
//
// An explicit key on the effect outranks even that, because it says outright
// which record is meant. send_event has always worked this way; write and
// delete did not, which is the inconsistency this closes.
func (ev *eval) carriedKey(resource, keyExpr string) string {
	if keyExpr != "" {
		return ""
	}
	return ev.resIDs[resource]
}

func (ev *eval) resourceKey(res bir.Resource, keyExpr, keyPath string) (string, error) {
	if keyExpr != "" {
		v, err := ev.eval(keyPath)
		if err != nil {
			return "", err
		}
		return fmt.Sprint(v), nil
	}
	if res.Singleton != "" {
		return res.Singleton, nil
	}
	for _, member := range res.ID.InputMembers {
		if v, ok := ev.req.Input[member]; ok {
			if s := fmt.Sprint(v); s != "" && s != "<nil>" {
				return s, nil
			}
		}
	}
	if res.ID.Derive != "" {
		v, err := ev.eval("resources." + resourceNameOf(ev.e.ir, res) + ".id.derive")
		if err != nil {
			return "", err
		}
		return fmt.Sprint(v), nil
	}
	return "", nil
}

func resourceNameOf(ir *bir.Service, want bir.Resource) string {
	for name, res := range ir.Resources {
		if res.Collection == want.Collection {
			return name
		}
	}
	return ""
}

// evalLets computes derived values, resolving the order from what each one
// actually needs.
//
// YAML mapping order does not survive into a Go map, and naming order is not
// dependency order -- an SQS send derives `dedupId` from `settings`, which
// sorts after it. Rather than make a bundle author think about that, each pass
// evaluates whatever can be evaluated now and repeats while progress is made.
// A binding that never becomes evaluable is reported with the error it kept
// producing, so a genuine mistake reads as itself rather than as "cycle".
func (ev *eval) evalLets(op bir.Operation) error {
	_, err := ev.resolveLets(op.Let, "operations."+ev.req.Operation+".let.")
	return err
}

// resolveLets binds a set of let expressions, resolving them by dependency
// rather than by name: a binding that names another is retried once the one it
// needs exists. It answers with the names it bound, so a caller working in a
// narrower scope -- a list's per-item lets -- can clear them again.
func (ev *eval) resolveLets(lets map[string]string, base string) ([]string, error) {
	pending := make([]string, 0, len(lets))
	for n := range lets {
		pending = append(pending, n)
	}
	sort.Strings(pending)
	bound := make([]string, 0, len(pending))

	lastErr := map[string]error{}
	for len(pending) > 0 {
		var stuck []string
		progress := false
		for _, n := range pending {
			v, err := ev.eval(base + n)
			if err != nil {
				lastErr[n] = err
				stuck = append(stuck, n)
				continue
			}
			ev.binds[n] = v
			bound = append(bound, n)
			progress = true
		}
		if !progress {
			return bound, fmt.Errorf("engine: %s%s: %w", base, stuck[0], lastErr[stuck[0]])
		}
		pending = stuck
	}
	return bound, nil
}

// checkRequires evaluates preconditions in declaration order. The first
// failure decides the error: that ordering is the service's error precedence,
// stated as data instead of buried in the order of Go if-statements.
func (ev *eval) checkRequires(op bir.Operation) *spi.Fault {
	for i, req := range op.Require {
		path := fmt.Sprintf("operations.%s.require[%d].cond", ev.req.Operation, i)
		ok, err := ev.evalBool(path)
		if err != nil {
			return &spi.Fault{Code: "InternalFailure", Message: err.Error(), HTTPStatus: 500, Fault: "server"}
		}
		if !ok {
			return ev.e.fault(req.Error, req.Message)
		}
	}
	return nil
}

// runEffects executes the closed mutation vocabulary in order.
func (ev *eval) runEffects(ctx context.Context, op bir.Operation) error {
	for i, eff := range op.Effects {
		path := fmt.Sprintf("operations.%s.effects[%d]", ev.req.Operation, i)
		switch {
		case eff.Create != nil:
			if err := ev.write(ctx, path+".create", *eff.Create, true); err != nil {
				return err
			}
		case eff.Put != nil:
			if err := ev.write(ctx, path+".put", *eff.Put, false); err != nil {
				return err
			}
		case eff.Patch != nil:
			if err := ev.write(ctx, path+".patch", *eff.Patch, false); err != nil {
				return err
			}
		case eff.Delete != nil:
			if err := ev.remove(ctx, path+".delete", *eff.Delete); err != nil {
				return err
			}
		case eff.Counter != nil:
			name, err := ev.interpolate(eff.Counter.Name)
			if err != nil {
				return err
			}
			n, err := ev.nextCounter(ctx, name)
			if err != nil {
				return err
			}
			ev.fx["counter"] = n
		case eff.Dedup != nil:
			short, err := ev.runDedup(ctx, path+".dedup", *eff.Dedup)
			if err != nil {
				return err
			}
			if short {
				// A deduplicated request answers with what the first one
				// answered and performs none of the effects behind it.
				return errShortCircuit
			}
		case eff.Generate != nil:
			if eff.Generate.When != "" {
				ok, err := ev.evalBool(path + ".generate.when")
				if err != nil {
					return err
				}
				if !ok {
					continue
				}
			}
			ev.fx[eff.Generate.Bind] = ev.generate(eff.Generate.Generate)
		case eff.SendEvent != nil:
			if err := ev.runSendEvent(ctx, path+".send_event", *eff.SendEvent); err != nil {
				return err
			}
		default:
			return fmt.Errorf("engine: %s: effect kind is not yet supported by this engine", path)
		}
	}
	return nil
}

// write creates or updates a record. On create, an ID is generated when the
// resource declares a generator — randomness is an effect, so the value comes
// from the deterministic Rand and never from an expression.
// write runs a create, put or patch: once, or once per element when the effect
// declares a for_each.
func (ev *eval) write(ctx context.Context, path string, w bir.WriteEffect, create bool) error {
	if _, ok := ev.e.ir.Resources[w.Resource]; !ok {
		return fmt.Errorf("engine: %s: unknown resource %q", path, w.Resource)
	}
	if w.When != "" {
		ok, err := ev.evalBool(path + ".when")
		if err != nil {
			return err
		}
		if !ok {
			return nil
		}
	}
	if w.ForEach == "" {
		return ev.writeOne(ctx, path, w, create)
	}

	elems, err := ev.eval(path + ".for_each")
	if err != nil {
		return err
	}
	// A batch that names nothing writes nothing. An absent member and an empty
	// list are the same request as far as the store is concerned, so neither
	// is an error -- the operation's own requires are where a bundle says a
	// list may not be empty.
	if elems == nil {
		return nil
	}
	list, ok := elems.([]any)
	if !ok {
		return fmt.Errorf("engine: %s.for_each: expected a list, got %T", path, elems)
	}
	// `item` is bound for the element and unbound afterwards, the same
	// contract a delete's `where` and a list's `filter` have, so a bundle
	// reads the same in all three places.
	prev, had := ev.binds["item"]
	defer func() {
		if had {
			ev.binds["item"] = prev
		} else {
			delete(ev.binds, "item")
		}
	}()
	for _, e := range list {
		ev.binds["item"] = e
		if err := ev.writeOne(ctx, path, w, create); err != nil {
			return err
		}
	}
	return nil
}

// writeOne stores one record. Everything it reads from the effect is the same
// whether the write runs once or per element; what differs is only whether
// `item` is bound around it.
func (ev *eval) writeOne(ctx context.Context, path string, w bir.WriteEffect, create bool) error {
	res, ok := ev.e.ir.Resources[w.Resource]
	if !ok {
		return fmt.Errorf("engine: %s: unknown resource %q", path, w.Resource)
	}

	// The identity first: record expressions may name `id`, and a generated ID
	// must be drawn from the deterministic Rand rather than computed by an
	// expression, so that a recorded run replays.
	key := ev.carriedKey(w.Resource, w.Key)
	if create && res.ID.Generate != nil {
		key = ev.generate(*res.ID.Generate)
	}
	if key == "" {
		k, err := ev.resourceKey(res, w.Key, path+".key")
		if err != nil {
			return err
		}
		key = k
	}
	ev.id = key

	// The resource's ARN template is a value the record can name, so a bundle
	// writes `ARN: arn` instead of concatenating the same five pieces the way
	// 189 hand-written call sites did -- each of which was free to get the
	// partition, the region or the separator subtly wrong.
	if res.ARN != "" {
		arn, err := ev.interpolate(res.ARN)
		if err != nil {
			return fmt.Errorf("engine: %s: arn: %w", path, err)
		}
		ev.binds["arn"] = arn
	}

	col, err := ev.collection(res)
	if err != nil {
		return err
	}

	rec := map[string]any{}
	if !create {
		// An update addresses an existing record. When the store key is a
		// record member rather than the ID, the key is whatever the caller
		// named, resolved the same way a read would resolve it.
		if res.Key != "" {
			k, err := ev.resourceKey(res, w.Key, path+".key")
			if err != nil {
				return err
			}
			if k != "" {
				key = k
			}
		}
		if raw, found, err := col.Get(ctx, key); err != nil {
			return err
		} else if found {
			if err := unmarshal(raw, &rec); err != nil {
				return err
			}
		}
	}

	// The request itself, when the write asks for it, underneath everything
	// declared. Only members the request actually carried are copied, which is
	// the half that cannot be written as expressions: a bundle enumerating the
	// input shape would store a null for each member the caller omitted, and a
	// later Get would answer those nulls.
	if w.Spread == "input" {
		for k, v := range ev.req.Input {
			rec[k] = v
		}
	}

	// Resource-level record members first, then effect-level overrides.
	for _, k := range sortedKeysAny(res.Record) {
		v, err := ev.recordValue(ctx, "resources."+w.Resource+".record."+k, res.Record[k])
		if err != nil {
			return err
		}
		rec[k] = v
	}
	for _, k := range sortedKeysAny(w.Record) {
		v, err := ev.recordValue(ctx, path+".record."+k, w.Record[k])
		if err != nil {
			return err
		}
		rec[k] = v
	}

	// A resource may key its records by one of their own members -- an SQS
	// message is addressed by its receipt handle, which is regenerated every
	// time the message becomes invisible. The member is computed above, so the
	// key is only knowable now.
	if res.Key != "" {
		v, ok := rec[res.Key]
		if !ok {
			return fmt.Errorf("engine: %s: resource keys on %q, which the record does not set",
				path, res.Key)
		}
		key = fmt.Sprint(v)
		ev.id = key
	}

	// A record with a lifecycle may be born somewhere other than its chart's
	// initial state, with a timer already armed: an SQS message sent with a
	// delay is invisible until its deadline passes, which is a deadline rather
	// than a background job.
	if res.Statechart != nil {
		if w.State != "" {
			st, err := ev.evalAt(path+".state", w.State)
			if err != nil {
				return err
			}
			name := fmt.Sprint(st)
			if _, ok := res.Statechart.States[name]; !ok {
				return fmt.Errorf("engine: %s.state: %q is not a defined state", path, name)
			}
			rec[stateMember] = name
		}
		if d := w.Deadline; d != nil {
			arm := true
			if d.When != "" {
				arm, err = ev.evalBool(path + ".deadline.when")
				if err != nil {
					return err
				}
			}
			if arm {
				after, err := ev.eval(path + ".deadline.after")
				if err != nil {
					return err
				}
				dur, ok := asDuration(after)
				if !ok {
					return fmt.Errorf("engine: %s.deadline.after: expected a duration, got %T", path, after)
				}
				setDeadline(rec, d.Name, ev.e.deps.Clock.Now().Add(dur))
			}
		}
	}

	if err := ev.putRecord(ctx, col, key, rec); err != nil {
		return err
	}
	ev.resIDs[w.Resource] = key
	ev.fx[strings.TrimPrefix(path[strings.LastIndex(path, ".")+1:], ".")] = rec
	ev.binds["rec"] = rec
	return nil
}

// putRecord marshals and stores one record.
func (ev *eval) putRecord(ctx context.Context, col spi.Collection, key string, rec map[string]any) error {
	blob, err := marshal(rec)
	if err != nil {
		return err
	}
	return col.Put(ctx, key, blob)
}

// recordValue evaluates one member of a record literal. A string is an
// expression; a single-key map is one of the two value forms that must not be
// expressions, because both draw on state an expression is not allowed to
// touch:
//
//	{ generate: { kind: hex, bytes: 64 } }   the deterministic Rand
//	{ counter: "queues/{queue.id}/seq" }     a monotonic sequence in the store
//
// Anything else is a literal.
func (ev *eval) recordValue(ctx context.Context, path string, raw any) (any, error) {
	switch v := raw.(type) {
	case string:
		return ev.evalAt(path, v)
	case map[string]any:
		if len(v) == 1 {
			if g, ok := v["generate"]; ok {
				spec, err := generateSpec(g)
				if err != nil {
					return nil, fmt.Errorf("engine: %s: %w", path, err)
				}
				return ev.generate(spec), nil
			}
			if c, ok := v["counter"]; ok {
				name, err := ev.interpolate(fmt.Sprint(c))
				if err != nil {
					return nil, fmt.Errorf("engine: %s: %w", path, err)
				}
				return ev.nextCounter(ctx, name)
			}
		}
		// Any other map is a nested record, and its members are members like
		// any other. Returning it untouched would store the expression sources
		// as text -- a canary's { Status: { State: "'READY'" } } would come
		// back with the quotes still on it, which is a wrong value rather than
		// an error and so the worst way to fail.
		out := make(map[string]any, len(v))
		for _, k := range sortedKeysAny(v) {
			member, err := ev.recordValue(ctx, path+"."+k, v[k])
			if err != nil {
				return nil, err
			}
			out[k] = member
		}
		return out, nil
	case []any:
		out := make([]any, len(v))
		for i, item := range v {
			member, err := ev.recordValue(ctx, fmt.Sprintf("%s[%d]", path, i), item)
			if err != nil {
				return nil, err
			}
			out[i] = member
		}
		return out, nil
	}
	return raw, nil
}

// generateSpec reads a { kind, bytes } map out of a record literal.
func generateSpec(raw any) (bir.Generate, error) {
	m, ok := raw.(map[string]any)
	if !ok {
		return bir.Generate{}, fmt.Errorf("generate: expected a map, got %T", raw)
	}
	g := bir.Generate{Kind: fmt.Sprint(m["kind"])}
	if n, ok := toFloat(m["bytes"]); ok {
		g.Bytes = int(n)
	}
	switch g.Kind {
	case "hex", "uuid", "int":
		return g, nil
	}
	return bir.Generate{}, fmt.Errorf("generate: unknown kind %q", g.Kind)
}

// countersCollection holds monotonic sequences. It is a store collection like
// any other, so a counter survives a snapshot and restore along with the
// records that reference it.
const countersCollection = "__counters"

// nextCounter increments a named sequence and returns the new value. Sequence
// numbers are state, not expressions: two calls in one request must differ, and
// a replay of the same request sequence must produce the same values.
func (ev *eval) nextCounter(ctx context.Context, name string) (int64, error) {
	col := ev.e.scope(ev.req).Collection(countersCollection)
	var n int64
	raw, found, err := col.Get(ctx, name)
	if err != nil {
		return 0, err
	}
	if found {
		var cur struct {
			N int64 `json:"n"`
		}
		if err := unmarshal(raw, &cur); err != nil {
			return 0, err
		}
		n = cur.N
	}
	n++
	blob, err := marshal(map[string]any{"n": n})
	if err != nil {
		return 0, err
	}
	if err := col.Put(ctx, name, blob); err != nil {
		return 0, err
	}
	return n, nil
}

// evalAt runs a compiled expression, falling back to the literal when the
// bundle was built in memory rather than loaded through bir.Load.
func (ev *eval) evalAt(path, src string) (any, error) {
	if _, ok := ev.e.programs[path]; ok {
		return ev.eval(path)
	}
	return src, nil
}

func (ev *eval) remove(ctx context.Context, path string, d bir.DeleteEffect) error {
	res, ok := ev.e.ir.Resources[d.Resource]
	if !ok {
		return fmt.Errorf("engine: %s: unknown resource %q", path, d.Resource)
	}
	if d.When != "" {
		ok, err := ev.evalBool(path + ".when")
		if err != nil {
			return err
		}
		if !ok {
			return nil
		}
	}
	col, err := ev.collection(res)
	if err != nil {
		return err
	}
	if d.Where != "" {
		return ev.removeWhere(ctx, path, col)
	}
	key := ev.carriedKey(d.Resource, d.Key)
	if key == "" {
		k, err := ev.resourceKey(res, d.Key, path+".key")
		if err != nil {
			return err
		}
		key = k
	}
	return col.Delete(ctx, key)
}

// removeWhere deletes every record the predicate accepts. The candidate is
// bound to `item` for the predicate and unbound afterwards, the same contract
// a list filter has, so a bundle reads the same either way.
//
// The keys are collected before anything is deleted: mutating a collection
// while iterating it is the kind of thing that works until the store changes
// underneath it.
func (ev *eval) removeWhere(ctx context.Context, path string, col spi.Collection) error {
	entries, _, err := col.List(ctx, "", "", 0)
	if err != nil {
		return err
	}
	var doomed []string
	for _, kv := range entries {
		rec := map[string]any{}
		if err := unmarshal(kv.Value, &rec); err != nil {
			return err
		}
		ev.binds["item"] = rec
		keep, evalErr := ev.evalBool(path + ".where")
		delete(ev.binds, "item")
		if evalErr != nil {
			return evalErr
		}
		if keep {
			doomed = append(doomed, kv.Key)
		}
	}
	for _, key := range doomed {
		if err := col.Delete(ctx, key); err != nil {
			return err
		}
	}
	return nil
}

// generate produces an identifier from the deterministic Rand, so the same
// seed and request sequence always yield the same IDs.
func (ev *eval) generate(g bir.Generate) string {
	switch g.Kind {
	case "uuid":
		return ev.e.deps.Rand.UUID()
	case "int":
		return fmt.Sprint(ev.e.deps.Rand.Intn(1 << 30))
	default:
		n := g.Bytes
		if n <= 0 {
			n = 8
		}
		return ev.e.deps.Rand.Hex(n)
	}
}

// runList serves the common "list a resource" shape, binding items to the
// output member the model declares. The member name is validated at load time,
// so a bundle cannot invent one no SDK can read.
func (ev *eval) runList(ctx context.Context, op bir.Operation, modelOp model.Operation) error {
	if op.List == nil {
		return nil
	}
	res, ok := ev.e.ir.Resources[op.List.Resource]
	if !ok {
		return fmt.Errorf("engine: unknown resource %q", op.List.Resource)
	}
	col, err := ev.collection(res)
	if err != nil {
		return err
	}
	// A key narrows the list to one record. Most Describe* operations take an
	// optional name and answer with one item or none; without a key they
	// answer with the page.
	base := "operations." + ev.req.Operation + ".list."
	key := ""
	if op.List.Key != "" {
		v, err := ev.eval(base + "key")
		if err != nil {
			return err
		}
		if s, ok := v.(string); ok {
			key = s
		}
	}

	var (
		entries []spi.KV
		more    bool
		last    string
	)
	if key != "" {
		// A named record that is absent is an empty answer, not a fault. An
		// operation that must fault says so with reads and require.
		value, found, getErr := col.Get(ctx, key)
		if getErr != nil {
			return getErr
		}
		if found {
			entries = []spi.KV{{Key: key, Value: value}}
		}
	} else {
		limit := 0
		after := ""
		if modelOp.Pagination != nil {
			if v, ok := ev.req.Input[modelOp.Pagination.InputToken]; ok {
				after = fmt.Sprint(v)
			}
			if v, ok := ev.req.Input[modelOp.Pagination.PageSize]; ok {
				if n, isNum := toFloat(v); isNum {
					limit = int(n)
				}
			}
		}
		entries, more, err = col.List(ctx, "", after, limit)
		if err != nil {
			return err
		}
	}

	items := make([]any, 0, len(entries))
	for _, kv := range entries {
		rec := map[string]any{}
		if err := unmarshal(kv.Value, &rec); err != nil {
			return err
		}
		if op.List.Filter != "" {
			ev.binds["item"] = rec
			joined, joinErr := ev.resolveListReads(ctx, op, base)
			var (
				derived []string
				letErr  error
			)
			if joinErr == nil {
				derived, letErr = ev.resolveLets(op.List.Let, base+"let.")
			}
			var keep bool
			var filterErr error
			if joinErr == nil && letErr == nil {
				keep, filterErr = ev.evalBool(base + "filter")
			}
			// Every per-item binding is scoped to its candidate: one left in
			// place would be visible to the next item's filter, and to the
			// output projection, as though it belonged to the operation.
			delete(ev.binds, "item")
			for _, name := range derived {
				delete(ev.binds, name)
			}
			for _, name := range joined {
				delete(ev.binds, name)
				delete(ev.binds, name+"_found")
			}
			switch {
			case joinErr != nil:
				return joinErr
			case letErr != nil:
				return letErr
			case filterErr != nil:
				return filterErr
			case !keep:
				continue
			}
		}
		if err := ev.applyViews(op.List.Resource, res, rec); err != nil {
			return err
		}
		items = append(items, rec)
		last = kv.Key
	}
	ev.binds["__list"] = items
	ev.binds["__list_more"] = more
	ev.binds["__list_last"] = last
	// `items` lets an operation project the listed records into something other
	// than the records themselves -- ListQueues answers with URLs, not queues.
	ev.binds["items"] = items
	return nil
}

// resolveListReads loads a list's per-item joins for the candidate currently
// bound as `item`, and answers with the names it bound so the caller can clear
// them again. The bindings are scoped to one candidate: a join left in place
// would be visible to the next item's filter, and to the output projection,
// as though it belonged to the whole operation.
//
// A join is keyed off the candidate, so unlike an operation's reads there is
// no fallback to the request's own identity members; the loader requires the
// key for that reason.
func (ev *eval) resolveListReads(ctx context.Context, op bir.Operation, base string) ([]string, error) {
	if len(op.List.Reads) == 0 {
		return nil, nil
	}
	names := make([]string, 0, len(op.List.Reads))
	for n := range op.List.Reads {
		names = append(names, n)
	}
	sort.Strings(names)

	bound := make([]string, 0, len(names))
	for _, name := range names {
		read := op.List.Reads[name]
		res, ok := ev.e.ir.Resources[read.Resource]
		if !ok {
			return bound, fmt.Errorf("engine: unknown resource %q", read.Resource)
		}
		key, err := ev.eval(base + "reads." + name + ".key")
		if err != nil {
			return bound, err
		}
		col, err := ev.collection(res)
		if err != nil {
			return bound, err
		}
		raw, found, err := col.Get(ctx, fmt.Sprint(key))
		if err != nil {
			return bound, err
		}
		rec := map[string]any{}
		if found {
			if err := unmarshal(raw, &rec); err != nil {
				return bound, err
			}
			if err := ev.applyViews(read.Resource, res, rec); err != nil {
				return bound, err
			}
		}
		ev.binds[name] = rec
		ev.binds[name+"_found"] = found
		bound = append(bound, name)
	}
	return bound, nil
}

// project builds the response, checking each value against the operation's
// output shape.
func (ev *eval) project(op bir.Operation, modelOp model.Operation) (map[string]any, error) {
	out := map[string]any{}
	if op.List != nil {
		out[op.List.Member] = ev.binds["__list"]
		if modelOp.Pagination != nil && modelOp.Pagination.OutputToken != "" {
			if more, _ := ev.binds["__list_more"].(bool); more {
				out[modelOp.Pagination.OutputToken] = ev.binds["__list_last"]
			}
		}
	}
	for _, member := range sortedKeys(op.Output) {
		v, err := ev.eval("operations." + ev.req.Operation + ".output." + member)
		if err != nil {
			return nil, err
		}
		out[member] = v
	}
	return out, nil
}

// fromCEL converts a CEL result into the Go-native shapes the codecs expect.
//
// CEL hands back map[any]any for maps; the protocol encoders need
// map[string]any, so the conversion recurses rather than trusting the
// top-level type.
func fromCEL(v any) any {
	if rv, ok := v.(ref.Val); ok {
		if rv == types.NullValue {
			return nil
		}
		native, err := rv.ConvertToNative(anyType)
		if err != nil {
			return normalize(rv.Value())
		}
		return normalize(native)
	}
	return normalize(v)
}

// normalize rewrites CEL's own container shapes into the Go-native ones the
// codecs and the store expect, recursively.
//
// CEL is not consistent about what a container converts to: a map that came
// from stored JSON arrives as map[string]any, one built by an expression
// arrives as map[ref.Val]ref.Val, and either can hold values that are still
// ref.Val. Anything not converted here reaches encoding/json as a type it
// refuses, so the reach is deliberately wide -- the reflect fallback catches
// the container shapes not named above rather than leaving them to fail at the
// point of use.
func normalize(v any) any {
	switch t := v.(type) {
	case map[any]any:
		out := make(map[string]any, len(t))
		for k, val := range t {
			out[fmt.Sprint(k)] = normalize(val)
		}
		return out
	case map[string]any:
		out := make(map[string]any, len(t))
		for k, val := range t {
			out[k] = normalize(val)
		}
		return out
	case map[ref.Val]ref.Val:
		out := make(map[string]any, len(t))
		for k, val := range t {
			out[fmt.Sprint(fromCEL(k))] = fromCEL(val)
		}
		return out
	case []any:
		out := make([]any, len(t))
		for i, val := range t {
			out[i] = normalize(val)
		}
		return out
	case []ref.Val:
		out := make([]any, len(t))
		for i, val := range t {
			out[i] = fromCEL(val)
		}
		return out
	case ref.Val:
		return fromCEL(t)
	case structpb.NullValue:
		// A null inside a container does not convert to Go's nil: CEL hands
		// back the protobuf enum, which is an int32. Left alone it reaches
		// encoding/json as 0 and a comparison as the string NULL_VALUE, so a
		// member a bundle deliberately set to null stops being null the moment
		// it travels inside a map or a list.
		return nil
	}

	switch rv := reflect.ValueOf(v); rv.Kind() {
	case reflect.Map:
		out := make(map[string]any, rv.Len())
		for iter := rv.MapRange(); iter.Next(); {
			out[fmt.Sprint(normalize(iter.Key().Interface()))] = normalize(iter.Value().Interface())
		}
		return out
	case reflect.Slice, reflect.Array:
		// A byte slice is a value, not a container: rewriting it element by
		// element would turn a blob into a list of numbers.
		if rv.Type().Elem().Kind() == reflect.Uint8 {
			return v
		}
		out := make([]any, rv.Len())
		for i := range out {
			out[i] = normalize(rv.Index(i).Interface())
		}
		return out
	}
	return v
}

var anyType = reflect.TypeOf((*any)(nil)).Elem()

func sortedKeys[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func sortedKeysAny(m map[string]any) []string { return sortedKeys(m) }

// endpoint is the base URL the caller reached, for services that return URLs
// pointing at themselves. It falls back to the default listen address when a
// request arrived without an HTTP context, which is how unit tests and replayed
// traces reach the engine.
func (ev *eval) endpoint() string {
	if ev.req.HTTP != nil && ev.req.HTTP.Host != "" {
		return "http://" + ev.req.HTTP.Host
	}
	return defaultEndpoint
}

// defaultEndpoint matches the address `mirror up` listens on, so a URL handed
// out in a test is the URL a client would have been given.
const defaultEndpoint = "http://127.0.0.1:4566"
