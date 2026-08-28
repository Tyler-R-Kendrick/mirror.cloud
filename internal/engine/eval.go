package engine

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"strings"

	"cel.dev/cel-go/common/types"
	"cel.dev/cel-go/common/types/ref"

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
}

func (e *Engine) newEval(req *spi.Request, op bir.Operation) *eval {
	return &eval{
		e:     e,
		req:   req,
		op:    op,
		binds: map[string]any{},
		fx:    map[string]any{},
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
func (ev *eval) resolveReads(ctx context.Context, op bir.Operation) error {
	names := make([]string, 0, len(op.Reads))
	for n := range op.Reads {
		names = append(names, n)
	}
	sort.Strings(names)

	for _, name := range names {
		read := op.Reads[name]
		res, ok := ev.e.ir.Resources[read.Resource]
		if !ok {
			return fmt.Errorf("engine: unknown resource %q", read.Resource)
		}
		key, err := ev.resourceKey(res, read.Key, name)
		if err != nil {
			return err
		}
		ev.id = key

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
		ev.binds[name] = rec
		ev.binds[name+"_found"] = found
	}
	return nil
}

// resourceKey determines which key a read or write addresses: an explicit key
// expression, a singleton's fixed key, the request members that carry the ID,
// or a derivation.
func (ev *eval) resourceKey(res bir.Resource, keyExpr, bindPath string) (string, error) {
	if keyExpr != "" {
		v, err := ev.eval("operations." + ev.req.Operation + ".reads." + bindPath + ".key")
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

// evalLets computes derived values in a stable order.
func (ev *eval) evalLets(op bir.Operation) error {
	names := make([]string, 0, len(op.Let))
	for n := range op.Let {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, n := range names {
		v, err := ev.eval("operations." + ev.req.Operation + ".let." + n)
		if err != nil {
			return err
		}
		ev.binds[n] = v
	}
	return nil
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
		default:
			return fmt.Errorf("engine: %s: effect kind is not yet supported by this engine", path)
		}
	}
	return nil
}

// write creates or updates a record. On create, an ID is generated when the
// resource declares a generator — randomness is an effect, so the value comes
// from the deterministic Rand and never from an expression.
func (ev *eval) write(ctx context.Context, path string, w bir.WriteEffect, create bool) error {
	res, ok := ev.e.ir.Resources[w.Resource]
	if !ok {
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

	key := ev.id
	if create && res.ID.Generate != nil {
		key = ev.generate(*res.ID.Generate)
	}
	if key == "" {
		k, err := ev.resourceKey(res, w.Key, "")
		if err != nil {
			return err
		}
		key = k
	}
	ev.id = key

	rec := map[string]any{}
	if !create {
		col, err := ev.collection(res)
		if err != nil {
			return err
		}
		if raw, found, err := col.Get(ctx, key); err != nil {
			return err
		} else if found {
			if err := unmarshal(raw, &rec); err != nil {
				return err
			}
		}
	}

	// Resource-level record members first, then effect-level overrides.
	for _, k := range sortedKeys(res.Record) {
		v, err := ev.evalAt("resources."+w.Resource+".record."+k, res.Record[k])
		if err != nil {
			return err
		}
		rec[k] = v
	}
	for _, k := range sortedKeysAny(w.Record) {
		raw := w.Record[k]
		if s, isStr := raw.(string); isStr {
			v, err := ev.evalAt(path+".record."+k, s)
			if err != nil {
				return err
			}
			rec[k] = v
			continue
		}
		rec[k] = raw
	}

	blob, err := marshal(rec)
	if err != nil {
		return err
	}
	col, err := ev.collection(res)
	if err != nil {
		return err
	}
	if err := col.Put(ctx, key, blob); err != nil {
		return err
	}
	ev.fx[strings.TrimPrefix(path[strings.LastIndex(path, ".")+1:], ".")] = rec
	ev.binds["rec"] = rec
	return nil
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
	key := ev.id
	if key == "" {
		k, err := ev.resourceKey(res, d.Key, "")
		if err != nil {
			return err
		}
		key = k
	}
	col, err := ev.collection(res)
	if err != nil {
		return err
	}
	return col.Delete(ctx, key)
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
	entries, more, err := col.List(ctx, "", after, limit)
	if err != nil {
		return err
	}
	items := make([]any, 0, len(entries))
	last := ""
	for _, kv := range entries {
		rec := map[string]any{}
		if err := unmarshal(kv.Value, &rec); err != nil {
			return err
		}
		items = append(items, rec)
		last = kv.Key
	}
	ev.binds["__list"] = items
	ev.binds["__list_more"] = more
	ev.binds["__list_last"] = last
	return nil
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

// normalize rewrites CEL's map[any]any into map[string]any, recursively.
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
	case []any:
		out := make([]any, len(t))
		for i, val := range t {
			out[i] = normalize(val)
		}
		return out
	case ref.Val:
		return fromCEL(t)
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
