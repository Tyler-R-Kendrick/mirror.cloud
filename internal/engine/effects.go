package engine

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/bir"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/execution"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

// errShortCircuit ends an operation early with the output already bound. It is
// a control signal rather than a failure, and never reaches a caller.
var errShortCircuit = errors.New("engine: short circuit")

// runDedup implements the TTL'd idempotency table FIFO deduplication needs.
//
// On a hit inside the window the recorded answer is bound as `hit` and the
// operation short-circuits: the caller gets the first request's identifiers
// back and nothing is written a second time. On a miss the key is recorded so
// the next duplicate finds it.
//
// The window is compared against the engine's own clock, so a test can step
// past it and see the deduplication lapse exactly when it should.
func (ev *eval) runDedup(ctx context.Context, path string, d bir.DedupEffect) (bool, error) {
	if d.When != "" {
		on, err := ev.evalBool(path + ".when")
		if err != nil {
			return false, err
		}
		if !on {
			return false, nil
		}
	}

	table, err := ev.interpolate(d.Table)
	if err != nil {
		return false, err
	}
	keyVal, err := ev.eval(path + ".key")
	if err != nil {
		return false, err
	}
	key := fmt.Sprint(keyVal)
	if key == "" || key == "<nil>" {
		return false, nil
	}

	col := ev.e.scope(ev.req).Collection(table)
	now := ev.e.deps.Clock.Now()

	if raw, found, err := col.Get(ctx, key); err != nil {
		return false, err
	} else if found {
		var entry struct {
			Until  int64          `json:"until"`
			Record map[string]any `json:"record"`
		}
		if err := unmarshal(raw, &entry); err != nil {
			return false, err
		}
		if now.UnixNano() < entry.Until {
			ev.binds["hit"] = entry.Record
			if len(d.OnHit) > 0 {
				out, err := ev.projectMap(path+".on_hit.output", d.OnHit["output"])
				if err != nil {
					return false, err
				}
				ev.shortOutput = out
			}
			return true, nil
		}
	}

	ttl, err := time.ParseDuration(normalizeTTL(d.TTL))
	if err != nil {
		return false, fmt.Errorf("engine: %s.ttl: %w", path, err)
	}
	rec := map[string]any{}
	for _, k := range sortedKeys(d.Record) {
		v, err := ev.eval(path + ".record." + k)
		if err != nil {
			return false, err
		}
		rec[k] = v
	}
	blob, err := marshal(map[string]any{"until": now.Add(ttl).UnixNano(), "record": rec})
	if err != nil {
		return false, err
	}
	// Remembered before the effects behind it run, so a duplicate that arrives
	// between them still finds the entry.
	if err := col.Put(ctx, key, blob); err != nil {
		return false, err
	}
	ev.dedupPending = &pendingDedup{col: table, key: key, until: now.Add(ttl)}
	return false, nil
}

// pendingDedup is the entry to complete once the operation's own effects have
// produced the values a later duplicate must be answered with.
type pendingDedup struct {
	col   string
	key   string
	until time.Time
}

// completeDedup records the operation's output against the deduplication key,
// so the next duplicate inside the window replays it.
func (ev *eval) completeDedup(ctx context.Context, out map[string]any) error {
	if ev.dedupPending == nil {
		return nil
	}
	blob, err := marshal(map[string]any{
		"until":  ev.dedupPending.until.UnixNano(),
		"record": out,
	})
	if err != nil {
		return err
	}
	return ev.e.scope(ev.req).Collection(ev.dedupPending.col).Put(ctx, ev.dedupPending.key, blob)
}

// normalizeTTL accepts the durations a bundle is likely to write. Go's parser
// has no day unit and no bare-seconds form; "5m" and "300s" both work already,
// and "5" means seconds.
func normalizeTTL(s string) string {
	if s == "" {
		return "0s"
	}
	last := s[len(s)-1]
	if last >= '0' && last <= '9' {
		return s + "s"
	}
	return s
}

// projectMap evaluates a map of output expressions declared inline in an
// effect, such as a dedup hit's replayed answer.
func (ev *eval) projectMap(path string, raw any) (map[string]any, error) {
	m, ok := raw.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("engine: %s: expected a map of output members, got %T", path, raw)
	}
	out := map[string]any{}
	for _, k := range sortedKeysAny(m) {
		v, err := ev.evalAt(path+"."+k, fmt.Sprint(m[k]))
		if err != nil {
			return nil, err
		}
		out[k] = v
	}
	return out, nil
}

// runSendEvent drives a statechart: over the current selection when `foreach`
// names it, or over one addressed record.
//
// A record whose transition moved it elsewhere is removed from where it was,
// which is what makes redrive to a dead-letter queue a single declared action
// rather than a delete and a write that could half-happen.
func (ev *eval) runSendEvent(ctx context.Context, path string, e bir.SendEvent) error {
	evtCtx := map[string]any{}
	for _, k := range sortedKeys(e.Context) {
		v, err := ev.eval(path + ".context." + k)
		if err != nil {
			return err
		}
		evtCtx[k] = v
	}

	if e.ForEach != "" {
		sel := ev.op.Select
		if sel == nil || sel.Binding != e.ForEach {
			return fmt.Errorf("engine: %s.foreach: %q is not this operation's selection", path, e.ForEach)
		}
		res, ok := ev.e.ir.Resources[sel.Resource]
		if !ok {
			return fmt.Errorf("engine: %s: unknown resource %q", path, sel.Resource)
		}
		col, err := ev.collection(res)
		if err != nil {
			return err
		}
		// A transition that redrives (or otherwise finals) a selected record
		// must not consume the caller's limit: the pack skips that delivery
		// and keeps looking. Refill from a fresh select until the original
		// limit is met or the queue has nothing left to hand out.
		want := len(ev.selection)
		var kept []selected
		seen := map[string]bool{}
		for len(kept) < want {
			if len(ev.selection) == 0 {
				if err := ev.runSelect(ctx, ev.op); err != nil {
					return err
				}
				filtered := ev.selection[:0]
				for _, s := range ev.selection {
					if !seen[s.key] {
						filtered = append(filtered, s)
					}
				}
				ev.selection = filtered
				if need := want - len(kept); len(ev.selection) > need {
					ev.selection = ev.selection[:need]
				}
				if len(ev.selection) == 0 {
					break
				}
			}
			batch := append([]selected(nil), ev.selection...)
			ev.selection = nil
			for _, s := range batch {
				if seen[s.key] {
					continue
				}
				seen[s.key] = true
				changed, moved, err := ev.sendEvent(ctx, sel.Resource, res, s.rec, e.Event, evtCtx)
				if err != nil {
					return err
				}
				if err := ev.settleWrite(ctx, col, s.key, res, s.rec, changed, moved); err != nil {
					return err
				}
				// Redrive is a receive that the caller must not see: skip it and
				// look for another message. A MOVE that leaves the collection is
				// still a successful selection — message-move counters read it.
				if res.Statechart != nil && stateOf(s.rec, res.Statechart) == "redriven" {
					continue
				}
				newKey := ev.recordKey(res, s.rec)
				if newKey == "" {
					newKey = s.key
				}
				kept = append(kept, selected{key: newKey, rec: s.rec})
				if len(kept) >= want {
					break
				}
			}
		}
		ev.setSelection(e.ForEach, kept)
		return nil
	}

	if e.Resource == "" {
		return fmt.Errorf("engine: %s: needs a resource or a foreach", path)
	}
	res, ok := ev.e.ir.Resources[e.Resource]
	if !ok {
		return fmt.Errorf("engine: %s: unknown resource %q", path, e.Resource)
	}
	col, err := ev.collection(res)
	if err != nil {
		return err
	}
	key := ev.id
	if e.Key != "" {
		v, err := ev.eval(path + ".key")
		if err != nil {
			return err
		}
		key = fmt.Sprint(v)
	}
	rec, found, _, err := ev.loadRecord(ctx, col, key, res)
	if err != nil {
		return err
	}
	if !found {
		if e.Missing == "ignore" || e.Missing == "" {
			return nil
		}
		return ev.e.fault(e.Missing, "")
	}
	changed, moved, err := ev.sendEvent(ctx, e.Resource, res, rec, e.Event, evtCtx)
	if err != nil {
		return err
	}
	return ev.settleWrite(ctx, col, key, res, rec, changed, moved)
}

// settleWrite persists the outcome of one event: delete the record when it
// reached a final state or moved away, otherwise write it back. The key may
// have changed under it -- an SQS receipt handle is regenerated on redrive --
// so the row it came from is removed explicitly rather than overwritten.
func (ev *eval) settleWrite(ctx context.Context, col spi.Collection, key string,
	res bir.Resource, rec map[string]any, changed, moved bool) error {

	if !changed && !moved {
		return nil
	}
	final := false
	if res.Statechart != nil {
		if st, ok := res.Statechart.States[stateOf(rec, res.Statechart)]; ok {
			final = st.Final
		}
	}
	if moved || final {
		return col.Delete(ctx, key)
	}
	newKey := ev.recordKey(res, rec)
	if newKey != "" && newKey != key {
		if err := ev.putKeyAlias(ctx, res, key, newKey); err != nil {
			return err
		}
		if err := col.Delete(ctx, key); err != nil {
			return err
		}
		key = newKey
	}
	return ev.putRecord(ctx, col, key, rec)
}

// putKeyAlias records old→new when a resource's store key changes, so a
// caller holding the previous key still resolves. No-op without KeyAliases.
func (ev *eval) putKeyAlias(ctx context.Context, res bir.Resource, oldKey, newKey string) error {
	if res.KeyAliases == "" || oldKey == "" || newKey == "" || oldKey == newKey {
		return nil
	}
	name, err := ev.interpolate(res.KeyAliases)
	if err != nil {
		return err
	}
	return ev.e.scope(ev.req).Collection(name).Put(ctx, oldKey, []byte(newKey))
}

func (ev *eval) runExecute(ctx context.Context, path string, e bir.ExecuteEffect) error {
	if e.When != "" {
		ok, err := ev.evalBool(path + ".when")
		if err != nil {
			return err
		}
		if !ok {
			return nil
		}
	}
	if ev.e.deps.Executor == nil {
		return &spi.Fault{Code: "InternalFailure", Message: "execute effect: no executor configured", HTTPStatus: 500, Fault: "server"}
	}
	args := map[string]any{}
	for _, k := range sortedKeys(e.Args) {
		v, err := ev.eval(path + ".args." + k)
		if err != nil {
			return err
		}
		args[k] = v
	}
	out, err := ev.e.deps.Executor.Execute(ctx, ev.req.Identity.Account, ev.req.Identity.Region, e.Action, args)
	if err != nil {
		var f *execution.Failure
		if errors.As(err, &f) && f.Class == execution.ClassAbsent && e.AbsentError != "" {
			msg := e.AbsentMessage
			if msg == "" {
				msg = e.AbsentError
			}
			return ev.e.fault(e.AbsentError, msg)
		}
		if errors.As(err, &f) {
			return &spi.Fault{Code: "InternalFailure", Message: f.Error(), HTTPStatus: 500, Fault: "server"}
		}
		return err
	}
	if e.Bind != "" {
		ev.fx[e.Bind] = out
	}
	return nil
}
