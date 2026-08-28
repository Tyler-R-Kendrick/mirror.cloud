package engine

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/bir"
)

// selected is one chosen record and the store key it came from.
type selected struct {
	key string
	rec map[string]any
}

// runSelect gathers the records an operation acts on -- an SQS receive is the
// case it exists for -- and binds them.
//
// It is the engine's observation point: every candidate is settled against the
// clock first, so a visibility timeout that expired while nobody was looking
// takes effect here and the message is visible again. Settled records are
// written back, because a lifecycle transition that only existed in memory
// would be undone by the next reader.
func (ev *eval) runSelect(ctx context.Context, op bir.Operation) error {
	sel := op.Select
	if sel == nil {
		return nil
	}
	res, ok := ev.e.ir.Resources[sel.Resource]
	if !ok {
		return fmt.Errorf("engine: unknown resource %q", sel.Resource)
	}
	col, err := ev.collection(res)
	if err != nil {
		return err
	}
	entries, _, err := col.List(ctx, "", "", 0)
	if err != nil {
		return err
	}

	base := "operations." + ev.req.Operation + ".select."

	// Pass one: settle every candidate and sort them into those the selector
	// may take and those whose state blocks their group.
	var candidates []selected
	blockedGroups := map[string]bool{}
	groupBy := sel.Group != nil && sel.Group.By != ""
	grouping := groupBy
	if groupBy && sel.Group.When != "" {
		grouping, err = ev.evalBool(base + "group.when")
		if err != nil {
			return err
		}
	}

	for _, kv := range entries {
		rec := map[string]any{}
		if err := unmarshal(kv.Value, &rec); err != nil {
			return err
		}
		changed, err := ev.settle(rec, res.Statechart)
		if err != nil {
			return err
		}
		if changed {
			if err := ev.putRecord(ctx, col, kv.Key, rec); err != nil {
				return err
			}
		}

		state := ""
		if res.Statechart != nil {
			state = stateOf(rec, res.Statechart)
		}
		if grouping && sel.Group.ExclusiveInFlight != "" && state == sel.Group.ExclusiveInFlight {
			// A FIFO group with a message still in flight is closed: nothing
			// behind it may be delivered until that one is settled or returns.
			blockedGroups[fmt.Sprint(rec[sel.Group.By])] = true
		}
		if sel.State != "" && state != sel.State {
			continue
		}
		if sel.Filter != "" {
			ev.binds["item"] = rec
			keep, err := ev.evalBool(base + "filter")
			delete(ev.binds, "item")
			if err != nil {
				return err
			}
			if !keep {
				continue
			}
		}
		candidates = append(candidates, selected{key: kv.Key, rec: rec})
	}

	if sel.OrderBy != "" {
		if err := ev.sortSelected(candidates, sel.OrderBy); err != nil {
			return err
		}
	}

	limit := 0
	if sel.Limit != "" {
		v, err := ev.eval(base + "limit")
		if err != nil {
			return err
		}
		if n, ok := toFloat(v); ok {
			limit = int(n)
		}
	}

	// Pass two: take in order, at most one per group when grouping, skipping
	// groups that are blocked.
	takenGroups := map[string]bool{}
	var taken []selected
	for _, c := range candidates {
		if grouping {
			g := fmt.Sprint(c.rec[sel.Group.By])
			if g != "" && g != "<nil>" {
				if blockedGroups[g] || takenGroups[g] {
					continue
				}
				takenGroups[g] = true
			}
		}
		taken = append(taken, c)
		if limit > 0 && len(taken) >= limit {
			break
		}
	}

	ev.setSelection(sel.Binding, taken)
	return nil
}

// setSelection binds the chosen records, both for expressions (as a list of
// records) and for effects (which need the store keys too).
func (ev *eval) setSelection(binding string, taken []selected) {
	recs := make([]any, 0, len(taken))
	for _, s := range taken {
		recs = append(recs, s.rec)
	}
	ev.binds[binding] = recs
	ev.selection = taken
}

// sortSelected orders candidates by an expression over each record. The
// expression is evaluated once per record rather than once per comparison, so
// a sort cannot observe a value that changed under it.
func (ev *eval) sortSelected(items []selected, orderBy string) error {
	path := "operations." + ev.req.Operation + ".select.order_by"
	keys := make([]float64, len(items))
	strs := make([]string, len(items))
	numeric := true
	for i, s := range items {
		ev.binds["item"] = s.rec
		v, err := ev.eval(path)
		delete(ev.binds, "item")
		if err != nil {
			return err
		}
		if n, ok := toFloat(v); ok {
			keys[i] = n
		} else {
			numeric = false
		}
		strs[i] = fmt.Sprint(v)
	}
	idx := make([]int, len(items))
	for i := range idx {
		idx[i] = i
	}
	sort.SliceStable(idx, func(a, b int) bool {
		if numeric {
			return keys[idx[a]] < keys[idx[b]]
		}
		return strs[idx[a]] < strs[idx[b]]
	})
	out := make([]selected, len(items))
	for i, j := range idx {
		out[i] = items[j]
	}
	copy(items, out)
	return nil
}

// runWait is long polling as an engine capability rather than per-service code.
//
// It re-runs the selection until the bundle's condition holds or the timeout
// expires, parking on the engine's own clock in between. On a controllable
// clock the park returns as soon as the test advances time, so a twenty-second
// long poll costs a test nothing; on a real clock it is a real wait.
//
// Every wake re-observes, which means deadlines that expired during the wait
// fire and their records become selectable -- the same code path as any other
// observation, not a second implementation of visibility.
func (ev *eval) runWait(ctx context.Context, op bir.Operation) error {
	w := op.Wait
	if w == nil {
		return nil
	}
	base := "operations." + ev.req.Operation + ".wait."

	done, err := ev.evalBool(base + "until")
	if err != nil {
		return err
	}
	if done {
		return nil
	}

	v, err := ev.eval(base + "timeout")
	if err != nil {
		return err
	}
	timeout, ok := asDuration(v)
	if !ok {
		return fmt.Errorf("engine: %s.timeout: expected a duration, got %T", base, v)
	}
	if timeout <= 0 {
		return nil
	}

	deadline := ev.e.deps.Clock.Now().Add(timeout)
	for {
		now := ev.e.deps.Clock.Now()
		if !now.Before(deadline) {
			return nil
		}
		step := deadline.Sub(now)
		if step > waitPoll {
			step = waitPoll
		}
		select {
		case <-ev.e.deps.Clock.After(step):
		case <-ctx.Done():
			return nil
		}
		if err := ev.runSelect(ctx, op); err != nil {
			return err
		}
		done, err := ev.evalBool(base + "until")
		if err != nil {
			return err
		}
		if done {
			return nil
		}
	}
}

// waitPoll bounds how long one park lasts, so a message that arrives mid-wait
// is picked up promptly instead of at the end of the whole timeout. A bus
// wakeup would be tighter; this is correct without one, and correctness first.
const waitPoll = 100 * time.Millisecond
