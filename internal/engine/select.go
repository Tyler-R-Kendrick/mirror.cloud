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
	drain := grouping && sel.Group.Drain
	// Drain needs every group member (including in-flight) to know which
	// groups are partial and where the leading selectable run ends.
	groupAll := map[string][]selected{}
	var groupOrder []string

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
		item := selected{key: kv.Key, rec: rec}
		if drain {
			g := fmt.Sprint(rec[sel.Group.By])
			if g == "" || g == "<nil>" {
				g = kv.Key
			}
			if _, ok := groupAll[g]; !ok {
				groupOrder = append(groupOrder, g)
			}
			groupAll[g] = append(groupAll[g], item)
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
		candidates = append(candidates, item)
	}

	if sel.OrderBy != "" {
		if err := ev.sortSelected(candidates, sel.OrderBy); err != nil {
			return err
		}
		for g, members := range groupAll {
			if err := ev.sortSelected(members, sel.OrderBy); err != nil {
				return err
			}
			groupAll[g] = members
		}
	}

	// Counted after the filter and before the limit: this is what the
	// selection was chosen from, which a limited selection cannot say for
	// itself.
	if sel.Count != "" {
		ev.binds[sel.Count] = len(candidates)
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

	var taken []selected
	if drain {
		taken = ev.drainGroups(groupOrder, groupAll, candidates, sel, blockedGroups, limit)
	} else {
		// Pass two: take in order, at most one per group when grouping, skipping
		// groups that are blocked.
		takenGroups := map[string]bool{}
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
	}

	ev.setSelection(sel.Binding, taken)
	return nil
}

// drainGroups is SQS FIFO receive: consecutive selectable messages from a
// group, groups with an in-flight member after complete ones, never-received
// groups before redelivered ones, then by the group's first order_by key.
func (ev *eval) drainGroups(order []string, all map[string][]selected, selectable []selected,
	sel *bir.Select, _ map[string]bool, limit int) []selected {

	selectableSet := map[string]bool{}
	for _, c := range selectable {
		selectableSet[c.key] = true
	}
	partial := map[string]bool{}
	received := map[string]bool{}
	firstRank := map[string]int{}
	for i, g := range order {
		firstRank[g] = i
		members := all[g]
		leading := 0
		for _, m := range members {
			if !selectableSet[m.key] {
				break
			}
			leading++
		}
		if leading > 0 && leading < len(members) {
			partial[g] = true
		}
		for _, m := range members {
			if n, ok := toFloat(m.rec["receiveCount"]); ok && n > 0 {
				received[g] = true
				break
			}
		}
	}
	sort.SliceStable(order, func(i, j int) bool {
		a, b := order[i], order[j]
		if partial[a] != partial[b] {
			return !partial[a]
		}
		if received[a] != received[b] {
			return !received[a]
		}
		return firstRank[a] < firstRank[b]
	})
	// Re-rank by first selectable's order among candidates when order_by set:
	// firstRank from discovery order is a stand-in; candidates are already
	// sorted, so prefer the group whose first selectable appears earliest.
	pos := map[string]int{}
	for i, c := range selectable {
		g := fmt.Sprint(c.rec[sel.Group.By])
		if g == "" || g == "<nil>" {
			g = c.key
		}
		if _, ok := pos[g]; !ok {
			pos[g] = i
		}
	}
	sort.SliceStable(order, func(i, j int) bool {
		a, b := order[i], order[j]
		if partial[a] != partial[b] {
			return !partial[a]
		}
		if received[a] != received[b] {
			return !received[a]
		}
		pa, oka := pos[a]
		pb, okb := pos[b]
		if oka && okb && pa != pb {
			return pa < pb
		}
		return firstRank[a] < firstRank[b]
	})

	var taken []selected
	for _, g := range order {
		for _, m := range all[g] {
			if !selectableSet[m.key] {
				break
			}
			taken = append(taken, m)
			if limit > 0 && len(taken) >= limit {
				return taken
			}
		}
	}
	return taken
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

// nextDeadline answers the earliest armed deadline among the selection's
// records that has not fired yet, so a waiter can park on the instant a
// record becomes selectable rather than on its own timeout.
func (ev *eval) nextDeadline(ctx context.Context, op bir.Operation) (time.Time, bool) {
	if op.Select == nil {
		return time.Time{}, false
	}
	res, ok := ev.e.ir.Resources[op.Select.Resource]
	if !ok || res.Statechart == nil {
		return time.Time{}, false
	}
	col, err := ev.collection(res)
	if err != nil {
		return time.Time{}, false
	}
	entries, _, err := col.List(ctx, "", "", 0)
	if err != nil {
		return time.Time{}, false
	}
	now := ev.e.deps.Clock.Now()
	var next time.Time
	for _, kv := range entries {
		rec := map[string]any{}
		if err := unmarshal(kv.Value, &rec); err != nil {
			continue
		}
		for _, at := range deadlinesOf(rec) {
			when := time.Unix(0, at)
			if when.After(now) && (next.IsZero() || when.Before(next)) {
				next = when
			}
		}
	}
	return next, !next.IsZero()
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

	// Subscribe before parking so a create that races the park still wakes us.
	var wake <-chan struct{}
	var cancel func()
	if ev.e.deps.Bus != nil && op.Select != nil {
		res, ok := ev.e.ir.Resources[op.Select.Resource]
		if ok {
			colName, err := ev.interpolate(res.Collection)
			if err == nil && colName != "" {
				ch := make(chan struct{}, 1)
				wake = ch
				cancel = ev.e.deps.Bus.Subscribe("collection:"+colName, func(context.Context, []byte) {
					select {
					case ch <- struct{}{}:
					default:
					}
				})
			}
		}
	}
	if cancel != nil {
		defer cancel()
	}

	for {
		now := ev.e.deps.Clock.Now()
		if !now.Before(deadline) {
			return nil
		}
		// Park until the deadline, the next armed deadline among the records
		// being waited on, or a bus wakeup from a write to this selection's
		// collection -- the long-poll contract, not a poll loop. An armed
		// deadline is why a record may become selectable with nobody writing:
		// a delayed message reappears on its own instant, and a waiter that
		// slept past it would answer empty with the message already visible.
		park := deadline
		if at, ok := ev.nextDeadline(ctx, op); ok && at.After(now) && at.Before(park) {
			park = at
		}
		select {
		case <-ev.e.deps.Clock.AfterTime(park):
		case <-wake:
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
