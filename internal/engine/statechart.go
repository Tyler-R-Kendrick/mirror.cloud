package engine

import (
	"context"
	"fmt"
	"time"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/bir"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

// A record with a lifecycle carries two reserved members.
//
// Reserved names are a compromise, and the alternative was worse: a parallel
// collection of lifecycle rows keyed alongside the records would have to be
// kept consistent with them across snapshot, restore and every write, and the
// first inconsistency would be silent. Keeping the state on the record makes
// it atomic with the data by construction. The loader rejects a bundle whose
// record or views use these names.
const (
	stateMember     = "__state"
	deadlinesMember = "__deadlines"
)

// Deadlines are stored as absolute nanoseconds on the engine's own clock, not
// as durations. A duration would have to be counted down by something; an
// instant is simply compared, which is why no goroutine is needed and why a
// controllable clock can jump forward without any of this noticing.

// stateOf reads a record's lifecycle state, defaulting to the chart's initial
// state so a record written before a chart existed still behaves.
func stateOf(rec map[string]any, chart *bir.Statechart) string {
	if s, ok := rec[stateMember].(string); ok && s != "" {
		return s
	}
	return chart.Initial
}

// deadlinesOf reads the armed deadlines off a record.
func deadlinesOf(rec map[string]any) map[string]int64 {
	out := map[string]int64{}
	raw, ok := rec[deadlinesMember].(map[string]any)
	if !ok {
		return out
	}
	for name, v := range raw {
		if n, ok := toFloat(v); ok {
			out[name] = int64(n)
		}
	}
	return out
}

func setDeadline(rec map[string]any, name string, at time.Time) {
	d, ok := rec[deadlinesMember].(map[string]any)
	if !ok {
		d = map[string]any{}
		rec[deadlinesMember] = d
	}
	d[name] = at.UnixNano()
}

// settle advances a record through any timer whose deadline has passed, and
// reports whether it changed.
//
// This is the "lazy timer" the design turns on: nothing schedules a callback
// when a visibility timeout is armed. The deadline sits on the record until
// something looks at it -- a receive, a describe, a select -- and the look is
// what makes it fire. Behavior is therefore a pure function of the clock
// reading at the observation point, which is what lets a test jump an hour
// forward and see exactly what an hour of waiting would have produced.
//
// Timers are applied repeatedly so a chain of expired deadlines settles in one
// observation. The loop is bounded by the number of states: a cycle of expired
// timers would otherwise spin forever, and a bundle that specifies one is a bug
// worth reporting rather than hanging on.
func (ev *eval) settle(rec map[string]any, chart *bir.Statechart) (bool, error) {
	if chart == nil {
		return false, nil
	}
	now := ev.e.deps.Clock.Now().UnixNano()
	changed := false

	for hops := 0; hops <= len(chart.States); hops++ {
		state, ok := chart.States[stateOf(rec, chart)]
		if !ok {
			return changed, fmt.Errorf("engine: record is in state %q, which the chart does not define",
				stateOf(rec, chart))
		}
		fired := false
		for _, timer := range state.Timers {
			at, armed := deadlinesOf(rec)[timer.Deadline]
			if !armed || now < at {
				continue
			}
			if _, ok := chart.States[timer.Target]; !ok {
				return changed, fmt.Errorf("engine: timer %q targets unknown state %q",
					timer.Deadline, timer.Target)
			}
			rec[stateMember] = timer.Target
			if d, ok := rec[deadlinesMember].(map[string]any); ok {
				delete(d, timer.Deadline)
			}
			fired, changed = true, true
			break
		}
		if !fired {
			return changed, nil
		}
	}
	return changed, fmt.Errorf("engine: timers did not settle; the chart has a cycle of expired deadlines")
}

// sendEvent applies one event to one record: settle first, then take the first
// transition whose guard holds. Returns whether the record changed and whether
// it moved to another collection.
//
// Settling before the event is what makes ordering intuitive: a message whose
// visibility timeout expired an hour ago is visible when the RECEIVE arrives,
// not still invisible because nobody had looked since.
func (ev *eval) sendEvent(ctx context.Context, resource string, res bir.Resource,
	rec map[string]any, event string, evtCtx map[string]any) (changed, moved bool, err error) {

	chart := res.Statechart
	if chart == nil {
		return false, false, fmt.Errorf("engine: %s has no statechart to send %q to", resource, event)
	}
	if changed, err = ev.settle(rec, chart); err != nil {
		return changed, false, err
	}

	state, ok := chart.States[stateOf(rec, chart)]
	if !ok {
		return changed, false, fmt.Errorf("engine: record is in state %q, which the chart does not define",
			stateOf(rec, chart))
	}
	transitions, handled := state.On[event]
	if !handled {
		// An event a state does not handle is ignored, as in SCXML. Deleting a
		// message twice is not an error in SQS, and a chart that wants it to be
		// says so with a require rule.
		return changed, false, nil
	}

	// Guards and actions see the record and the event's context.
	saved := ev.pushEventScope(rec, evtCtx)
	defer saved()

	base := "resources." + resource + ".statechart.states." + stateOf(rec, chart) + ".on." + event
	for i, t := range transitions {
		if t.Guard != "" {
			ok, err := ev.evalBool(fmt.Sprintf("%s[%d].guard", base, i))
			if err != nil {
				return changed, false, err
			}
			if !ok {
				continue
			}
		}
		if _, ok := chart.States[t.Target]; !ok {
			return changed, false, fmt.Errorf("engine: %s[%d] targets unknown state %q", base, i, t.Target)
		}
		for j, action := range t.Actions {
			p := fmt.Sprintf("%s[%d].actions[%d]", base, i, j)
			didMove, err := ev.runAction(ctx, p, resource, res, rec, action)
			if err != nil {
				return changed, moved, err
			}
			moved = moved || didMove
		}
		rec[stateMember] = t.Target
		return true, moved, nil
	}
	return changed, false, nil
}

// pushEventScope binds `rec` and `event` for the duration of a transition and
// returns the restore function.
func (ev *eval) pushEventScope(rec map[string]any, evtCtx map[string]any) func() {
	prevRec, hadRec := ev.binds["rec"]
	prevEvt, hadEvt := ev.binds["event"]
	ev.binds["rec"] = rec
	if evtCtx == nil {
		evtCtx = map[string]any{}
	}
	ev.binds["event"] = evtCtx
	return func() {
		restore(ev.binds, "rec", prevRec, hadRec)
		restore(ev.binds, "event", prevEvt, hadEvt)
	}
}

func restore(m map[string]any, key string, prev any, had bool) {
	if had {
		m[key] = prev
	} else {
		delete(m, key)
	}
}

// runAction performs one transition action. It reports whether the record was
// moved to another collection, in which case the caller must not write it back
// where it came from.
func (ev *eval) runAction(ctx context.Context, path, resource string, res bir.Resource,
	rec map[string]any, a bir.Action) (moved bool, err error) {

	switch {
	case a.Set != nil:
		for _, k := range sortedKeys(a.Set) {
			v, err := ev.eval(path + ".set." + k)
			if err != nil {
				return false, err
			}
			rec[k] = v
			// The record the next expression sees must include this write, so
			// two actions in one transition compose.
			ev.binds["rec"] = rec
		}
		return false, nil

	case a.Deadline != nil:
		d, err := ev.eval(path + ".deadline.after")
		if err != nil {
			return false, err
		}
		dur, ok := asDuration(d)
		if !ok {
			return false, fmt.Errorf("engine: %s.deadline.after: expected a duration, got %T", path, d)
		}
		setDeadline(rec, a.Deadline.Name, ev.e.deps.Clock.Now().Add(dur))
		ev.binds["rec"] = rec
		return false, nil

	case a.Move != nil:
		return true, ev.moveRecord(ctx, path, resource, res, rec, *a.Move)
	}
	return false, fmt.Errorf("engine: %s: empty action", path)
}

// moveRecord relocates a record into another collection of the same resource --
// SQS redrive to a dead-letter queue is the case this exists for. The
// destination is addressed by overriding the collection template's bindings, so
// "move this message to that queue" is stated as data rather than as a second
// service reaching into the first one's records.
func (ev *eval) moveRecord(ctx context.Context, path, resource string, res bir.Resource,
	rec map[string]any, m bir.MoveAction) error {

	// New members first: a moved record usually needs a fresh key, and the
	// destination is computed from the same expressions.
	for _, k := range sortedKeysAny(m.Set) {
		v, err := ev.recordValue(ctx, path+".move.set."+k, m.Set[k])
		if err != nil {
			return err
		}
		rec[k] = v
		ev.binds["rec"] = rec
	}
	if m.State != "" {
		rec[stateMember] = m.State
		delete(rec, deadlinesMember)
	}

	// The destination's collection is this resource's template with the named
	// bindings overridden -- { queue: "<name>" } re-points "msgs:{queue.id}".
	saved := map[string]string{}
	for _, k := range sortedKeys(m.To) {
		v, err := ev.eval(path + ".move.to." + k)
		if err != nil {
			return err
		}
		if k == "resource" {
			continue
		}
		saved[k] = ev.resIDs[k]
		ev.resIDs[k] = fmt.Sprint(v)
	}
	defer func() {
		for k, v := range saved {
			ev.resIDs[k] = v
		}
	}()

	dest, err := ev.collection(res)
	if err != nil {
		return err
	}
	key := ev.recordKey(res, rec)
	if key == "" {
		return fmt.Errorf("engine: %s.move: the moved record has no key", path)
	}
	return ev.putRecord(ctx, dest, key, rec)
}

// recordKey is the store key for a record: its keying member when the resource
// declares one, otherwise the current ID.
func (ev *eval) recordKey(res bir.Resource, rec map[string]any) string {
	if res.Key != "" {
		if v, ok := rec[res.Key]; ok {
			return fmt.Sprint(v)
		}
		return ""
	}
	return ev.id
}

// asDuration accepts what CEL hands back for a duration expression.
func asDuration(v any) (time.Duration, bool) {
	switch d := v.(type) {
	case time.Duration:
		return d, true
	case int64:
		return time.Duration(d) * time.Second, true
	case float64:
		return time.Duration(d) * time.Second, true
	}
	return 0, false
}

// loadRecord reads one record of a resource and settles its timers, reporting
// whether the settling changed it. Every read path that cares about lifecycle
// goes through here, so there is one observation point rather than several
// that could disagree.
func (ev *eval) loadRecord(ctx context.Context, col spi.Collection, key string,
	res bir.Resource) (rec map[string]any, found, changed bool, err error) {

	raw, found, err := col.Get(ctx, key)
	if err != nil || !found {
		return nil, found, false, err
	}
	rec = map[string]any{}
	if err := unmarshal(raw, &rec); err != nil {
		return nil, true, false, err
	}
	changed, err = ev.settle(rec, res.Statechart)
	return rec, true, changed, err
}
