package spi

import (
	"context"
	"reflect"
	"testing"
	"time"
)

// Members reads name.member.1, name.member.2 and on to the first index
// missing, as the query decoder does: a gap ends the list. Before, it
// counted the members and read that many indices, so member.1 and member.3
// answered [a] and member.2 and member.3 answered [b], neither the decoder's.
func TestMembersEndsAtAGapAsTheDecoderDoes(t *testing.T) {
	for _, c := range []struct {
		in   map[string]any
		want []any
	}{
		{map[string]any{"L.member.1": "a", "L.member.2": "b", "L.member.3": "c"}, []any{"a", "b", "c"}},
		{map[string]any{"L.member.1": "a", "L.member.3": "c"}, []any{"a"}},
		{map[string]any{"L.member.2": "b", "L.member.3": "c"}, []any{}},
		{map[string]any{"L.member.x": "a", "Other.member.1": "o"}, []any{}},
	} {
		if got := Members(c.in, "L"); !reflect.DeepEqual(got, c.want) {
			t.Errorf("Members(%v) = %#v, want %#v", c.in, got, c.want)
		}
	}
}

// Fold reads a key as given first, and else one equal to it in another case.
func TestFoldReadsAKeyInAnyCase(t *testing.T) {
	rec := map[string]string{"Name": "a", "name": "b", "Age": "3"}
	for key, want := range map[string]string{"Name": "a", "name": "b", "AGE": "3", "age": "3", "missing": ""} {
		if got := Fold(rec, key); got != want {
			t.Errorf("Fold(%q) = %q, want %q", key, got, want)
		}
	}
}

// GetJSON answers the object held, not-held for none, and the decode's error for a value that is not one.
func TestGetJSONReadsAnObject(t *testing.T) {
	ctx, c := context.Background(), memCollection{"obj": []byte(`{"a":1}`), "bad": []byte(`[1]`), "null": []byte(`null`)}
	if rec, ok, err := GetJSON(ctx, c, "obj"); !ok || err != nil || rec["a"] != float64(1) {
		t.Errorf("obj: %v %v %v", rec, ok, err)
	}
	if rec, ok, err := GetJSON(ctx, c, "none"); ok || err != nil || rec != nil {
		t.Errorf("none: %v %v %v", rec, ok, err)
	}
	if _, ok, err := GetJSON(ctx, c, "bad"); ok || err == nil {
		t.Errorf("bad: %v %v", ok, err)
	}
	if rec, ok, err := GetJSON(ctx, c, "null"); !ok || err != nil || rec != nil {
		t.Errorf("null: %v %v %v", rec, ok, err)
	}
}

// ReadJSON overlays what c holds on v's defaults, and leaves them when c holds nothing.
func TestReadJSONOverlaysTheRecordOnDefaults(t *testing.T) {
	ctx, c := context.Background(), memCollection{"obj": []byte(`{"a":1}`)}
	m := map[string]any{"a": 0, "b": 2}
	if !ReadJSON(ctx, c, "obj", &m) || m["a"] != float64(1) || m["b"] != 2 {
		t.Errorf("obj: %v", m)
	}
	d := map[string]any{"b": 2}
	if ReadJSON(ctx, c, "none", &d) || len(d) != 1 || d["b"] != 2 {
		t.Errorf("none: %v", d)
	}
}

// memCollection is a Collection of fixed values; only Get is read.
type memCollection map[string][]byte

func (m memCollection) Get(_ context.Context, key string) ([]byte, bool, error) {
	v, ok := m[key]
	return v, ok, nil
}

func (memCollection) Put(context.Context, string, []byte) error { return nil }
func (memCollection) Delete(context.Context, string) error      { return nil }
func (memCollection) List(context.Context, string, string, int) ([]KV, bool, error) {
	return nil, false, nil
}
func (memCollection) Txn(context.Context, func(Tx) error) error { return nil }

// awaitClock is a Clock whose AfterTime answers a channel the test fires.
type awaitClock struct {
	Clock
	due chan time.Time
}

func (c awaitClock) AfterTime(time.Time) <-chan time.Time { return c.due }

// Await ends true when the clock reaches next or the worker is woken, false when it is stopped, and with
// no next it asks the clock for nothing.
func TestAwaitWaitsForTheClockAWakeOrAStop(t *testing.T) {
	next := time.Unix(1, 0)
	for name, c := range map[string]struct {
		next            time.Time
		due, wake, stop bool
		want            bool
	}{
		"due":           {next, true, false, false, true},
		"woken":         {next, false, true, false, true},
		"stopped":       {next, false, false, true, false},
		"idle, woken":   {time.Time{}, true, true, false, true}, // the clock's channel is not read with no next
		"idle, stopped": {time.Time{}, true, false, true, false},
	} {
		clock, wake, stop := awaitClock{due: make(chan time.Time, 1)}, make(chan struct{}, 1), make(chan struct{}, 1)
		if c.due {
			clock.due <- next
		}
		if c.wake {
			wake <- struct{}{}
		}
		if c.stop {
			stop <- struct{}{}
		}
		if got := Await(clock, c.next, wake, stop); got != c.want {
			t.Errorf("%s: Await = %v, want %v", name, got, c.want)
		}
		if c.next.IsZero() && len(clock.due) != 1 {
			t.Errorf("%s: Await read the clock with no next", name)
		}
	}
}
