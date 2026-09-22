package execution_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/execution"
)

// fakeBackend is one hand-rolled backend: its own action names, its own
// storage map, no knowledge of any provider. The point of the pair below is
// that the SAME registry machinery serves both without a provider-specific
// branch anywhere.
type fakeBackend struct {
	name    string
	version string
	descs   []execution.Descriptor
	// store is the fake's authoritative state, keyed by Ref.String().
	store map[string]map[string]any
	// calls records every action that reached Call, so a test can prove a
	// validation failure never touched the backend.
	calls []string
	// lostWrite simulates a transmitted mutation whose reply was lost.
	lostWrite bool
	// putAction and getAction name this fake's own vocabulary; Call routes
	// on them so the fake never needs to know another backend's names.
	putAction string
	getAction string
}

func (f *fakeBackend) Identity() (string, string) { return f.name, f.version }
func (f *fakeBackend) Describe() []execution.Descriptor {
	return f.descs
}

func (f *fakeBackend) Call(ctx context.Context, req execution.Request) (execution.Response, error) {
	f.calls = append(f.calls, req.Action)
	switch req.Action {
	case f.putAction:
		if f.lostWrite {
			return execution.Response{}, &execution.Failure{
				Class: execution.ClassUnknownOutcome, Action: req.Action,
				Detail: "transmitted; reply lost",
			}
		}
		if f.store == nil {
			f.store = map[string]map[string]any{}
		}
		k := req.Ref.String()
		if f.store[k] == nil {
			f.store[k] = map[string]any{}
		}
		f.store[k][req.Args["key"].(string)] = req.Args["value"]
		return execution.Response{Output: map[string]any{"stored": true}}, nil
	case f.getAction:
		k := req.Ref.String()
		if rec := f.store[k]; rec != nil {
			if v, ok := rec[req.Args["key"].(string)]; ok {
				return execution.Response{Output: map[string]any{"value": v}}, nil
			}
		}
		return execution.Response{}, &execution.Failure{
			Class: execution.ClassAbsent, Action: req.Action, Detail: "no such key",
		}
	}
	return execution.Response{}, &execution.Failure{
		Class: execution.ClassUnsupported, Action: req.Action, Detail: "not implemented by this fake",
	}
}

// backendA and backendB use different action names, different versions, and
// different resource kinds. Neither could satisfy the other's descriptor set,
// which is exactly what makes a shared registry meaningful.
func backendA() *fakeBackend {
	return &fakeBackend{name: "alpha-runtime", version: "1.2.3",
		putAction: "alpha.putValue", getAction: "alpha.getValue",
		descs: []execution.Descriptor{{
			Name: "alpha.putValue", Version: 1, Resource: "kv-namespace",
			Body: execution.BodyNone, Effect: execution.EffectWrite,
			Input: map[string]execution.Field{
				"key":   {Type: "string", Required: true},
				"value": {Type: "bytes", Required: true},
			},
			Output: map[string]execution.Field{"stored": {Type: "bool"}},
		}, {
			Name: "alpha.getValue", Version: 1, Resource: "kv-namespace",
			Body: execution.BodyNone, Effect: execution.EffectRead,
			Input:  map[string]execution.Field{"key": {Type: "string", Required: true}},
			Output: map[string]execution.Field{"value": {Type: "bytes"}},
		}}}
}

func backendB() *fakeBackend {
	return &fakeBackend{name: "beta-runtime", version: "0.9.0",
		putAction: "beta.persist", getAction: "beta.fetch",
		descs: []execution.Descriptor{{
			Name: "beta.persist", Version: 2, Resource: "durable-object",
			Body: execution.BodyNone, Effect: execution.EffectWrite,
			Input: map[string]execution.Field{
				"key":   {Type: "string", Required: true},
				"value": {Type: "json", Required: true},
			},
			Output: map[string]execution.Field{"stored": {Type: "bool"}},
		}, {
			Name: "beta.fetch", Version: 2, Resource: "durable-object",
			Body: execution.BodyNone, Effect: execution.EffectRead,
			Input:  map[string]execution.Field{"key": {Type: "string", Required: true}},
			Output: map[string]execution.Field{"value": {Type: "json"}},
		}}}
}

// driveOne runs the same acceptance sequence against either backend through
// the generic registry: round-trip a value, then hit every failure class and
// assert the class name. No branch anywhere mentions which backend is under
// test -- that is the provider-neutrality claim, made executable. version is
// the fake's own descriptor version, so Covers is exercised against the
// truth rather than a hard-coded number.
func driveOne(t *testing.T, b *fakeBackend, putAction, getAction, resource string, version int) {
	t.Helper()
	reg, err := execution.NewRegistry(b)
	if err != nil {
		t.Fatal(err)
	}
	ref := execution.Ref{Environment: "env1", Account: "acct1", Region: "us-east-1", Kind: resource, ID: "res1", Generation: 1}
	ctx := context.Background()

	// Round-trip: put then get, value preserved exactly.
	if _, err := reg.Dispatch(ctx, execution.Request{
		Action: putAction, Ref: ref,
		Args: map[string]any{"key": "k1", "value": []byte{0x00, 0xff, 'a'}},
	}); err != nil {
		t.Fatalf("put: %v", err)
	}
	resp, err := reg.Dispatch(ctx, execution.Request{
		Action: getAction, Ref: ref,
		Args: map[string]any{"key": "k1"},
	})
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	got, _ := resp.Output["value"].([]byte)
	if string(got) != string([]byte{0x00, 0xff, 'a'}) {
		t.Fatalf("round-trip got %v", got)
	}

	// Unknown action -> unsupported, and the backend never sees it.
	_, err = reg.Dispatch(ctx, execution.Request{Action: "nope.missing", Ref: ref, Args: map[string]any{}})
	assertClass(t, err, execution.ClassUnsupported)
	if len(b.calls) != 2 {
		t.Fatalf("backend saw %d calls, want 2 (validation must not reach it)", len(b.calls))
	}

	// Missing required input -> validation, backend untouched.
	_, err = reg.Dispatch(ctx, execution.Request{Action: putAction, Ref: ref, Args: map[string]any{}})
	assertClass(t, err, execution.ClassValidation)

	// Wrong type on a strictly typed field -> validation naming the field.
	// (The `key` field is string in every fake; `value` is deliberately
	// bytes in one and json in the other, so only `key` is portable.)
	_, err = reg.Dispatch(ctx, execution.Request{
		Action: putAction, Ref: ref,
		Args: map[string]any{"key": 42, "value": []byte("v")},
	})
	assertClass(t, err, execution.ClassValidation)

	// Wrong resource kind -> validation (kind mismatch refused before Call).
	_, err = reg.Dispatch(ctx, execution.Request{
		Action: putAction, Ref: execution.Ref{Environment: "env1", Account: "acct1", Region: "us-east-1", Kind: "other-kind", ID: "x"},
		Args: map[string]any{"key": "k", "value": []byte("v")},
	})
	assertClass(t, err, execution.ClassValidation)

	// Missing trusted scope -> validation: a reference without an
	// environment or account is never dispatched.
	_, err = reg.Dispatch(ctx, execution.Request{
		Action: putAction, Ref: execution.Ref{Kind: resource, ID: "x"},
		Args: map[string]any{"key": "k", "value": []byte("v")},
	})
	assertClass(t, err, execution.ClassValidation)

	// Absent resource -> absent, distinguishable from every failure above.
	_, err = reg.Dispatch(ctx, execution.Request{
		Action: getAction, Ref: ref,
		Args: map[string]any{"key": "never-written"},
	})
	assertClass(t, err, execution.ClassAbsent)

	// Cancellation before dispatch -> unavailable.
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	_, err = reg.Dispatch(cancelled, execution.Request{
		Action: putAction, Ref: ref,
		Args: map[string]any{"key": "k", "value": []byte("v")},
	})
	assertClass(t, err, execution.ClassUnavailable)

	// A transmitted mutation whose reply was lost -> outcome_unknown, and
	// it is the backend (not the registry) that classified it, because
	// only the backend knows the reply was lost.
	b.lostWrite = true
	_, err = reg.Dispatch(ctx, execution.Request{
		Action: putAction, Ref: ref,
		Args: map[string]any{"key": "k2", "value": []byte("v")},
	})
	assertClass(t, err, execution.ClassUnknownOutcome)
	b.lostWrite = false

	// Covers: the bundle's declared actions must match versions exactly.
	if err := reg.Covers(map[string]int{putAction: version}); err != nil {
		t.Fatalf("Covers matched set: %v", err)
	}
	if err := reg.Covers(map[string]int{putAction: version + 99}); err == nil {
		t.Fatal("Covers accepted a wrong version")
	}
	if err := reg.Covers(map[string]int{"ghost.action": 1}); err == nil {
		t.Fatal("Covers accepted an unknown action")
	}
}

func assertClass(t *testing.T, err error, want execution.Class) {
	t.Helper()
	var f *execution.Failure
	if !errors.As(err, &f) {
		t.Fatalf("want *Failure/%s, got %v", want, err)
	}
	if f.Class != want {
		t.Fatalf("class %q, want %q (err: %v)", f.Class, want, err)
	}
}

// TestSameContractAcrossBackends is the CONTRACT acceptance: two fakes with
// different names, versions, kinds and action vocabularies, driven by one
// generic path. Fake success proves dispatcher correctness only; live
// integrations remain required and are separate suites.
func TestSameContractAcrossBackends(t *testing.T) {
	t.Run("alpha", func(t *testing.T) {
		driveOne(t, backendA(), "alpha.putValue", "alpha.getValue", "kv-namespace", 1)
	})
	t.Run("beta", func(t *testing.T) {
		driveOne(t, backendB(), "beta.persist", "beta.fetch", "durable-object", 2)
	})
}

// TestEmptyBackendRefused: a backend that describes nothing cannot serve --
// an empty registry would answer "unsupported" to everything, which looks
// like a working boundary that never works.
func TestEmptyBackendRefused(t *testing.T) {
	b := backendA()
	b.descs = nil
	if _, err := execution.NewRegistry(b); err == nil {
		t.Fatal("empty descriptor set accepted")
	}
}

// TestRegistryRefusesBadDescriptors pins load-time rejection: a descriptor
// that lies about its backend, names an unknown mode, or carries a bad field
// type must fail NewRegistry, not wait for a request.
func TestRegistryRefusesBadDescriptors(t *testing.T) {
	cases := []struct {
		name string
		desc execution.Descriptor
		want string
	}{{
		name: "wrong backend",
		desc: execution.Descriptor{Name: "x.put", Version: 1, Resource: "r",
			Body: execution.BodyNone, Effect: execution.EffectWrite, Backend: "some-other-runtime"},
		want: "requires backend",
	}, {
		name: "zero version",
		desc: execution.Descriptor{Name: "x.put", Version: 0, Resource: "r",
			Body: execution.BodyNone, Effect: execution.EffectWrite},
		want: "version must be >= 1",
	}, {
		name: "unknown body mode",
		desc: execution.Descriptor{Name: "x.put", Version: 1, Resource: "r",
			Body: "quantum", Effect: execution.EffectWrite},
		want: "unknown body mode",
	}, {
		name: "unknown effect",
		desc: execution.Descriptor{Name: "x.put", Version: 1, Resource: "r",
			Body: execution.BodyNone, Effect: "vibes"},
		want: "unknown effect class",
	}, {
		name: "bad field type",
		desc: execution.Descriptor{Name: "x.put", Version: 1, Resource: "r",
			Body: execution.BodyNone, Effect: execution.EffectWrite,
			Input: map[string]execution.Field{"k": {Type: "timestamp", Required: true}}},
		want: "unknown type",
	}, {
		name: "no resource kind",
		desc: execution.Descriptor{Name: "x.put", Version: 1, Resource: "",
			Body: execution.BodyNone, Effect: execution.EffectWrite},
		want: "no allowed resource kind",
	}}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b := backendA()
			b.descs = []execution.Descriptor{tc.desc}
			_, err := execution.NewRegistry(b)
			if err == nil {
				t.Fatal("bad descriptor accepted")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error %q does not say %q", err, tc.want)
			}
		})
	}
}
