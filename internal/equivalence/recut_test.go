package equivalence

import (
	"context"
	"encoding/json"
	"os"
	"strconv"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/bundled"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/specboot"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spitest"
)

// The recordings' directory, as replay_test names it from the external test
// package; this file is internal so it can reach the replay loop, and cannot
// share the constant.
const tracesDir = "traces"

// TestRecut rewrites listed expectations to what the bundle answers, when a
// reviewer has established that the bundle is what the reference does and
// the pack was not.
//
// It runs only with RECUT_SPEC set, naming a JSON file:
//
//	{"service": "aws.sqs", "steps": {"7": "why, citing the reference", ...}}
//
// Each listed step's output or fault is replaced by the bundle's, its
// `recut` is set to the reason, and any superseded marks on it are cleared,
// because a re-cut step is gated in full again. Nothing else in the file
// moves. It is a test rather than a command so it can reach the replay loop
// unexported -- the same resolution of chained inputs, against the bundle's
// own answers, that the gate uses -- and so that a re-cut is reproducible by
// anyone from the spec file alone.
func TestRecut(t *testing.T) {
	specPath := os.Getenv("RECUT_SPEC")
	if specPath == "" {
		t.Skip("set RECUT_SPEC to a spec file to re-cut a recording")
	}
	var spec struct {
		Service string            `json:"service"`
		Steps   map[string]string `json:"steps"`
	}
	raw, err := os.ReadFile(specPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw, &spec); err != nil {
		t.Fatal(err)
	}
	want := map[int]string{}
	for k, reason := range spec.Steps {
		i, err := strconv.Atoi(k)
		if err != nil || reason == "" {
			t.Fatalf("spec step %q: need an index and a non-empty reason", k)
		}
		want[i] = reason
	}

	name := tracesDir + "/" + spec.Service + ".json"
	f, err := LoadFile(os.DirFS("."), name)
	if err != nil {
		t.Fatal(err)
	}
	pack, err := bundled.New(spec.Service, spitest.Deps(t))
	if err != nil {
		t.Fatal(err)
	}
	trace := f.Trace()
	trace.Model = specboot.Bundle().ServiceByID(spec.Service)

	var answered []Outcome
	for i, step := range trace.Steps {
		step.Input, _ = resolveInputs(step.Input, answered, trace.canonicalPath).(map[string]any)
		got, err := invoke(context.Background(), pack, step)
		if err != nil {
			t.Fatalf("step %d: %v", i, err)
		}
		answered = append(answered, Outcome{Output: trace.canonical(step.Operation, got.Output), Fault: got.Fault})
		reason, listed := want[i]
		if !listed {
			continue
		}
		e := &f.Steps[i]
		e.Output, e.Fault = nil, nil
		if got.Fault != nil {
			e.Fault = &FaultEntry{Code: got.Fault.Code, HTTPStatus: got.Fault.HTTPStatus, Fault: got.Fault.Fault}
		} else if len(got.Output) > 0 {
			e.Output = got.Output
		}
		e.Recut, e.Superseded, e.SupersededMembers = reason, "", nil
		delete(want, i)
	}
	for i := range want {
		t.Errorf("spec names step %d, which the recording does not have", i)
	}
	out, err := f.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(name, out, 0o644); err != nil {
		t.Fatal(err)
	}
	t.Logf("re-cut %d step(s) of %s", len(spec.Steps), name)
}
