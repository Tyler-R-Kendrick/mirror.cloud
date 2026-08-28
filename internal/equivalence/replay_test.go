package equivalence_test

import (
	"context"
	"os"
	"sort"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/bundled"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/equivalence"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spitest"
)

// tracesDir holds one recording per extracted service, taken from the
// hand-written pack in the commit that deleted it.
const tracesDir = "traces"

// TestBundlesMatchRecordedPacks is the standing extraction gate.
//
// For every recording under traces/, the engine serving that service's bundle
// must answer exactly as the pack did. This is what makes deleting a pack a
// safe operation rather than a hopeful one: the pack's semantics were the only
// description of itself, so they were frozen before it went, and the bundle
// keeps having to reproduce them.
//
// A divergence here is not automatically a bug in the bundle. When probed
// evidence shows the pack was wrong, the recording is re-cut deliberately with
// the cassette cited — but that is a visible decision, which is exactly what
// this test forces.
func TestBundlesMatchRecordedPacks(t *testing.T) {
	files, err := equivalence.LoadDir(os.DirFS("."), tracesDir)
	if err != nil {
		t.Fatalf("load recordings: %v", err)
	}
	if len(files) == 0 {
		t.Fatal("no recordings; every extracted service must leave one behind")
	}

	for _, f := range files {
		t.Run(f.Service, func(t *testing.T) {
			pack, err := bundled.New(f.Service, spitest.Deps(t))
			if err != nil {
				t.Fatalf("build the bundled service: %v", err)
			}
			diffs, err := equivalence.Replay(context.Background(), pack, f.Trace())
			if err != nil {
				t.Fatalf("replay: %v", err)
			}
			for _, d := range diffs {
				t.Errorf("divergence from %s: %s", f.Source, d)
			}
			// Superseded steps are reported every run, by index and reason.
			// A recording that accumulates them has stopped gating much, and
			// the only defence against that is having to read them.
			superseded := f.Superseded()
			for _, i := range sortedIdx(superseded) {
				t.Logf("step %d output not matched: %s", i, superseded[i])
			}
			if len(diffs) == 0 {
				t.Logf("%d steps equivalent to %s (%d superseded)",
					len(f.Steps), f.Source, len(superseded))
			}
		})
	}
}

// TestRecordingsAreServedByBundles keeps the two halves paired: a recording
// whose service has no bundle would silently stop gating anything.
func TestRecordingsAreServedByBundles(t *testing.T) {
	files, err := equivalence.LoadDir(os.DirFS("."), tracesDir)
	if err != nil {
		t.Fatal(err)
	}
	have := map[string]bool{}
	for _, id := range bundled.ServiceIDs() {
		have[id] = true
	}
	for _, f := range files {
		if !have[f.Service] {
			t.Errorf("%s has a recording but no behavior bundle", f.Service)
		}
	}
}

// TestReplayCatchesDivergence keeps the gate honest: a harness that always
// passes proves nothing, so this checks it fails when behavior really differs.
func TestReplayCatchesDivergence(t *testing.T) {
	trace := &equivalence.Trace{
		Steps: []equivalence.Step{{
			Operation: "DescribeProtection",
			Input:     map[string]any{"ProtectionId": "absent"},
			Identity:  spi.Identity{Account: "000000000000", Region: "us-east-1"},
		}},
		Outcomes: []equivalence.Outcome{{
			Fault: &spi.Fault{Code: "ResourceNotFoundException", HTTPStatus: 400, Fault: "client"},
		}},
	}

	// Same logical error at a different status — precisely the inconsistency
	// the hand-written packs contain, where one service answers 400 for a
	// missing resource and another answers 404.
	diffs, err := equivalence.Replay(context.Background(), &wrongFault{}, trace)
	if err != nil {
		t.Fatal(err)
	}
	if len(diffs) == 0 {
		t.Fatal("replay accepted a handler that returns a different status")
	}
}

// TestReplayCatchesReusedIdentifier covers the unification rule: two distinct
// recorded identifiers may not both be answered with the same new one, which
// is how a bundle that collapses two records into one would show up.
func TestReplayCatchesReusedIdentifier(t *testing.T) {
	acct := spi.Identity{Account: "000000000000", Region: "us-east-1"}
	step := equivalence.Step{Operation: "Get", Input: map[string]any{}, Identity: acct}
	trace := &equivalence.Trace{
		Steps: []equivalence.Step{step, step},
		Outcomes: []equivalence.Outcome{
			{Output: map[string]any{"Id": "aaaaaaaa"}},
			{Output: map[string]any{"Id": "bbbbbbbb"}},
		},
	}
	diffs, err := equivalence.Replay(context.Background(), &fixedID{}, trace)
	if err != nil {
		t.Fatal(err)
	}
	if len(diffs) == 0 {
		t.Fatal("replay accepted one identifier standing in for two")
	}
}

type wrongFault struct{}

func (*wrongFault) ServiceID() string    { return "aws.shield" }
func (*wrongFault) Operations() []string { return []string{"DescribeProtection"} }
func (*wrongFault) Invoke(context.Context, *spi.Request) (*spi.Response, error) {
	return nil, &spi.Fault{Code: "ResourceNotFoundException", HTTPStatus: 404, Fault: "client"}
}

type fixedID struct{}

func (*fixedID) ServiceID() string    { return "aws.shield" }
func (*fixedID) Operations() []string { return []string{"Get"} }
func (*fixedID) Invoke(context.Context, *spi.Request) (*spi.Response, error) {
	return &spi.Response{Output: map[string]any{"Id": "cccccccc"}}, nil
}

func sortedIdx(m map[int]string) []int {
	out := make([]int, 0, len(m))
	for i := range m {
		out = append(out, i)
	}
	sort.Ints(out)
	return out
}
