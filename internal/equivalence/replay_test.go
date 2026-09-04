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
	// A shadow bundle is gated but not serving, and the gate is this test's
	// concern: TestBundlesMatchRecordedPacks builds the bundle directly, so a
	// recording keeps proving something whether or not the edge routes to it.
	for id := range bundled.ShadowIDs() {
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

// TestSupersededMemberExemptsOnlyThatMember is what makes the narrow form
// worth having. A pack that answers a member its response shape does not
// declare diverges on that member and matches on every other, and the
// whole-step exemption drops the body entirely to excuse it -- so a bundle
// that also got the status wrong would pass.
//
// Three recordings had reached that point between them, and the members they
// were excusing were ids the caller had already supplied while the members
// being thrown away were the ones that said what the service did.
func TestSupersededMemberExemptsOnlyThatMember(t *testing.T) {
	acct := spi.Identity{Account: "000000000000", Region: "us-east-1"}
	step := equivalence.Step{
		Operation: "Get", Input: map[string]any{}, Identity: acct,
		SupersededMembers: map[string]string{
			"Id": "the response shape does not declare it",
		},
	}
	trace := &equivalence.Trace{
		Steps: []equivalence.Step{step},
		Outcomes: []equivalence.Outcome{
			{Output: map[string]any{"Id": "aaaaaaaa", "Status": "ENABLED"}},
		},
	}
	// The candidate drops Id, which is exempt, and gets Status wrong, which
	// is not.
	diffs, err := equivalence.Replay(context.Background(), &wrongStatus{}, trace)
	if err != nil {
		t.Fatal(err)
	}
	if len(diffs) != 1 {
		t.Fatalf("%d diffs, want exactly the Status one: %v", len(diffs), diffs)
	}
	if diffs[0].Path != "Status" {
		t.Errorf("diff on %q, want Status: the exemption covered the wrong member",
			diffs[0].Path)
	}
}

// TestSupersededMemberThatExcusesNothingIsReported keeps the exemption from
// rotting into a lie. A member that is renamed, or a bundle that is fixed so
// it no longer diverges, leaves an exemption behind that reads as a documented
// divergence while the step is in fact clean -- and every later reader of the
// recording believes it.
func TestSupersededMemberThatExcusesNothingIsReported(t *testing.T) {
	acct := spi.Identity{Account: "000000000000", Region: "us-east-1"}
	trace := &equivalence.Trace{
		Steps: []equivalence.Step{{
			Operation: "Get", Input: map[string]any{}, Identity: acct,
			SupersededMembers: map[string]string{
				"Status":  "the response shape does not declare it",
				"Renamed": "stale: this member no longer exists",
			},
		}},
		Outcomes: []equivalence.Outcome{
			{Output: map[string]any{"Status": "ENABLED"}},
		},
	}
	diffs, err := equivalence.Replay(context.Background(), &wrongStatus{}, trace)
	if err != nil {
		t.Fatal(err)
	}
	if len(diffs) != 1 {
		t.Fatalf("%d diffs, want exactly the stale exemption: %v", len(diffs), diffs)
	}
	if diffs[0].Path != "Renamed" {
		t.Errorf("reported %q; Status diverges and is excused, Renamed excuses "+
			"nothing and is the one to report", diffs[0].Path)
	}
}

type wrongStatus struct{}

func (*wrongStatus) ServiceID() string    { return "aws.shield" }
func (*wrongStatus) Operations() []string { return []string{"Get"} }
func (*wrongStatus) Invoke(context.Context, *spi.Request) (*spi.Response, error) {
	return &spi.Response{Output: map[string]any{"Status": "DISABLED"}}, nil
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
