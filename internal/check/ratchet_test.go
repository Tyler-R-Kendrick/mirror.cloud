package check

import (
	"encoding/json"
	"os/exec"
	"strings"
	"testing"
)

// TestRatchetNotExceeded is the anti-drift gate: the hand-written service
// surface may shrink but never grow, and no new hand-written pack may appear.
//
// If this fails because you added behavior, the fix is not to raise the
// baseline. Behavior belongs in behavior/ as B-IR data served by the engine
// (docs/BEHAVIOR_IR.md). If it fails because you deleted a pack, run
// `go run ./cmd/ratchet -write` to record the lower numbers.
func TestRatchetNotExceeded(t *testing.T) {
	root := findMod(t)
	baseline, err := LoadBaseline(root)
	if err != nil {
		t.Fatalf("load baseline: %v", err)
	}
	current, err := Measure(root)
	if err != nil {
		t.Fatalf("measure: %v", err)
	}

	regressions, newPacks := Compare(baseline, current)
	for _, r := range regressions {
		t.Errorf("ratchet regression — %s. Hand-written service surface may only shrink; "+
			"add behavior as B-IR data under behavior/, not Go under internal/services.", r)
	}
	for _, p := range newPacks {
		t.Errorf("new hand-written pack %q. New services enter as a specs/ entry plus "+
			"behavior/ data, never as a Go pack.", p)
	}
}

// TestRatchetBaselineOnlyFalls guards the guard: it compares the committed
// ratchet.json against the version on the integration branch and fails if any
// metric was raised. Without this, the ratchet is one edit away from useless.
//
// Skips when git or the base revision is unavailable (fresh clone, shallow CI
// checkout, or the commit that introduces ratchet.json).
func TestRatchetBaselineOnlyFalls(t *testing.T) {
	root := findMod(t)
	current, err := LoadBaseline(root)
	if err != nil {
		t.Fatalf("load baseline: %v", err)
	}

	prev, ref, ok := baselineAtBase(t, root)
	if !ok {
		t.Skip("no base revision with ratchet.json available; nothing to compare")
	}

	raised, addedPacks := Compare(prev, current)
	for _, r := range raised {
		t.Errorf("ratchet.json raised %s relative to %s. The baseline may only be "+
			"lowered — raising it defeats the migration gate.", r, ref)
	}
	for _, p := range addedPacks {
		t.Errorf("ratchet.json adds pack %q to the allowlist relative to %s. "+
			"The allowlist may only lose entries.", p, ref)
	}
}

// baselineAtBase returns ratchet.json as committed at the merge-base with the
// integration branch.
func baselineAtBase(t *testing.T, root string) (Metrics, string, bool) {
	t.Helper()
	for _, ref := range []string{"origin/main", "main"} {
		base, err := git(root, "merge-base", "HEAD", ref)
		if err != nil {
			continue
		}
		base = strings.TrimSpace(base)
		if base == "" {
			continue
		}
		blob, err := git(root, "show", base+":"+ratchetFile)
		if err != nil {
			continue // not present at the base: this commit introduces it
		}
		var m Metrics
		if err := json.Unmarshal([]byte(blob), &m); err != nil {
			t.Fatalf("parse %s at %s: %v", ratchetFile, base, err)
		}
		// A metric the base did not carry is being introduced by this commit,
		// and its first value cannot be a regression: there is nothing to have
		// regressed from. Without this, adding a metric that measures an
		// existing defect is unmergeable, which would mean the only metrics
		// that can ever be added are ones that start at zero -- exactly the
		// ones not worth adding.
		var present map[string]json.RawMessage
		if err := json.Unmarshal([]byte(blob), &present); err != nil {
			t.Fatalf("parse %s at %s: %v", ratchetFile, base, err)
		}
		if _, ok := present["protocol_mismatches"]; !ok {
			m.ProtocolMismatches = -1 // sentinel: not comparable
			m.ProtocolMismatchServices = nil
		}
		return m, ref, true
	}
	return Metrics{}, "", false
}

func git(root string, args ...string) (string, error) {
	cmd := exec.Command("git", args...)
	cmd.Dir = root
	out, err := cmd.Output()
	return string(out), err
}

// TestRatchetBaselineMatchesTree keeps ratchet.json honest in the other
// direction: once the surface shrinks, the baseline must be re-recorded, so a
// stale-high baseline cannot bank headroom for future growth.
func TestRatchetBaselineMatchesTree(t *testing.T) {
	root := findMod(t)
	baseline, err := LoadBaseline(root)
	if err != nil {
		t.Fatalf("load baseline: %v", err)
	}
	current, err := Measure(root)
	if err != nil {
		t.Fatalf("measure: %v", err)
	}
	if baseline.Packs != current.Packs || baseline.CaseLabels != current.CaseLabels ||
		baseline.ServicesLOC != current.ServicesLOC || baseline.FaultSites != current.FaultSites {
		t.Errorf("ratchet.json is stale (baseline packs=%d cases=%d loc=%d faults=%d, "+
			"tree packs=%d cases=%d loc=%d faults=%d). Run: go run ./cmd/ratchet -write",
			baseline.Packs, baseline.CaseLabels, baseline.ServicesLOC, baseline.FaultSites,
			current.Packs, current.CaseLabels, current.ServicesLOC, current.FaultSites)
	}
}
