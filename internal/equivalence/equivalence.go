// Package equivalence proves that an engine-served bundle behaves like the
// hand-written pack it replaces.
//
// This is the gate a pack deletion must pass. It exists because the 152 packs
// are the only description of their own semantics: their tests encode what the
// implementer believed the service does, and extraction must not quietly
// change that. When probed evidence later contradicts a pack, the corpus wins
// and the expectation is updated deliberately, citing the recording — but that
// is a separate, visible decision, not an accident of translation.
//
// Comparison unifies generated identifiers. Both sides draw from the same
// deterministic Rand but consume it in different orders, so a literal diff
// would fail on every ID. Instead the first occurrence of a token-shaped
// string binds old to new, and every later occurrence must agree with that
// binding — which catches a swapped or reused identifier while tolerating a
// different draw order.
package equivalence

import (
	"context"
	"fmt"
	"regexp"
	"sort"
	"strings"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

// Step is one request in a recorded sequence.
type Step struct {
	Operation string
	Input     map[string]any
	Identity  spi.Identity
	// Superseded says the recorded output is known to be wrong and is not
	// compared. The step still runs, so the state it produces is real and
	// every later step is still gated.
	Superseded bool
	// SupersededMembers names individual output paths that are not compared,
	// leaving the rest of the step's body gated.
	//
	// Superseding a whole step to excuse one member is most of what the
	// recordings were doing: a pack that answers an ARN or an id its response
	// shape does not declare diverges on that member and matches on every
	// other, and dropping the whole body to excuse it discards the assertions
	// that mattered -- a status, a stored value, an update that moved. Three
	// recordings had reached that point, costing twelve steps of coverage
	// between them, which is more hole than the reason justified.
	SupersededMembers map[string]string
}

// Outcome is what a handler did with one step.
type Outcome struct {
	Output map[string]any
	Fault  *spi.Fault
}

// Trace is a recorded run: the steps and what the reference handler produced.
type Trace struct {
	Steps    []Step
	Outcomes []Outcome
}

// Record runs steps against a handler and captures the result of each.
func Record(ctx context.Context, h spi.Handler, steps []Step) (*Trace, error) {
	t := &Trace{}
	for _, s := range steps {
		// A step may name a value an earlier step produced, which is how a
		// recording expresses read-after-create for a generated identifier.
		s.Input, _ = resolveInputs(s.Input, t.Outcomes).(map[string]any)
		out, err := invoke(ctx, h, s)
		if err != nil {
			return nil, err
		}
		t.Steps = append(t.Steps, s)
		t.Outcomes = append(t.Outcomes, out)
	}
	return t, nil
}

func sortedPaths(m map[string]string) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// Diff is one behavioral divergence between the reference and the candidate.
type Diff struct {
	Step int
	Path string
	Want any
	Got  any
	Note string
}

func (d Diff) String() string {
	if d.Note != "" {
		return fmt.Sprintf("step %d %s: %s (want %v, got %v)", d.Step, d.Path, d.Note, d.Want, d.Got)
	}
	return fmt.Sprintf("step %d %s: want %v, got %v", d.Step, d.Path, d.Want, d.Got)
}

// Replay runs a trace's steps against a candidate handler and reports every
// divergence. An empty result means the candidate is behaviorally equivalent
// over that sequence.
func Replay(ctx context.Context, h spi.Handler, t *Trace) ([]Diff, error) {
	var diffs []Diff
	u := &unifier{bound: map[string]string{}, used: map[string]string{}}

	var answered []Outcome
	for i, step := range t.Steps {
		// Inputs that name an earlier answer are resolved against what this
		// candidate answered, not against what the reference did.
		step.Input, _ = resolveInputs(step.Input, answered).(map[string]any)
		got, err := invoke(ctx, h, step)
		if err != nil {
			return nil, err
		}
		answered = append(answered, got)
		want := t.Outcomes[i]

		switch {
		case want.Fault != nil && got.Fault == nil:
			diffs = append(diffs, Diff{Step: i, Path: "fault",
				Want: want.Fault.Code, Got: "success", Note: "expected a fault"})
		case want.Fault == nil && got.Fault != nil:
			diffs = append(diffs, Diff{Step: i, Path: "fault",
				Want: "success", Got: got.Fault.Code, Note: "unexpected fault"})
		case want.Fault != nil && got.Fault != nil:
			if want.Fault.Code != got.Fault.Code {
				diffs = append(diffs, Diff{Step: i, Path: "fault.code",
					Want: want.Fault.Code, Got: got.Fault.Code})
			}
			if want.Fault.HTTPStatus != got.Fault.HTTPStatus {
				diffs = append(diffs, Diff{Step: i, Path: "fault.status",
					Want: want.Fault.HTTPStatus, Got: got.Fault.HTTPStatus})
			}
			if want.Fault.Fault != got.Fault.Fault {
				diffs = append(diffs, Diff{Step: i, Path: "fault.class",
					Want: want.Fault.Fault, Got: got.Fault.Fault})
			}
		default:
			if step.Superseded {
				// Both succeeded, which is all this step still asserts. The
				// call ran, so the state behind the remaining steps is real.
				continue
			}
			used := map[string]bool{}
			for _, d := range u.compare(i, "", want.Output, got.Output) {
				if _, exempt := step.SupersededMembers[d.Path]; exempt {
					used[d.Path] = true
					continue
				}
				diffs = append(diffs, d)
			}
			// An exemption that excuses nothing is a hole in the reporting
			// rather than in the gate: it reads as a documented divergence
			// while the step is in fact clean, and it survives the member
			// being renamed or the bundle being fixed. Either way the
			// recording is now saying something untrue about itself, so it
			// is reported like any other divergence.
			for _, path := range sortedPaths(step.SupersededMembers) {
				if used[path] {
					continue
				}
				diffs = append(diffs, Diff{Step: i, Path: path,
					Want: "a divergence to excuse", Got: "none",
					Note: "superseded_members excuses this member and it does " +
						"not diverge; drop the exemption"})
			}
		}
	}
	return diffs, nil
}

func invoke(ctx context.Context, h spi.Handler, s Step) (Outcome, error) {
	resp, err := h.Invoke(ctx, &spi.Request{
		ServiceID: h.ServiceID(),
		Operation: s.Operation,
		Input:     s.Input,
		Identity:  s.Identity,
	})
	if err != nil {
		if f, ok := err.(*spi.Fault); ok {
			return Outcome{Fault: f}, nil
		}
		return Outcome{}, err
	}
	if resp == nil {
		return Outcome{Output: map[string]any{}}, nil
	}
	return Outcome{Output: resp.Output}, nil
}

// unifier tracks the token substitutions discovered so far, in both
// directions, so a single new value cannot stand in for two old ones.
type unifier struct {
	bound map[string]string // reference token -> candidate token
	used  map[string]string // candidate token -> reference token
}

var tokenRE = regexp.MustCompile(`^(?:[0-9a-f]{8,}|[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})$`)

// looksGenerated reports whether a string is shaped like a generated
// identifier. Anything else must match exactly.
func looksGenerated(s string) bool { return tokenRE.MatchString(strings.ToLower(s)) }

// blank reports whether a value carries no information: absent, or an empty
// map or list. A recording round-trips through JSON, where an empty output map
// is omitted and an empty list may be written as null, so these forms are not
// distinguishable on disk — and they are not distinguishable to a caller
// either, since every codec renders them the same way. Treating them as equal
// keeps the gate from failing on serialization rather than on behavior.
func blank(v any) bool {
	switch t := v.(type) {
	case nil:
		return true
	case map[string]any:
		return len(t) == 0
	case []any:
		return len(t) == 0
	}
	return false
}

func (u *unifier) compare(step int, path string, want, got any) []Diff {
	if blank(want) && blank(got) {
		return nil
	}
	switch w := want.(type) {
	case map[string]any:
		g, ok := got.(map[string]any)
		if !ok {
			return []Diff{{Step: step, Path: path, Want: want, Got: got, Note: "shape differs"}}
		}
		var diffs []Diff
		keys := map[string]bool{}
		for k := range w {
			keys[k] = true
		}
		for k := range g {
			keys[k] = true
		}
		names := make([]string, 0, len(keys))
		for k := range keys {
			names = append(names, k)
		}
		sort.Strings(names)
		for _, k := range names {
			wv, inWant := w[k]
			gv, inGot := g[k]
			switch {
			case !inGot:
				if blank(wv) {
					continue // an absent member and an empty one say the same thing
				}
				diffs = append(diffs, Diff{Step: step, Path: join(path, k), Want: wv, Got: nil, Note: "member missing"})
			case !inWant:
				if blank(gv) {
					continue
				}
				diffs = append(diffs, Diff{Step: step, Path: join(path, k), Want: nil, Got: gv, Note: "unexpected member"})
			default:
				diffs = append(diffs, u.compare(step, join(path, k), wv, gv)...)
			}
		}
		return diffs

	case []any:
		g, ok := got.([]any)
		if !ok {
			return []Diff{{Step: step, Path: path, Want: want, Got: got, Note: "shape differs"}}
		}
		if len(w) != len(g) {
			return []Diff{{Step: step, Path: path, Want: len(w), Got: len(g), Note: "length differs"}}
		}
		var diffs []Diff
		for i := range w {
			diffs = append(diffs, u.compare(step, fmt.Sprintf("%s[%d]", path, i), w[i], g[i])...)
		}
		return diffs

	case string:
		g, ok := got.(string)
		if !ok {
			return []Diff{{Step: step, Path: path, Want: want, Got: got, Note: "type differs"}}
		}
		if w == g {
			return nil
		}
		if looksGenerated(w) && looksGenerated(g) {
			return u.unify(step, path, w, g)
		}
		return []Diff{{Step: step, Path: path, Want: w, Got: g}}
	}

	if fmt.Sprint(want) != fmt.Sprint(got) {
		return []Diff{{Step: step, Path: path, Want: want, Got: got}}
	}
	return nil
}

// unify records a token substitution, rejecting one that contradicts an
// earlier binding in either direction.
func (u *unifier) unify(step int, path, want, got string) []Diff {
	if prev, ok := u.bound[want]; ok && prev != got {
		return []Diff{{Step: step, Path: path, Want: prev, Got: got,
			Note: fmt.Sprintf("identifier %s was already matched to %s", want, prev)}}
	}
	if prev, ok := u.used[got]; ok && prev != want {
		return []Diff{{Step: step, Path: path, Want: want, Got: got,
			Note: fmt.Sprintf("identifier %s is already standing in for %s", got, prev)}}
	}
	u.bound[want] = got
	u.used[got] = want
	return nil
}

func join(path, key string) string {
	if path == "" {
		return key
	}
	return path + "." + key
}
