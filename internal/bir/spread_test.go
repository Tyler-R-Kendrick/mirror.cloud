package bir

import (
	"strings"
	"testing"
)

// `spread` widened from "the whole request" to three forms, and what keeps it
// safe is which three. Each of these is a form that must not be accepted, and
// the reason is the same in every case: the bound on a copy this wide is that
// the value came from a request the engine validated, and these do not.

func TestSpreadRefusesWhatDidNotComeFromTheRequest(t *testing.T) {
	for _, tc := range []struct{ name, spread, want string }{
		{
			name:   "a read binding",
			spread: "rec",
			want:   "A read binding is not a form",
		},
		{
			name:   "a bare expression",
			spread: "merge(input, {})",
			want:   "a write may spread",
		},
		{
			name:   "an element of a read binding",
			spread: "rec.Thing",
			want:   "a write may spread",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body := strings.Replace(demoBundle,
				"      - create: { resource: thing }",
				"      - create: { resource: thing, spread: \""+tc.spread+"\" }", 1)
			_, err := loadDemo(t, body)
			if err == nil {
				t.Fatalf("a write spreading %q loaded", tc.spread)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error does not say why: %v", err)
			}
		})
	}
}

// TestSpreadOfAnElementNeedsAForEach. `item` is the element a for_each is on.
// Spreading it on a write that has no for_each would resolve to whatever
// `item` happened to be bound to by an enclosing list -- or to nothing.
func TestSpreadOfAnElementNeedsAForEach(t *testing.T) {
	body := strings.Replace(demoBundle,
		"      - create: { resource: thing }",
		"      - create: { resource: thing, spread: item }", 1)
	_, err := loadDemo(t, body)
	if err == nil {
		t.Fatal("a write spreading `item` with no for_each loaded")
	}
	if !strings.Contains(err.Error(), "has no for_each") {
		t.Errorf("error does not name the reason: %v", err)
	}
}

// TestSpreadOfAnUndeclaredMemberIsRefused. A spread of a member the operation
// does not declare copies nothing and says nothing, so the record silently
// loses whatever the author thought it was storing -- which is how a bundle
// comes to keep a name and no configuration.
func TestSpreadOfAnUndeclaredMemberIsRefused(t *testing.T) {
	body := strings.Replace(demoBundle,
		"      - create: { resource: thing }",
		"      - create: { resource: thing, spread: input.Nonsense }", 1)
	_, err := loadDemo(t, body)
	if err == nil {
		t.Fatal("a write spreading a member the request does not declare loaded")
	}
	if !strings.Contains(err.Error(), "the spread copies nothing") {
		t.Errorf("error does not say what happens: %v", err)
	}
}

// TestSpreadOfADeclaredMemberLoads keeps the check from being a blanket
// refusal of the narrow form it was added for.
func TestSpreadOfADeclaredMemberLoads(t *testing.T) {
	body := strings.Replace(demoBundle,
		"      - create: { resource: thing }",
		"      - create: { resource: thing, spread: input.Name }", 1)
	if _, err := loadDemo(t, body); err != nil {
		t.Fatalf("spreading a declared member should load: %v", err)
	}
}
