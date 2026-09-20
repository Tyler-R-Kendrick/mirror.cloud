package check

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Stripe parity is scored against the vendor-authored oracle
// (vercel-labs/emulate), the same way Vercel is: unique route registrations
// from the pinned clone plus its test census. This dies if the pin's
// denominators drift or if PARITY.md drops a serve-state row.
func TestStripeCensusDenominators(t *testing.T) {
	root := findMod(t)
	inv := loadJSON(t, filepath.Join(root, "specs", "stripe", "emulate-inventory.json"))
	if got := int(inv["routeCount"].(float64)); got != 27 {
		t.Fatalf("stripe emulate-inventory routeCount = %d, want 27", got)
	}
	if got := int(inv["testCount"].(float64)); got != 23 {
		t.Fatalf("stripe emulate-inventory testCount = %d, want 23", got)
	}
	routes, ok := inv["routes"].([]any)
	if !ok || len(routes) != 27 {
		t.Fatalf("stripe emulate-inventory routes = %d, want 27", len(routes))
	}

	parity, err := os.ReadFile(filepath.Join(root, "docs", "PARITY.md"))
	if err != nil {
		t.Fatal(err)
	}
	doc := string(parity)
	for _, needle := range []string{
		"| emulate Stripe routes (vendor-authored oracle) | 27 |",
		"| emulate Stripe test functions | 23 |",
		"| emulate Stripe routes served by mirror | 0 / 27 |",
	} {
		if !strings.Contains(doc, needle) {
			t.Fatalf("PARITY.md is missing the Stripe denominator row %q", needle)
		}
	}
}
