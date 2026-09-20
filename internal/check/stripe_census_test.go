package check

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/generated"
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
		"| emulate Stripe routes served by mirror | 27 / 27 |",
	} {
		if !strings.Contains(doc, needle) {
			t.Fatalf("PARITY.md is missing the Stripe denominator row %q", needle)
		}
	}
}

// TestStripeRoutesServed wires the denominator to the serving: every route
// the inventory records must have a generated-model operation with the same
// method and URI pattern, so a served operation cannot silently stop matching
// the oracle route it claims.
func TestStripeRoutesServed(t *testing.T) {
	root := findMod(t)
	inv := loadJSON(t, filepath.Join(root, "specs", "stripe", "emulate-inventory.json"))
	routes, _ := inv["routes"].([]any)

	model, err := generated.Model("stripe.api")
	if err != nil {
		t.Fatalf("no generated model for stripe.api: %v", err)
	}
	type binding struct{ method, uri string }
	have := map[binding]string{}
	for _, op := range model.Operations {
		have[binding{op.HTTP.Method, op.HTTP.URI}] = op.Name
	}
	unmatched := []string{}
	for _, r := range routes {
		rm := r.(map[string]any)
		uri := stripePattern(rm["path"].(string))
		b := binding{strings.ToUpper(rm["method"].(string)), uri}
		if _, ok := have[b]; !ok {
			unmatched = append(unmatched, rm["method"].(string)+" "+rm["path"].(string))
		}
	}
	if len(unmatched) > 0 {
		t.Fatalf("inventory routes with no generated-model operation: %v", unmatched)
	}
	if len(model.Operations) != 27 {
		t.Fatalf("generated stripe.api operations = %d, want 27", len(model.Operations))
	}
}

// stripePattern turns an oracle :param path into the model's {param} spelling.
func stripePattern(p string) string {
	parts := strings.Split(p, "/")
	for i, part := range parts {
		if strings.HasPrefix(part, ":") {
			parts[i] = "{" + strings.TrimPrefix(part, ":") + "}"
		}
	}
	return strings.Join(parts, "/")
}
