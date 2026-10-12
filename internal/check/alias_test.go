package check

import (
	"net/http/httptest"
	"regexp"
	"sort"
	"strings"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/proto/aws/httpuri"
)

var labelRE = regexp.MustCompile(`\{([^}]+?)\+?\}`)

// labelsOf lists the path labels of an address, in order.
func labelsOf(uri string) []string {
	var out []string
	for _, m := range labelRE.FindAllStringSubmatch(uri, -1) {
		out = append(out, m[1])
	}
	return out
}

// shapeOf erases label names: /x/{a} and /x/{b} are one address to a router.
func shapeOf(uri string) string { return labelRE.ReplaceAllString(uri, "{}") }

// TestAlternateAddressesAreSound holds every committed model to what an alias
// promises. An alternate address is the same operation under another path, so
// it must bind the same labels (the codec reads the input from them), must not
// be an address another operation already answers at (the router would choose
// between the two by name, which is a guess), and must route back to the
// operation that asked for it.
func TestAlternateAddressesAreSound(t *testing.T) {
	aliased := 0
	for id, svc := range loadGenerated(t) {
		// Canonical addresses are the document's business -- RPC-style services
		// bind every operation to "/" -- so only an alias is held to a unique
		// address, against everything else the service answers at.
		owner := map[string]string{}
		for _, op := range svc.Operations {
			if op.HTTP.URI != "/" {
				owner[strings.ToUpper(op.HTTP.Method)+" "+shapeOf(op.HTTP.URI)] = op.Name
			}
		}
		for _, op := range svc.Operations {
			for _, alt := range op.HTTP.AltURIs {
				key := strings.ToUpper(op.HTTP.Method) + " " + shapeOf(alt)
				if who, taken := owner[key]; taken && who != op.Name {
					t.Errorf("%s: %s and %s are both served at %s", id, who, op.Name, key)
				}
				owner[key] = op.Name
			}
		}
		for i := range svc.Operations {
			op := &svc.Operations[i]
			if len(op.HTTP.AltURIs) == 0 {
				continue
			}
			aliased++
			canonical := labelsOf(op.HTTP.URI)
			seen := map[string]bool{op.HTTP.URI: true}
			if !sort.StringsAreSorted(op.HTTP.AltURIs) {
				t.Errorf("%s %s: AltURIs are not sorted, so regenerating would reorder them", id, op.Name)
			}
			for _, alt := range op.HTTP.AltURIs {
				if !strings.HasPrefix(alt, "/") || seen[alt] {
					t.Errorf("%s %s: alternate %q is malformed or repeated", id, op.Name, alt)
				}
				seen[alt] = true
				if got := labelsOf(alt); strings.Join(got, ",") != strings.Join(canonical, ",") {
					t.Errorf("%s %s: alternate %s binds labels %v, the document's address binds %v", id, op.Name, alt, got, canonical)
				}
				path := labelRE.ReplaceAllString(alt, "value")
				r := httptest.NewRequest(op.HTTP.Method, path, nil)
				got, _, ok := httpuri.Match(svc, r)
				if !ok || got.Name != op.Name {
					t.Errorf("%s %s: %s %s routes to %v, not back to it", id, op.Name, op.HTTP.Method, alt, got)
				}
			}
		}
	}
	if aliased == 0 {
		t.Skip("no committed model carries alternate addresses")
	}
}
