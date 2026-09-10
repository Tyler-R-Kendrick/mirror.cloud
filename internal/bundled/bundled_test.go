package bundled_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/bundled"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spitest"
)

// TestEveryBundleBuilds fails the build rather than the first request when a
// bundle has no generated model, disagrees with the one it has, or is
// otherwise unservable. A data-defined service that cannot start is a CI
// failure by construction, which is the whole reason bundles are validated
// against models instead of interpreted hopefully.
func TestEveryBundleBuilds(t *testing.T) {
	ids := allBundles()
	if len(ids) == 0 {
		t.Fatal("no bundles; behavior/ should hold at least the extracted services")
	}
	for _, id := range ids {
		t.Run(id, func(t *testing.T) {
			pack, err := bundled.New(id, spitest.Deps(t))
			if err != nil {
				t.Fatalf("build: %v", err)
			}
			if pack.ServiceID() != id {
				t.Errorf("bundle reports %q", pack.ServiceID())
			}
			if len(pack.Operations()) == 0 {
				t.Error("serves no operations")
			}
		})
	}
}

// TestBundlesAreRegistered checks the init side: a bundle that loads but never
// reaches the registry would be dead data, and the service it replaced would
// simply be gone.
func TestBundlesAreRegistered(t *testing.T) {
	registered := map[string]int{}
	for _, f := range registry.Factories() {
		registered[f.ServiceID]++
	}
	shadow := bundled.ShadowIDs()
	for _, id := range allBundles() {
		if reason, isShadow := shadow[id]; isShadow {
			// A shadow bundle is gated but not serving, so the pack it
			// shadows is the one in the registry.
			if strings.TrimSpace(reason) == "" {
				t.Errorf("%s is shadowed with no reason; a shadow bundle that "+
					"does not say what is missing is how a half-migration becomes permanent", id)
			}
			continue
		}
		switch registered[id] {
		case 0:
			t.Errorf("%s has a bundle but is not registered", id)
		case 1:
		default:
			t.Errorf("%s is registered %d times; a service is served by a pack "+
				"or by its bundle, not both", id, registered[id])
		}
	}
}

// allBundles is every bundle under behavior/, serving or shadowed. Both kinds
// must load and build; only the serving ones reach the registry.
func allBundles() []string {
	seen := map[string]bool{}
	var out []string
	for _, id := range bundled.ServiceIDs() {
		seen[id] = true
		out = append(out, id)
	}
	for id := range bundled.ShadowIDs() {
		if !seen[id] {
			out = append(out, id)
		}
	}
	return out
}

// TestShadowBundlesAreStillGated is the rule that keeps shadow honest: a
// bundle that is not serving must still be replayed against the recording of
// the pack that is. Otherwise "proven but not serving" decays into "written
// and unchecked".
func TestShadowBundlesAreStillGated(t *testing.T) {
	for id := range bundled.ShadowIDs() {
		path := filepath.Join("..", "equivalence", "traces", id+".json")
		if _, err := os.Stat(path); err != nil {
			t.Errorf("%s is shadowed but has no recording at %s", id, path)
		}
	}
}
