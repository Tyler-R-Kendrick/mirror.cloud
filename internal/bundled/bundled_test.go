package bundled_test

import (
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
	ids := bundled.ServiceIDs()
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
	for _, id := range bundled.ServiceIDs() {
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
