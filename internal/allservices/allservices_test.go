package allservices_test

import (
	"testing"

	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/allservices"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spitest"
)

// TestEveryServiceBuilds constructs the registry from everything this package
// registers -- which is what `mirror up` does, and the only place the whole set
// meets at once.
//
// It exists because that meeting had no test. Package-level tests build one
// service at a time and pass whatever else is broken; the equivalence gate
// builds a bundle directly, bypassing the registry entirely. So a bundle that
// registered alongside the hand-written pack it was supposed to be shadowing
// broke `mirror up` outright while every suite stayed green -- a service can
// be served by a pack or by its bundle, never by both, and nothing was
// checking. Any failure to build any service is a failure to start, so this
// test is the standing check that the binary comes up at all.
func TestEveryServiceBuilds(t *testing.T) {
	r, err := registry.New(spitest.Deps(t), nil, nil)
	if err != nil {
		t.Fatalf("the registry every entrypoint builds does not build: %v", err)
	}
	if len(r.Enabled()) == 0 {
		t.Fatal("no services registered; the blank imports are not doing their job")
	}
}

// TestNoServiceIsRegisteredTwice names the specific failure above, so a
// duplicate is reported as a duplicate rather than as whatever registry.New
// happens to say first.
func TestNoServiceIsRegisteredTwice(t *testing.T) {
	seen := map[string]int{}
	for _, f := range registry.Factories() {
		seen[f.ServiceID]++
	}
	for id, n := range seen {
		if n > 1 {
			t.Errorf("%s has %d factories; a service is served either by a pack "+
				"or by its behavior bundle, not both", id, n)
		}
	}
}
