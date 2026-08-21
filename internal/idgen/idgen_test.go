package idgen

import (
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/rand"
)

func TestNextDeterministic(t *testing.T) {
	a := Next(rand.New("s"))
	b := Next(rand.New("s"))
	if a != b || a == "" {
		t.Fatalf("%q %q", a, b)
	}
}
