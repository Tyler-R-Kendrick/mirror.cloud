package rand_test

import (
	"bytes"
	"strings"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/rand"
)

func TestDeterministicSequence(t *testing.T) {
	a := rand.New("seed")
	b := rand.New("seed")
	c := rand.New("other")
	for i := 0; i < 32; i++ {
		if a.Intn(1000) != b.Intn(1000) {
			t.Fatalf("Intn diverged at %d", i)
		}
		if a.Hex(16) != b.Hex(16) {
			t.Fatalf("Hex diverged at %d", i)
		}
		if a.UUID() != b.UUID() {
			t.Fatalf("UUID diverged at %d", i)
		}
		if !bytes.Equal(a.Bytes(7), b.Bytes(7)) {
			t.Fatalf("Bytes diverged at %d", i)
		}
	}
	if rand.New("seed").Hex(32) == c.Hex(32) {
		t.Fatal("different seeds produced the same Hex")
	}
}

func TestHexUUIDShape(t *testing.T) {
	r := rand.New("shape")
	if got := r.Hex(13); len(got) != 13 {
		t.Fatalf("Hex length %d", len(got))
	}
	if got := r.Bytes(9); len(got) != 9 {
		t.Fatalf("Bytes length %d", len(got))
	}
	u := r.UUID()
	parts := strings.Split(u, "-")
	if len(parts) != 5 || len(parts[0]) != 8 || len(parts[1]) != 4 || len(parts[2]) != 4 || len(parts[3]) != 4 || len(parts[4]) != 12 {
		t.Fatalf("uuid shape: %s", u)
	}
	if parts[2][0] != '4' {
		t.Fatalf("uuid version: %s", u)
	}
}

func TestDeriveIndependence(t *testing.T) {
	parent := rand.New("seed")
	before := rand.New("seed")
	a := parent.Derive("a")
	b := parent.Derive("b")
	if parent.Intn(1<<30) != before.Intn(1<<30) {
		t.Fatal("Derive consumed parent entropy")
	}
	seqA := a.Hex(32) + a.Hex(32)
	seqB := b.Hex(32) + b.Hex(32)
	if seqA == seqB {
		t.Fatal("Derive(a) and Derive(b) produced the same sequence")
	}
	a2 := rand.New("seed").Derive("a")
	if a2.Hex(32)+a2.Hex(32) != seqA {
		t.Fatal("Derive is not deterministic")
	}
}
