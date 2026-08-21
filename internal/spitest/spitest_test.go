package spitest_test

import (
	"context"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/spitest"
)

func TestDeps(t *testing.T) {
	d := spitest.Deps(t)
	if d.Store == nil || d.Blobs == nil || d.Bus == nil || d.Clock == nil || d.Rand == nil || d.Journal == nil || d.Model == nil {
		t.Fatalf("nil dep in %+v", d)
	}
	ctx := context.Background()
	c := d.Store.Scope("1", "r").Collection("c")
	if err := c.Put(ctx, "k", []byte("v")); err != nil {
		t.Fatal(err)
	}
	got, ok, err := c.Get(ctx, "k")
	if err != nil || !ok || string(got) != "v" {
		t.Fatalf("store via Deps: ok=%v val=%q err=%v", ok, got, err)
	}
	start := d.Clock.Now()
	if err := d.Clock.Advance(0); err != nil {
		t.Fatal(err)
	}
	if !d.Clock.Now().Equal(start) {
		t.Fatal("controllable clock moved without duration")
	}
}
