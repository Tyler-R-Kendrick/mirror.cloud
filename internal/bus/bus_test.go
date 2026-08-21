package bus_test

import (
	"context"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/bus"
)

func TestPublishSubscribeCancel(t *testing.T) {
	b := bus.New()
	ctx := context.Background()
	var got []string
	cancel := b.Subscribe("t1", func(_ context.Context, p []byte) {
		got = append(got, string(p))
	})
	other := 0
	b.Subscribe("t2", func(_ context.Context, _ []byte) { other++ })

	if err := b.Publish(ctx, "t1", []byte("one")); err != nil {
		t.Fatal(err)
	}
	if err := b.Publish(ctx, "t2", []byte("x")); err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0] != "one" {
		t.Fatalf("got %v", got)
	}
	if other != 1 {
		t.Fatalf("other topic = %d", other)
	}

	payload := []byte("mut")
	if err := b.Publish(ctx, "t1", payload); err != nil {
		t.Fatal(err)
	}
	payload[0] = 'X'
	if got[1] != "mut" {
		t.Fatalf("payload copy: %q", got[1])
	}

	cancel()
	if err := b.Publish(ctx, "t1", []byte("after")); err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("cancel leaked: %v", got)
	}
}
