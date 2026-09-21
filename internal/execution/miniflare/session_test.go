package miniflare_test

import (
	"context"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/execution/miniflare"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func TestDescribeUnset(t *testing.T) {
	s := &miniflare.Session{}
	id, caps, err := s.Describe(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if id.Name != "miniflare" {
		t.Fatalf("backend: got %q", id.Name)
	}
	if len(caps) != 0 {
		t.Fatalf("want empty descriptors until configured, got %d", len(caps))
	}
}

func TestCallUnsetUnavailable(t *testing.T) {
	s := &miniflare.Session{}
	_, err := s.Call(context.Background(), spi.CapabilityCall{Action: "kv.get"})
	ce, ok := err.(*spi.CapabilityError)
	if !ok || ce.Kind != spi.CapErrUnavailable {
		t.Fatalf("want CapErrUnavailable, got %T %v", err, err)
	}
	if ce.Message == "" {
		t.Fatal("want clear unavailable message")
	}
}

func TestStartUnsetNoDownload(t *testing.T) {
	s := &miniflare.Session{}
	err := s.Start(context.Background())
	ce, ok := err.(*spi.CapabilityError)
	if !ok || ce.Kind != spi.CapErrUnavailable {
		t.Fatalf("want CapErrUnavailable, got %T %v", err, err)
	}
}
