package registry

import (
	"context"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spitest"
)

type stub struct{}

func (stub) ServiceID() string    { return "aws.test" }
func (stub) Tier() model.Tier     { return model.TierEmulate }
func (stub) Operations() []string { return []string{"Ping"} }
func (stub) Invoke(context.Context, *spi.Request) (*spi.Response, error) {
	return &spi.Response{Output: map[string]any{}}, nil
}

type closingStub struct {
	stub
	closed *bool
}

func (s closingStub) ServiceID() string { return "aws.close" }
func (s closingStub) Close() error {
	*s.closed = true
	return nil
}

func TestEnabledFilter(t *testing.T) {
	Register(Factory{ServiceID: "aws.test", Tier: model.TierEmulate, New: func(spi.Deps) (spi.BehaviorPack, error) {
		return stub{}, nil
	}})
	r, err := New(spitest.Deps(t), []string{"aws.test"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := r.Resolve("aws.test"); !ok {
		t.Fatal("missing")
	}
	if _, ok := r.Resolve("aws.other"); ok {
		t.Fatal("other")
	}
}

func TestCloseEnabledPacks(t *testing.T) {
	closed := false
	Register(Factory{ServiceID: "aws.close", Tier: model.TierEmulate, New: func(spi.Deps) (spi.BehaviorPack, error) {
		return closingStub{closed: &closed}, nil
	}})
	r, err := New(spitest.Deps(t), []string{"aws.close"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Close(); err != nil || !closed {
		t.Fatalf("close error %v closed %v", err, closed)
	}
}
