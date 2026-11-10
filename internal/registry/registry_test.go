package registry

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

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

// Factories run concurrently, and nothing a caller can see may depend on it: the
// services come back in the order they were registered, and a failure is the
// first one in that order, not whichever goroutine lost the race.
func TestFactoriesRunConcurrentlyAndResultsKeepTheirOrder(t *testing.T) {
	ids := []string{"aws.order0", "aws.order1", "aws.order2", "aws.order3", "aws.order4", "aws.order5"}
	failing := map[string]bool{}
	for i, id := range ids {
		Register(Factory{ServiceID: id, Tier: model.TierEmulate, New: func(spi.Deps) (spi.BehaviorPack, error) {
			if i == 0 { // the first to be registered is the slowest to finish
				time.Sleep(20 * time.Millisecond)
			}
			if failing[id] {
				return nil, errors.New("failed " + id)
			}
			return namedStub{id}, nil
		}})
	}
	r, err := New(spitest.Deps(t), ids, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(r.Enabled(), " "); got != strings.Join(ids, " ") {
		t.Errorf("services came back as %s, registered as %s", got, strings.Join(ids, " "))
	}
	failing["aws.order1"], failing["aws.order4"] = true, true
	if _, err := New(spitest.Deps(t), ids, nil); err == nil || err.Error() != "failed aws.order1" {
		t.Errorf("error %v, want the first failure in registration order (aws.order1)", err)
	}
}

type namedStub struct{ id string }

func (s namedStub) ServiceID() string  { return s.id }
func (namedStub) Tier() model.Tier     { return model.TierEmulate }
func (namedStub) Operations() []string { return nil }
func (namedStub) Invoke(context.Context, *spi.Request) (*spi.Response, error) {
	return &spi.Response{Output: map[string]any{}}, nil
}
