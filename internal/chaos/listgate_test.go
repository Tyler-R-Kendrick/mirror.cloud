package chaos

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

// listGate makes concurrent readers of one collection work from the same
// snapshot, on purpose, instead of hoping the scheduler lines them up.
//
// A check-then-act race has one precondition: every contender has read before
// any has written. Racing goroutines against each other reaches that only by
// chance -- a few percent of rounds for the DynamoDB TTL scan -- so a test
// built that way catches a missing recheck most of the time and a mutation
// suite built on it fails one run in ten on unchanged code. The gate holds each
// caller that lists the collection until `scans` of them have been handed their
// listing, which is the interleaving, forced every time and with no sleep.
//
// It decorates the store the code under test is given, so nothing in production
// changes and no test-only hook exists there.
type listGate struct {
	t          testing.TB
	collection string
	scans      int32
	listed     atomic.Int32
	once       sync.Once
	open       chan struct{}
}

func newListGate(t testing.TB, collection string, scans int) *listGate {
	return &listGate{t: t, collection: collection, scans: int32(scans), open: make(chan struct{})}
}

// wrap returns deps whose Store gates the listings of g's collection. Setup
// should use the original deps, so only the contenders wait.
func (g *listGate) wrap(deps spi.Deps) spi.Deps {
	deps.Store = gatedStore{Store: deps.Store, gate: g}
	return deps
}

// arrive blocks the caller until every expected scan has its listing. A scan
// that never comes (the code stopped listing this collection) is reported as a
// failure after a bound, not left to hang until the package timeout.
func (g *listGate) arrive() {
	if g.listed.Add(1) >= g.scans {
		g.once.Do(func() { close(g.open) })
	}
	select {
	case <-g.open:
	case <-time.After(10 * time.Second):
		g.t.Errorf("fewer than %d scans listed %q: the test no longer reaches the interleaving it exists to force", g.scans, g.collection)
	}
}

type gatedStore struct {
	spi.Store
	gate *listGate
}

func (s gatedStore) Scope(account, region string) spi.Scope {
	return gatedScope{Scope: s.Store.Scope(account, region), gate: s.gate}
}

type gatedScope struct {
	spi.Scope
	gate *listGate
}

func (s gatedScope) Collection(name string) spi.Collection {
	c := s.Scope.Collection(name)
	if name != s.gate.collection {
		return c
	}
	return gatedCollection{Collection: c, gate: s.gate}
}

type gatedCollection struct {
	spi.Collection
	gate *listGate
}

// List reads first and waits after, so the caller holds a listing taken before
// any contender's write.
func (c gatedCollection) List(ctx context.Context, prefix, after string, limit int) ([]spi.KV, bool, error) {
	entries, more, err := c.Collection.List(ctx, prefix, after, limit)
	c.gate.arrive()
	return entries, more, err
}
