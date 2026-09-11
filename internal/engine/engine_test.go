package engine_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/tyler-r-kendrick/mirror.cloud/behavior"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/engine"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/generated"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spitest"
)

const serviceID = "aws.shield"

func newEngine(t *testing.T) (*engine.Engine, spi.Deps) {
	t.Helper()
	svc := generatedModel(t, serviceID)
	ir, err := behaviors.Load(serviceID, svc)
	if err != nil {
		t.Fatalf("load bundle: %v", err)
	}
	deps := spitest.Deps(t)
	e, err := engine.New(deps, ir, svc)
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}
	return e, deps
}

func call(t *testing.T, e *engine.Engine, op string, in map[string]any) (*spi.Response, error) {
	t.Helper()
	return e.Invoke(context.Background(), &spi.Request{
		ServiceID: serviceID,
		Operation: op,
		Input:     in,
		Identity:  spi.Identity{Account: "000000000000", Region: "us-east-1"},
	})
}

// TestServesCRUDFromData is the point of the whole exercise: a create, read,
// list and delete cycle served entirely from behavior/aws/shield/service.yaml
// with no service-specific Go anywhere in the path.
func TestServesCRUDFromData(t *testing.T) {
	e, _ := newEngine(t)

	created, err := call(t, e, "CreateProtection", map[string]any{
		"Name":        "web",
		"ResourceArn": "arn:aws:cloudfront::000000000000:distribution/d1",
	})
	if err != nil {
		t.Fatalf("CreateProtection: %v", err)
	}
	id, _ := created.Output["ProtectionId"].(string)
	if id == "" {
		t.Fatalf("no ProtectionId in %v", created.Output)
	}

	got, err := call(t, e, "DescribeProtection", map[string]any{"ProtectionId": id})
	if err != nil {
		t.Fatalf("DescribeProtection: %v", err)
	}
	prot, _ := got.Output["Protection"].(map[string]any)
	if prot["Name"] != "web" {
		t.Fatalf("round-trip lost the record: %v", got.Output)
	}
	if prot["Id"] != id {
		t.Fatalf("record Id %v does not match %s", prot["Id"], id)
	}

	listed, err := call(t, e, "ListProtections", map[string]any{})
	if err != nil {
		t.Fatalf("ListProtections: %v", err)
	}
	items, _ := listed.Output["Protections"].([]any)
	if len(items) != 1 {
		t.Fatalf("want one protection, got %v", listed.Output)
	}

	if _, err := call(t, e, "DeleteProtection", map[string]any{"ProtectionId": id}); err != nil {
		t.Fatalf("DeleteProtection: %v", err)
	}
	if _, err := call(t, e, "DescribeProtection", map[string]any{"ProtectionId": id}); err == nil {
		t.Fatal("DescribeProtection succeeded after delete")
	}
}

// TestErrorTableDrivesFaults checks that a failed precondition produces the
// row from the bundle's error table rather than a code invented at the call
// site — the fix for the same logical error being rendered 400 in some packs
// and 404 in others.
func TestErrorTableDrivesFaults(t *testing.T) {
	e, _ := newEngine(t)
	_, err := call(t, e, "DescribeProtection", map[string]any{"ProtectionId": "absent"})
	if err == nil {
		t.Fatal("describing a missing protection succeeded")
	}
	fault, ok := err.(*spi.Fault)
	if !ok {
		t.Fatalf("want a *spi.Fault, got %T: %v", err, err)
	}
	if fault.Code != "ResourceNotFoundException" || fault.HTTPStatus != 400 || fault.Fault != "client" {
		t.Fatalf("fault does not match the error table: %+v", fault)
	}
}

// TestRequiredMembersEnforced covers the validation the empty-shape catalog
// disabled: the model says CreateProtection needs a Name, so a request without
// one must be rejected before any effect runs.
func TestRequiredMembersEnforced(t *testing.T) {
	e, deps := newEngine(t)
	_, err := call(t, e, "CreateProtection", map[string]any{"ResourceArn": "arn:aws:s3:::b"})
	if err == nil {
		t.Fatal("a request missing a required member was accepted")
	}
	fault, ok := err.(*spi.Fault)
	if !ok || fault.Code != "ValidationException" {
		t.Fatalf("want ValidationException, got %v", err)
	}
	// Nothing may have been written.
	col := deps.Store.Scope("000000000000", "us-east-1").Collection("shprot")
	entries, _, err := col.List(context.Background(), "", "", 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("a rejected request still wrote %d record(s)", len(entries))
	}
}

// TestDefaultFromExpression exercises a projection that reads a binding's
// _found companion, which is how a bundle states a documented default without
// branching in Go.
func TestDefaultFromExpression(t *testing.T) {
	e, _ := newEngine(t)
	got, err := call(t, e, "GetSubscriptionState", map[string]any{})
	if err != nil {
		t.Fatalf("GetSubscriptionState: %v", err)
	}
	if got.Output["SubscriptionState"] != "INACTIVE" {
		t.Fatalf("want INACTIVE before subscribing, got %v", got.Output)
	}
	if _, err := call(t, e, "CreateSubscription", map[string]any{}); err != nil {
		t.Fatalf("CreateSubscription: %v", err)
	}
	got, err = call(t, e, "GetSubscriptionState", map[string]any{})
	if err != nil {
		t.Fatalf("GetSubscriptionState: %v", err)
	}
	if got.Output["SubscriptionState"] != "ACTIVE" {
		t.Fatalf("want ACTIVE after subscribing, got %v", got.Output)
	}
}

// TestTenantIsolation: the engine reaches the store only through the request's
// account and region scope, so isolation is structural rather than per-service.
func TestTenantIsolation(t *testing.T) {
	e, _ := newEngine(t)
	mk := func(account string) string {
		t.Helper()
		resp, err := e.Invoke(context.Background(), &spi.Request{
			ServiceID: serviceID,
			Operation: "CreateProtection",
			Input:     map[string]any{"Name": "n", "ResourceArn": "arn:aws:s3:::b"},
			Identity:  spi.Identity{Account: account, Region: "us-east-1"},
		})
		if err != nil {
			t.Fatalf("create in %s: %v", account, err)
		}
		return resp.Output["ProtectionId"].(string)
	}
	idA := mk("000000000000")
	mk("111111111111")

	resp, err := e.Invoke(context.Background(), &spi.Request{
		ServiceID: serviceID,
		Operation: "ListProtections",
		Input:     map[string]any{},
		Identity:  spi.Identity{Account: "111111111111", Region: "us-east-1"},
	})
	if err != nil {
		t.Fatal(err)
	}
	items, _ := resp.Output["Protections"].([]any)
	if len(items) != 1 {
		t.Fatalf("account 111111111111 sees %d protections; tenants are not isolated", len(items))
	}
	if got := items[0].(map[string]any)["Id"]; got == idA {
		t.Fatal("account 111111111111 sees account 000000000000's record")
	}
}

// TestDeterministicIDs: identical seeds and request sequences must produce
// identical identifiers, which is what makes a recorded run replayable.
func TestDeterministicIDs(t *testing.T) {
	run := func() string {
		e, _ := newEngine(t)
		resp, err := call(t, e, "CreateProtection", map[string]any{"Name": "n", "ResourceArn": "arn:aws:s3:::b"})
		if err != nil {
			t.Fatal(err)
		}
		return resp.Output["ProtectionId"].(string)
	}
	if a, b := run(), run(); a != b {
		t.Fatalf("identifiers are not reproducible from the seed: %s vs %s", a, b)
	}
}

// TestUnknownOperationIsDistinguishable: an operation the bundle does not
// define must return the not-implemented fault, never a plausible-looking
// success.
func TestUnknownOperationIsDistinguishable(t *testing.T) {
	e, _ := newEngine(t)
	_, err := call(t, e, "AssociateDRTRole", map[string]any{})
	fault, ok := err.(*spi.Fault)
	if !ok || fault.Code != "MirrorNotImplemented" || fault.HTTPStatus != 501 {
		t.Fatalf("want MirrorNotImplemented/501, got %v", err)
	}
}

// TestRefusesModelWithoutShapes locks in the condition that made the
// bootstrap catalog dangerous: with no shapes there is nothing to validate
// against, so the engine must refuse to start rather than serve blind.
func TestRefusesModelWithoutShapes(t *testing.T) {
	svc := generatedModel(t, serviceID)
	ir, err := behaviors.Load(serviceID, svc)
	if err != nil {
		t.Fatal(err)
	}
	bare := *svc
	bare.Shapes = map[string]model.Shape{}
	if _, err := engine.New(spitest.Deps(t), ir, &bare); err == nil {
		t.Fatal("engine started against a model with no shapes")
	}
}

func generatedModel(t *testing.T, serviceID string) *model.Service {
	t.Helper()
	svc, err := generated.Model(serviceID)
	if err != nil {
		t.Fatalf("%v\nRun: make specs-sync && make generate", err)
	}
	return svc
}

// TestConcurrentCreatesLeaveOneWinner states the guarantee the packs provided
// and the engine replacing them did not.
//
// Uniqueness in a bundle is a read plus a precondition, and the engine
// resolved the read, evaluated the rule and applied the effect as three
// separate store operations. Concurrent creates of one name each read the
// absence before any of them wrote, so two or three won. Every pack held a
// mutex across the whole of Invoke and produced exactly one.
//
// The interleaving is forced rather than raced for. Left to the scheduler the
// window between the read and the write is a few microseconds wide, and the
// unfixed engine loses it on roughly one run in four -- which is a test that
// reports a guarantee three times out of four without having checked it. The
// store below holds each reader at the index lookup until its peers arrive or
// a short timeout passes, so an engine without the lock always produces more
// than one winner and an engine with it always produces exactly one: the
// second caller cannot reach the barrier until the first has returned.
//
// hetzner.v1 rather than aws.shield, because shield has no uniqueness rule to
// race: every CreateProtection draws its own id and all of them are supposed
// to succeed. Only three bundles express uniqueness as `!x_found`, and the
// other two are covered by internal/chaos.
func TestConcurrentCreatesLeaveOneWinner(t *testing.T) {
	const id = "hetzner.v1"
	const racers = 4
	svc := generatedModel(t, id)
	ir, err := behaviors.Load(id, svc)
	if err != nil {
		t.Fatalf("load bundle: %v", err)
	}
	deps := spitest.Deps(t)
	// hzsname is the name index CreateServer reads to decide whether the name
	// is taken, and writes to claim it.
	deps.Store = &barrierStore{Store: deps.Store, on: "hzsname", want: racers, wait: 50 * time.Millisecond}
	e, err := engine.New(deps, ir, svc)
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}

	errs := make(chan error, racers)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for range racers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, err := e.Invoke(context.Background(), &spi.Request{
				ServiceID: id,
				Operation: "CreateServer",
				Input: map[string]any{
					"name": "race", "server_type": "cx22", "image": "ubuntu-24.04",
				},
				Identity: spi.Identity{Account: "000000000000", Region: "us-east-1"},
			})
			errs <- err
		}()
	}
	close(start)
	wg.Wait()
	close(errs)

	winners := 0
	for err := range errs {
		if err == nil {
			winners++
			continue
		}
		var fault *spi.Fault
		if !errors.As(err, &fault) || fault.Code != "uniqueness_error" {
			t.Fatalf("concurrent create: %v", err)
		}
	}
	if winners != 1 {
		t.Fatalf("successful creates = %d, want 1; the read, the precondition "+
			"and the effect have to be one critical section", winners)
	}
}

// barrierStore holds readers of one collection at the point where a bundle
// checks whether a name is taken, so the interleaving a uniqueness rule has to
// survive is produced on purpose instead of waited for.
//
// The wait is bounded: an engine that serializes its requests can only ever
// have one caller at the barrier, so an unbounded rendezvous would deadlock
// the very implementation the test is asserting is correct.
type barrierStore struct {
	spi.Store
	on   string
	want int
	wait time.Duration

	mu      sync.Mutex
	arrived int
	gate    chan struct{}
}

func (b *barrierStore) Scope(account, region string) spi.Scope {
	return &barrierScope{Scope: b.Store.Scope(account, region), b: b}
}

// hold blocks until `want` readers have arrived or `wait` elapses.
func (b *barrierStore) hold() {
	b.mu.Lock()
	if b.gate == nil {
		b.gate = make(chan struct{})
	}
	gate := b.gate
	b.arrived++
	full := b.arrived >= b.want
	if full {
		close(gate)
		b.gate = nil
		b.arrived = 0
	}
	b.mu.Unlock()
	if full {
		return
	}
	select {
	case <-gate:
	case <-time.After(b.wait):
	}
}

type barrierScope struct {
	spi.Scope
	b *barrierStore
}

func (s *barrierScope) Collection(name string) spi.Collection {
	c := s.Scope.Collection(name)
	if name != s.b.on {
		return c
	}
	return &barrierCollection{Collection: c, b: s.b}
}

type barrierCollection struct {
	spi.Collection
	b *barrierStore
}

func (c *barrierCollection) Get(ctx context.Context, key string) ([]byte, bool, error) {
	v, ok, err := c.Collection.Get(ctx, key)
	c.b.hold()
	return v, ok, err
}
