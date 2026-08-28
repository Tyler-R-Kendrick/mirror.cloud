package engine_test

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/behavior"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/engine"
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
	provider, service := serviceID[:3], serviceID[4:]
	path := filepath.Join("..", "generated", provider, service, "model.json.gz")
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("no generated model at %s: %v\nRun: make specs-sync && make generate", path, err)
	}
	defer f.Close()
	zr, err := gzip.NewReader(f)
	if err != nil {
		t.Fatal(err)
	}
	defer zr.Close()
	var svc model.Service
	if err := json.NewDecoder(zr).Decode(&svc); err != nil {
		t.Fatal(err)
	}
	return &svc
}
