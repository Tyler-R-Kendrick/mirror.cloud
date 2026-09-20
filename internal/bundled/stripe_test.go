package bundled_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/bundled"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spitest"
)

// TestStripeBundleBehaves covers what the differential corpus proves against
// the oracle only through exact bodies: the traps the resources are shaped
// around. A patch must not re-prefix the resolved key (an edit that moves the
// customer row to cus_cus_... orphans it), an update must not move created,
// and the delete cascade must nullify the references without moving the rows
// it touches.
func TestStripeBundleBehaves(t *testing.T) {
	pack, err := bundled.New("stripe.api", spitest.Deps(t))
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	id := spi.Identity{Account: "000000000000", Region: "us-east-1"}
	call := func(op string, in map[string]any) (*spi.Response, error) {
		return pack.Invoke(ctx, &spi.Request{ServiceID: "stripe.api", Operation: op, Identity: id, Input: in})
	}
	ok := func(t *testing.T, op string, in map[string]any) map[string]any {
		t.Helper()
		res, err := call(op, in)
		if err != nil {
			t.Fatalf("%s: %v", op, err)
		}
		return res.Output
	}
	fault := func(t *testing.T, err error, code string, status int) {
		t.Helper()
		f, isFault := err.(*spi.Fault)
		if !isFault {
			t.Fatalf("want %s, got %v", code, err)
		}
		if f.Code != code || f.HTTPStatus != status {
			t.Fatalf("want %s/%d, got %s/%d", code, status, f.Code, f.HTTPStatus)
		}
	}

	made := ok(t, "CreateCustomer", map[string]any{"email": "a@b.c", "name": "Ann"})
	cid, _ := made["id"].(string)
	if !strings.HasPrefix(cid, "cus_") || made["created"] == nil {
		t.Fatalf("create answered %v", made)
	}

	// UpdateCustomer keeps the id and the creation instant, and the row does
	// not move: a second update still finds it by the same id.
	updated := ok(t, "UpdateCustomer", map[string]any{"id": cid, "name": "Ann v2"})
	if updated["id"] != cid || updated["name"] != "Ann v2" || fmt.Sprint(updated["created"]) != fmt.Sprint(made["created"]) {
		t.Fatalf("update answered %v", updated)
	}
	again := ok(t, "UpdateCustomer", map[string]any{"id": cid, "email": "b@c.d"})
	if again["id"] != cid || again["email"] != "b@c.d" || again["name"] != "Ann v2" {
		t.Fatalf("second update answered %v", again)
	}
	if _, err := call("GetCustomer", map[string]any{"id": cid}); err != nil {
		t.Fatalf("get after updates: %v", err)
	}
	if _, err := call("UpdateCustomer", map[string]any{"id": "cus_nope"}); err == nil {
		t.Fatal("a missing customer was updated")
	} else {
		fault(t, err, "resource_missing", 404)
	}

	// The cascade: a payment intent, a charge and a session reference the
	// customer; the delete nullifies all three and answers the stub, and the
	// touched rows keep their ids.
	prod := ok(t, "CreateProduct", map[string]any{"name": "P"})
	price := ok(t, "CreatePrice", map[string]any{"currency": "usd", "product": prod["id"], "unit_amount": 100})
	pi := ok(t, "CreatePaymentIntent", map[string]any{"amount": 100, "currency": "usd", "customer": cid})
	piID, _ := pi["id"].(string)
	confirmed := ok(t, "ConfirmPaymentIntent", map[string]any{"id": piID})
	if confirmed["status"] != "succeeded" {
		t.Fatalf("confirm answered %v", confirmed)
	}
	sess := ok(t, "CreateCheckoutSession", map[string]any{
		"mode": "payment", "customer": cid,
		"line_items": []any{map[string]any{"price": price["id"], "quantity": 1}},
	})
	sessID, _ := sess["id"].(string)
	out := ok(t, "DeleteCustomer", map[string]any{"id": cid})
	if out["deleted"] != true || out["id"] != cid {
		t.Fatalf("delete answered %v", out)
	}
	if got := ok(t, "GetPaymentIntent", map[string]any{"id": piID}); got["customer"] != nil || got["id"] != piID {
		t.Fatalf("intent after cascade %v", got)
	}
	if got := ok(t, "GetCheckoutSession", map[string]any{"id": sessID}); got["customer"] != nil || got["id"] != sessID {
		t.Fatalf("session after cascade %v", got)
	}
	listed := ok(t, "ListCharges", map[string]any{})
	charges, _ := listed["data"].([]any)
	if len(charges) != 1 {
		t.Fatalf("charges after cascade %v", listed)
	}
	if ch, _ := charges[0].(map[string]any); ch["customer"] != nil {
		t.Fatalf("charge after cascade %v", ch)
	}

	// Newest-first: the last created customer lists first.
	c2 := ok(t, "CreateCustomer", map[string]any{"email": "z@y.x"})
	first := ok(t, "ListCustomers", map[string]any{"limit": "1"})
	items, _ := first["data"].([]any)
	if len(items) != 1 || items[0].(map[string]any)["id"] != c2["id"] {
		t.Fatalf("newest-first listing %v", first)
	}
}
