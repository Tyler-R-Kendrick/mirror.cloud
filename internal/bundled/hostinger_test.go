package bundled_test

import (
	"context"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/bundled"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spitest"
)

// TestHostingerBundleBehaves exercises what a bundle does, which is not what
// TestEveryBundleLoads checks.
//
// Loading validates names: every operation exists in the model, every output
// member is declared, every error reference resolves. It says nothing about
// meaning -- and `require` is the place where meaning inverts silently. A rule
// reads `{cond: rec_found, error: NotFound}` and the engine faults when the
// condition does *not* hold, so a bundle written as though `cond` were the
// trigger has every precondition backwards. That bundle validates perfectly.
// This one did, on its first commit: every operation answered a fault, and
// nothing in the suite noticed until it was asked to serve a request.
//
// So this asks. It is the smallest thing that distinguishes a bundle which
// works from one which merely parses.
func TestHostingerBundleBehaves(t *testing.T) {
	pack, err := bundled.New("hostinger.api", spitest.Deps(t))
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	id := spi.Identity{Account: "000000000000", Region: "us-east-1"}
	call := func(op string, in map[string]any) (*spi.Response, error) {
		return pack.Invoke(ctx, &spi.Request{ServiceID: "hostinger.api", Operation: op, Identity: id, Input: in})
	}
	fault := func(t *testing.T, err error, code string) {
		t.Helper()
		f, ok := err.(*spi.Fault)
		if !ok {
			t.Fatalf("want %s fault, got %v", code, err)
		}
		if f.Code != code {
			t.Fatalf("want %s, got %s: %s", code, f.Code, f.Message)
		}
	}
	list := func(t *testing.T, resp *spi.Response) []any {
		t.Helper()
		items, _ := resp.Output["_list"].([]any)
		return items
	}

	// Purchasing answers with an order, which is what the specification says
	// this operation is. See the quirk in the bundle.
	order, err := call("DomainsPurchaseNewDomainV1",
		map[string]any{"domain": "Example.COM", "item_id": "hostingercom-domain"})
	if err != nil {
		t.Fatalf("purchase: %v", err)
	}
	if order.Output["id"] == nil || order.Output["status"] != "completed" {
		t.Fatalf("purchase answered %v", order.Output)
	}

	// The domain is the key, case-folded: a second purchase of the same name
	// in different case is the same domain.
	_, err = call("DomainsPurchaseNewDomainV1", map[string]any{"domain": "example.com", "item_id": "x"})
	fault(t, err, "conflict")

	details, err := call("DomainsGetDomainDetailsV1", map[string]any{"domain": "EXAMPLE.com"})
	if err != nil {
		t.Fatalf("details: %v", err)
	}
	if details.Output["domain"] != "example.com" || details.Output["status"] != "active" {
		t.Fatalf("details answered %v", details.Output)
	}

	_, err = call("DomainsGetDomainDetailsV1", map[string]any{"domain": "absent.example"})
	fault(t, err, "not_found")

	listed, err := call("DomainsGetDomainListV1", map[string]any{})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(list(t, listed)) != 1 {
		t.Fatalf("list answered %v", listed.Output)
	}

	// A fresh domain has a zone, so this is an empty list rather than a miss.
	records, err := call("DNSGetDNSRecordsV1", map[string]any{"domain": "example.com"})
	if err != nil {
		t.Fatalf("records: %v", err)
	}
	if got := list(t, records); len(got) != 0 {
		t.Fatalf("new zone is not empty: %v", got)
	}

	// Updates append unless the caller asks to replace.
	for _, name := range []string{"@", "www"} {
		if _, err := call("DNSUpdateDNSRecordsV1", map[string]any{
			"domain": "example.com",
			"zone":   []any{map[string]any{"name": name, "type": "A"}},
		}); err != nil {
			t.Fatalf("update %s: %v", name, err)
		}
	}
	records, _ = call("DNSGetDNSRecordsV1", map[string]any{"domain": "example.com"})
	if got := list(t, records); len(got) != 2 {
		t.Fatalf("append left %v", got)
	}

	if _, err := call("DNSUpdateDNSRecordsV1", map[string]any{
		"domain": "example.com", "overwrite": true,
		"zone": []any{map[string]any{"name": "only", "type": "A"}},
	}); err != nil {
		t.Fatalf("overwrite: %v", err)
	}
	records, _ = call("DNSGetDNSRecordsV1", map[string]any{"domain": "example.com"})
	if got := list(t, records); len(got) != 1 {
		t.Fatalf("overwrite left %v", got)
	}

	// An explicit null passes the model's required-member check, so the
	// bundle's own rule is what catches it. See the quirk.
	_, err = call("DNSUpdateDNSRecordsV1", map[string]any{"domain": "example.com", "zone": nil})
	fault(t, err, "validation_error")

	// Deleting empties the zone and leaves the domain.
	if _, err := call("DNSDeleteDNSRecordsV1", map[string]any{
		"domain": "example.com", "filters": []any{},
	}); err != nil {
		t.Fatalf("delete: %v", err)
	}
	records, _ = call("DNSGetDNSRecordsV1", map[string]any{"domain": "example.com"})
	if got := list(t, records); len(got) != 0 {
		t.Fatalf("delete left %v", got)
	}
	if _, err := call("DomainsGetDomainDetailsV1", map[string]any{"domain": "example.com"}); err != nil {
		t.Fatalf("delete removed the domain: %v", err)
	}

	// Every DNS operation refuses a domain that does not exist.
	for _, op := range []string{"DNSGetDNSRecordsV1", "DNSDeleteDNSRecordsV1"} {
		_, err := call(op, map[string]any{"domain": "absent.example", "filters": []any{}})
		fault(t, err, "not_found")
	}
}
