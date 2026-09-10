package api

import (
	"context"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spitest"
)

func TestDomainAndDNSLifecycle(t *testing.T) {
	p := New(spitest.Deps(t))
	ctx := context.Background()
	id := spi.Identity{Account: "000000000000", Region: "us-east-1"}
	inv := func(op string, in map[string]any) *spi.Response {
		t.Helper()
		res, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: op, Input: in})
		if err != nil {
			t.Fatal(err)
		}
		return res
	}
	created := inv("CreateDomain", map[string]any{"domain": "docs.test"})
	if created.Output["domain"] != "docs.test" {
		t.Fatalf("create %#v", created.Output)
	}
	got := inv("GetDomain", map[string]any{"domain": "docs.test"})
	if got.Output["domain"] != "docs.test" {
		t.Fatalf("get %#v", got.Output)
	}
	list := inv("ListDomains", nil)
	if len(list.Output["_list"].([]any)) != 1 {
		t.Fatalf("list %#v", list.Output)
	}
	zone := []any{map[string]any{"name": "@", "type": "A", "ttl": 300, "records": []any{map[string]any{"content": "1.2.3.4"}}}}
	upd := inv("UpdateDNSRecords", map[string]any{"domain": "docs.test", "overwrite": true, "zone": zone})
	if upd.Output["message"] != "Request accepted" {
		t.Fatalf("update %#v", upd.Output)
	}
	recs := inv("GetDNSRecords", map[string]any{"domain": "docs.test"})
	if len(recs.Output["_list"].([]any)) != 1 {
		t.Fatalf("records %#v", recs.Output)
	}
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "DeleteDNSRecords", Input: map[string]any{"domain": "docs.test"}}); err != nil {
		t.Fatal(err)
	}
	empty := inv("GetDNSRecords", map[string]any{"domain": "docs.test"})
	if len(empty.Output["_list"].([]any)) != 0 {
		t.Fatalf("after delete %#v", empty.Output)
	}
	_, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "GetDomain", Input: map[string]any{"domain": "missing.test"}})
	if f, ok := err.(*spi.Fault); !ok || f.HTTPStatus != 404 || f.Code != "not_found" {
		t.Fatalf("missing %#v", err)
	}
}

func TestCreateDomainRejectsEmptyAndDuplicate(t *testing.T) {
	p := New(spitest.Deps(t))
	ctx := context.Background()
	id := spi.Identity{Account: "000000000000", Region: "us-east-1"}
	_, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateDomain", Input: map[string]any{}})
	if f, ok := err.(*spi.Fault); !ok || f.HTTPStatus != 422 {
		t.Fatalf("empty %#v", err)
	}
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateDomain", Input: map[string]any{"domain": "dup.test"}}); err != nil {
		t.Fatal(err)
	}
	_, err = p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateDomain", Input: map[string]any{"domain": "dup.test"}})
	if f, ok := err.(*spi.Fault); !ok || f.HTTPStatus != 409 {
		t.Fatalf("duplicate %#v", err)
	}
}

func TestAccountsDoNotShareDomains(t *testing.T) {
	p := New(spitest.Deps(t))
	ctx := context.Background()
	a := spi.Identity{Account: "111111111111", Region: "us-east-1"}
	b := spi.Identity{Account: "222222222222", Region: "us-east-1"}
	if _, err := p.Invoke(ctx, &spi.Request{Identity: a, Operation: "CreateDomain", Input: map[string]any{"domain": "shared.test"}}); err != nil {
		t.Fatal(err)
	}
	_, err := p.Invoke(ctx, &spi.Request{Identity: b, Operation: "GetDomain", Input: map[string]any{"domain": "shared.test"}})
	if f, ok := err.(*spi.Fault); !ok || f.Code != "not_found" {
		t.Fatalf("cross-account %#v", err)
	}
}
