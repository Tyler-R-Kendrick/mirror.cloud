package api

import (
	"context"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spitest"
)

func TestNamespaceAndValueLifecycle(t *testing.T) {
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
	created := inv("CreateNamespace", map[string]any{"account_id": "acct1", "title": "docs"})
	nid := created.Output["id"].(string)
	if nid == "" || created.Output["title"] != "docs" {
		t.Fatalf("create %#v", created.Output)
	}
	got := inv("GetNamespace", map[string]any{"account_id": "acct1", "namespace_id": nid})
	if got.Output["id"] != nid {
		t.Fatalf("get %#v", got.Output)
	}
	list := inv("ListNamespaces", map[string]any{"account_id": "acct1"})
	if len(list.Output["_list"].([]any)) != 1 {
		t.Fatalf("list %#v", list.Output)
	}
	put := inv("PutValue", map[string]any{"account_id": "acct1", "namespace_id": nid, "key": "k", "value": "v"})
	if _, ok := put.Output["_null"]; !ok {
		t.Fatalf("put %#v", put.Output)
	}
	get := inv("GetValue", map[string]any{"account_id": "acct1", "namespace_id": nid, "key": "k"})
	if get.Output["_raw"] != "v" {
		t.Fatalf("get value %#v", get.Output)
	}
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "DeleteValue", Input: map[string]any{"account_id": "acct1", "namespace_id": nid, "key": "k"}}); err != nil {
		t.Fatal(err)
	}
	_, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "GetValue", Input: map[string]any{"account_id": "acct1", "namespace_id": nid, "key": "k"}})
	if f, ok := err.(*spi.Fault); !ok || f.HTTPStatus != 404 || f.Code != "10009" {
		t.Fatalf("missing value %#v", err)
	}
	_, err = p.Invoke(ctx, &spi.Request{Identity: id, Operation: "GetNamespace", Input: map[string]any{"account_id": "acct1", "namespace_id": "nope"}})
	if f, ok := err.(*spi.Fault); !ok || f.Code != "10013" {
		t.Fatalf("missing ns %#v", err)
	}
}

func TestCreateNamespaceRejectsEmptyAndDuplicateTitles(t *testing.T) {
	p := New(spitest.Deps(t))
	ctx := context.Background()
	id := spi.Identity{Account: "000000000000", Region: "us-east-1"}
	_, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateNamespace", Input: map[string]any{"account_id": "acct1"}})
	if f, ok := err.(*spi.Fault); !ok || f.HTTPStatus != 400 || f.Code != "10007" {
		t.Fatalf("empty title %#v", err)
	}
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateNamespace", Input: map[string]any{"account_id": "acct1", "title": "dup"}}); err != nil {
		t.Fatal(err)
	}
	_, err = p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateNamespace", Input: map[string]any{"account_id": "acct1", "title": "dup"}})
	if f, ok := err.(*spi.Fault); !ok || f.HTTPStatus != 400 || f.Code != "10014" {
		t.Fatalf("duplicate %#v", err)
	}
}

func TestAccountsDoNotShareNamespaces(t *testing.T) {
	p := New(spitest.Deps(t))
	ctx := context.Background()
	id := spi.Identity{Account: "000000000000", Region: "us-east-1"}
	created, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateNamespace", Input: map[string]any{"account_id": "a", "title": "shared"}})
	if err != nil {
		t.Fatal(err)
	}
	nid := created.Output["id"].(string)
	_, err = p.Invoke(ctx, &spi.Request{Identity: id, Operation: "GetNamespace", Input: map[string]any{"account_id": "b", "namespace_id": nid}})
	if f, ok := err.(*spi.Fault); !ok || f.Code != "10013" {
		t.Fatalf("cross-account %#v", err)
	}
}
