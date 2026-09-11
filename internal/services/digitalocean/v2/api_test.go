package v2

import (
	"context"
	"strconv"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spitest"
)

func TestDropletAndDomainLifecycle(t *testing.T) {
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
	created := inv("CreateDroplet", map[string]any{"name": "web", "region": "nyc3", "size": "s-1vcpu-1gb"})
	drop, _ := created.Output["droplet"].(map[string]any)
	if drop["name"] != "web" {
		t.Fatalf("create %#v", created.Output)
	}
	did := strID(drop["id"])
	got := inv("GetDroplet", map[string]any{"id": did})
	if got.Output["droplet"].(map[string]any)["name"] != "web" {
		t.Fatalf("get %#v", got.Output)
	}
	list := inv("ListDroplets", nil)
	if len(list.Output["_list"].([]any)) != 1 {
		t.Fatalf("list %#v", list.Output)
	}
	dom := inv("CreateDomain", map[string]any{"name": "ex.test"})
	if dom.Output["domain"].(map[string]any)["name"] != "ex.test" {
		t.Fatalf("domain %#v", dom.Output)
	}
	gotd := inv("GetDomain", map[string]any{"name": "ex.test"})
	if gotd.Output["domain"].(map[string]any)["name"] != "ex.test" {
		t.Fatalf("get domain %#v", gotd.Output)
	}
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "DeleteDroplet", Input: map[string]any{"id": did}}); err != nil {
		t.Fatal(err)
	}
	_, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "GetDroplet", Input: map[string]any{"id": did}})
	if f, ok := err.(*spi.Fault); !ok || f.HTTPStatus != 404 || f.Code != "not_found" {
		t.Fatalf("missing droplet %#v", err)
	}
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "DeleteDomain", Input: map[string]any{"name": "ex.test"}}); err != nil {
		t.Fatal(err)
	}
	_, err = p.Invoke(ctx, &spi.Request{Identity: id, Operation: "GetDomain", Input: map[string]any{"name": "missing.test"}})
	if f, ok := err.(*spi.Fault); !ok || f.HTTPStatus != 404 || f.Code != "not_found" {
		t.Fatalf("missing domain %#v", err)
	}
}

func TestDeleteMissingDropletAndDomain(t *testing.T) {
	p := New(spitest.Deps(t))
	ctx := context.Background()
	id := spi.Identity{Account: "000000000000", Region: "us-east-1"}
	_, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "DeleteDroplet", Input: map[string]any{"id": "missing"}})
	if f, ok := err.(*spi.Fault); !ok || f.HTTPStatus != 404 || f.Code != "not_found" {
		t.Fatalf("delete missing droplet %#v", err)
	}
	_, err = p.Invoke(ctx, &spi.Request{Identity: id, Operation: "DeleteDomain", Input: map[string]any{"name": "missing.test"}})
	if f, ok := err.(*spi.Fault); !ok || f.HTTPStatus != 404 || f.Code != "not_found" {
		t.Fatalf("delete missing domain %#v", err)
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
	_, err = p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateDroplet", Input: map[string]any{}})
	if f, ok := err.(*spi.Fault); !ok || f.HTTPStatus != 422 {
		t.Fatalf("empty droplet %#v", err)
	}
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateDomain", Input: map[string]any{"name": "dup.test"}}); err != nil {
		t.Fatal(err)
	}
	_, err = p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateDomain", Input: map[string]any{"name": "dup.test"}})
	if f, ok := err.(*spi.Fault); !ok || f.HTTPStatus != 409 || f.Code != "conflict" {
		t.Fatalf("duplicate %#v", err)
	}
}

func strID(v any) string {
	switch n := v.(type) {
	case string:
		return n
	case int:
		return strconv.Itoa(n)
	case float64:
		return strconv.Itoa(int(n))
	default:
		return str(v)
	}
}
