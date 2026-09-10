package v1

import (
	"context"
	"strconv"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spitest"
)

func TestServerAndSSHKeyLifecycle(t *testing.T) {
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
	created := inv("CreateServer", map[string]any{"name": "web", "server_type": "cx22", "image": "ubuntu-24.04"})
	srv, _ := created.Output["server"].(map[string]any)
	if srv["name"] != "web" {
		t.Fatalf("create %#v", created.Output)
	}
	sid := strID(srv["id"])
	got := inv("GetServer", map[string]any{"id": sid})
	if got.Output["server"].(map[string]any)["name"] != "web" {
		t.Fatalf("get %#v", got.Output)
	}
	list := inv("ListServers", nil)
	if len(list.Output["_list"].([]any)) != 1 {
		t.Fatalf("list %#v", list.Output)
	}
	key := inv("CreateSSHKey", map[string]any{"name": "laptop", "public_key": "ssh-ed25519 AAAA"})
	if key.Output["ssh_key"].(map[string]any)["name"] != "laptop" {
		t.Fatalf("ssh %#v", key.Output)
	}
	kid := strID(key.Output["ssh_key"].(map[string]any)["id"])
	gotk := inv("GetSSHKey", map[string]any{"id": kid})
	if gotk.Output["ssh_key"].(map[string]any)["fingerprint"] != "ssh-ed25519 AAAA" {
		t.Fatalf("get ssh %#v", gotk.Output)
	}
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "DeleteServer", Input: map[string]any{"id": sid}}); err != nil {
		t.Fatal(err)
	}
	_, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "GetServer", Input: map[string]any{"id": sid}})
	if f, ok := err.(*spi.Fault); !ok || f.HTTPStatus != 404 || f.Code != "not_found" {
		t.Fatalf("missing server %#v", err)
	}
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "DeleteSSHKey", Input: map[string]any{"id": kid}}); err != nil {
		t.Fatal(err)
	}
	_, err = p.Invoke(ctx, &spi.Request{Identity: id, Operation: "GetSSHKey", Input: map[string]any{"id": kid}})
	if f, ok := err.(*spi.Fault); !ok || f.HTTPStatus != 404 || f.Code != "not_found" {
		t.Fatalf("missing ssh after delete %#v", err)
	}
}

func TestDeleteMissingServerAndSSHKey(t *testing.T) {
	p := New(spitest.Deps(t))
	ctx := context.Background()
	id := spi.Identity{Account: "000000000000", Region: "us-east-1"}
	_, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "DeleteServer", Input: map[string]any{"id": "missing"}})
	if f, ok := err.(*spi.Fault); !ok || f.HTTPStatus != 404 || f.Code != "not_found" {
		t.Fatalf("delete missing server %#v", err)
	}
	_, err = p.Invoke(ctx, &spi.Request{Identity: id, Operation: "DeleteSSHKey", Input: map[string]any{"id": "missing"}})
	if f, ok := err.(*spi.Fault); !ok || f.HTTPStatus != 404 || f.Code != "not_found" {
		t.Fatalf("delete missing ssh %#v", err)
	}
}

func TestCreateServerRejectsEmptyAndDuplicate(t *testing.T) {
	p := New(spitest.Deps(t))
	ctx := context.Background()
	id := spi.Identity{Account: "000000000000", Region: "us-east-1"}
	_, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateServer", Input: map[string]any{}})
	if f, ok := err.(*spi.Fault); !ok || f.HTTPStatus != 400 || f.Code != "invalid_input" {
		t.Fatalf("empty %#v", err)
	}
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateServer", Input: map[string]any{"name": "dup"}}); err != nil {
		t.Fatal(err)
	}
	_, err = p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateServer", Input: map[string]any{"name": "dup"}})
	if f, ok := err.(*spi.Fault); !ok || f.HTTPStatus != 409 || f.Code != "uniqueness_error" {
		t.Fatalf("duplicate %#v", err)
	}
	_, err = p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateSSHKey", Input: map[string]any{"name": "k"}})
	if f, ok := err.(*spi.Fault); !ok || f.HTTPStatus != 400 {
		t.Fatalf("empty key %#v", err)
	}
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateSSHKey", Input: map[string]any{"name": "k", "public_key": "ssh-ed25519 AAAA"}}); err != nil {
		t.Fatal(err)
	}
	_, err = p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateSSHKey", Input: map[string]any{"name": "k2", "public_key": "ssh-ed25519 AAAA"}})
	if f, ok := err.(*spi.Fault); !ok || f.HTTPStatus != 409 || f.Code != "uniqueness_error" {
		t.Fatalf("dup fingerprint %#v", err)
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
