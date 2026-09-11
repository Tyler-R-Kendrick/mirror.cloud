package bundled_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/bundled"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spitest"
)

// TestHetznerBundleBehaves is the same question TestHostingerBundleBehaves
// asks, for the same reason: loading a bundle validates names, not meaning,
// and `require` is where meaning inverts silently. A whole bundle written as
// though `cond` were the trigger rather than the precondition validates
// perfectly and faults every request.
//
// It also pins the three answers the document changed: a create is 201 with an
// action beside the server, a key delete answers nothing at all, and a name
// freed by a delete can be taken again.
func TestHetznerBundleBehaves(t *testing.T) {
	pack, err := bundled.New("hetzner.v1", spitest.Deps(t))
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	id := spi.Identity{Account: "000000000000", Region: "us-east-1"}
	call := func(op string, in map[string]any) (*spi.Response, error) {
		return pack.Invoke(ctx, &spi.Request{ServiceID: "hetzner.v1", Operation: op, Identity: id, Input: in})
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
	server := map[string]any{"name": "web-1", "server_type": "cx22", "image": "ubuntu-24.04"}

	// An empty account lists nothing rather than faulting.
	if out := ok(t, "ListServers", map[string]any{}); len(out["servers"].([]any)) != 0 {
		t.Fatalf("fresh account lists %v", out["servers"])
	}

	created := ok(t, "CreateServer", server)
	rec, _ := created["server"].(map[string]any)
	if rec["name"] != "web-1" || rec["status"] != "running" {
		t.Fatalf("create answered %v", created)
	}
	// server_type and image are objects, which is what every hcloud client
	// reads; the pack stored the caller's bare strings.
	if st, _ := rec["server_type"].(map[string]any); st["name"] != "cx22" {
		t.Fatalf("server_type = %v, want an object naming cx22", rec["server_type"])
	}
	action, _ := created["action"].(map[string]any)
	if action["command"] != "create_server" || action["status"] != "success" {
		t.Fatalf("action = %v", created["action"])
	}
	sid := fmt.Sprint(rec["id"])

	if _, err := call("CreateServer", server); err == nil {
		t.Fatal("a duplicate name was accepted")
	} else {
		fault(t, err, "uniqueness_error", 409)
	}
	// The model check runs before any rule here, and names its failure with
	// Hetzner's own code rather than the ValidationException default.
	if _, err := call("CreateServer", map[string]any{"name": "partial"}); err == nil {
		t.Fatal("a request missing server_type and image was accepted")
	} else {
		fault(t, err, "invalid_input", 400)
	}

	got := ok(t, "GetServer", map[string]any{"id": sid})
	if g, _ := got["server"].(map[string]any); g["name"] != "web-1" {
		t.Fatalf("get answered %v", got)
	}
	if _, err := call("GetServer", map[string]any{"id": "404404404"}); err == nil {
		t.Fatal("a missing server was found")
	} else {
		fault(t, err, "not_found", 404)
	}

	if out := ok(t, "DeleteServer", map[string]any{"id": sid}); out["action"] == nil {
		t.Fatal("a delete answered no action")
	}
	if _, err := call("DeleteServer", map[string]any{"id": sid}); err == nil {
		t.Fatal("deleting twice succeeded")
	} else {
		fault(t, err, "not_found", 404)
	}
	// The name index is dropped with the record, so the name is free again.
	// This is the assertion a delete that forgot the index would fail.
	ok(t, "CreateServer", server)

	key := map[string]any{"name": "laptop", "public_key": "ssh-ed25519 AAAA user@host"}
	madeKey := ok(t, "CreateSshKey", key)
	kr, _ := madeKey["ssh_key"].(map[string]any)
	if kr["name"] != "laptop" || kr["public_key"] != key["public_key"] {
		t.Fatalf("create ssh key answered %v", madeKey)
	}
	// A digest, not the key itself -- which is what the pack stored here.
	if fp, _ := kr["fingerprint"].(string); fp == key["public_key"] || len(fp) != 32 {
		t.Fatalf("fingerprint = %q, want a digest", kr["fingerprint"])
	}
	if _, err := call("CreateSshKey", map[string]any{"name": "other", "public_key": key["public_key"]}); err == nil {
		t.Fatal("a duplicate public key was accepted")
	} else {
		fault(t, err, "uniqueness_error", 409)
	}
	kid := fmt.Sprint(kr["id"])
	if out := ok(t, "DeleteSshKey", map[string]any{"id": kid}); len(out) != 0 {
		t.Fatalf("a 204 answered a body: %v", out)
	}
	if _, err := call("GetSshKey", map[string]any{"id": kid}); err == nil {
		t.Fatal("a deleted key was found")
	} else {
		fault(t, err, "not_found", 404)
	}
	// The fingerprint index went with it, so the same key can be added again.
	ok(t, "CreateSshKey", key)
	if out := ok(t, "ListSshKeys", map[string]any{}); len(out["ssh_keys"].([]any)) != 1 {
		t.Fatalf("list after re-create: %v", out["ssh_keys"])
	}
}
