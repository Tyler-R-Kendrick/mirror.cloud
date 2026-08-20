package iam

import (
	"context"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spitest"
)

func TestRoleRoundTrip(t *testing.T) {
	p := &Pack{deps: spitest.Deps(t)}
	ctx := context.Background()
	id := spi.Identity{Account: "000000000000", Region: "us-east-1"}
	_, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateRole", Input: map[string]any{"RoleName": "r", "AssumeRolePolicyDocument": "{}"}})
	if err != nil {
		t.Fatal(err)
	}
	got, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "GetRole", Input: map[string]any{"RoleName": "r"}})
	if err != nil {
		t.Fatal(err)
	}
	role := got.Output["Role"].(map[string]any)
	if role["RoleName"] != "r" {
		t.Fatalf("%v", role)
	}
	list, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "ListRoles", Input: map[string]any{}})
	if err != nil {
		t.Fatal(err)
	}
	roles := list.Output["Roles"].([]any)
	if len(roles) != 1 {
		t.Fatalf("list %v", list.Output)
	}
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "GetRole", Input: map[string]any{"RoleName": "missing"}}); err == nil {
		t.Fatal("expected NoSuchEntity")
	}
}
