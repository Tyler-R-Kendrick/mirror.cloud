package iam

import (
	"context"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/bundled"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spitest"
)

func TestRoleRoundTrip(t *testing.T) {
	p := bundled.Handler("aws.iam", spitest.Deps(t))
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
	slr, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateServiceLinkedRole", Input: map[string]any{"AWSServiceName": "autoscaling.amazonaws.com"}})
	if err != nil {
		t.Fatal(err)
	}
	if slr.Output == nil {
		t.Fatalf("slr %v", slr)
	}
}

func TestAccountSummaryCounts(t *testing.T) {
	p := bundled.Handler("aws.iam", spitest.Deps(t))
	ctx := context.Background()
	id := spi.Identity{Account: "000000000000", Region: "us-east-1"}
	for _, name := range []string{"a", "b"} {
		if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateUser", Input: map[string]any{"UserName": name}}); err != nil {
			t.Fatal(err)
		}
	}
	got, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "GetAccountSummary", Input: map[string]any{}})
	if err != nil {
		t.Fatal(err)
	}
	if summary := got.Output["SummaryMap"].(map[string]any); summary["Users"] != 2 || summary["Roles"] != 0 {
		t.Fatalf("summary %v", summary)
	}
}
