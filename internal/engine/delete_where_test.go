package engine_test

import (
	"context"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/bundled"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spitest"
)

// A revoke names no row: it describes which rows to drop. aws.sso-admin is the
// service that needed it -- an account assignment is addressed by nothing, so
// there is no key a delete could take.

const ssoID = "aws.sso-admin"

func ssoAdmin(t *testing.T) spi.BehaviorPack {
	t.Helper()
	p, err := bundled.New(ssoID, spitest.Deps(t))
	if err != nil {
		t.Fatalf("build the bundled service: %v", err)
	}
	return p
}

func sso(t *testing.T, p spi.BehaviorPack, op string, in map[string]any) *spi.Response {
	t.Helper()
	resp, err := p.Invoke(context.Background(), &spi.Request{
		ServiceID: ssoID,
		Operation: op,
		Input:     in,
		Identity:  spi.Identity{Account: "000000000000", Region: "us-east-1"},
	})
	if err != nil {
		t.Fatalf("%s: %v", op, err)
	}
	return resp
}

func assign(t *testing.T, p spi.BehaviorPack, principal, account string) {
	t.Helper()
	sso(t, p, "CreateAccountAssignment", map[string]any{
		"InstanceArn":      "arn:aws:sso:::instance/ssoins-local",
		"PermissionSetArn": "arn:aws:sso:::permissionSet/ssoins-local/ps-1",
		"PrincipalId":      principal, "PrincipalType": "USER",
		"TargetId": account, "TargetType": "AWS_ACCOUNT",
	})
}

func assignments(t *testing.T, p spi.BehaviorPack) []any {
	t.Helper()
	resp := sso(t, p, "ListAccountAssignments", map[string]any{
		"AccountId":        "000000000000",
		"InstanceArn":      "arn:aws:sso:::instance/ssoins-local",
		"PermissionSetArn": "arn:aws:sso:::permissionSet/ssoins-local/ps-1",
	})
	items, _ := resp.Output["AccountAssignments"].([]any)
	return items
}

// TestDeleteWhereRemovesEveryMatch is the effect's whole point: the predicate
// decides, and the rows it does not accept survive.
func TestDeleteWhereRemovesEveryMatch(t *testing.T) {
	p := ssoAdmin(t)
	assign(t, p, "alice", "111111111111")
	assign(t, p, "alice", "222222222222")
	assign(t, p, "bob", "111111111111")

	if got := len(assignments(t, p)); got != 3 {
		t.Fatalf("want 3 assignments before the revoke, got %d", got)
	}

	sso(t, p, "DeleteAccountAssignment", map[string]any{
		"InstanceArn":      "arn:aws:sso:::instance/ssoins-local",
		"PermissionSetArn": "arn:aws:sso:::permissionSet/ssoins-local/ps-1",
		"PrincipalId":      "alice", "PrincipalType": "USER",
		"TargetId": "111111111111", "TargetType": "AWS_ACCOUNT",
	})

	left := assignments(t, p)
	if len(left) != 1 {
		t.Fatalf("want 1 assignment after the revoke, got %d: %v", len(left), left)
	}
	if who := left[0].(map[string]any)["PrincipalId"]; who != "bob" {
		t.Fatalf("the surviving assignment is %v, want bob's", who)
	}
}

// TestDeleteWhereMatchingNothingIsNotAFault: a predicate no row satisfies
// removes nothing and succeeds, the same way a keyed delete with
// `missing: ignore` does.
func TestDeleteWhereMatchingNothingIsNotAFault(t *testing.T) {
	p := ssoAdmin(t)
	assign(t, p, "alice", "111111111111")

	sso(t, p, "DeleteAccountAssignment", map[string]any{
		"InstanceArn":      "arn:aws:sso:::instance/ssoins-local",
		"PermissionSetArn": "arn:aws:sso:::permissionSet/ssoins-local/ps-1",
		"PrincipalId":      "nobody", "PrincipalType": "USER",
		"TargetId": "111111111111", "TargetType": "AWS_ACCOUNT",
	})

	if got := len(assignments(t, p)); got != 1 {
		t.Fatalf("a predicate matching nothing removed %d rows", 1-got)
	}
}

// TestDeleteWhereLeavesItemUnbound: the candidate binding is scoped to the
// predicate. One left behind would be visible to the output projection as
// though it belonged to the operation, which is the bug the list filter had to
// avoid too.
func TestDeleteWhereLeavesItemUnbound(t *testing.T) {
	p := ssoAdmin(t)
	assign(t, p, "alice", "111111111111")

	resp := sso(t, p, "DeleteAccountAssignment", map[string]any{
		"InstanceArn":      "arn:aws:sso:::instance/ssoins-local",
		"PermissionSetArn": "arn:aws:sso:::permissionSet/ssoins-local/ps-1",
		"PrincipalId":      "alice", "PrincipalType": "USER",
		"TargetId": "111111111111", "TargetType": "AWS_ACCOUNT",
	})
	if _, leaked := resp.Output["item"]; leaked {
		t.Fatal("the candidate binding escaped into the answer")
	}
}
