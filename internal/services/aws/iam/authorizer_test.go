package iam

import (
	"context"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spitest"
)

func TestAuthorizerExplicitDeny(t *testing.T) {
	deps := spitest.Deps(t)
	p := New(deps)
	ctx := context.Background()
	id := spi.Identity{Account: "000000000000", Region: "us-east-1"}
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateRole", Input: map[string]any{"RoleName": "denied"}}); err != nil {
		t.Fatal(err)
	}
	doc := `{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Action":"s3:DeleteBucket","Resource":"*"}]}`
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "PutRolePolicy", Input: map[string]any{"RoleName": "denied", "PolicyName": "d", "PolicyDocument": doc}}); err != nil {
		t.Fatal(err)
	}
	az := NewAuthorizer(deps.Store)
	err := az.Authorize(ctx, spi.Identity{Account: id.Account, Region: id.Region, ARN: "arn:aws:sts::000000000000:assumed-role/denied/x"}, "aws.s3", "DeleteBucket", "aws.s3:DeleteBucket")
	if err == nil {
		t.Fatal("expected deny")
	}
	if f, ok := err.(*spi.Fault); !ok || f.HTTPStatus != 403 {
		t.Fatalf("%v", err)
	}
	if err := az.Authorize(ctx, spi.Identity{Account: id.Account, Region: id.Region, ARN: "arn:aws:sts::000000000000:assumed-role/denied/x"}, "aws.s3", "GetObject", "aws.s3:GetObject"); err == nil {
		t.Fatal("GetObject should implicit-deny when role has policies but no Allow")
	}
}

func TestAuthorizerAllowThenDeny(t *testing.T) {
	deps := spitest.Deps(t)
	p := New(deps)
	ctx := context.Background()
	id := spi.Identity{Account: "000000000000", Region: "us-east-1"}
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateRole", Input: map[string]any{"RoleName": "rw"}}); err != nil {
		t.Fatal(err)
	}
	doc := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:GetObject","Resource":"*"},{"Effect":"Deny","Action":"s3:DeleteBucket","Resource":"*"}]}`
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "PutRolePolicy", Input: map[string]any{"RoleName": "rw", "PolicyName": "p", "PolicyDocument": doc}}); err != nil {
		t.Fatal(err)
	}
	az := NewAuthorizer(deps.Store)
	caller := spi.Identity{Account: id.Account, Region: id.Region, ARN: "arn:aws:sts::000000000000:assumed-role/rw/x"}
	if err := az.Authorize(ctx, caller, "aws.s3", "GetObject", "*"); err != nil {
		t.Fatalf("GetObject allow: %v", err)
	}
	if err := az.Authorize(ctx, caller, "aws.s3", "PutBucketPolicy", "*"); err == nil {
		t.Fatal("PutBucketPolicy should implicit-deny")
	}
	if err := az.Authorize(ctx, caller, "aws.s3", "DeleteBucket", "*"); err == nil {
		t.Fatal("DeleteBucket should explicit-deny")
	}
	if err := az.Authorize(ctx, spi.Identity{Account: id.Account, Region: id.Region}, "aws.s3", "DeleteBucket", "*"); err != nil {
		t.Fatalf("no role still allows: %v", err)
	}
}

func TestAuthorizerUserAndGroupPolicies(t *testing.T) {
	deps := spitest.Deps(t)
	p := New(deps)
	ctx := context.Background()
	id := spi.Identity{Account: "000000000000", Region: "us-east-1"}
	invoke := func(operation string, input map[string]any) *spi.Response {
		t.Helper()
		resp, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: operation, Input: input})
		if err != nil {
			t.Fatal(err)
		}
		return resp
	}
	invoke("CreateUser", map[string]any{"UserName": "alice"})
	invoke("CreatePolicy", map[string]any{
		"PolicyName": "read", "PolicyDocument": `{"Statement":{"Effect":"Allow","Action":"s3:*Object","Resource":"*"}}`,
	})
	invoke("AttachUserPolicy", map[string]any{"UserName": "alice", "PolicyArn": "arn:aws:iam::000000000000:policy/read"})
	invoke("CreateGroup", map[string]any{"GroupName": "guardrails"})
	invoke("PutGroupPolicy", map[string]any{
		"GroupName": "guardrails", "PolicyName": "deny-delete",
		"PolicyDocument": `{"Statement":{"Effect":"Deny","Action":"s3:Delete*","Resource":"*"}}`,
	})
	invoke("AddUserToGroup", map[string]any{"GroupName": "guardrails", "UserName": "alice"})
	key := invoke("CreateAccessKey", map[string]any{"UserName": "alice"})
	ak := str(key.Output["AccessKey"].(map[string]any)["AccessKeyId"])
	caller := spi.Identity{Account: id.Account, Region: id.Region, AccessKeyID: ak, ARN: "arn:aws:iam::000000000000:user/alice"}
	az := NewAuthorizer(deps.Store)
	if err := az.Authorize(ctx, caller, "aws.s3", "GetObject", "*"); err != nil {
		t.Fatalf("GetObject allow: %v", err)
	}
	if err := az.Authorize(ctx, caller, "aws.s3", "PutBucketPolicy", "*"); err == nil {
		t.Fatal("PutBucketPolicy should implicit-deny")
	}
	if err := az.Authorize(ctx, caller, "aws.s3", "DeleteBucket", "*"); err == nil {
		t.Fatal("DeleteBucket should be denied by group policy")
	}
	invoke("UpdateAccessKey", map[string]any{"UserName": "alice", "AccessKeyId": ak, "Status": "Inactive"})
	if err := az.Authorize(ctx, caller, "aws.s3", "GetObject", "*"); err == nil {
		t.Fatal("inactive access key should be denied")
	}
}
