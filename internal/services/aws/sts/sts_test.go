package sts

import (
	"context"
	"strings"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spitest"
)

func TestGetCallerIdentity(t *testing.T) {
	p := &Pack{deps: spitest.Deps(t)}
	resp, err := p.Invoke(context.Background(), &spi.Request{
		ServiceID: "aws.sts",
		Operation: "GetCallerIdentity",
		Input:     map[string]any{},
		Identity: spi.Identity{
			Account:     "123456789012",
			Region:      "us-east-1",
			AccessKeyID: "AKIAEXAMPLE",
			ARN:         "arn:aws:iam::123456789012:user/alice",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Output["Account"] != "123456789012" {
		t.Fatalf("account: %v", resp.Output["Account"])
	}
	if resp.Output["Arn"] != "arn:aws:iam::123456789012:user/alice" {
		t.Fatalf("arn: %v", resp.Output["Arn"])
	}
	if resp.Output["UserId"] != "AKIAEXAMPLE" {
		t.Fatalf("user: %v", resp.Output["UserId"])
	}
}

func TestAssumeRoleARNShape(t *testing.T) {
	p := &Pack{deps: spitest.Deps(t)}
	resp, err := p.Invoke(context.Background(), &spi.Request{
		ServiceID: "aws.sts",
		Operation: "AssumeRole",
		Input: map[string]any{
			"RoleArn":         "arn:aws:iam::123456789012:role/Admin",
			"RoleSessionName": "sess",
		},
		Identity: spi.Identity{Account: "123456789012", Region: "us-east-1"},
	})
	if err != nil {
		t.Fatal(err)
	}
	user, _ := resp.Output["AssumedRoleUser"].(map[string]any)
	arn, _ := user["Arn"].(string)
	want := "arn:aws:sts::123456789012:assumed-role/Admin/sess"
	if arn != want {
		t.Fatalf("arn %q want %q", arn, want)
	}
	if !strings.HasPrefix(arn, "arn:aws:sts::") || !strings.Contains(arn, ":assumed-role/") {
		t.Fatalf("arn shape: %q", arn)
	}
	creds, _ := resp.Output["Credentials"].(map[string]any)
	if creds["AccessKeyId"] == nil || creds["SecretAccessKey"] == nil || creds["SessionToken"] == nil {
		t.Fatalf("credentials: %v", creds)
	}
	if creds["Expiration"] == nil {
		t.Fatal("missing expiration")
	}
}

func TestSessionAndFederationDeterministic(t *testing.T) {
	p := New(spitest.Deps(t))
	id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
	a, err := p.Invoke(context.Background(), &spi.Request{Identity: id, Operation: "GetFederationToken", Input: map[string]any{"Name": "bob"}})
	if err != nil {
		t.Fatal(err)
	}
	b, err := p.Invoke(context.Background(), &spi.Request{Identity: id, Operation: "GetSessionToken", Input: map[string]any{}})
	if err != nil {
		t.Fatal(err)
	}
	fed := a.Output["FederatedUser"].(map[string]any)["Arn"].(string)
	if fed != "arn:aws:sts::123456789012:federated-user/bob" {
		t.Fatalf("fed arn %q", fed)
	}
	if a.Output["Credentials"].(map[string]any)["Expiration"] == nil {
		t.Fatal("fed expiration")
	}
	if b.Output["Credentials"].(map[string]any)["AccessKeyId"] == nil {
		t.Fatal("session key")
	}
}
