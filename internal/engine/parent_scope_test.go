package engine_test

import (
	"context"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/bundled"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spitest"
)

// A parent-scoped resource lives in a collection named for its parent, so an
// operation on it reads the parent first. That combination -- read one
// resource, write another -- is the shape these tests hold still, because
// three separate engine defects lived in it and none of them announced itself:
// each one silently addressed or answered the wrong thing.

func served(t *testing.T, service string) spi.BehaviorPack {
	t.Helper()
	p, err := bundled.New(service, spitest.Deps(t))
	if err != nil {
		t.Fatalf("build the bundled service: %v", err)
	}
	return p
}

func invoke(t *testing.T, p spi.BehaviorPack, op string, in map[string]any) map[string]any {
	t.Helper()
	resp, err := p.Invoke(context.Background(), &spi.Request{
		ServiceID: p.ServiceID(), Operation: op, Input: in,
		Identity: spi.Identity{Account: "000000000000", Region: "us-east-1"},
	})
	if err != nil {
		t.Fatalf("%s: %v", op, err)
	}
	return resp.Output
}

func faults(t *testing.T, p spi.BehaviorPack, op string, in map[string]any) error {
	t.Helper()
	_, err := p.Invoke(context.Background(), &spi.Request{
		ServiceID: p.ServiceID(), Operation: op, Input: in,
		Identity: spi.Identity{Account: "000000000000", Region: "us-east-1"},
	})
	if err == nil {
		t.Fatalf("%s: expected a fault", op)
	}
	return err
}

// TestChildIsAddressedByItsOwnKey pins the rule that an effect inherits only
// the id resolved for its own resource. Reading the server binds the server's
// key; if the following write or delete took that key, every user in the
// service would be stored under its server's id -- one user per server, each
// overwriting the last, and none of them findable by name.
func TestChildIsAddressedByItsOwnKey(t *testing.T) {
	p := served(t, "aws.transfer")
	first := invoke(t, p, "CreateServer", map[string]any{})["ServerId"]
	second := invoke(t, p, "CreateServer", map[string]any{})["ServerId"]

	role := "arn:aws:iam::000000000000:role/xfer"
	invoke(t, p, "CreateUser", map[string]any{"ServerId": first, "UserName": "amy", "Role": role})
	invoke(t, p, "CreateUser", map[string]any{"ServerId": first, "UserName": "bob", "Role": role})
	// The same name on the other server is a different user, which is the
	// whole point of scoping users to their server.
	invoke(t, p, "CreateUser", map[string]any{"ServerId": second, "UserName": "amy", "Role": role})

	for _, server := range []any{first, second} {
		out := invoke(t, p, "DescribeUser", map[string]any{"ServerId": server, "UserName": "amy"})
		user, _ := out["User"].(map[string]any)
		if user["UserName"] != "amy" {
			t.Fatalf("server %v: describing amy answered %#v", server, out["User"])
		}
	}
	if n := len(users(t, p, first)); n != 2 {
		t.Errorf("first server: want 2 users, got %d", n)
	}

	// A delete is addressed the same way: it must remove the user the request
	// names, not the server the read resolved.
	invoke(t, p, "DeleteUser", map[string]any{"ServerId": first, "UserName": "amy"})
	if n := len(users(t, p, first)); n != 1 {
		t.Errorf("after deleting amy: want 1 user, got %d", n)
	}
	if n := len(users(t, p, second)); n != 1 {
		t.Errorf("other server: want 1 user, got %d", n)
	}
}

func users(t *testing.T, p spi.BehaviorPack, server any) []any {
	t.Helper()
	out := invoke(t, p, "ListUsers", map[string]any{"ServerId": server})
	items, _ := out["Users"].([]any)
	return items
}

// TestReadsRunInDependencyOrder holds the rule that a read waits for the reads
// its collection template names. aws.codecommit is the case that proves it:
// GetBranch binds `branch` and `repository`, and a branch's collection is
// named for its repository, so resolving reads in name order would expand
// "ccbr:{repository.id}" before anything had resolved the repository.
func TestReadsRunInDependencyOrder(t *testing.T) {
	p := served(t, "aws.codecommit")
	invoke(t, p, "CreateRepository", map[string]any{"repositoryName": "app"})
	invoke(t, p, "CreateBranch", map[string]any{
		"repositoryName": "app", "branchName": "main", "commitId": "0123456789abcdef",
	})

	out := invoke(t, p, "GetBranch", map[string]any{"repositoryName": "app", "branchName": "main"})
	branch, _ := out["branch"].(map[string]any)
	if branch["branchName"] != "main" || branch["commitId"] != "0123456789abcdef" {
		t.Fatalf("GetBranch answered %#v", out["branch"])
	}
	// A missing branch still has to reach the fault rather than an unresolved
	// template, which is what a name-ordered resolve would produce.
	faults(t, p, "GetBranch", map[string]any{"repositoryName": "app", "branchName": "nope"})
}

// TestNullSurvivesInsideARecord pins that a member a bundle set to null comes
// back null. CEL's null converts to a protobuf enum rather than to nil, which
// is an int32: left unconverted it reaches JSON as 0 and a comparison as the
// string NULL_VALUE, so a member deliberately left empty starts answering a
// number instead.
func TestNullSurvivesInsideARecord(t *testing.T) {
	p := served(t, "aws.transfer")
	id := invoke(t, p, "CreateServer", map[string]any{})["ServerId"]

	out := invoke(t, p, "DescribeServer", map[string]any{"ServerId": id})
	server, _ := out["Server"].(map[string]any)
	got, present := server["Protocols"]
	if !present {
		t.Fatalf("Protocols is absent from %#v", server)
	}
	if got != nil {
		t.Errorf("Protocols: want nil, got %#v (%T)", got, got)
	}
}
