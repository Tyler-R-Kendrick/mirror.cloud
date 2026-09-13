package graphql

import (
	"context"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spitest"
)

func TestProjectAndServiceLifecycle(t *testing.T) {
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
	// An operation's output is the field's value, with no field name inside it:
	// which field answered is what the codec knows from the operation, and a
	// pack that repeated it was telling a generic layer its own convention.
	created := inv("projectCreate", map[string]any{"name": "web"})
	proj := created.Output
	if proj["name"] != "web" {
		t.Fatalf("create %#v", created.Output)
	}
	pid := str(proj["id"])
	got := inv("project", map[string]any{"id": pid})
	if got.Output["name"] != "web" {
		t.Fatalf("get %#v", got.Output)
	}
	// The schema says `projects` answers a connection, so the pack builds one.
	list := inv("projects", nil)
	if len(list.Output["edges"].([]any)) != 1 {
		t.Fatalf("list %#v", list.Output)
	}
	svc := inv("serviceCreate", map[string]any{"name": "api", "projectId": pid})
	if svc.Output["name"] != "api" {
		t.Fatalf("service %#v", svc.Output)
	}
	sid := str(svc.Output["id"])
	gots := inv("service", map[string]any{"id": sid})
	if gots.Output["id"] != sid {
		t.Fatalf("get service %#v", gots.Output)
	}
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "serviceDelete", Input: map[string]any{"id": sid}}); err != nil {
		t.Fatal(err)
	}
	_, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "service", Input: map[string]any{"id": sid}})
	if f, ok := err.(*spi.Fault); !ok || f.Code != "NOT_FOUND" {
		t.Fatalf("deleted service %#v", err)
	}
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "projectDelete", Input: map[string]any{"id": pid}}); err != nil {
		t.Fatal(err)
	}
	_, err = p.Invoke(ctx, &spi.Request{Identity: id, Operation: "project", Input: map[string]any{"id": pid}})
	if f, ok := err.(*spi.Fault); !ok || f.Code != "NOT_FOUND" {
		t.Fatalf("missing project %#v", err)
	}
	_, err = p.Invoke(ctx, &spi.Request{Identity: id, Operation: "service", Input: map[string]any{"id": "missing"}})
	if f, ok := err.(*spi.Fault); !ok || f.Code != "NOT_FOUND" {
		t.Fatalf("missing service %#v", err)
	}
}

func TestProjectCreateRejectsEmptyName(t *testing.T) {
	p := New(spitest.Deps(t))
	ctx := context.Background()
	id := spi.Identity{Account: "000000000000", Region: "us-east-1"}
	_, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "projectCreate", Input: map[string]any{}})
	if f, ok := err.(*spi.Fault); !ok || f.Code != "BAD_USER_INPUT" {
		t.Fatalf("empty %#v", err)
	}
}

func TestServiceCreateRejectsEmptyName(t *testing.T) {
	p := New(spitest.Deps(t))
	ctx := context.Background()
	id := spi.Identity{Account: "000000000000", Region: "us-east-1"}
	_, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "serviceCreate", Input: map[string]any{}})
	if f, ok := err.(*spi.Fault); !ok || f.Code != "BAD_USER_INPUT" {
		t.Fatalf("empty service %#v", err)
	}
}

func TestDeleteMissingProject(t *testing.T) {
	p := New(spitest.Deps(t))
	ctx := context.Background()
	id := spi.Identity{Account: "000000000000", Region: "us-east-1"}
	_, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "projectDelete", Input: map[string]any{"id": "missing"}})
	if f, ok := err.(*spi.Fault); !ok || f.Code != "NOT_FOUND" {
		t.Fatalf("delete missing %#v", err)
	}
}

func TestDeleteMissingService(t *testing.T) {
	p := New(spitest.Deps(t))
	ctx := context.Background()
	id := spi.Identity{Account: "000000000000", Region: "us-east-1"}
	_, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "serviceDelete", Input: map[string]any{"id": "missing"}})
	if f, ok := err.(*spi.Fault); !ok || f.Code != "NOT_FOUND" {
		t.Fatalf("delete missing service %#v", err)
	}
}
