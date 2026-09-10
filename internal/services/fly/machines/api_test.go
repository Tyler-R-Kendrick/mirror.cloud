package machines

import (
	"context"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spitest"
)

func TestAppAndMachineLifecycle(t *testing.T) {
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
	created := inv("CreateApp", map[string]any{"app_name": "web", "org_slug": "personal"})
	if created.Status != 201 {
		t.Fatalf("create status %#v", created)
	}
	got := inv("GetApp", map[string]any{"app_name": "web"})
	if got.Output["app"].(map[string]any)["name"] != "web" {
		t.Fatalf("get %#v", got.Output)
	}
	list := inv("ListApps", nil)
	if len(list.Output["_list"].([]any)) != 1 {
		t.Fatalf("list %#v", list.Output)
	}
	mach := inv("CreateMachine", map[string]any{"app_name": "web", "config": map[string]any{"image": "nginx"}})
	mid := str(mach.Output["machine"].(map[string]any)["id"])
	if mach.Output["machine"].(map[string]any)["config"].(map[string]any)["image"] != "nginx" {
		t.Fatalf("machine %#v", mach.Output)
	}
	gotm := inv("GetMachine", map[string]any{"id": mid})
	if gotm.Output["machine"].(map[string]any)["id"] != mid {
		t.Fatalf("get machine %#v", gotm.Output)
	}
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "DeleteMachine", Input: map[string]any{"id": mid}}); err != nil {
		t.Fatal(err)
	}
	_, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "GetMachine", Input: map[string]any{"id": mid}})
	if f, ok := err.(*spi.Fault); !ok || f.HTTPStatus != 404 || f.Code != "not_found" {
		t.Fatalf("missing machine %#v", err)
	}
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "DeleteApp", Input: map[string]any{"app_name": "web"}}); err != nil {
		t.Fatal(err)
	}
	_, err = p.Invoke(ctx, &spi.Request{Identity: id, Operation: "GetApp", Input: map[string]any{"app_name": "web"}})
	if f, ok := err.(*spi.Fault); !ok || f.HTTPStatus != 404 || f.Code != "not_found" {
		t.Fatalf("missing app %#v", err)
	}
}

func TestCreateAppRejectsEmptyAndDuplicate(t *testing.T) {
	p := New(spitest.Deps(t))
	ctx := context.Background()
	id := spi.Identity{Account: "000000000000", Region: "us-east-1"}
	_, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateApp", Input: map[string]any{}})
	if f, ok := err.(*spi.Fault); !ok || f.HTTPStatus != 400 || f.Code != "invalid" {
		t.Fatalf("empty %#v", err)
	}
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateApp", Input: map[string]any{"app_name": "web"}}); err != nil {
		t.Fatal(err)
	}
	_, err = p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateApp", Input: map[string]any{"app_name": "web"}})
	if f, ok := err.(*spi.Fault); !ok || f.HTTPStatus != 422 || f.Code != "taken" {
		t.Fatalf("duplicate %#v", err)
	}
}

func TestDeleteMissingAppAndMachine(t *testing.T) {
	p := New(spitest.Deps(t))
	ctx := context.Background()
	id := spi.Identity{Account: "000000000000", Region: "us-east-1"}
	_, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "DeleteApp", Input: map[string]any{"app_name": "missing"}})
	if f, ok := err.(*spi.Fault); !ok || f.HTTPStatus != 404 || f.Code != "not_found" {
		t.Fatalf("delete missing app %#v", err)
	}
	_, err = p.Invoke(ctx, &spi.Request{Identity: id, Operation: "DeleteMachine", Input: map[string]any{"id": "missing"}})
	if f, ok := err.(*spi.Fault); !ok || f.HTTPStatus != 404 || f.Code != "not_found" {
		t.Fatalf("delete missing machine %#v", err)
	}
}
