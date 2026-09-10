package api

import (
	"context"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spitest"
)

func TestProjectDeploymentEnvAndKV(t *testing.T) {
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
	created := inv("CreateProject", map[string]any{"name": "docs"})
	pid := created.Output["id"].(string)
	if pid == "" || created.Output["name"] != "docs" {
		t.Fatalf("create %#v", created.Output)
	}
	got := inv("GetProject", map[string]any{"id": "docs"})
	if got.Output["id"] != pid {
		t.Fatalf("get by name %#v", got.Output)
	}
	list := inv("ListProjects", nil)
	if len(list.Output["projects"].([]any)) != 1 {
		t.Fatalf("list %#v", list.Output)
	}
	env := inv("CreateProjectEnv", map[string]any{"id": pid, "key": "FOO", "value": "bar"})
	if env.Output["key"] != "FOO" {
		t.Fatalf("env %#v", env.Output)
	}
	envs := inv("ListProjectEnv", map[string]any{"id": pid})
	if len(envs.Output["envs"].([]any)) != 1 {
		t.Fatalf("envs %#v", envs.Output)
	}
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "DeleteProjectEnv", Input: map[string]any{"id": pid, "envId": env.Output["id"]}}); err != nil {
		t.Fatal(err)
	}
	if n := len(inv("ListProjectEnv", map[string]any{"id": pid}).Output["envs"].([]any)); n != 0 {
		t.Fatalf("envs after delete %d", n)
	}
	dom := inv("AddProjectDomain", map[string]any{"id": pid, "name": "docs.example.test"})
	if dom.Output["name"] != "docs.example.test" {
		t.Fatalf("domain %#v", dom.Output)
	}
	dpl := inv("CreateDeployment", map[string]any{"name": "docs", "project": "docs"})
	if dpl.Output["readyState"] != "READY" || dpl.Output["projectId"] != pid {
		t.Fatalf("deploy %#v", dpl.Output)
	}
	gotDpl := inv("GetDeployment", map[string]any{"id": dpl.Output["id"]})
	if gotDpl.Output["id"] != dpl.Output["id"] {
		t.Fatalf("get deploy %#v", gotDpl.Output)
	}
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "DeleteDeployment", Input: map[string]any{"id": dpl.Output["id"]}}); err != nil {
		t.Fatal(err)
	}
	set := inv("KvCommand", map[string]any{"_redis": []any{"SET", "k", "v"}})
	if set.Output["result"] != "OK" {
		t.Fatalf("set %#v", set.Output)
	}
	get := inv("KvCommand", map[string]any{"_redis": []any{"GET", "k"}})
	if get.Output["result"] != "v" {
		t.Fatalf("get %#v", get.Output)
	}
	del := inv("KvCommand", map[string]any{"_redis": []any{"DEL", "k"}})
	if del.Output["result"] != 1 {
		t.Fatalf("del %#v", del.Output)
	}
	gone := inv("KvCommand", map[string]any{"_redis": []any{"GET", "k"}})
	if gone.Output["result"] != nil {
		t.Fatalf("get after del %#v", gone.Output)
	}
	missing, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "GetProject", Input: map[string]any{"id": "nope"}})
	if err == nil || missing != nil {
		t.Fatalf("missing: %#v %v", missing, err)
	}
	if f, ok := err.(*spi.Fault); !ok || f.Code != "not_found" {
		t.Fatalf("fault %#v", err)
	}
}

func TestCreateProjectRejectsEmptyAndDuplicateNames(t *testing.T) {
	p := New(spitest.Deps(t))
	ctx := context.Background()
	id := spi.Identity{Account: "000000000000", Region: "us-east-1"}
	_, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateProject", Input: map[string]any{}})
	if f, ok := err.(*spi.Fault); !ok || f.HTTPStatus != 400 {
		t.Fatalf("empty name %#v", err)
	}
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateProject", Input: map[string]any{"name": "dup"}}); err != nil {
		t.Fatal(err)
	}
	_, err = p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateProject", Input: map[string]any{"name": "dup"}})
	if f, ok := err.(*spi.Fault); !ok || f.HTTPStatus != 409 {
		t.Fatalf("duplicate %#v", err)
	}
}

func TestDeleteProjectByNameRemovesLookup(t *testing.T) {
	p := New(spitest.Deps(t))
	ctx := context.Background()
	id := spi.Identity{Account: "000000000000", Region: "us-east-1"}
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateProject", Input: map[string]any{"name": "gone"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "DeleteProject", Input: map[string]any{"id": "gone"}}); err != nil {
		t.Fatal(err)
	}
	_, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "GetProject", Input: map[string]any{"id": "gone"}})
	if f, ok := err.(*spi.Fault); !ok || f.Code != "not_found" {
		t.Fatalf("after delete %#v", err)
	}
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateProject", Input: map[string]any{"name": "gone"}}); err != nil {
		t.Fatal(err)
	}
}

func TestDeleteDeploymentMissingIsNotFound(t *testing.T) {
	p := New(spitest.Deps(t))
	_, err := p.Invoke(context.Background(), &spi.Request{Identity: spi.Identity{Account: "1", Region: "us-east-1"}, Operation: "DeleteDeployment", Input: map[string]any{"id": "dpl_missing"}})
	if f, ok := err.(*spi.Fault); !ok || f.Code != "not_found" {
		t.Fatalf("missing deploy %#v", err)
	}
}

func TestEnvDomainAndKVRejectBadInput(t *testing.T) {
	p := New(spitest.Deps(t))
	ctx := context.Background()
	id := spi.Identity{Account: "000000000000", Region: "us-east-1"}
	created, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateProject", Input: map[string]any{"name": "app"}})
	if err != nil {
		t.Fatal(err)
	}
	pid := created.Output["id"].(string)
	_, err = p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateProjectEnv", Input: map[string]any{"id": pid}})
	if f, ok := err.(*spi.Fault); !ok || f.HTTPStatus != 400 {
		t.Fatalf("empty env key %#v", err)
	}
	_, err = p.Invoke(ctx, &spi.Request{Identity: id, Operation: "AddProjectDomain", Input: map[string]any{"id": pid}})
	if f, ok := err.(*spi.Fault); !ok || f.HTTPStatus != 400 {
		t.Fatalf("empty domain %#v", err)
	}
	_, err = p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateDeployment", Input: map[string]any{}})
	if f, ok := err.(*spi.Fault); !ok || f.HTTPStatus != 400 {
		t.Fatalf("empty deploy %#v", err)
	}
	set, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "KvCommand", Input: map[string]any{"_redis": []any{"SET", "n", 3}}})
	if err != nil || set.Output["result"] != "OK" {
		t.Fatalf("numeric set %#v %v", set, err)
	}
	get, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "KvCommand", Input: map[string]any{"_redis": []any{"GET", "n"}}})
	if err != nil || get.Output["result"] != "3" {
		t.Fatalf("numeric get %#v %v", get, err)
	}
}

func TestAccountsDoNotShareProjects(t *testing.T) {
	p := New(spitest.Deps(t))
	ctx := context.Background()
	a := spi.Identity{Account: "111111111111", Region: "us-east-1"}
	b := spi.Identity{Account: "222222222222", Region: "us-east-1"}
	if _, err := p.Invoke(ctx, &spi.Request{Identity: a, Operation: "CreateProject", Input: map[string]any{"name": "shared"}}); err != nil {
		t.Fatal(err)
	}
	_, err := p.Invoke(ctx, &spi.Request{Identity: b, Operation: "GetProject", Input: map[string]any{"id": "shared"}})
	if f, ok := err.(*spi.Fault); !ok || f.Code != "not_found" {
		t.Fatalf("cross-account %#v", err)
	}
}
