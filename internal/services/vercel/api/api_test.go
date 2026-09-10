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
	dpl := inv("CreateDeployment", map[string]any{"name": "docs", "project": "docs"})
	if dpl.Output["readyState"] != "READY" || dpl.Output["projectId"] != pid {
		t.Fatalf("deploy %#v", dpl.Output)
	}
	set := inv("KvCommand", map[string]any{"_redis": []any{"SET", "k", "v"}})
	if set.Output["result"] != "OK" {
		t.Fatalf("set %#v", set.Output)
	}
	get := inv("KvCommand", map[string]any{"_redis": []any{"GET", "k"}})
	if get.Output["result"] != "v" {
		t.Fatalf("get %#v", get.Output)
	}
	missing, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "GetProject", Input: map[string]any{"id": "nope"}})
	if err == nil || missing != nil {
		t.Fatalf("missing: %#v %v", missing, err)
	}
	if f, ok := err.(*spi.Fault); !ok || f.Code != "not_found" {
		t.Fatalf("fault %#v", err)
	}
}
