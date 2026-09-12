package bundled_test

import (
	"context"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/bundled"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/golden"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spitest"
)

func railwayPack(t testing.TB) spi.BehaviorPack {
	t.Helper()
	p, err := bundled.New("railway.graphql", spitest.Deps(t))
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func railwayData(t testing.TB, res *spi.Response, field string) map[string]any {
	t.Helper()
	data, _ := res.Output["data"].(map[string]any)
	rec, _ := data[field].(map[string]any)
	if rec == nil {
		t.Fatalf("output %#v", res.Output)
	}
	return rec
}

func TestRailwayProjectAndServiceLifecycle(t *testing.T) {
	p := railwayPack(t)
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
	created := inv("projectCreate", map[string]any{"name": "web"})
	proj := railwayData(t, created, "projectCreate")
	if proj["name"] != "web" {
		t.Fatalf("create %#v", created.Output)
	}
	pid, _ := proj["id"].(string)
	got := inv("project", map[string]any{"id": pid})
	if railwayData(t, got, "project")["name"] != "web" {
		t.Fatalf("get %#v", got.Output)
	}
	list := inv("projects", nil)
	data, _ := list.Output["data"].(map[string]any)
	conn, _ := data["projects"].(map[string]any)
	edges, _ := conn["edges"].([]any)
	if len(edges) != 1 {
		t.Fatalf("list %#v", list.Output)
	}
	svc := inv("serviceCreate", map[string]any{"name": "api", "projectId": pid})
	srec := railwayData(t, svc, "serviceCreate")
	if srec["name"] != "api" {
		t.Fatalf("service %#v", svc.Output)
	}
	sid, _ := srec["id"].(string)
	gots := inv("service", map[string]any{"id": sid})
	if railwayData(t, gots, "service")["id"] != sid {
		t.Fatalf("get service %#v", gots.Output)
	}
	del := inv("serviceDelete", map[string]any{"id": sid})
	if d, _ := del.Output["data"].(map[string]any); d["serviceDelete"] != true {
		t.Fatalf("delete service %#v", del.Output)
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

func TestRailwayCreateRejectsEmptyName(t *testing.T) {
	p := railwayPack(t)
	ctx := context.Background()
	id := spi.Identity{Account: "000000000000", Region: "us-east-1"}
	_, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "projectCreate", Input: map[string]any{}})
	if f, ok := err.(*spi.Fault); !ok || f.Code != "BAD_USER_INPUT" || f.HTTPStatus != 200 {
		t.Fatalf("empty %#v", err)
	}
	_, err = p.Invoke(ctx, &spi.Request{Identity: id, Operation: "serviceCreate", Input: map[string]any{}})
	if f, ok := err.(*spi.Fault); !ok || f.Code != "BAD_USER_INPUT" || f.HTTPStatus != 200 {
		t.Fatalf("empty service %#v", err)
	}
}

func TestRailwayServiceCreateValidatesProject(t *testing.T) {
	p := railwayPack(t)
	ctx := context.Background()
	id := spi.Identity{Account: "000000000000", Region: "us-east-1"}
	_, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "serviceCreate", Input: map[string]any{"name": "api", "projectId": "missing"}})
	if f, ok := err.(*spi.Fault); !ok || f.Code != "NOT_FOUND" || f.Message != "Project not found" {
		t.Fatalf("missing project %#v", err)
	}
}

func TestRailwayDeleteMissingProjectAndService(t *testing.T) {
	p := railwayPack(t)
	ctx := context.Background()
	id := spi.Identity{Account: "000000000000", Region: "us-east-1"}
	_, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "projectDelete", Input: map[string]any{"id": "missing"}})
	if f, ok := err.(*spi.Fault); !ok || f.Code != "NOT_FOUND" {
		t.Fatalf("delete missing %#v", err)
	}
	_, err = p.Invoke(ctx, &spi.Request{Identity: id, Operation: "serviceDelete", Input: map[string]any{"id": "missing"}})
	if f, ok := err.(*spi.Fault); !ok || f.Code != "NOT_FOUND" {
		t.Fatalf("delete missing service %#v", err)
	}
}

// hydrate(): GraphQL arguments arrive four ways and the first non-empty wins:
// direct, under variables, under variables.input, under a top-level input.
func TestRailwayHydratesVariablesAndInput(t *testing.T) {
	p := railwayPack(t)
	ctx := context.Background()
	id := spi.Identity{Account: "000000000000", Region: "us-east-1"}
	inv := func(op string, in map[string]any) *spi.Response {
		t.Helper()
		res, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: op, Input: in})
		if err != nil {
			t.Fatalf("%s: %v", op, err)
		}
		return res
	}
	a := inv("projectCreate", map[string]any{"variables": map[string]any{"input": map[string]any{"name": "via-vars-input"}}})
	pa := railwayData(t, a, "projectCreate")["id"]
	b := inv("projectCreate", map[string]any{"variables": map[string]any{"name": "via-vars"}})
	pb := railwayData(t, b, "projectCreate")["id"]
	c := inv("projectCreate", map[string]any{"input": map[string]any{"name": "via-input"}})
	pc := railwayData(t, c, "projectCreate")["id"]
	if railwayData(t, inv("project", map[string]any{"variables": map[string]any{"id": pa}}), "project")["name"] != "via-vars-input" {
		t.Fatalf("get via variables %#v", pa)
	}
	if railwayData(t, inv("project", map[string]any{"input": map[string]any{"id": pb}}), "project")["name"] != "via-vars" {
		t.Fatalf("get via input %#v", pb)
	}
	// A direct member beats the nested spellings.
	s := inv("serviceCreate", map[string]any{"name": "api", "variables": map[string]any{"input": map[string]any{"projectId": pc, "name": "wrong"}}})
	if railwayData(t, s, "serviceCreate")["projectId"] != pc {
		t.Fatalf("precedence %#v", s.Output)
	}
	// projectID is the fallback spelling of projectId.
	s2 := inv("serviceCreate", map[string]any{"name": "api2", "projectID": pc})
	if railwayData(t, s2, "serviceCreate")["projectId"] != pc {
		t.Fatalf("projectID fallback %#v", s2.Output)
	}
}

func TestRailwayGraphQLCharacterization(t *testing.T) {
	p := railwayPack(t)
	ctx := context.Background()
	id := spi.Identity{Account: "000000000000", Region: "us-east-1"}
	inv := func(op string, in map[string]any) any {
		t.Helper()
		res, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: op, Input: in})
		if err != nil {
			f := err.(*spi.Fault)
			return map[string]any{"error": f.Code, "status": f.HTTPStatus, "message": f.Message}
		}
		return map[string]any{"status": res.Status, "output": res.Output}
	}
	create := inv("projectCreate", map[string]any{"name": "web"})
	pid := ""
	if m, ok := create.(map[string]any); ok {
		if out, ok := m["output"].(map[string]any); ok {
			if data, ok := out["data"].(map[string]any); ok {
				if rec, ok := data["projectCreate"].(map[string]any); ok {
					pid, _ = rec["id"].(string)
				}
			}
		}
	}
	svc := inv("serviceCreate", map[string]any{"name": "api", "projectId": pid})
	sid := ""
	if m, ok := svc.(map[string]any); ok {
		if out, ok := m["output"].(map[string]any); ok {
			if data, ok := out["data"].(map[string]any); ok {
				if rec, ok := data["serviceCreate"].(map[string]any); ok {
					sid, _ = rec["id"].(string)
				}
			}
		}
	}
	golden.AssertJSON(t, map[string]any{
		"create":      create,
		"get":         inv("project", map[string]any{"id": pid}),
		"list":        inv("projects", nil),
		"empty":       inv("projectCreate", map[string]any{}),
		"service":     svc,
		"get_svc":     inv("service", map[string]any{"id": sid}),
		"del_svc":     inv("serviceDelete", map[string]any{"id": sid}),
		"miss_after":  inv("service", map[string]any{"id": sid}),
		"delete":      inv("projectDelete", map[string]any{"id": pid}),
		"missing":     inv("project", map[string]any{"id": pid}),
		"del_miss":    inv("projectDelete", map[string]any{"id": "missing"}),
		"miss_svc":    inv("service", map[string]any{"id": "missing"}),
		"del_miss_sv": inv("serviceDelete", map[string]any{"id": "missing"}),
		"svc_no_proj": inv("serviceCreate", map[string]any{"name": "api", "projectId": "missing"}),
	})
}

func FuzzRailwayCreateBody(f *testing.F) {
	f.Add("web")
	f.Add("")
	f.Add("api")
	f.Fuzz(func(t *testing.T, name string) {
		p := railwayPack(t)
		ctx := context.Background()
		id := spi.Identity{Account: "000000000000", Region: "us-east-1"}
		created, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "projectCreate", Input: map[string]any{"name": name}})
		if err == nil && created != nil {
			data, _ := created.Output["data"].(map[string]any)
			rec, _ := data["projectCreate"].(map[string]any)
			pid, _ := rec["id"].(string)
			_, _ = p.Invoke(ctx, &spi.Request{Identity: id, Operation: "serviceCreate", Input: map[string]any{"name": name, "projectId": pid}})
			_, _ = p.Invoke(ctx, &spi.Request{Identity: id, Operation: "project", Input: map[string]any{"id": pid}})
			_, _ = p.Invoke(ctx, &spi.Request{Identity: id, Operation: "projects", Input: map[string]any{}})
		}
	})
}
