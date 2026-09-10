package engine_test

import (
	"context"
	"testing"
	"testing/fstest"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/bir"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/bundled"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/engine"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/generated"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spitest"
)

// These cover the machinery aws.memorydb needed that aws.shield did not: a
// list that narrows to one record by name, an ARN built from the resource's
// template, and model-driven validation that is stricter than the pack the
// bundle replaced.

const memorydbID = "aws.memorydb"

func memorydb(t *testing.T) spi.BehaviorPack {
	t.Helper()
	p, err := bundled.New(memorydbID, spitest.Deps(t))
	if err != nil {
		t.Fatalf("build the bundled service: %v", err)
	}
	return p
}

func send(t *testing.T, p spi.BehaviorPack, op string, in map[string]any) (*spi.Response, error) {
	t.Helper()
	return p.Invoke(context.Background(), &spi.Request{
		ServiceID: memorydbID,
		Operation: op,
		Input:     in,
		Identity:  spi.Identity{Account: "000000000000", Region: "us-east-1"},
	})
}

func makeCluster(t *testing.T, p spi.BehaviorPack, name string) {
	t.Helper()
	_, err := send(t, p, "CreateCluster", map[string]any{
		"ClusterName": name, "NodeType": "db.t4g.small", "ACLName": "open-access",
	})
	if err != nil {
		t.Fatalf("CreateCluster %s: %v", name, err)
	}
}

func clusters(t *testing.T, p spi.BehaviorPack, in map[string]any) []any {
	t.Helper()
	resp, err := send(t, p, "DescribeClusters", in)
	if err != nil {
		t.Fatalf("DescribeClusters %v: %v", in, err)
	}
	items, _ := resp.Output["Clusters"].([]any)
	return items
}

// TestListKeyNarrowsToOne is the describe-one-or-all shape: with a name, one
// record; without, the page. It is in the engine because it was the same eight
// lines in well over a hundred hand-written packs.
func TestListKeyNarrowsToOne(t *testing.T) {
	p := memorydb(t)
	makeCluster(t, p, "c1")
	makeCluster(t, p, "c2")

	if got := clusters(t, p, map[string]any{}); len(got) != 2 {
		t.Fatalf("unnamed describe returned %d clusters, want 2", len(got))
	}

	one := clusters(t, p, map[string]any{"ClusterName": "c1"})
	if len(one) != 1 {
		t.Fatalf("named describe returned %d clusters, want 1", len(one))
	}
	if name := one[0].(map[string]any)["Name"]; name != "c1" {
		t.Fatalf("named describe returned %v", name)
	}
}

// TestListKeyAbsentIsEmptyNotAFault: naming a record that does not exist is an
// empty answer here, not an error. An operation that must fault says so with
// reads and require instead, and the difference is stated per service.
func TestListKeyAbsentIsEmptyNotAFault(t *testing.T) {
	p := memorydb(t)
	makeCluster(t, p, "c1")

	resp, err := send(t, p, "DescribeClusters", map[string]any{"ClusterName": "nope"})
	if err != nil {
		t.Fatalf("describing an absent cluster faulted: %v", err)
	}
	if items, _ := resp.Output["Clusters"].([]any); len(items) != 0 {
		t.Fatalf("want no clusters, got %v", items)
	}
}

// TestARNComesFromTheResourceTemplate: the record names `arn` and the engine
// expands the resource's template against the request identity. The 189
// hand-built ARN concatenations this replaces were each free to get the
// partition, region or separator subtly wrong.
func TestARNComesFromTheResourceTemplate(t *testing.T) {
	p := memorydb(t)
	makeCluster(t, p, "c1")

	got := clusters(t, p, map[string]any{"ClusterName": "c1"})[0].(map[string]any)["ARN"]
	const want = "arn:aws:memorydb:us-east-1:000000000000:cluster/c1"
	if got != want {
		t.Fatalf("ARN %v, want %s", got, want)
	}
}

// TestModelRequiredIsStricterThanThePack locks in a deliberate behavior
// change. The pack accepted a CreateCluster with no ACLName; the generated
// model marks it @required and the engine enforces the model, because a
// declared trait outranks a pack's authored leniency. The bundle records the
// divergence as a quirk; this test keeps it from being undone by accident.
func TestModelRequiredIsStricterThanThePack(t *testing.T) {
	p := memorydb(t)
	_, err := send(t, p, "CreateCluster", map[string]any{
		"ClusterName": "c1", "NodeType": "db.t4g.small",
	})
	if err == nil {
		t.Fatal("CreateCluster without ACLName was accepted")
	}
	fault, ok := err.(*spi.Fault)
	if !ok || fault.Code != "ValidationException" {
		t.Fatalf("want ValidationException, got %v", err)
	}
}

// TestListFilterDropsNonMatchingRecords exercises `filter`, which was declared
// in the schema and validated but never evaluated. The bundle here is built
// for the test rather than shipped: no service in behavior/ needs a filtered
// list yet, and a schema field the engine ignores is worse than one that does
// not exist.
func TestListFilterDropsNonMatchingRecords(t *testing.T) {
	const src = `
schema: bir/1
service: aws.memorydb
provenance: authored
resources:
  cluster:
    collection: mdbcl
    id:
      input_members: [ClusterName]
    record:
      Name: id
      NodeType: input.NodeType
operations:
  CreateCluster:
    effects:
      - create: { resource: cluster }
    output:
      Cluster: rec
  DescribeClusters:
    list:
      resource: cluster
      member: Clusters
      filter: "item.NodeType == 'db.t4g.small'"
`
	svc, err := generated.Model(memorydbID)
	if err != nil {
		t.Fatal(err)
	}
	ir, err := bir.Load(fstest.MapFS{"service.yaml": {Data: []byte(src)}}, ".", svc)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	p, err := engine.New(spitest.Deps(t), ir, svc)
	if err != nil {
		t.Fatal(err)
	}

	for name, node := range map[string]string{"small": "db.t4g.small", "large": "db.r6g.large"} {
		if _, err := send(t, p, "CreateCluster", map[string]any{
			"ClusterName": name, "NodeType": node, "ACLName": "open-access",
		}); err != nil {
			t.Fatalf("create %s: %v", name, err)
		}
	}

	resp, err := send(t, p, "DescribeClusters", map[string]any{})
	if err != nil {
		t.Fatal(err)
	}
	items, _ := resp.Output["Clusters"].([]any)
	if len(items) != 1 {
		t.Fatalf("filter kept %d clusters, want 1: %v", len(items), items)
	}
	if got := items[0].(map[string]any)["Name"]; got != "small" {
		t.Fatalf("filter kept %v", got)
	}
}
