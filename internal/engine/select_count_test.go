package engine_test

import (
	"testing"
	"testing/fstest"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/bir"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/engine"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/generated"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spitest"
)

// TestSelectCountIsTakenBeforeTheLimit is SQS's ApproximateNumberOfMessagesToMove:
// a move task answers how many messages were eligible, which a selection
// capped by MaxNumberOfMessagesPerSecond cannot say for itself. `count` binds
// the size of the filtered candidates before the limit cuts them.
func TestSelectCountIsTakenBeforeTheLimit(t *testing.T) {
	src := `schema: bir/1
service: aws.memorydb
provenance: authored
resources:
  cluster:
    collection: mdb
    id:
      input_members: [ClusterName]
    record:
      Name: id
operations:
  CreateCluster:
    effects:
      - create: { resource: cluster }
    output:
      Cluster: rec
  DescribeClusters:
    select:
      binding: page
      resource: cluster
      limit: "1"
      count: total
    output:
      Clusters: page
      NextToken: string(total)
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
	for _, name := range []string{"a", "b", "c"} {
		if _, err := send(t, p, "CreateCluster", map[string]any{"ClusterName": name, "NodeType": "db.t4g.small", "ACLName": "open-access"}); err != nil {
			t.Fatalf("create %s: %v", name, err)
		}
	}
	resp, err := send(t, p, "DescribeClusters", map[string]any{})
	if err != nil {
		t.Fatal(err)
	}
	if page, _ := resp.Output["Clusters"].([]any); len(page) != 1 {
		t.Fatalf("limit 1 took %d: %v", len(page), page)
	}
	if got := resp.Output["NextToken"]; got != "3" {
		t.Fatalf("count = %v, want 3 (the candidates before the limit)", got)
	}
}
