package engine_test

import (
	"strings"
	"testing"
	"testing/fstest"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/bir"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/engine"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/generated"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spitest"
)

// TestALetMayReadTheSelection is the case a bundle hits when it wants one
// value computed from the records a select gathers -- "which of the names the
// request asked for already exist" -- and then uses it in a precondition and
// again in the answer. Before, a let ran ahead of the selection and could only
// see the request, so every use restated the comparison.
//
// The let here reads the selection's binding; a second let reads the first, and
// the precondition reads the second. All three have to wait for the selection,
// and the rule has to wait with them rather than run on the first pass with a
// name that does not yet exist.
func TestALetMayReadTheSelection(t *testing.T) {
	src := `schema: bir/1
service: aws.memorydb
provenance: authored
errors:
  ClusterNotFoundFault:
    code: ClusterNotFoundFault
    http: 404
    fault: client
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
    let:
      want: "'ClusterName' in input ? [string(input.ClusterName)] : []"
      known: "page.map(c, c.Name)"
      missing: "want.filter(n, !(n in known))"
    select:
      binding: page
      resource: cluster
    require:
      - { cond: "size(missing) == 0", error: ClusterNotFoundFault, message: "no such cluster" }
    output:
      Clusters: page
      NextToken: "string(size(known)) + ':' + string(size(missing))"
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
	for _, name := range []string{"a", "b"} {
		if _, err := send(t, p, "CreateCluster", map[string]any{"ClusterName": name, "NodeType": "db.t4g.small", "ACLName": "open-access"}); err != nil {
			t.Fatalf("create %s: %v", name, err)
		}
	}
	resp, err := send(t, p, "DescribeClusters", map[string]any{"ClusterName": "a"})
	if err != nil {
		t.Fatalf("a request for a name that exists was refused: %v", err)
	}
	if got := resp.Output["NextToken"]; got != "2:0" {
		t.Fatalf("known:missing = %v, want 2:0", got)
	}
	_, err = send(t, p, "DescribeClusters", map[string]any{"ClusterName": "zzz"})
	if err == nil || !strings.Contains(err.Error(), "no such cluster") {
		t.Fatalf("a name that does not exist was answered: %v", err)
	}
}
