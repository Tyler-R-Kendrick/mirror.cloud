package engine_test

import (
	"fmt"
	"testing"
)

// A batch update over ids the caller supplied cannot check them one at a time,
// so before `missing: ignore` the choice was between not expressing the
// operation and conjuring a record per unknown id. EMR's TerminateJobFlows is
// the shape: a list of cluster ids, each moved to TERMINATED, and the pack it
// was taken from skipped the ones that were not there.

func clusterIDs(t *testing.T, out map[string]any) []string {
	t.Helper()
	list, _ := out["Clusters"].([]any)
	var ids []string
	for _, c := range list {
		rec, ok := c.(map[string]any)
		if !ok {
			t.Fatalf("cluster %#v is not a record", c)
		}
		ids = append(ids, fmt.Sprint(rec["Id"]))
	}
	return ids
}

func state(t *testing.T, out map[string]any, member string) string {
	t.Helper()
	rec, ok := out[member].(map[string]any)
	if !ok {
		t.Fatalf("%s is %#v, not a record", member, out[member])
	}
	status, ok := rec["Status"].(map[string]any)
	if !ok {
		t.Fatalf("Status is %#v, not a record", rec["Status"])
	}
	return fmt.Sprint(status["State"])
}

// TestAnUpdateThatIgnoresMissingWritesNothing is the property: the known id
// moves and the unknown one leaves no trace. The count matters as much as the
// state -- a phantom row is a record with a status and no name, which
// ListClusters would answer as a cluster.
func TestAnUpdateThatIgnoresMissingWritesNothing(t *testing.T) {
	p := served(t, "aws.elasticmapreduce")
	id := fmt.Sprint(invoke(t, p, "RunJobFlow", map[string]any{
		"Name": "analytics", "Instances": map[string]any{"InstanceCount": 1},
	})["JobFlowId"])

	invoke(t, p, "TerminateJobFlows", map[string]any{"JobFlowIds": []any{id, "j-absent"}})

	if got := clusterIDs(t, invoke(t, p, "ListClusters", map[string]any{})); len(got) != 1 || got[0] != id {
		t.Fatalf("clusters after terminating one known and one unknown id: %v", got)
	}
	if got := state(t, invoke(t, p, "DescribeCluster", map[string]any{"ClusterId": id}), "Cluster"); got != "TERMINATED" {
		t.Errorf("the known cluster is %s, want TERMINATED", got)
	}
	// The id that was never a cluster is still not one.
	faults(t, p, "DescribeCluster", map[string]any{"ClusterId": "j-absent"})
}

// TestTheDefaultUpdateStillCreates holds the other half. Most of these
// operations are "update or create" and eighteen bundles patch without
// guarding on the record being there, so `missing: ignore` had to be opt-in
// rather than a change of default.
//
// Directory Service's CreateAlias is the transcribed example: the pack read
// the directory, set the alias and wrote it back, so aliasing a directory
// that does not exist left a record carrying only the id and the alias. That
// is what a patch on an absent record does, and it must go on doing it.
func TestTheDefaultUpdateStillCreates(t *testing.T) {
	p := served(t, "aws.ds")
	invoke(t, p, "CreateAlias", map[string]any{"DirectoryId": "d-never", "Alias": "orphan"})

	out := invoke(t, p, "DescribeDirectories", map[string]any{"DirectoryIds": []any{"d-never"}})
	list, _ := out["DirectoryDescriptions"].([]any)
	if len(list) != 1 {
		t.Fatalf("aliasing a directory that was never created left %d records: %v", len(list), out)
	}
	rec, ok := list[0].(map[string]any)
	if !ok {
		t.Fatalf("%#v is not a record", list[0])
	}
	if fmt.Sprint(rec["Alias"]) != "orphan" {
		t.Errorf("the conjured record carries Alias %v", rec["Alias"])
	}
}
