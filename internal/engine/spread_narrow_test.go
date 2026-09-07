package engine_test

import (
	"fmt"
	"testing"
)

// A write used to spread only the whole request. Two shapes it could not
// express turn up together in Config: three of its Put operations carry the
// document to store inside one member -- PutConfigurationRecorder sends a
// ConfigurationRecorder, PutConfigRule a ConfigRule -- and PutEvaluations
// sends a list of them. Enumerating those members instead would not reproduce
// the pack: a recorder sent with no recordingGroup would gain a null one that
// a later describe would answer.

func recorders(t *testing.T, out map[string]any) []map[string]any {
	t.Helper()
	list, _ := out["ConfigurationRecorders"].([]any)
	var got []map[string]any
	for _, r := range list {
		rec, ok := r.(map[string]any)
		if !ok {
			t.Fatalf("recorder %#v is not a record", r)
		}
		got = append(got, rec)
	}
	return got
}

// TestSpreadOfAMemberStoresTheDocument. The nested document is stored whole,
// and the name the service owns is forced on top of it.
func TestSpreadOfAMemberStoresTheDocument(t *testing.T) {
	p := served(t, "aws.config")
	invoke(t, p, "PutConfigurationRecorder", map[string]any{
		"ConfigurationRecorder": map[string]any{
			"name":           "primary",
			"roleARN":        "arn:aws:iam::000000000000:role/config",
			"recordingGroup": map[string]any{"allSupported": true},
		},
	})
	got := recorders(t, invoke(t, p, "DescribeConfigurationRecorders", map[string]any{}))
	if len(got) != 1 {
		t.Fatalf("%d recorders, want 1: %v", len(got), got)
	}
	if got[0]["roleARN"] != "arn:aws:iam::000000000000:role/config" {
		t.Errorf("the roleARN the caller sent is %v", got[0]["roleARN"])
	}
	group, ok := got[0]["recordingGroup"].(map[string]any)
	if !ok {
		t.Fatalf("recordingGroup is %#v, not a record", got[0]["recordingGroup"])
	}
	if group["allSupported"] != true {
		t.Errorf("the nested recordingGroup did not survive: %v", group)
	}
	if got[0]["name"] != "primary" {
		t.Errorf("name is %v", got[0]["name"])
	}
}

// TestSpreadOfAMemberStoresNothingItWasNotSent is the other half, and the
// reason the copy is declared rather than expressed: a bundle spelling the
// members out would store a null for each one the caller omitted.
func TestSpreadOfAMemberStoresNothingItWasNotSent(t *testing.T) {
	p := served(t, "aws.config")
	invoke(t, p, "PutConfigurationRecorder", map[string]any{
		"ConfigurationRecorder": map[string]any{"name": "bare"},
	})
	got := recorders(t, invoke(t, p, "DescribeConfigurationRecorders", map[string]any{}))
	if len(got) != 1 {
		t.Fatalf("%d recorders, want 1", len(got))
	}
	if len(got[0]) != 1 {
		t.Errorf("a recorder sent with only a name came back with %d members: %v", len(got[0]), got[0])
	}
}

func resourceIDs(t *testing.T, out map[string]any) []string {
	t.Helper()
	list, _ := out["resourceIdentifiers"].([]any)
	var ids []string
	for _, r := range list {
		rec, ok := r.(map[string]any)
		if !ok {
			t.Fatalf("identifier %#v is not a record", r)
		}
		ids = append(ids, fmt.Sprint(rec["resourceId"]))
	}
	return ids
}

// TestSpreadOfAnElementStoresEachOne. `item` is the element a for_each is on,
// and spreading it stores the element the caller sent rather than a handful of
// members enumerated from a shape with five of them and a batch sibling with
// forty.
func TestSpreadOfAnElementStoresEachOne(t *testing.T) {
	p := served(t, "aws.config")
	invoke(t, p, "PutEvaluations", map[string]any{
		"ResultToken": "token",
		"Evaluations": []any{
			map[string]any{
				"ComplianceResourceId":   "bucket-a",
				"ComplianceResourceType": "AWS::S3::Bucket",
				"ComplianceType":         "NON_COMPLIANT",
				"OrderingTimestamp":      "2020-01-01T00:00:00Z",
			},
			map[string]any{
				"ComplianceResourceId":   "bucket-b",
				"ComplianceResourceType": "AWS::S3::Bucket",
				"ComplianceType":         "COMPLIANT",
				"OrderingTimestamp":      "2020-01-01T00:00:00Z",
			},
		},
	})
	got := resourceIDs(t, invoke(t, p, "ListDiscoveredResources",
		map[string]any{"resourceType": "AWS::S3::Bucket"}))
	if len(got) != 2 || got[0] != "bucket-a" || got[1] != "bucket-b" {
		t.Fatalf("stored %v, want one record per element addressed by its own id", got)
	}
}

// TestAPerElementGuardSkipsOnlyTheElementsThatFailIt. The guard on a for_each
// write is evaluated per element, because a batch's elements are not all
// alike: Config's pack skipped the entries carrying no resource id, and a
// guard evaluated once before any element is bound cannot express that.
//
// The failure this defends against is not a missing row. It is a row keyed by
// the empty string that every such entry in the account would then share.
func TestAPerElementGuardSkipsOnlyTheElementsThatFailIt(t *testing.T) {
	p := served(t, "aws.config")
	invoke(t, p, "PutEvaluations", map[string]any{
		"ResultToken": "token",
		"Evaluations": []any{
			map[string]any{
				"ComplianceResourceId":   "bucket-a",
				"ComplianceResourceType": "AWS::S3::Bucket",
				"ComplianceType":         "COMPLIANT",
				"OrderingTimestamp":      "2020-01-01T00:00:00Z",
			},
			map[string]any{
				"ComplianceResourceId":   "",
				"ComplianceResourceType": "AWS::S3::Bucket",
				"ComplianceType":         "COMPLIANT",
				"OrderingTimestamp":      "2020-01-01T00:00:00Z",
			},
			map[string]any{
				"ComplianceResourceId":   "bucket-c",
				"ComplianceResourceType": "AWS::S3::Bucket",
				"ComplianceType":         "COMPLIANT",
				"OrderingTimestamp":      "2020-01-01T00:00:00Z",
			},
		},
	})
	got := resourceIDs(t, invoke(t, p, "ListDiscoveredResources",
		map[string]any{"resourceType": "AWS::S3::Bucket"}))
	// Two records, and the elements either side of the skipped one are both
	// there: a guard evaluated once would have written all three or none.
	if len(got) != 2 || got[0] != "bucket-a" || got[1] != "bucket-c" {
		t.Fatalf("stored %v, want bucket-a and bucket-c and no row for the entry with no id", got)
	}
}
