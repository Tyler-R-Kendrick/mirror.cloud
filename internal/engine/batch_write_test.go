package engine_test

import (
	"fmt"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/bir"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/generated"
)

// A write with a for_each runs once per element of the list it names. These
// tests hold two things: that N elements become N records addressed by their
// own keys, and that the detector behavior a superseded recording step can no
// longer gate is still gated somewhere.

func members(t *testing.T, out map[string]any) map[string]string {
	t.Helper()
	list, _ := out["Members"].([]any)
	got := map[string]string{}
	for _, m := range list {
		rec, ok := m.(map[string]any)
		if !ok {
			t.Fatalf("member %#v is not a record", m)
		}
		got[fmt.Sprint(rec["AccountId"])] = fmt.Sprint(rec["Email"])
	}
	return got
}

// TestForEachWritesOneRecordPerElement is the shape six services need and none
// could express: a batch operation that stores N records from one request.
//
// The keys matter as much as the count. A for_each whose elements all resolved
// the same key would write one record and answer as though it had written
// three, which is the silent single-row overwrite the loader now refuses --
// so this asserts the accounts are addressable individually, not just that
// three rows exist.
func TestForEachWritesOneRecordPerElement(t *testing.T) {
	p := served(t, "aws.guardduty")
	det := invoke(t, p, "CreateDetector", map[string]any{"Enable": true})["DetectorId"]

	invoke(t, p, "CreateMembers", map[string]any{
		"DetectorId": det,
		"AccountDetails": []any{
			map[string]any{"AccountId": "111111111111", "Email": "a@example.invalid"},
			map[string]any{"AccountId": "222222222222", "Email": "b@example.invalid"},
			map[string]any{"AccountId": "333333333333", "Email": "c@example.invalid"},
		},
	})
	got := members(t, invoke(t, p, "ListMembers", map[string]any{"DetectorId": det}))
	want := map[string]string{
		"111111111111": "a@example.invalid",
		"222222222222": "b@example.invalid",
		"333333333333": "c@example.invalid",
	}
	if len(got) != len(want) {
		t.Fatalf("%d members, want %d: %v", len(got), len(want), got)
	}
	for id, email := range want {
		if got[id] != email {
			t.Errorf("member %s has email %q, want %q", id, got[id], email)
		}
	}

	// A second batch over one existing and one new account updates the one and
	// adds the other -- each element addressed by its own key, not by whatever
	// key the element before it resolved.
	invoke(t, p, "CreateMembers", map[string]any{
		"DetectorId": det,
		"AccountDetails": []any{
			map[string]any{"AccountId": "111111111111", "Email": "changed@example.invalid"},
			map[string]any{"AccountId": "444444444444", "Email": "d@example.invalid"},
		},
	})
	got = members(t, invoke(t, p, "ListMembers", map[string]any{"DetectorId": det}))
	if len(got) != 4 {
		t.Fatalf("%d members after the second batch, want 4: %v", len(got), got)
	}
	if got["111111111111"] != "changed@example.invalid" {
		t.Errorf("the second batch did not update 111111111111: %q", got["111111111111"])
	}
	if got["444444444444"] != "d@example.invalid" {
		t.Errorf("the second batch did not add 444444444444: %v", got)
	}
}

// TestForEachOverNothingWritesNothing pins the empty case. An absent member and
// an empty list are the same request as far as the store is concerned, and
// neither is an error: an operation that must reject an empty batch says so in
// its own requires.
func TestForEachOverNothingWritesNothing(t *testing.T) {
	p := served(t, "aws.guardduty")
	det := invoke(t, p, "CreateDetector", map[string]any{"Enable": true})["DetectorId"]

	invoke(t, p, "CreateMembers", map[string]any{
		"DetectorId": det, "AccountDetails": []any{},
	})
	if got := members(t, invoke(t, p, "ListMembers", map[string]any{"DetectorId": det})); len(got) != 0 {
		t.Errorf("an empty batch wrote %v", got)
	}
}

// TestForEachScopesEachBatchToItsParent keeps the two features composed. The
// member collection is named for its detector, so a batch under one detector
// must not be visible under another -- the failure being one shared collection
// that every account in the account-and-region lands in.
func TestForEachScopesEachBatchToItsParent(t *testing.T) {
	p := served(t, "aws.guardduty")
	first := invoke(t, p, "CreateDetector", map[string]any{"Enable": true})["DetectorId"]
	second := invoke(t, p, "CreateDetector", map[string]any{"Enable": true})["DetectorId"]

	invoke(t, p, "CreateMembers", map[string]any{
		"DetectorId":     first,
		"AccountDetails": []any{map[string]any{"AccountId": "111111111111"}},
	})
	if got := members(t, invoke(t, p, "ListMembers", map[string]any{"DetectorId": second})); len(got) != 0 {
		t.Errorf("the other detector sees %v", got)
	}
	if got := members(t, invoke(t, p, "ListMembers", map[string]any{"DetectorId": first})); len(got) != 1 {
		t.Errorf("the batch's own detector sees %v", got)
	}
}

// TestDeleteWhereRemovesTheAccountsNamed is the other half of the batch pair:
// members go in per element and come out per predicate. Deleting an account
// that was never a member succeeds, and takes nothing else with it.
func TestDeleteWhereRemovesTheAccountsNamed(t *testing.T) {
	p := served(t, "aws.guardduty")
	det := invoke(t, p, "CreateDetector", map[string]any{"Enable": true})["DetectorId"]
	invoke(t, p, "CreateMembers", map[string]any{
		"DetectorId": det,
		"AccountDetails": []any{
			map[string]any{"AccountId": "111111111111"},
			map[string]any{"AccountId": "222222222222"},
		},
	})

	invoke(t, p, "DeleteMembers", map[string]any{
		"DetectorId": det, "AccountIds": []any{"222222222222", "999999999999"},
	})
	got := members(t, invoke(t, p, "ListMembers", map[string]any{"DetectorId": det}))
	if len(got) != 1 {
		t.Fatalf("%d members after the delete, want 1: %v", len(got), got)
	}
	if _, ok := got["111111111111"]; !ok {
		t.Errorf("the delete took an account it was not given: %v", got)
	}
}

// TestDetectorStatusIsWhateverEnableStringifiedTo carries what the recording
// can no longer gate. Four GetDetector steps in the guardduty trace are
// superseded, because the pack answered a DetectorId that GetDetectorResponse
// does not declare -- and superseding a step drops its whole body, including
// the part worth keeping.
//
// The part worth keeping is the service's sharpest quirk. DetectorStatus is an
// enumeration of ENABLED and DISABLED. `true` and an absent flag are both
// rewritten to ENABLED; `false` is stringified and stored as it fell out, so
// the one request that says "do not enable this" produces the one status no
// SDK can interpret.
func TestDetectorStatusIsWhateverEnableStringifiedTo(t *testing.T) {
	p := served(t, "aws.guardduty")
	status := func(in map[string]any) any {
		id := invoke(t, p, "CreateDetector", in)["DetectorId"]
		return invoke(t, p, "GetDetector", map[string]any{"DetectorId": id})["Status"]
	}
	if got := status(map[string]any{"Enable": true}); got != "ENABLED" {
		t.Errorf("Enable: true gave Status %v, want ENABLED", got)
	}
	if got := status(map[string]any{"Enable": false}); got != "false" {
		t.Errorf("Enable: false gave Status %v, want the string \"false\" the pack "+
			"stored -- neither ENABLED nor DISABLED", got)
	}
}

// TestUpdateDetectorMovesOnlyANonEmptyFrequency carries the rest of what those
// superseded steps would have gated: the one member an update can change, and
// that an update sending anything else changes nothing.
func TestUpdateDetectorMovesOnlyANonEmptyFrequency(t *testing.T) {
	p := served(t, "aws.guardduty")
	det := invoke(t, p, "CreateDetector", map[string]any{
		"Enable": true, "FindingPublishingFrequency": "FIFTEEN_MINUTES",
	})["DetectorId"]
	freq := func() any {
		return invoke(t, p, "GetDetector", map[string]any{
			"DetectorId": det,
		})["FindingPublishingFrequency"]
	}

	invoke(t, p, "UpdateDetector", map[string]any{
		"DetectorId": det, "FindingPublishingFrequency": "ONE_HOUR",
	})
	if got := freq(); got != "ONE_HOUR" {
		t.Errorf("frequency = %v after the update, want ONE_HOUR", got)
	}
	// Enable is accepted and ignored here, unlike on create.
	invoke(t, p, "UpdateDetector", map[string]any{"DetectorId": det, "Enable": false})
	if got := freq(); got != "ONE_HOUR" {
		t.Errorf("frequency = %v after an update that named neither, want ONE_HOUR", got)
	}
	if got := invoke(t, p, "GetDetector", map[string]any{"DetectorId": det})["Status"]; got != "ENABLED" {
		t.Errorf("Status = %v; an update disabled a detector a create could not", got)
	}
}

// TestForEachWithoutAKeyIsRefused is the loader guard, and it is there because
// of what the failure looks like without it. A for_each whose elements do not
// each resolve a key writes every element to the same row: the last one wins,
// the operation succeeds, and the response says nothing -- the same silent
// single-row overwrite that `workspaces` and `dms` turned out to be. A bundle
// that would do that does not load.
func TestForEachWithoutAKeyIsRefused(t *testing.T) {
	svc, err := generated.Model("aws.guardduty")
	if err != nil {
		t.Fatal(err)
	}
	doc := `schema: bir/1
service: aws.guardduty
provenance: authored
resources:
  member:
    collection: gdmem
    id:
      input_members: [AccountId]
    record:
      AccountId: id
operations:
  CreateMembers:
    effects:
      - create:
          resource: member
          for_each: "input.AccountDetails"
          record:
            Email: "'Email' in item ? string(item.Email) : ''"
    output:
      UnprocessedAccounts: "[]"
`
	fsys := fstest.MapFS{"svc/service.yaml": &fstest.MapFile{Data: []byte(doc)}}
	if _, err := bir.Load(fsys, "svc", svc); err == nil {
		t.Fatal("a for_each with no key loaded; every element would write the same row")
	} else if !strings.Contains(err.Error(), "for_each without a key") {
		t.Fatalf("the complaint does not say what is wrong: %v", err)
	}
}
