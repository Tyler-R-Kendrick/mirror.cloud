package bundled_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/bundled"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/specboot"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spitest"
)

// The oracle for these tests is AWS's own documentation, as it ships in the
// vendored model (specs/mirror.lock: aws/api-models-aws
// secrets-manager-2017-10-17.json, sha256 2738bd8d...). The quoted sentences
// are copied from its smithy.api#documentation traits.

type secretsCall func(op string, in map[string]any) (map[string]any, error)

func secretsManager(t *testing.T) (secretsCall, func(t *testing.T, op string, in map[string]any) map[string]any, func(t *testing.T, err error, code string)) {
	t.Helper()
	return secretsManagerOn(t, spitest.Deps(t))
}

// secretsManagerOn is secretsManager over deps the test keeps, for a test that
// moves the clock.
func secretsManagerOn(t *testing.T, deps spi.Deps) (secretsCall, func(t *testing.T, op string, in map[string]any) map[string]any, func(t *testing.T, err error, code string)) {
	t.Helper()
	pack, err := bundled.New("aws.secretsmanager", deps)
	if err != nil {
		t.Fatal(err)
	}
	id := spi.Identity{Account: "000000000000", Region: "us-east-1"}
	call := func(op string, in map[string]any) (map[string]any, error) {
		res, err := pack.Invoke(context.Background(), &spi.Request{ServiceID: "aws.secretsmanager", Operation: op, Identity: id, Input: in})
		if err != nil {
			return nil, err
		}
		return res.Output, nil
	}
	fault := func(t *testing.T, err error, code string) {
		t.Helper()
		f, isFault := err.(*spi.Fault)
		if !isFault || f.Code != code {
			t.Fatalf("want %s, got %v", code, err)
		}
	}
	ok := func(t *testing.T, op string, in map[string]any) map[string]any {
		t.Helper()
		out, err := call(op, in)
		if err != nil {
			t.Fatalf("%s: %v", op, err)
		}
		return out
	}
	return call, ok, fault
}

// at reports whether a timestamp member is the instant given in epoch seconds.
// A value the bundle computes answers as an int64 and one read back from the
// store as a float64; both write the same JSON number.
func at(v any, seconds int64) bool {
	switch n := v.(type) {
	case int64:
		return n == seconds
	case float64:
		return n == float64(seconds)
	}
	return false
}

// DescribeSecretResponse.Description: "The description of the secret."
// UpdateSecretRequest.Description: "The description of the secret."
// UpdateSecret: "If you include SecretString or SecretBinary to create a new
// secret version, Secrets Manager automatically moves the staging label
// AWSCURRENT to the new version." UpdateSecretResponse.VersionId: "If Secrets
// Manager created a new version of the secret during this operation, then
// VersionId contains the unique identifier of the new version."
func TestSecretsManagerKeepsTheDescription(t *testing.T) {
	_, ok, _ := secretsManager(t)

	ok(t, "CreateSecret", map[string]any{"Name": "stripe", "SecretString": "v1", "Description": "payment key"})
	if got := ok(t, "DescribeSecret", map[string]any{"SecretId": "stripe"}); got["Description"] != "payment key" {
		t.Fatalf("after CreateSecret, Description = %#v", got["Description"])
	}

	updated := ok(t, "UpdateSecret", map[string]any{"SecretId": "stripe", "Description": "rotated payment key"})
	if updated["VersionId"] != nil {
		t.Fatalf("a description-only UpdateSecret answered a VersionId %#v: no version was created", updated["VersionId"])
	}
	if got := ok(t, "DescribeSecret", map[string]any{"SecretId": "stripe"}); got["Description"] != "rotated payment key" {
		t.Fatalf("after UpdateSecret, Description = %#v", got["Description"])
	}
	if value := ok(t, "GetSecretValue", map[string]any{"SecretId": "stripe"}); value["SecretString"] != "v1" {
		t.Fatalf("a description-only UpdateSecret changed the value to %#v", value["SecretString"])
	}
	versions := ok(t, "ListSecretVersionIds", map[string]any{"SecretId": "stripe"})
	if list, _ := versions["Versions"].([]any); len(list) != 1 {
		t.Fatalf("a description-only UpdateSecret left %d versions, want 1", len(list))
	}

	// A value written without a description keeps the one already there.
	ok(t, "PutSecretValue", map[string]any{"SecretId": "stripe", "SecretString": "v2"})
	if got := ok(t, "DescribeSecret", map[string]any{"SecretId": "stripe"}); got["Description"] != "rotated payment key" {
		t.Fatalf("after PutSecretValue, Description = %#v", got["Description"])
	}
	withValue := ok(t, "UpdateSecret", map[string]any{"SecretId": "stripe", "SecretString": "v3"})
	if withValue["VersionId"] == nil {
		t.Fatal("an UpdateSecret with a SecretString answered no VersionId")
	}
	if value := ok(t, "GetSecretValue", map[string]any{"SecretId": "stripe"}); value["SecretString"] != "v3" {
		t.Fatalf("UpdateSecret with a SecretString left the value at %#v", value["SecretString"])
	}
}

// DeleteSecret: "Secrets Manager attaches a DeletionDate stamp to the secret
// that specifies the end of the recovery window." RecoveryWindowInDays: "If you
// don't use either, then by default Secrets Manager uses a 30 day recovery
// window." DeleteSecretResponse.DeletionDate: "This value is the date and time
// of the delete request plus the number of days in RecoveryWindowInDays."
// DescribeSecretResponse.DeletedDate: "The date the secret is scheduled for
// deletion. If it is not scheduled for deletion, this field is omitted."
// RestoreSecret: "Cancels the scheduled deletion of a secret by removing the
// DeletedDate time stamp."
func TestSecretsManagerDescribesASecretScheduledForDeletion(t *testing.T) {
	call, ok, fault := secretsManager(t)
	ok(t, "CreateSecret", map[string]any{"Name": "s", "SecretString": "v"})

	if got := ok(t, "DescribeSecret", map[string]any{"SecretId": "s"}); got["DeletedDate"] != nil {
		t.Fatalf("a secret not scheduled for deletion answered DeletedDate %#v", got["DeletedDate"])
	}

	deleted := ok(t, "DeleteSecret", map[string]any{"SecretId": "s"})
	// spitest's clock stands at the Unix epoch.
	if !at(deleted["DeletionDate"], 30*86400) || deleted["Name"] != "s" ||
		deleted["ARN"] != "arn:aws:secretsmanager:us-east-1:000000000000:secret:s" {
		t.Fatalf("DeleteSecret answered %#v, want Name, ARN and the request time plus 30 days", deleted)
	}
	described, err := call("DescribeSecret", map[string]any{"SecretId": "s"})
	if err != nil {
		t.Fatalf("DescribeSecret of a secret scheduled for deletion faulted: %v", err)
	}
	if !at(described["DeletedDate"], 0) || described["Name"] != "s" {
		t.Fatalf("DescribeSecret of a secret scheduled for deletion answered %#v", described)
	}
	// DeleteSecret: "When a secret is scheduled for deletion, you cannot
	// retrieve the secret value."
	_, err = call("GetSecretValue", map[string]any{"SecretId": "s"})
	fault(t, err, "InvalidRequestException")

	restored := ok(t, "RestoreSecret", map[string]any{"SecretId": "s"})
	if restored["ARN"] != "arn:aws:secretsmanager:us-east-1:000000000000:secret:s" || restored["Name"] != "s" {
		t.Fatalf("RestoreSecret answered %#v, want ARN and Name", restored)
	}
	if got := ok(t, "DescribeSecret", map[string]any{"SecretId": "s"}); got["DeletedDate"] != nil {
		t.Fatalf("after RestoreSecret, DeletedDate = %#v", got["DeletedDate"])
	}

	ok(t, "CreateSecret", map[string]any{"Name": "week", "SecretString": "v"})
	week := ok(t, "DeleteSecret", map[string]any{"SecretId": "week", "RecoveryWindowInDays": 7})
	if !at(week["DeletionDate"], 7*86400) {
		t.Fatalf("a 7-day window answered DeletionDate %#v", week["DeletionDate"])
	}
}

// InvalidRequestException: "A parameter value is not valid for the current
// state of the resource. Possible causes: The secret is scheduled for
// deletion." ForceDeleteWithoutRecovery: "If you forcibly delete an already
// deleted or nonexistent secret, the operation does not return
// ResourceNotFoundException." RecoveryWindowInDays: "You can't use both this
// parameter and ForceDeleteWithoutRecovery in the same call."
func TestSecretsManagerDeletesASecretOnce(t *testing.T) {
	deps := spitest.Deps(t)
	call, ok, fault := secretsManagerOn(t, deps)
	ok(t, "CreateSecret", map[string]any{"Name": "s", "SecretString": "v"})
	ok(t, "DeleteSecret", map[string]any{"SecretId": "s"})

	if err := deps.Clock.Advance(time.Hour); err != nil {
		t.Fatal(err)
	}
	_, err := call("DeleteSecret", map[string]any{"SecretId": "s"})
	fault(t, err, "InvalidRequestException")
	if got := ok(t, "DescribeSecret", map[string]any{"SecretId": "s"}); !at(got["DeletedDate"], 0) {
		t.Fatalf("a refused second DeleteSecret moved DeletedDate to %#v", got["DeletedDate"])
	}

	// Forcing the delete of one already scheduled removes it now.
	ok(t, "DeleteSecret", map[string]any{"SecretId": "s", "ForceDeleteWithoutRecovery": true})
	_, err = call("DescribeSecret", map[string]any{"SecretId": "s"})
	fault(t, err, "ResourceNotFoundException")

	// Forcing it again, or forcing one that never existed, is not a fault.
	if _, err := call("DeleteSecret", map[string]any{"SecretId": "s", "ForceDeleteWithoutRecovery": true}); err != nil {
		t.Fatalf("a forced delete of a deleted secret faulted: %v", err)
	}
	if _, err := call("DeleteSecret", map[string]any{"SecretId": "never", "ForceDeleteWithoutRecovery": true}); err != nil {
		t.Fatalf("a forced delete of a secret that never existed faulted: %v", err)
	}
	// Unforced, a secret that does not exist is still not found.
	_, err = call("DeleteSecret", map[string]any{"SecretId": "never"})
	fault(t, err, "ResourceNotFoundException")

	ok(t, "CreateSecret", map[string]any{"Name": "both", "SecretString": "v"})
	_, err = call("DeleteSecret", map[string]any{"SecretId": "both", "ForceDeleteWithoutRecovery": true, "RecoveryWindowInDays": 7})
	fault(t, err, "InvalidParameterException")
}

// epochSeconds answers every timestamp member an answer carries, as the JSON
// the awsJson1_1 codec writes for it, and fails on one that is not a number.
// awsJson1_1 writes a timestamp as epoch seconds, a JSON number; the Go SDK v2
// decodes one only from a json.Number, so a string there fails the whole
// response. The walk follows the model's output shape, so a timestamp member
// added later is held to this without naming it here.
func epochSeconds(t *testing.T, op string, out map[string]any) map[string]json.Number {
	t.Helper()
	svc := specboot.Bundle().ServiceByID("aws.secretsmanager")
	raw, err := json.Marshal(out)
	if err != nil {
		t.Fatal(err)
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var wire any
	if err := dec.Decode(&wire); err != nil {
		t.Fatal(err)
	}
	found := map[string]json.Number{}
	var walk func(path, shapeID string, v any)
	walk = func(path, shapeID string, v any) {
		shape, known := svc.Shapes[shapeID]
		if !known || v == nil {
			return
		}
		switch shape.Kind {
		case model.KindTimestamp:
			n, isNumber := v.(json.Number)
			if !isNumber {
				t.Fatalf("%s %s is %#v on the wire; awsJson1_1 writes a timestamp as epoch seconds, a JSON number", op, path, v)
			}
			found[path] = n
		case model.KindStructure:
			m, _ := v.(map[string]any)
			for name, member := range shape.Members {
				walk(path+"."+name, member.Shape, m[name])
			}
		case model.KindList:
			list, _ := v.([]any)
			for _, item := range list {
				walk(path+"[]", shape.Member, item)
			}
		}
	}
	walk(op, svc.OperationByName(op).Output, wire)
	return found
}

// DeleteSecretResponse.DeletionDate and DescribeSecretResponse.DeletedDate are
// timestamps (DeletionDateType and DeletedDateType, "type": "timestamp"), and
// awsJson1_1 writes a timestamp as epoch seconds. The Go SDK v2 module for
// Secrets Manager is not in this module cache, so the test holds the JSON
// form to what its deserializer accepts, a json.Number.
func TestSecretsManagerTimestampsAreEpochSeconds(t *testing.T) {
	_, ok, _ := secretsManager(t)
	ok(t, "CreateSecret", map[string]any{"Name": "s", "SecretString": "v"})

	deleted := epochSeconds(t, "DeleteSecret", ok(t, "DeleteSecret", map[string]any{"SecretId": "s"}))
	// spitest's clock stands at the Unix epoch; the window is 30 days.
	if got := deleted["DeleteSecret.DeletionDate"]; got != "2592000" {
		t.Fatalf("DeletionDate = %q, want 2592000 (the request time plus 30 days, in epoch seconds)", got)
	}
	described := epochSeconds(t, "DescribeSecret", ok(t, "DescribeSecret", map[string]any{"SecretId": "s"}))
	if _, has := described["DescribeSecret.DeletedDate"]; !has {
		t.Fatal("DescribeSecret of a secret scheduled for deletion answered no DeletedDate")
	}
	epochSeconds(t, "ListSecrets", ok(t, "ListSecrets", map[string]any{"IncludePlannedDeletion": true}))
}

// DeleteSecretRequest.RecoveryWindowInDays: "The number of days from 7 to 30
// that Secrets Manager waits before permanently deleting the secret."
// DeleteSecret: "The minimum recovery window is 7 days." InvalidParameterException:
// "The parameter name or value is invalid." The window is checked before any
// arithmetic, so a huge one is refused rather than overflowing the date.
func TestSecretsManagerRecoveryWindowIsSevenToThirtyDays(t *testing.T) {
	for _, days := range []int64{0, 6, 31, -5, 1 << 60} {
		t.Run(fmt.Sprint(days), func(t *testing.T) {
			call, ok, fault := secretsManager(t)
			ok(t, "CreateSecret", map[string]any{"Name": "s", "SecretString": "v"})
			_, err := call("DeleteSecret", map[string]any{"SecretId": "s", "RecoveryWindowInDays": days})
			fault(t, err, "InvalidParameterException")
			if got := ok(t, "DescribeSecret", map[string]any{"SecretId": "s"}); got["DeletedDate"] != nil {
				t.Fatalf("a refused window scheduled the secret: DeletedDate %#v", got["DeletedDate"])
			}
		})
	}
	for _, days := range []int64{7, 30} {
		t.Run(fmt.Sprint(days), func(t *testing.T) {
			_, ok, _ := secretsManager(t)
			ok(t, "CreateSecret", map[string]any{"Name": "s", "SecretString": "v"})
			deleted := ok(t, "DeleteSecret", map[string]any{"SecretId": "s", "RecoveryWindowInDays": days})
			if !at(deleted["DeletionDate"], days*86400) {
				t.Fatalf("DeletionDate %#v, want the request time plus %d days", deleted["DeletionDate"], days)
			}
		})
	}
}

// SecretListEntry.DeletedDate: "The date and time the deletion of the secret
// occurred." DeleteSecretResponse.DeletionDate: "This value is the date and
// time of the delete request plus the number of days in RecoveryWindowInDays."
// DescribeSecretResponse.DeletedDate calls the same member "The date the secret
// is scheduled for deletion". The two readings conflict; the bundle chooses the
// listing's, the time of the delete request, and DeletionDate is the end of the
// window. This pins that choice.
func TestSecretsManagerDeletedDateIsWhenDeletionWasRequested(t *testing.T) {
	deps := spitest.Deps(t)
	_, ok, _ := secretsManagerOn(t, deps)
	ok(t, "CreateSecret", map[string]any{"Name": "s", "SecretString": "v"})
	if err := deps.Clock.Advance(time.Hour); err != nil {
		t.Fatal(err)
	}
	deleted := ok(t, "DeleteSecret", map[string]any{"SecretId": "s", "RecoveryWindowInDays": 7})
	if !at(deleted["DeletionDate"], 3600+7*86400) {
		t.Fatalf("DeletionDate %#v, want the request time plus 7 days", deleted["DeletionDate"])
	}
	if err := deps.Clock.Advance(time.Hour); err != nil {
		t.Fatal(err)
	}
	if got := ok(t, "DescribeSecret", map[string]any{"SecretId": "s"}); !at(got["DeletedDate"], 3600) {
		t.Fatalf("DeletedDate %#v, want 3600, the time the deletion was requested", got["DeletedDate"])
	}
}

// DeleteSecretResponse.DeletionDate: "This value is the date and time of the
// delete request plus the number of days in RecoveryWindowInDays." A forced
// delete has no window, so its DeletionDate is the time of the request; it was
// answered as null.
func TestSecretsManagerForcedDeleteAnswersTheRequestTime(t *testing.T) {
	deps := spitest.Deps(t)
	_, ok, _ := secretsManagerOn(t, deps)
	ok(t, "CreateSecret", map[string]any{"Name": "s", "SecretString": "v"})
	if err := deps.Clock.Advance(time.Hour); err != nil {
		t.Fatal(err)
	}
	forced := ok(t, "DeleteSecret", map[string]any{"SecretId": "s", "ForceDeleteWithoutRecovery": true})
	if !at(forced["DeletionDate"], 3600) {
		t.Fatalf("a forced delete answered DeletionDate %#v, want 3600, the time of the request", forced["DeletionDate"])
	}
	epochSeconds(t, "DeleteSecret", forced)
	never := ok(t, "DeleteSecret", map[string]any{"SecretId": "never", "ForceDeleteWithoutRecovery": true})
	if !at(never["DeletionDate"], 3600) {
		t.Fatalf("a forced delete of a secret that never existed answered DeletionDate %#v", never["DeletionDate"])
	}
}

// InvalidRequestException: "A parameter value is not valid for the current
// state of the resource. Possible causes: The secret is scheduled for
// deletion." CreateSecret, PutSecretValue, UpdateSecret, RotateSecret,
// TagResource, UntagResource and GetSecretValue each document it. DeleteSecret:
// "When a secret is scheduled for deletion, you cannot retrieve the secret
// value. You must first cancel the deletion with RestoreSecret and then you can
// retrieve the secret." A write to a scheduled secret used to undelete it.
func TestSecretsManagerRefusesWritesToASecretScheduledForDeletion(t *testing.T) {
	for _, refused := range []struct {
		name, op string
		in       map[string]any
	}{
		{"CreateSecret", "CreateSecret", map[string]any{"Name": "s", "SecretString": "v2"}},
		{"PutSecretValue", "PutSecretValue", map[string]any{"SecretId": "s", "SecretString": "v2"}},
		{"UpdateSecret/value", "UpdateSecret", map[string]any{"SecretId": "s", "SecretString": "v2"}},
		{"UpdateSecret/description", "UpdateSecret", map[string]any{"SecretId": "s", "Description": "other"}},
		{"RotateSecret", "RotateSecret", map[string]any{"SecretId": "s"}},
		{"TagResource", "TagResource", map[string]any{"SecretId": "s", "Tags": []any{map[string]any{"Key": "k", "Value": "other"}}}},
		{"UntagResource", "UntagResource", map[string]any{"SecretId": "s", "TagKeys": []any{"k"}}},
		{"GetSecretValue", "GetSecretValue", map[string]any{"SecretId": "s"}},
	} {
		t.Run(refused.name, func(t *testing.T) {
			call, ok, fault := secretsManager(t)
			ok(t, "CreateSecret", map[string]any{"Name": "s", "SecretString": "v1", "Description": "d"})
			ok(t, "TagResource", map[string]any{"SecretId": "s", "Tags": []any{map[string]any{"Key": "k", "Value": "v"}}})
			ok(t, "PutResourcePolicy", map[string]any{"SecretId": "s", "ResourcePolicy": `{"Version":"2012-10-17","Statement":[]}`})
			state := func() []map[string]any {
				return []map[string]any{
					ok(t, "DescribeSecret", map[string]any{"SecretId": "s"}),
					ok(t, "GetResourcePolicy", map[string]any{"SecretId": "s"}),
					ok(t, "ListSecretVersionIds", map[string]any{"SecretId": "s"}),
					ok(t, "GetSecretValue", map[string]any{"SecretId": "s"}),
				}
			}
			before := state()
			ok(t, "DeleteSecret", map[string]any{"SecretId": "s"})

			_, err := call(refused.op, refused.in)
			fault(t, err, "InvalidRequestException")
			if got := ok(t, "DescribeSecret", map[string]any{"SecretId": "s"}); got["DeletedDate"] == nil {
				t.Fatal("the refused call undeleted the secret")
			}
			// Nothing the refused call carried landed: after RestoreSecret every
			// read answers what it did before the delete.
			ok(t, "RestoreSecret", map[string]any{"SecretId": "s"})
			if after := state(); !reflect.DeepEqual(after, before) {
				t.Fatalf("the refused call changed the secret:\nbefore %#v\nafter  %#v", before, after)
			}
		})
	}
}

// BatchGetSecretValue: "If Secrets Manager encounters errors ... while
// attempting to retrieve any of the secrets, you can see the errors in Errors
// in the response." APIErrorType is "The error Secrets Manager encountered
// while retrieving an individual secret as part of BatchGetSecretValue", with
// SecretId, ErrorCode ("for example ResourceNotFoundException,
// InvalidParameterException, InvalidRequestException ...") and Message. A secret
// scheduled for deletion is one whose value "you cannot retrieve" (DeleteSecret),
// so it is an Errors row, not a value.
func TestSecretsManagerBatchGetLeavesOutASecretScheduledForDeletion(t *testing.T) {
	_, ok, _ := secretsManager(t)
	ok(t, "CreateSecret", map[string]any{"Name": "gone", "SecretString": "hidden"})
	ok(t, "CreateSecret", map[string]any{"Name": "kept", "SecretString": "shown"})
	ok(t, "DeleteSecret", map[string]any{"SecretId": "gone"})

	got := ok(t, "BatchGetSecretValue", map[string]any{"SecretIdList": []any{"gone", "kept"}})
	values, _ := got["SecretValues"].([]any)
	if len(values) != 1 || values[0].(map[string]any)["Name"] != "kept" || values[0].(map[string]any)["SecretString"] != "shown" {
		t.Fatalf("SecretValues = %#v, want only kept", got["SecretValues"])
	}
	errs, _ := got["Errors"].([]any)
	if len(errs) != 1 {
		t.Fatalf("Errors = %#v, want one row for gone", got["Errors"])
	}
	row := errs[0].(map[string]any)
	if row["SecretId"] != "gone" || row["ErrorCode"] != "InvalidRequestException" || row["Message"] == nil || row["Message"] == "" {
		t.Fatalf("Errors row = %#v, want SecretId gone, ErrorCode InvalidRequestException and a Message", row)
	}
}

// DescribeSecret: "Secrets Manager only returns fields that have a value in the
// response." DescribeSecretResponse.DeletedDate: "If it is not scheduled for
// deletion, this field is omitted." A member with no value is left out rather
// than answered as null; so are UpdateSecret's VersionId when it created no
// version and DeleteSecret's ARN for a secret that never existed.
func TestSecretsManagerOmitsMembersWithNoValue(t *testing.T) {
	_, ok, _ := secretsManager(t)
	ok(t, "CreateSecret", map[string]any{"Name": "plain", "SecretString": "v"})
	described := ok(t, "DescribeSecret", map[string]any{"SecretId": "plain"})
	for _, member := range []string{"Description", "DeletedDate", "Tags", "ReplicationStatus", "RotationEnabled", "RotationLambdaARN", "RotationRules"} {
		if v, present := described[member]; present {
			t.Errorf("DescribeSecret answered %s = %#v for a secret that has none", member, v)
		}
	}
	if updated := ok(t, "UpdateSecret", map[string]any{"SecretId": "plain", "Description": "d"}); hasKey(updated, "VersionId") {
		t.Errorf("a description-only UpdateSecret answered VersionId = %#v", updated["VersionId"])
	}
	if forced := ok(t, "DeleteSecret", map[string]any{"SecretId": "never", "ForceDeleteWithoutRecovery": true}); hasKey(forced, "ARN") {
		t.Errorf("a forced delete of a secret that never existed answered ARN = %#v", forced["ARN"])
	}
}

func hasKey(m map[string]any, k string) bool {
	_, present := m[k]
	return present
}

// SecretListEntry.Description: "The user-provided description of the secret."
// SecretListEntry.DeletedDate: "The date and time the deletion of the secret
// occurred. Not present on active secrets." ListSecretsRequest.
// IncludePlannedDeletion: "Specifies whether to include secrets scheduled for
// deletion."
func TestSecretsManagerListSecretsAnswersDescriptionAndDeletedDate(t *testing.T) {
	deps := spitest.Deps(t)
	_, ok, _ := secretsManagerOn(t, deps)
	ok(t, "CreateSecret", map[string]any{"Name": "active", "SecretString": "v", "Description": "kept"})
	ok(t, "CreateSecret", map[string]any{"Name": "scheduled", "SecretString": "v"})
	if err := deps.Clock.Advance(time.Hour); err != nil {
		t.Fatal(err)
	}
	ok(t, "DeleteSecret", map[string]any{"SecretId": "scheduled"})

	entries := func(in map[string]any) map[string]map[string]any {
		out := ok(t, "ListSecrets", in)
		epochSeconds(t, "ListSecrets", out)
		byName := map[string]map[string]any{}
		list, _ := out["SecretList"].([]any)
		for _, e := range list {
			m := e.(map[string]any)
			byName[m["Name"].(string)] = m
		}
		return byName
	}
	listed := entries(map[string]any{})
	if len(listed) != 1 || listed["active"]["Description"] != "kept" || hasKey(listed["active"], "DeletedDate") {
		t.Fatalf("ListSecrets answered %#v, want active alone, with its Description and no DeletedDate", listed)
	}
	planned := entries(map[string]any{"IncludePlannedDeletion": true})
	if len(planned) != 2 || !at(planned["scheduled"]["DeletedDate"], 3600) || hasKey(planned["scheduled"], "Description") {
		t.Fatalf("with IncludePlannedDeletion, scheduled answered %#v, want DeletedDate 3600 and no Description", planned["scheduled"])
	}
	if hasKey(planned["active"], "DeletedDate") || planned["active"]["Description"] != "kept" {
		t.Fatalf("with IncludePlannedDeletion, active answered %#v", planned["active"])
	}
}

// CreateSecret: "Creates a new secret." ResourceExistsException: "A resource
// with the ID you requested already exists." ResourceNotFoundException:
// "Secrets Manager can't find the resource that you asked for." PutSecretValue,
// UpdateSecret and RotateSecret each document ResourceNotFoundException. A
// second CreateSecret of one name used to write a new version, and a put,
// update or rotation of a name never created used to create it.
func TestSecretsManagerCreatesOnceAndWritesOnlyWhatExists(t *testing.T) {
	call, ok, _ := secretsManager(t)
	ok(t, "CreateSecret", map[string]any{"Name": "s", "SecretString": "v1"})
	_, err := call("CreateSecret", map[string]any{"Name": "s", "SecretString": "v2"})
	if f, isFault := err.(*spi.Fault); !isFault || f.Code != "ResourceExistsException" {
		t.Errorf("a second CreateSecret of one name: want ResourceExistsException, got %v", err)
	}
	if got := ok(t, "GetSecretValue", map[string]any{"SecretId": "s"}); got["SecretString"] != "v1" {
		t.Errorf("a second CreateSecret changed the value to %#v", got["SecretString"])
	}
	if versions, _ := ok(t, "ListSecretVersionIds", map[string]any{"SecretId": "s"})["Versions"].([]any); len(versions) != 1 {
		t.Errorf("a second CreateSecret left %d versions, want 1", len(versions))
	}

	for _, missing := range []struct {
		op string
		in map[string]any
	}{
		{"PutSecretValue", map[string]any{"SecretId": "never", "SecretString": "v"}},
		{"UpdateSecret", map[string]any{"SecretId": "never", "SecretString": "v"}},
		{"UpdateSecret", map[string]any{"SecretId": "never", "Description": "d"}},
		{"RotateSecret", map[string]any{"SecretId": "never"}},
	} {
		_, err := call(missing.op, missing.in)
		if f, isFault := err.(*spi.Fault); !isFault || f.Code != "ResourceNotFoundException" {
			t.Errorf("%s %v of a secret never created: want ResourceNotFoundException, got %v", missing.op, missing.in, err)
		}
		if _, err := call("DescribeSecret", map[string]any{"SecretId": "never"}); err == nil {
			t.Errorf("%s created the secret it could not find", missing.op)
			ok(t, "DeleteSecret", map[string]any{"SecretId": "never", "ForceDeleteWithoutRecovery": true})
		}
	}
}

// CreateSecret: "If you include SecretString or SecretBinary then Secrets
// Manager creates an initial secret version and automatically attaches the
// staging label AWSCURRENT to it." Without either it creates the secret and
// no version, so it answers no VersionId, and there is no value to get:
// GetSecretValue is ResourceNotFoundException ("Secrets Manager can't find the
// resource that you asked for"), and BatchGetSecretValue answers an Errors row.
// PutSecretValueRequest.SecretString: "You must include SecretBinary or
// SecretString, but not both." Neither is InvalidParameterException ("The
// parameter name or value is invalid"), the documented error that is about
// the request rather than the secret's state.
func TestSecretsManagerASecretWithoutAValueHasNoVersion(t *testing.T) {
	call, ok, fault := secretsManager(t)
	created := ok(t, "CreateSecret", map[string]any{"Name": "empty", "Description": "filled later"})
	if hasKey(created, "VersionId") {
		t.Fatalf("CreateSecret without a value answered VersionId %#v", created["VersionId"])
	}
	if versions, _ := ok(t, "ListSecretVersionIds", map[string]any{"SecretId": "empty"})["Versions"].([]any); len(versions) != 0 {
		t.Fatalf("CreateSecret without a value made %d versions", len(versions))
	}
	if got := ok(t, "DescribeSecret", map[string]any{"SecretId": "empty"}); got["Description"] != "filled later" {
		t.Fatalf("DescribeSecret answered %#v", got)
	}
	_, err := call("GetSecretValue", map[string]any{"SecretId": "empty"})
	fault(t, err, "ResourceNotFoundException")
	batch := ok(t, "BatchGetSecretValue", map[string]any{"SecretIdList": []any{"empty"}})
	errs, _ := batch["Errors"].([]any)
	if values, _ := batch["SecretValues"].([]any); len(values) != 0 || len(errs) != 1 ||
		errs[0].(map[string]any)["ErrorCode"] != "ResourceNotFoundException" {
		t.Fatalf("BatchGetSecretValue of a secret with no value answered %#v", batch)
	}

	_, err = call("PutSecretValue", map[string]any{"SecretId": "empty"})
	fault(t, err, "InvalidParameterException")

	put := ok(t, "PutSecretValue", map[string]any{"SecretId": "empty", "SecretString": "first"})
	value := ok(t, "GetSecretValue", map[string]any{"SecretId": "empty"})
	if value["SecretString"] != "first" || value["VersionId"] != put["VersionId"] {
		t.Fatalf("after the first PutSecretValue, GetSecretValue answered %#v", value)
	}
	if stages, _ := value["VersionStages"].([]any); len(stages) != 1 || stages[0] != "AWSCURRENT" {
		t.Fatalf("the first version is staged %#v, want AWSCURRENT alone", value["VersionStages"])
	}
}

// The rest of the writes to a secret: staging labels, the resource policy,
// replication and rotation. UpdateSecretVersionStage, PutResourcePolicy,
// DeleteResourcePolicy, ReplicateSecretToRegions, RemoveRegionsFromReplication,
// StopReplicationToReplica and CancelRotateSecret each document
// InvalidRequestException, "A parameter value is not valid for the current
// state of the resource. Possible causes: The secret is scheduled for
// deletion." Each refuses a secret scheduled for deletion and changes nothing
// a read of it shows once it is restored.
func TestSecretsManagerRefusesEveryOtherWriteToASecretScheduledForDeletion(t *testing.T) {
	for _, refused := range []struct {
		op string
		in func(first, second string) map[string]any
	}{
		{"UpdateSecretVersionStage", func(first, second string) map[string]any {
			return map[string]any{"SecretId": "s", "VersionStage": "AWSCURRENT", "MoveToVersionId": first, "RemoveFromVersionId": second}
		}},
		{"PutResourcePolicy", func(string, string) map[string]any {
			return map[string]any{"SecretId": "s", "ResourcePolicy": `{"Version":"2012-10-17","Statement":[{"Effect":"Deny"}]}`}
		}},
		{"DeleteResourcePolicy", func(string, string) map[string]any { return map[string]any{"SecretId": "s"} }},
		{"ReplicateSecretToRegions", func(string, string) map[string]any {
			return map[string]any{"SecretId": "s", "AddReplicaRegions": []any{map[string]any{"Region": "eu-west-1"}}}
		}},
		{"RemoveRegionsFromReplication", func(string, string) map[string]any {
			return map[string]any{"SecretId": "s", "RemoveReplicaRegions": []any{"us-west-2"}}
		}},
		{"StopReplicationToReplica", func(string, string) map[string]any { return map[string]any{"SecretId": "s"} }},
		{"CancelRotateSecret", func(string, string) map[string]any { return map[string]any{"SecretId": "s"} }},
	} {
		t.Run(refused.op, func(t *testing.T) {
			call, ok, fault := secretsManager(t)
			first := ok(t, "CreateSecret", map[string]any{"Name": "s", "SecretString": "v1"})["VersionId"].(string)
			second := ok(t, "PutSecretValue", map[string]any{"SecretId": "s", "SecretString": "v2"})["VersionId"].(string)
			ok(t, "PutResourcePolicy", map[string]any{"SecretId": "s", "ResourcePolicy": `{"Version":"2012-10-17","Statement":[]}`})
			// A replicated primary cannot be scheduled for deletion, so the
			// replica goes before the delete; a refused replication call must
			// still leave none.
			ok(t, "ReplicateSecretToRegions", map[string]any{"SecretId": "s", "AddReplicaRegions": []any{map[string]any{"Region": "us-west-2"}}})
			ok(t, "RemoveRegionsFromReplication", map[string]any{"SecretId": "s", "RemoveReplicaRegions": []any{"us-west-2"}})
			state := func() []map[string]any {
				return []map[string]any{
					ok(t, "DescribeSecret", map[string]any{"SecretId": "s"}),
					ok(t, "GetResourcePolicy", map[string]any{"SecretId": "s"}),
					ok(t, "ListSecretVersionIds", map[string]any{"SecretId": "s"}),
					ok(t, "GetSecretValue", map[string]any{"SecretId": "s"}),
				}
			}
			before := state()
			ok(t, "DeleteSecret", map[string]any{"SecretId": "s"})

			_, err := call(refused.op, refused.in(first, second))
			fault(t, err, "InvalidRequestException")
			if got := ok(t, "DescribeSecret", map[string]any{"SecretId": "s"}); got["DeletedDate"] == nil {
				t.Fatal("the refused call undeleted the secret")
			}
			ok(t, "RestoreSecret", map[string]any{"SecretId": "s"})
			if after := state(); !reflect.DeepEqual(after, before) {
				t.Fatalf("the refused call changed the secret:\nbefore %#v\nafter  %#v", before, after)
			}
		})
	}
}

// CreateSecretRequest.SecretString: "Either SecretString or SecretBinary must
// have a value, but not both." PutSecretValueRequest.SecretString: "You must
// include SecretBinary or SecretString, but not both." UpdateSecretRequest:
// "Either SecretBinary or SecretString must have a value, but not both." Both
// at once is InvalidParameterException, "The parameter name or value is
// invalid": the model names no error for the combination, and this is the one
// about the request rather than the secret's state.
func TestSecretsManagerRefusesAStringAndABinaryTogether(t *testing.T) {
	for _, op := range []string{"CreateSecret", "PutSecretValue", "UpdateSecret"} {
		t.Run(op, func(t *testing.T) {
			call, ok, fault := secretsManager(t)
			ok(t, "CreateSecret", map[string]any{"Name": "existing", "SecretString": "v1"})
			in := map[string]any{"SecretId": "existing", "SecretString": "a", "SecretBinary": "YQ=="}
			if op == "CreateSecret" {
				in = map[string]any{"Name": "fresh", "SecretString": "a", "SecretBinary": "YQ=="}
			}
			_, err := call(op, in)
			fault(t, err, "InvalidParameterException")
			if _, err := call("DescribeSecret", map[string]any{"SecretId": "fresh"}); err == nil {
				t.Fatal("a refused CreateSecret created the secret")
			}
			if got := ok(t, "GetSecretValue", map[string]any{"SecretId": "existing"}); got["SecretString"] != "v1" {
				t.Fatalf("a refused %s changed the value to %#v", op, got["SecretString"])
			}
		})
	}
}

// SecretStringType and SecretBinaryType carry "smithy.api#length": {"min": 1}.
// An empty value is refused the way the engine refuses a member outside its
// length constraint, ValidationException, "must be at least 1 characters".
// The Smithy receiver carries no length traits into the generated model, so
// the engine's own check never sees this one.
func TestSecretsManagerRefusesAnEmptyValue(t *testing.T) {
	for _, tc := range []struct{ op, member string }{
		{"CreateSecret", "SecretString"}, {"CreateSecret", "SecretBinary"},
		{"PutSecretValue", "SecretString"}, {"PutSecretValue", "SecretBinary"},
		{"UpdateSecret", "SecretString"}, {"UpdateSecret", "SecretBinary"},
	} {
		t.Run(tc.op+"/"+tc.member, func(t *testing.T) {
			call, ok, fault := secretsManager(t)
			ok(t, "CreateSecret", map[string]any{"Name": "existing", "SecretString": "v1"})
			in := map[string]any{"SecretId": "existing", tc.member: ""}
			if tc.op == "CreateSecret" {
				in = map[string]any{"Name": "fresh", tc.member: ""}
			}
			_, err := call(tc.op, in)
			fault(t, err, "ValidationException")
			if got := ok(t, "GetSecretValue", map[string]any{"SecretId": "existing"}); got["SecretString"] != "v1" {
				t.Fatalf("a refused %s changed the value to %#v", tc.op, got["SecretString"])
			}
		})
	}
}

// DeleteSecretRequest.RecoveryWindowInDays targets RecoveryWindowInDaysType, a
// "long". A value that is not a whole number is not a long: it is refused as
// the awsJson1_1 codec refuses a body it cannot decode, SerializationException,
// before the window is checked. A whole number written as a JSON float, as
// every JSON decoder hands one over, is still that number.
func TestSecretsManagerRecoveryWindowIsALong(t *testing.T) {
	for _, window := range []any{7.5, 29.9, "7", true, map[string]any{}} {
		t.Run(fmt.Sprintf("%#v", window), func(t *testing.T) {
			call, ok, fault := secretsManager(t)
			ok(t, "CreateSecret", map[string]any{"Name": "s", "SecretString": "v"})
			_, err := call("DeleteSecret", map[string]any{"SecretId": "s", "RecoveryWindowInDays": window})
			fault(t, err, "SerializationException")
			if got := ok(t, "DescribeSecret", map[string]any{"SecretId": "s"}); got["DeletedDate"] != nil {
				t.Fatalf("a refused window scheduled the secret: DeletedDate %#v", got["DeletedDate"])
			}
		})
	}
	_, ok, _ := secretsManager(t)
	ok(t, "CreateSecret", map[string]any{"Name": "s", "SecretString": "v"})
	if got := ok(t, "DeleteSecret", map[string]any{"SecretId": "s", "RecoveryWindowInDays": float64(7)}); !at(got["DeletionDate"], 7*86400) {
		t.Fatalf("a window of 7.0 answered DeletionDate %#v, want 7 days", got["DeletionDate"])
	}
}

// The model's own examples write a timestamp with milliseconds: "CreatedDate":
// 1523477145.713 (GetSecretValue), "LastChangedDate": 1523477145.729
// (ListSecrets). Where the clock has sub-second time the answer carries it, to
// the millisecond.
func TestSecretsManagerTimestampsCarryMilliseconds(t *testing.T) {
	deps := spitest.Deps(t)
	_, ok, _ := secretsManagerOn(t, deps)
	ok(t, "CreateSecret", map[string]any{"Name": "s", "SecretString": "v"})
	ok(t, "CreateSecret", map[string]any{"Name": "f", "SecretString": "v"})
	if err := deps.Clock.Advance(1500 * time.Millisecond); err != nil {
		t.Fatal(err)
	}
	deleted := epochSeconds(t, "DeleteSecret", ok(t, "DeleteSecret", map[string]any{"SecretId": "s"}))
	if got := deleted["DeleteSecret.DeletionDate"]; got != "2592001.5" {
		t.Fatalf("DeletionDate %q, want 2592001.5", got)
	}
	described := epochSeconds(t, "DescribeSecret", ok(t, "DescribeSecret", map[string]any{"SecretId": "s"}))
	if got := described["DescribeSecret.DeletedDate"]; got != "1.5" {
		t.Fatalf("DeletedDate %q, want 1.5", got)
	}
	listed := epochSeconds(t, "ListSecrets", ok(t, "ListSecrets", map[string]any{"IncludePlannedDeletion": true}))
	if got := listed["ListSecrets.SecretList[].DeletedDate"]; got != "1.5" {
		t.Fatalf("SecretListEntry.DeletedDate %q, want 1.5", got)
	}
	forced := epochSeconds(t, "DeleteSecret", ok(t, "DeleteSecret", map[string]any{"SecretId": "f", "ForceDeleteWithoutRecovery": true}))
	if got := forced["DeleteSecret.DeletionDate"]; got != "1.5" {
		t.Fatalf("a forced delete's DeletionDate %q, want 1.5", got)
	}
}

// A ListSecrets entry carries Tags only when the secret has some, as the rest
// of the answers on this service leave out a member with no value; so does
// DescribeSecret once the last tag is removed.
func TestSecretsManagerOmitsTagsWhenThereAreNone(t *testing.T) {
	_, ok, _ := secretsManager(t)
	ok(t, "CreateSecret", map[string]any{"Name": "bare", "SecretString": "v"})
	ok(t, "CreateSecret", map[string]any{"Name": "tagged", "SecretString": "v"})
	ok(t, "TagResource", map[string]any{"SecretId": "tagged", "Tags": []any{map[string]any{"Key": "k", "Value": "v"}}})
	entries := func() map[string]map[string]any {
		byName := map[string]map[string]any{}
		list, _ := ok(t, "ListSecrets", map[string]any{})["SecretList"].([]any)
		for _, e := range list {
			byName[e.(map[string]any)["Name"].(string)] = e.(map[string]any)
		}
		return byName
	}
	listed := entries()
	if hasKey(listed["bare"], "Tags") {
		t.Errorf("ListSecrets answered Tags %#v for a secret with none", listed["bare"]["Tags"])
	}
	if tags, _ := listed["tagged"]["Tags"].([]any); len(tags) != 1 {
		t.Errorf("ListSecrets answered Tags %#v for a secret with one", listed["tagged"]["Tags"])
	}
	ok(t, "UntagResource", map[string]any{"SecretId": "tagged", "TagKeys": []any{"k"}})
	if listed := entries(); hasKey(listed["tagged"], "Tags") {
		t.Errorf("after its last tag was removed, ListSecrets answered Tags %#v", listed["tagged"]["Tags"])
	}
	if got := ok(t, "DescribeSecret", map[string]any{"SecretId": "tagged"}); hasKey(got, "Tags") {
		t.Errorf("after its last tag was removed, DescribeSecret answered Tags %#v", got["Tags"])
	}
}

// SecretListEntry.SecretVersionsToStages: "A version that does not have any
// SecretVersionStage is considered deprecated and subject to deletion. Such
// versions are not included in this list." DescribeSecretResponse.
// VersionIdsToStages: "A list of the versions of the secret that have staging
// labels attached."
func TestSecretsManagerStageMapsLeaveOutDeprecatedVersions(t *testing.T) {
	_, ok, _ := secretsManager(t)
	first := ok(t, "CreateSecret", map[string]any{"Name": "s", "SecretString": "v1"})["VersionId"].(string)
	ok(t, "PutSecretValue", map[string]any{"SecretId": "s", "SecretString": "v2"})
	ok(t, "PutSecretValue", map[string]any{"SecretId": "s", "SecretString": "v3"})
	// v1 lost AWSPREVIOUS to v2 and has no label left.
	described := ok(t, "DescribeSecret", map[string]any{"SecretId": "s"})["VersionIdsToStages"].(map[string]any)
	if _, has := described[first]; has || len(described) != 2 {
		t.Errorf("DescribeSecret VersionIdsToStages = %#v, want the two labelled versions only", described)
	}
	listed := ok(t, "ListSecrets", map[string]any{})["SecretList"].([]any)[0].(map[string]any)["SecretVersionsToStages"].(map[string]any)
	if _, has := listed[first]; has || len(listed) != 2 {
		t.Errorf("ListSecrets SecretVersionsToStages = %#v, want the two labelled versions only", listed)
	}
}

// DeleteSecret: "You can't delete a primary secret that is replicated to other
// Regions. You must first delete the replicas using RemoveRegionsFromReplication,
// and then delete the primary secret." The refusal is the model's error for the
// secret's current state, InvalidRequestException, forced or not.
func TestSecretsManagerRefusesToDeleteAReplicatedPrimary(t *testing.T) {
	for _, force := range []bool{false, true} {
		t.Run(fmt.Sprintf("force=%v", force), func(t *testing.T) {
			call, ok, fault := secretsManager(t)
			ok(t, "CreateSecret", map[string]any{"Name": "s", "SecretString": "v"})
			ok(t, "ReplicateSecretToRegions", map[string]any{"SecretId": "s", "AddReplicaRegions": []any{map[string]any{"Region": "us-west-2"}}})
			in := map[string]any{"SecretId": "s"}
			if force {
				in["ForceDeleteWithoutRecovery"] = true
			}
			_, err := call("DeleteSecret", in)
			fault(t, err, "InvalidRequestException")
			if got := ok(t, "DescribeSecret", map[string]any{"SecretId": "s"}); got["DeletedDate"] != nil {
				t.Fatalf("a refused delete scheduled the secret: %#v", got)
			}
			ok(t, "RemoveRegionsFromReplication", map[string]any{"SecretId": "s", "RemoveReplicaRegions": []any{"us-west-2"}})
			ok(t, "DeleteSecret", in)
		})
	}
}

// BatchGetSecretValueRequest.Filters: "The filters to choose which secrets to
// retrieve. You must include Filters or SecretIdList, but not both." MaxResults:
// "To use this parameter, you must also use the Filters parameter." The
// filters are ListSecrets' (Filter.Key): "description: Prefix match, not
// case-sensitive. name: Prefix match, case-sensitive. tag-key ... tag-value:
// Prefix match, case-sensitive ... all: Breaks the filter value string into
// words and then searches all attributes for matches. Not case-sensitive."
// Filter.Values: "You can prefix your search value with an exclamation mark
// (!) in order to perform negation filters."
func TestSecretsManagerFiltersChooseSecrets(t *testing.T) {
	call, ok, fault := secretsManager(t)
	tag := func(k, v string) []any { return []any{map[string]any{"Key": k, "Value": v}} }
	ok(t, "CreateSecret", map[string]any{"Name": "app/db", "SecretString": "db", "Description": "Primary database"})
	ok(t, "TagResource", map[string]any{"SecretId": "app/db", "Tags": tag("env", "prod")})
	ok(t, "CreateSecret", map[string]any{"Name": "app/cache", "SecretString": "cache", "Description": "cache"})
	ok(t, "TagResource", map[string]any{"SecretId": "app/cache", "Tags": tag("env", "dev")})
	ok(t, "CreateSecret", map[string]any{"Name": "app/empty"})
	ok(t, "CreateSecret", map[string]any{"Name": "other", "SecretString": "other"})
	ok(t, "CreateSecret", map[string]any{"Name": "app/gone", "SecretString": "gone"})
	ok(t, "DeleteSecret", map[string]any{"SecretId": "app/gone"})

	filter := func(key string, values ...any) []any {
		return []any{map[string]any{"Key": key, "Values": values}}
	}
	names := func(list any, member string) []string {
		var out []string
		rows, _ := list.([]any)
		for _, r := range rows {
			out = append(out, fmt.Sprint(r.(map[string]any)[member]))
		}
		return out
	}
	batch := func(filters []any) ([]string, []string) {
		t.Helper()
		got := ok(t, "BatchGetSecretValue", map[string]any{"Filters": filters})
		return names(got["SecretValues"], "Name"), names(got["Errors"], "SecretId")
	}
	for _, tc := range []struct {
		filters      []any
		values, errs []string
	}{
		// A secret with no value is an Errors row, as GetSecretValue would
		// answer it; one scheduled for deletion is not listed.
		{filter("name", "app/"), []string{"app/cache", "app/db"}, []string{"app/empty"}},
		{filter("name", "App/"), nil, nil},
		{filter("name", "!app/"), []string{"other"}, nil},
		{filter("description", "PRIMARY"), []string{"app/db"}, nil},
		{filter("tag-key", "en"), []string{"app/cache", "app/db"}, nil},
		{filter("tag-value", "prod"), []string{"app/db"}, nil},
		{filter("tag-value", "prod", "dev"), []string{"app/cache", "app/db"}, nil},
		{append(filter("name", "app/"), filter("tag-value", "dev")...), []string{"app/cache"}, nil},
		{filter("all", "PROD database"), []string{"app/db"}, nil},
		{filter("primary-region", "us-east"), []string{"app/cache", "app/db", "other"}, []string{"app/empty"}},
	} {
		values, errs := batch(tc.filters)
		if !reflect.DeepEqual(values, tc.values) || !reflect.DeepEqual(errs, tc.errs) {
			t.Errorf("Filters %v: SecretValues %v Errors %v, want %v and %v", tc.filters, values, errs, tc.values, tc.errs)
		}
	}
	if got, _ := ok(t, "BatchGetSecretValue", map[string]any{"Filters": filter("name", "app/db")})["SecretValues"].([]any); len(got) != 1 || got[0].(map[string]any)["SecretString"] != "db" {
		t.Errorf("the filter form answered %#v, want the value", got)
	}

	// The filter form pages as ListSecrets does.
	first := ok(t, "BatchGetSecretValue", map[string]any{"Filters": filter("name", "app/"), "MaxResults": 1})
	if token, _ := first["NextToken"].(string); token == "" || len(names(first["SecretValues"], "Name"))+len(names(first["Errors"], "SecretId")) != 1 {
		t.Fatalf("MaxResults 1 answered %#v, want one secret and a NextToken", first)
	}
	seen := 1
	for token, pages := first["NextToken"], 0; token != nil && token != ""; pages++ {
		if pages > 10 {
			t.Fatal("still paging after 10 pages")
		}
		page := ok(t, "BatchGetSecretValue", map[string]any{"Filters": filter("name", "app/"), "MaxResults": 1, "NextToken": token})
		seen += len(names(page["SecretValues"], "Name")) + len(names(page["Errors"], "SecretId"))
		token = page["NextToken"]
	}
	if seen != 3 {
		t.Errorf("paging through name app/ saw %d secrets, want 3", seen)
	}

	// The same filters choose what ListSecrets lists.
	if listed := names(ok(t, "ListSecrets", map[string]any{"Filters": filter("name", "app/")})["SecretList"], "Name"); !reflect.DeepEqual(listed, []string{"app/cache", "app/db", "app/empty"}) {
		t.Errorf("ListSecrets name app/ listed %v", listed)
	}

	_, err := call("BatchGetSecretValue", map[string]any{"Filters": filter("name", "app/"), "SecretIdList": []any{"other"}})
	fault(t, err, "InvalidParameterException")
	_, err = call("BatchGetSecretValue", map[string]any{})
	fault(t, err, "InvalidParameterException")

	// The SecretIdList form answers each id it names, and only the filter
	// form pages (TestSecretsManagerBatchGetSecretIdListNeverPages).
	rows := ok(t, "BatchGetSecretValue", map[string]any{"SecretIdList": []any{"app/db", "app/gone", "missing"}})
	if values, errs := names(rows["SecretValues"], "Name"), names(rows["Errors"], "ErrorCode"); !reflect.DeepEqual(values, []string{"app/db"}) ||
		!reflect.DeepEqual(errs, []string{"InvalidRequestException", "ResourceNotFoundException"}) {
		t.Errorf("SecretIdList answered %v and %v", values, errs)
	}
}

// GetRandomPassword: "By default, Secrets Manager uses uppercase and lowercase
// letters, numbers, and the following characters in passwords:
// !\"#$%&'()*+,-./:;?@[\\]^_`{|}~". PasswordLength: "If you don't include this
// parameter, the default length is 32 characters." ExcludeCharacters: "A string
// of the characters that you don't want in the password." ExcludeLowercase,
// ExcludeUppercase, ExcludeNumbers and ExcludePunctuation each drop a class;
// IncludeSpace: "the password can contain space characters".
// RequireEachIncludedType: "If you don't include this switch, the password
// contains at least one of every character type." PasswordLengthType's range
// is 1 to 4096.
func TestSecretsManagerGetRandomPasswordHonoursItsParameters(t *testing.T) {
	call, ok, fault := secretsManager(t)
	const (
		lower       = "abcdefghijklmnopqrstuvwxyz"
		upper       = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
		digits      = "0123456789"
		punctuation = "!\"#$%&'()*+,-./:;?@[\\]^_`{|}~"
	)
	password := func(in map[string]any) string {
		t.Helper()
		pw, _ := ok(t, "GetRandomPassword", in)["RandomPassword"].(string)
		return pw
	}
	has := func(pw, set string) bool { return strings.ContainsAny(pw, set) }

	pw := password(map[string]any{})
	if len(pw) != 32 {
		t.Fatalf("the default password %q has %d characters, want 32", pw, len(pw))
	}
	for _, c := range pw {
		if !strings.ContainsRune(lower+upper+digits+punctuation, c) {
			t.Fatalf("the default password %q holds %q, outside the documented characters", pw, c)
		}
	}
	for _, set := range []string{lower, upper, digits, punctuation} {
		if !has(pw, set) {
			t.Fatalf("the default password %q has none of %q", pw, set)
		}
	}
	if pw := password(map[string]any{"PasswordLength": 16}); len(pw) != 16 {
		t.Fatalf("PasswordLength 16 answered %q", pw)
	}
	if pw := password(map[string]any{"PasswordLength": 200, "ExcludeCharacters": "abcABC123!@"}); has(pw, "abcABC123!@") {
		t.Fatalf("ExcludeCharacters left one in %q", pw)
	}
	for member, set := range map[string]string{"ExcludeLowercase": lower, "ExcludeUppercase": upper, "ExcludeNumbers": digits, "ExcludePunctuation": punctuation} {
		if pw := password(map[string]any{"PasswordLength": 200, member: true}); has(pw, set) || len(pw) != 200 {
			t.Fatalf("%s answered %q", member, pw)
		}
	}
	if pw := password(map[string]any{"PasswordLength": 1, "RequireEachIncludedType": false}); len(pw) != 1 {
		t.Fatalf("a one-character password answered %q", pw)
	}
	for _, in := range []map[string]any{
		{"PasswordLength": 0},
		{"PasswordLength": 4097},
		{"PasswordLength": 3}, // four types are required by default
		{"ExcludeLowercase": true, "ExcludeUppercase": true, "ExcludeNumbers": true, "ExcludePunctuation": true},
		{"ExcludeNumbers": true, "ExcludePunctuation": true, "ExcludeUppercase": true, "ExcludeCharacters": lower},
	} {
		_, err := call("GetRandomPassword", in)
		fault(t, err, "InvalidParameterException")
	}
}

// ticking is a clock that moves a millisecond every time it is read, as a
// real one may between two readings within one request.
type ticking struct{ spi.Clock }

func (c ticking) Now() time.Time {
	now := c.Clock.Now()
	_ = c.Clock.Advance(time.Millisecond)
	return now
}

// DeleteSecretResponse.DeletionDate: "the date and time of the delete request
// plus the number of days in RecoveryWindowInDays". DeletedDate is that same
// request time (the bundle's reading), so the two differ by exactly the window
// however the clock moves while the request is served.
func TestSecretsManagerDeletionDatesComeFromOneRequestTime(t *testing.T) {
	deps := spitest.Deps(t)
	deps.Clock = ticking{deps.Clock}
	_, ok, _ := secretsManagerOn(t, deps)
	ok(t, "CreateSecret", map[string]any{"Name": "s", "SecretString": "v"})
	deletion, _ := ok(t, "DeleteSecret", map[string]any{"SecretId": "s", "RecoveryWindowInDays": 7})["DeletionDate"].(float64)
	deleted, _ := ok(t, "DescribeSecret", map[string]any{"SecretId": "s"})["DeletedDate"].(float64)
	if deletion-deleted != 7*86400 {
		t.Fatalf("DeletionDate %v - DeletedDate %v = %v, want exactly 7 days (604800)", deletion, deleted, deletion-deleted)
	}
}

// BatchGetSecretValue's MaxResults: "To use this parameter, you must also use
// the Filters parameter." The SecretIdList form with MaxResults, or with a
// NextToken, which only the Filters form answers, is
// InvalidParameterException, and the SecretIdList form never answers a
// NextToken. MaxResults outside MaxResultsBatchType's range, 1 to 20, is
// ValidationException. Before, SecretIdList with MaxResults answered a
// NextToken whenever more secrets were stored than the page held, and every
// page repeated the same values, so a caller following it paged forever.
func TestSecretsManagerBatchGetSecretIdListNeverPages(t *testing.T) {
	call, ok, _ := secretsManager(t)
	var ids []any
	for i := range 25 {
		ok(t, "CreateSecret", map[string]any{"Name": fmt.Sprintf("s%02d", i), "SecretString": "v"})
		ids = append(ids, fmt.Sprintf("s%02d", i))
	}
	code := func(err error) string {
		if f, isFault := err.(*spi.Fault); isFault {
			return f.Code
		}
		return ""
	}
	for _, tc := range []struct {
		in   map[string]any
		want string
	}{
		{map[string]any{"SecretIdList": ids[:2], "MaxResults": 1}, "InvalidParameterException"},
		{map[string]any{"SecretIdList": ids[:2], "NextToken": "s00"}, "InvalidParameterException"},
		{map[string]any{"Filters": []any{map[string]any{"Key": "name", "Values": []any{"s"}}}, "MaxResults": 0}, "ValidationException"},
		{map[string]any{"Filters": []any{map[string]any{"Key": "name", "Values": []any{"s"}}}, "MaxResults": 21}, "ValidationException"},
		{map[string]any{"SecretIdList": ids[:2], "MaxResults": 21}, "ValidationException"},
	} {
		if out, err := call("BatchGetSecretValue", tc.in); code(err) != tc.want {
			t.Errorf("BatchGetSecretValue %v answered %v (%v), want %s", tc.in, out, err, tc.want)
		}
	}
	rows := ok(t, "BatchGetSecretValue", map[string]any{"SecretIdList": ids})
	if values, _ := rows["SecretValues"].([]any); len(values) != 25 || rows["NextToken"] != nil {
		t.Errorf("SecretIdList of 25 answered %d values and NextToken %v, want all 25 and none", len(values), rows["NextToken"])
	}
	page := ok(t, "BatchGetSecretValue", map[string]any{"Filters": []any{map[string]any{"Key": "name", "Values": []any{"s"}}}, "MaxResults": 20})
	if values, _ := page["SecretValues"].([]any); len(values) != 20 || page["NextToken"] == nil {
		t.Errorf("Filters with MaxResults 20 answered %d values and NextToken %v, want 20 and a token", len(values), page["NextToken"])
	}
}

// IncludeSpace: "If you include this switch, the password can contain space
// characters." RequireEachIncludedType asks for "at least one upper and
// lowercase letter, one number, and one punctuation", not a space. So a space
// may be drawn but is never required: {"PasswordLength": 4, "IncludeSpace":
// true} is one of each of the four types, and a one-character password whose
// only type is punctuation is a punctuation mark. Before, the space counted
// as a fifth required type, which refused both.
func TestSecretsManagerIncludeSpaceNeverRequiresASpace(t *testing.T) {
	call, _, _ := secretsManager(t)
	const punctuation = "!\"#$%&'()*+,-./:;?@[\\]^_`{|}~"
	password := func(in map[string]any) string {
		t.Helper()
		out, err := call("GetRandomPassword", in)
		if err != nil {
			t.Errorf("GetRandomPassword %v: %v", in, err)
			return ""
		}
		return fmt.Sprint(out["RandomPassword"])
	}
	for range 20 {
		if pw := password(map[string]any{"PasswordLength": 4, "IncludeSpace": true}); len(pw) == 4 && (!strings.ContainsAny(pw, "abcdefghijklmnopqrstuvwxyz") ||
			!strings.ContainsAny(pw, "ABCDEFGHIJKLMNOPQRSTUVWXYZ") || !strings.ContainsAny(pw, "0123456789") || !strings.ContainsAny(pw, punctuation)) {
			t.Errorf("PasswordLength 4 with IncludeSpace answered %q, want one of each of the four types", pw)
		}
		one := map[string]any{"PasswordLength": 1, "IncludeSpace": true, "ExcludeLowercase": true, "ExcludeUppercase": true, "ExcludeNumbers": true}
		if pw := password(one); pw != "" && !strings.Contains(punctuation, pw) {
			t.Errorf("a one-character punctuation password with IncludeSpace answered %q", pw)
		}
	}
	if pw := password(map[string]any{"PasswordLength": 4096, "IncludeSpace": true}); !strings.Contains(pw, " ") {
		t.Errorf("IncludeSpace drew no space in 4096 characters")
	}
}
