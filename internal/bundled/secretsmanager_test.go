package bundled_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"testing"

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
	pack, err := bundled.New("aws.secretsmanager", spitest.Deps(t))
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
	if !at(described["DeletedDate"], 30*86400) || described["Name"] != "s" {
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
	call, ok, fault := secretsManager(t)
	ok(t, "CreateSecret", map[string]any{"Name": "s", "SecretString": "v"})
	ok(t, "DeleteSecret", map[string]any{"SecretId": "s"})

	_, err := call("DeleteSecret", map[string]any{"SecretId": "s"})
	fault(t, err, "InvalidRequestException")
	if got := ok(t, "DescribeSecret", map[string]any{"SecretId": "s"}); !at(got["DeletedDate"], 30*86400) {
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
