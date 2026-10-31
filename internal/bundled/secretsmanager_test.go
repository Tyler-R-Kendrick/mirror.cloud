package bundled_test

import (
	"context"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/bundled"
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
	if deleted["DeletionDate"] != "1970-01-31T00:00:00Z" || deleted["Name"] != "s" ||
		deleted["ARN"] != "arn:aws:secretsmanager:us-east-1:000000000000:secret:s" {
		t.Fatalf("DeleteSecret answered %#v, want Name, ARN and the request time plus 30 days", deleted)
	}
	described, err := call("DescribeSecret", map[string]any{"SecretId": "s"})
	if err != nil {
		t.Fatalf("DescribeSecret of a secret scheduled for deletion faulted: %v", err)
	}
	if described["DeletedDate"] != "1970-01-31T00:00:00Z" || described["Name"] != "s" {
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
	if week["DeletionDate"] != "1970-01-08T00:00:00Z" {
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
	if got := ok(t, "DescribeSecret", map[string]any{"SecretId": "s"}); got["DeletedDate"] != "1970-01-31T00:00:00Z" {
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
