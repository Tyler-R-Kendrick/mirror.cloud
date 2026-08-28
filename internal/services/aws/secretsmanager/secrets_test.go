package secretsmanager

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spitest"
)

func TestCreateGetDeleteRestore(t *testing.T) {
	p := &Pack{deps: spitest.Deps(t)}
	ctx := context.Background()
	id := spi.Identity{Account: "a", Region: "r"}
	created, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateSecret", Input: map[string]any{"Name": "n", "SecretString": "v"}})
	if err != nil {
		t.Fatal(err)
	}
	got, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "GetSecretValue", Input: map[string]any{"SecretId": created.Output["ARN"]}})
	if err != nil {
		t.Fatal(err)
	}
	if got.Output["SecretString"] != "v" {
		t.Fatalf("%v", got.Output)
	}
	_, err = p.Invoke(ctx, &spi.Request{Identity: id, Operation: "DeleteSecret", Input: map[string]any{"SecretId": "n"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "GetSecretValue", Input: map[string]any{"SecretId": "n"}}); err == nil {
		t.Fatal("expected deleted")
	}
	_, err = p.Invoke(ctx, &spi.Request{Identity: id, Operation: "RestoreSecret", Input: map[string]any{"SecretId": "n"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "GetSecretValue", Input: map[string]any{"SecretId": "n"}}); err != nil {
		t.Fatal(err)
	}
}

func TestSecretsManagerOperationsAndMetadata(t *testing.T) {
	ctx := context.Background()
	deps := spitest.Deps(t)
	p := New(deps)
	id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
	call := func(operation string, input map[string]any) (*spi.Response, error) {
		t.Helper()
		return p.Invoke(ctx, &spi.Request{Identity: id, Operation: operation, Input: input})
	}
	must := func(operation string, input map[string]any) *spi.Response {
		t.Helper()
		response, err := call(operation, input)
		if err != nil {
			t.Fatalf("%s: %v", operation, err)
		}
		return response
	}
	if p.ServiceID() != "aws.secretsmanager" || p.Tier() != model.TierEmulate || len(p.Operations()) != 23 {
		t.Fatalf("Secrets Manager metadata %s %s %d", p.ServiceID(), p.Tier(), len(p.Operations()))
	}
	created := must("CreateSecret", map[string]any{"Name": "primary", "SecretString": "one"})
	arn := created.Output["ARN"].(string)
	versionOne := created.Output["VersionId"].(string)
	must("TagResource", map[string]any{"SecretId": arn, "Tags": []any{
		map[string]any{"Key": "env", "Value": "dev"}, map[string]any{"Key": "team", "Value": "platform"},
	}})
	must("TagResource", map[string]any{"SecretId": "primary", "Tags": []any{map[string]any{"Key": "env", "Value": "prod"}}})
	must("UntagResource", map[string]any{"SecretId": "primary", "TagKeys": []any{"team", "missing"}})
	description := must("DescribeSecret", map[string]any{"SecretId": arn}).Output
	tags := description["Tags"].([]any)
	if description["SecretString"] != nil || len(tags) != 1 || asMap(tags[0])["Value"] != "prod" || len(description["VersionIdsToStages"].(map[string]any)) != 1 {
		t.Fatalf("secret description %#v", description)
	}
	listed := must("ListSecrets", nil).Output["SecretList"].([]any)
	if len(listed) != 1 || len(asMap(listed[0])["Tags"].([]any)) != 1 || len(asMap(listed[0])["SecretVersionsToStages"].(map[string]any)) != 1 {
		t.Fatalf("secret list %#v", listed)
	}
	if _, err := call("TagResource", map[string]any{"SecretId": "missing", "Tags": []any{}}); err == nil {
		t.Fatal("tagged missing secret")
	}
	if _, err := call("UntagResource", map[string]any{"SecretId": "missing", "TagKeys": []any{"x"}}); err == nil {
		t.Fatal("untagged missing secret")
	}

	legacy := must("CreateSecret", map[string]any{"Name": "legacy", "SecretString": "old"})
	_ = p.col(&spi.Request{Identity: id}).Put(ctx, "legacy:tags", mustJSON([]any{map[string]any{"Key": "old", "Value": "tag"}}))
	must("TagResource", map[string]any{"SecretId": legacy.Output["ARN"], "Tags": []any{map[string]any{"Key": "new", "Value": "tag"}}})
	if tags := must("DescribeSecret", map[string]any{"SecretId": "legacy"}).Output["Tags"].([]any); len(tags) != 2 {
		t.Fatalf("migrated tags %#v", tags)
	}

	put := must("PutSecretValue", map[string]any{"SecretId": arn, "SecretString": "two"})
	versionTwo := put.Output["VersionId"].(string)
	if previous := must("GetSecretValue", map[string]any{"SecretId": arn, "VersionStage": "AWSPREVIOUS"}).Output; previous["SecretString"] != "one" || previous["VersionId"] != versionOne {
		t.Fatalf("previous version %#v", previous)
	}
	updated := must("UpdateSecret", map[string]any{"SecretId": "primary", "SecretString": "three"})
	versionThree := updated.Output["VersionId"].(string)
	if current := must("GetSecretValue", map[string]any{"SecretId": arn}).Output; current["SecretString"] != "three" || current["VersionId"] != versionThree {
		t.Fatalf("current version %#v", current)
	}
	if exact := must("GetSecretValue", map[string]any{"SecretId": arn, "VersionId": versionTwo}).Output; exact["SecretString"] != "two" {
		t.Fatalf("exact version %#v", exact)
	}
	if versions := must("ListSecretVersionIds", map[string]any{"SecretId": arn}).Output["Versions"].([]any); len(versions) != 3 {
		t.Fatalf("versions %#v", versions)
	}
	must("UpdateSecretVersionStage", map[string]any{"SecretId": arn, "VersionStage": "BLUE", "MoveToVersionId": versionOne})
	must("UpdateSecretVersionStage", map[string]any{"SecretId": arn, "VersionStage": "BLUE", "MoveToVersionId": versionTwo, "RemoveFromVersionId": versionOne})
	if blue := must("GetSecretValue", map[string]any{"SecretId": arn, "VersionStage": "BLUE"}).Output; blue["VersionId"] != versionTwo {
		t.Fatalf("BLUE version %#v", blue)
	}

	batch := must("BatchGetSecretValue", map[string]any{"SecretIdList": []any{"primary", "missing"}}).Output
	if len(batch["SecretValues"].([]any)) != 1 || len(batch["Errors"].([]any)) != 1 {
		t.Fatalf("batch values %#v", batch)
	}
	if single := must("BatchGetSecretValue", map[string]any{"SecretId": "primary"}).Output["SecretValues"].([]any); len(single) != 1 {
		t.Fatalf("single batch %#v", single)
	}
	policy := `{"Version":"2012-10-17","Statement":[]}`
	must("PutResourcePolicy", map[string]any{"SecretId": arn, "ResourcePolicy": policy})
	if got := must("GetResourcePolicy", map[string]any{"SecretId": "primary"}).Output["ResourcePolicy"]; got != policy {
		t.Fatalf("resource policy %q", got)
	}
	must("DeleteResourcePolicy", map[string]any{"SecretId": arn})
	if got := must("GetResourcePolicy", map[string]any{"SecretId": arn}).Output["ResourcePolicy"]; got != nil {
		t.Fatalf("deleted resource policy %#v", got)
	}
	if _, err := call("PutResourcePolicy", map[string]any{"SecretId": "missing", "ResourcePolicy": policy}); err == nil {
		t.Fatal("put policy on missing secret")
	}
	if _, err := call("ValidateResourcePolicy", map[string]any{"ResourcePolicy": "{"}); err == nil {
		t.Fatal("validated malformed policy")
	}
	if errors := must("ValidateResourcePolicy", map[string]any{"ResourcePolicy": policy}).Output["ValidationErrors"].([]any); len(errors) != 0 {
		t.Fatalf("policy validation %#v", errors)
	}

	replicated := must("ReplicateSecretToRegions", map[string]any{"SecretId": arn, "AddReplicaRegions": []any{
		map[string]any{"Region": "us-west-1"}, map[string]any{},
	}, "Region": "us-west-2"}).Output["ReplicationStatus"].([]any)
	if len(replicated) != 2 {
		t.Fatalf("replicas %#v", replicated)
	}
	remaining := must("RemoveRegionsFromReplication", map[string]any{"SecretId": arn, "RemoveReplicaRegions": []any{"us-west-1"}}).Output["ReplicationStatus"].([]any)
	if len(remaining) != 1 || asMap(remaining[0])["Region"] != "us-west-2" {
		t.Fatalf("remaining replicas %#v", remaining)
	}
	if described := must("DescribeSecret", map[string]any{"SecretId": arn}).Output["ReplicationStatus"].([]any); len(described) != 1 {
		t.Fatalf("described replicas %#v", described)
	}
	if stopped := must("StopReplicationToReplica", map[string]any{"SecretId": "primary"}).Output["ARN"]; stopped != arn {
		t.Fatalf("stopped replica %#v", stopped)
	}
	rotated := must("RotateSecret", map[string]any{"SecretId": arn})
	if rotated.Output["VersionId"] == nil || must("GetSecretValue", map[string]any{"SecretId": arn}).Output["SecretString"] != "three" {
		t.Fatalf("rotation %#v", rotated.Output)
	}
	must("CancelRotateSecret", map[string]any{"SecretId": arn})

	if password := must("GetRandomPassword", nil).Output["RandomPassword"].(string); len(password) != 32 {
		t.Fatalf("random password %q", password)
	}
	must("DeleteSecret", map[string]any{"SecretId": arn})
	if secrets := must("ListSecrets", nil).Output["SecretList"].([]any); len(secrets) != 1 {
		t.Fatalf("planned deletion was listed %#v", secrets)
	}
	if secrets := must("ListSecrets", map[string]any{"IncludePlannedDeletion": "True"}).Output["SecretList"].([]any); len(secrets) != 2 {
		t.Fatalf("planned deletion was omitted %#v", secrets)
	}
	must("RestoreSecret", map[string]any{"SecretId": "primary"})
	must("DeleteSecret", map[string]any{"SecretId": "legacy", "ForceDeleteWithoutRecovery": "True"})
	if _, err := call("RestoreSecret", map[string]any{"SecretId": "legacy"}); err == nil {
		t.Fatal("restored force-deleted secret")
	}
	if _, err := call("DeleteSecret", map[string]any{"SecretId": "missing"}); err == nil {
		t.Fatal("deleted missing secret")
	}
	if _, err := call("Unknown", nil); err == nil || !strings.Contains(err.Error(), "MirrorNotImplemented") {
		t.Fatalf("unknown operation %v", err)
	}
}

func mustJSON(value any) []byte {
	encoded, _ := json.Marshal(value)
	return encoded
}
