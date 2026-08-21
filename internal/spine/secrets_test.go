package spine

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/config"
	rtpkg "github.com/tyler-r-kendrick/mirror.cloud/internal/runtime"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/secretsmanager"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spitest"
)

func TestBootedServerSecretsSection48(t *testing.T) {
	cfg := config.Default()
	cfg.Services = []string{"aws.secretsmanager"}
	cfg.Seed = "sec-48"
	rt, err := rtpkg.Boot(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(rt.Handler())
	defer ts.Close()
	auth := "AWS4-HMAC-SHA256 Credential=test/20200101/us-east-1/secretsmanager/aws4_request, SignedHeaders=host, Signature=00"
	call := func(op, body string) (int, map[string]any) {
		t.Helper()
		req, _ := http.NewRequest(http.MethodPost, ts.URL+"/", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/x-amz-json-1.1")
		req.Header.Set("X-Amz-Target", "secretsmanager."+op)
		req.Header.Set("Authorization", auth)
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		raw, _ := io.ReadAll(res.Body)
		res.Body.Close()
		out := map[string]any{}
		_ = json.Unmarshal(raw, &out)
		if res.StatusCode >= 300 && op != "GetSecretValue" && op != "DescribeSecret" {
			t.Fatalf("%s %d %s", op, res.StatusCode, raw)
		}
		if res.StatusCode < 300 && res.Header.Get("x-mirror-fidelity") != "emulate" {
			t.Fatalf("fidelity %q %s", res.Header.Get("x-mirror-fidelity"), op)
		}
		return res.StatusCode, out
	}

	_, created := call("CreateSecret", `{"Name":"n","SecretString":"v1"}`)
	if str(created["Name"]) != "n" || str(created["VersionId"]) == "" {
		t.Fatalf("create %v", created)
	}
	v1 := str(created["VersionId"])
	_, got := call("GetSecretValue", `{"SecretId":"n"}`)
	if str(got["SecretString"]) != "v1" {
		t.Fatalf("get %v", got)
	}
	_, put := call("PutSecretValue", `{"SecretId":"n","SecretString":"v2"}`)
	v2 := str(put["VersionId"])
	if v2 == "" || v2 == v1 {
		t.Fatalf("put version %v", put)
	}
	_, cur := call("GetSecretValue", `{"SecretId":"n"}`)
	if str(cur["SecretString"]) != "v2" {
		t.Fatalf("current %v", cur)
	}
	_, prev := call("GetSecretValue", `{"SecretId":"n","VersionStage":"AWSPREVIOUS"}`)
	if str(prev["SecretString"]) != "v1" {
		t.Fatalf("previous %v", prev)
	}
	_, byID := call("GetSecretValue", `{"SecretId":"n","VersionId":"`+v1+`"}`)
	if str(byID["SecretString"]) != "v1" {
		t.Fatalf("by id %v", byID)
	}
	_, listed := call("ListSecretVersionIds", `{"SecretId":"n"}`)
	if len(asSlice(listed["Versions"])) != 2 {
		t.Fatalf("versions %v", listed)
	}
	call("UpdateSecret", `{"SecretId":"n","SecretString":"v3"}`)
	_, afterUp := call("GetSecretValue", `{"SecretId":"n"}`)
	if str(afterUp["SecretString"]) != "v3" {
		t.Fatalf("update %v", afterUp)
	}
	_, desc := call("DescribeSecret", `{"SecretId":"n"}`)
	if str(desc["Name"]) != "n" {
		t.Fatalf("describe %v", desc)
	}
	_, ls := call("ListSecrets", `{}`)
	if len(asSlice(ls["SecretList"])) == 0 {
		t.Fatalf("list %v", ls)
	}
	call("TagResource", `{"SecretId":"n","Tags":[{"Key":"k","Value":"v"}]}`)
	call("UntagResource", `{"SecretId":"n"}`)
	_, pw := call("GetRandomPassword", `{}`)
	if str(pw["RandomPassword"]) == "" {
		t.Fatalf("password %v", pw)
	}

	call("DeleteSecret", `{"SecretId":"n"}`)
	code, del := call("GetSecretValue", `{"SecretId":"n"}`)
	if code < 300 {
		t.Fatalf("deleted still readable %d %v", code, del)
	}
	call("RestoreSecret", `{"SecretId":"n"}`)
	_, restored := call("GetSecretValue", `{"SecretId":"n"}`)
	if str(restored["SecretString"]) != "v3" {
		t.Fatalf("restore %v", restored)
	}
	_, batch := call("BatchGetSecretValue", `{"SecretIdList":["n"]}`)
	vals := asSlice(batch["SecretValues"])
	if len(vals) != 1 || str(asM(vals[0])["SecretString"]) != "v3" {
		t.Fatalf("batch %v", batch)
	}
	call("PutResourcePolicy", `{"SecretId":"n","ResourcePolicy":"{\"Version\":\"2012-10-17\"}"}`)
	_, pol := call("GetResourcePolicy", `{"SecretId":"n"}`)
	if !strings.Contains(str(pol["ResourcePolicy"]), "2012-10-17") {
		t.Fatalf("policy %v", pol)
	}
	call("ValidateResourcePolicy", `{"SecretId":"n","ResourcePolicy":"{\"Version\":\"2012-10-17\"}"}`)
	call("DeleteResourcePolicy", `{"SecretId":"n"}`)
	_, emptyPol := call("GetResourcePolicy", `{"SecretId":"n"}`)
	if str(emptyPol["ResourcePolicy"]) != "" {
		t.Fatalf("policy leftover %v", emptyPol)
	}
	call("ReplicateSecretToRegions", `{"SecretId":"n","AddReplicaRegions":[{"Region":"us-west-2"}]}`)
	call("RemoveRegionsFromReplication", `{"SecretId":"n","RemoveReplicaRegions":["us-west-2"]}`)
	call("StopReplicationToReplica", `{"SecretId":"n"}`)
	_, rot := call("RotateSecret", `{"SecretId":"n"}`)
	if str(rot["VersionId"]) == "" {
		t.Fatalf("rotate %v", rot)
	}
	call("CancelRotateSecret", `{"SecretId":"n"}`)
	_, afterRot := call("GetSecretValue", `{"SecretId":"n"}`)
	if str(afterRot["SecretString"]) != "v3" {
		t.Fatalf("rotated value %v", afterRot)
	}
	call("UpdateSecretVersionStage", `{"SecretId":"n","VersionStage":"AWSCURRENT","MoveToVersionId":"`+v1+`"}`)
	call("DeleteSecret", `{"SecretId":"n","ForceDeleteWithoutRecovery":true}`)
	code, gone := call("GetSecretValue", `{"SecretId":"n"}`)
	if code < 300 {
		t.Fatalf("force delete left secret %d %v", code, gone)
	}
}

func TestSecretsHTTPProvenOps(t *testing.T) {
	want := []string{"CreateSecret", "GetSecretValue", "PutSecretValue", "UpdateSecret", "DeleteSecret",
		"RestoreSecret", "ListSecrets", "DescribeSecret", "ListSecretVersionIds",
		"GetRandomPassword", "TagResource", "UntagResource",
		"BatchGetSecretValue", "PutResourcePolicy", "GetResourcePolicy", "DeleteResourcePolicy",
		"ValidateResourcePolicy", "ReplicateSecretToRegions", "RemoveRegionsFromReplication",
		"StopReplicationToReplica", "RotateSecret", "CancelRotateSecret", "UpdateSecretVersionStage"}

	assertSame(t, "secrets", secretsmanager.New(spitest.Deps(t)).Operations(), want)
}
