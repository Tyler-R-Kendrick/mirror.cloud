package kms

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/config"
	rtpkg "github.com/tyler-r-kendrick/mirror.cloud/internal/runtime"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spitest"
)

func TestEncryptDecryptRoundTrip(t *testing.T) {
	p := New(spitest.Deps(t))
	ctx := context.Background()
	id := spi.Identity{Account: "000000000000", Region: "us-east-1"}
	created, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateKey", Input: map[string]any{}})
	if err != nil {
		t.Fatal(err)
	}
	kid := created.Output["KeyMetadata"].(map[string]any)["KeyId"].(string)
	enc, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "Encrypt", Input: map[string]any{"KeyId": kid, "Plaintext": []byte("hello-kms")}})
	if err != nil {
		t.Fatal(err)
	}
	dec, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "Decrypt", Input: map[string]any{"CiphertextBlob": enc.Output["CiphertextBlob"]}})
	if err != nil {
		t.Fatal(err)
	}
	pt, _ := dec.Output["Plaintext"].([]byte)
	if string(pt) != "hello-kms" {
		t.Fatalf("decrypt %v", dec.Output)
	}
	dk, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "GenerateDataKey", Input: map[string]any{"KeyId": kid}})
	if err != nil {
		t.Fatal(err)
	}
	if dk.Output["Plaintext"] == nil || dk.Output["CiphertextBlob"] == nil {
		t.Fatalf("datakey %v", dk.Output)
	}
}

func TestBootedServerKMSEncryptDecrypt(t *testing.T) {
	cfg := config.Default()
	cfg.Services = []string{"aws.kms"}
	cfg.Seed = "kms-48"
	rt, err := rtpkg.Boot(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(rt.Handler())
	defer ts.Close()
	auth := "AWS4-HMAC-SHA256 Credential=test/20200101/us-east-1/kms/aws4_request, SignedHeaders=host, Signature=00"
	call := func(op, body string) (int, map[string]any) {
		t.Helper()
		req, _ := http.NewRequest(http.MethodPost, ts.URL+"/", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/x-amz-json-1.1")
		req.Header.Set("X-Amz-Target", "TrentService."+op)
		req.Header.Set("Authorization", auth)
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		raw, _ := io.ReadAll(res.Body)
		res.Body.Close()
		out := map[string]any{}
		_ = json.Unmarshal(raw, &out)
		if res.StatusCode >= 300 {
			t.Fatalf("%s %d %s", op, res.StatusCode, raw)
		}
		if res.Header.Get("x-mirror-fidelity") != "emulate" {
			t.Fatalf("fidelity %q", res.Header.Get("x-mirror-fidelity"))
		}
		return res.StatusCode, out
	}
	_, created := call("CreateKey", `{}`)
	md, _ := created["KeyMetadata"].(map[string]any)
	kid, _ := md["KeyId"].(string)
	if kid == "" {
		t.Fatalf("create %v", created)
	}
	pt := base64.StdEncoding.EncodeToString([]byte("hello-kms"))
	_, enc := call("Encrypt", `{"KeyId":"`+kid+`","Plaintext":"`+pt+`"}`)
	ct, _ := enc["CiphertextBlob"].(string)
	if ct == "" {
		t.Fatalf("encrypt %v", enc)
	}
	_, dec := call("Decrypt", `{"CiphertextBlob":"`+ct+`"}`)
	got, _ := base64.StdEncoding.DecodeString(str(dec["Plaintext"]))
	if !bytes.Equal(got, []byte("hello-kms")) && str(dec["Plaintext"]) != "hello-kms" {
		t.Fatalf("decrypt %v", dec)
	}
	_, listed := call("ListKeys", `{}`)
	if listed["Keys"] == nil {
		t.Fatalf("list %v", listed)
	}
	call("CreateAlias", `{"AliasName":"alias/x","TargetKeyId":"`+kid+`"}`)
	_, listedA := call("ListAliases", `{}`)
	if listedA["Aliases"] == nil {
		t.Fatalf("aliases %v", listedA)
	}
	_, encA := call("Encrypt", `{"KeyId":"alias/x","Plaintext":"`+pt+`"}`)
	if encA["CiphertextBlob"] == nil {
		t.Fatalf("alias encrypt %v", encA)
	}
	call("PutKeyPolicy", `{"KeyId":"`+kid+`","PolicyName":"default","Policy":"{}"}`)
	_, pol := call("GetKeyPolicy", `{"KeyId":"`+kid+`"}`)
	if pol["Policy"] == nil {
		t.Fatalf("policy %v", pol)
	}
	_, grant := call("CreateGrant", `{"KeyId":"`+kid+`","GranteePrincipal":"arn:aws:iam::000000000000:root","Operations":["Decrypt"]}`)
	if grant["GrantId"] == nil {
		t.Fatalf("grant %v", grant)
	}
	call("ListGrants", `{"KeyId":"`+kid+`"}`)
	call("RevokeGrant", `{"KeyId":"`+kid+`","GrantId":"`+str(grant["GrantId"])+`"}`)
	msg := base64.StdEncoding.EncodeToString([]byte("payload"))
	_, signed := call("Sign", `{"KeyId":"`+kid+`","Message":"`+msg+`"}`)
	sig, _ := signed["Signature"].(string)
	_, ver := call("Verify", `{"KeyId":"`+kid+`","Message":"`+msg+`","Signature":"`+sig+`"}`)
	if ver["SignatureValid"] != true {
		t.Fatalf("verify %v", ver)
	}
	call("GenerateRandom", `{"NumberOfBytes":16}`)
	call("EnableKeyRotation", `{"KeyId":"`+kid+`"}`)
	_, rot := call("GetKeyRotationStatus", `{"KeyId":"`+kid+`"}`)
	if rot["KeyRotationEnabled"] != true {
		t.Fatalf("rotation %v", rot)
	}
	call("DisableKey", `{"KeyId":"`+kid+`"}`)
	call("EnableKey", `{"KeyId":"`+kid+`"}`)
	call("TagResource", `{"KeyId":"`+kid+`","Tags":[{"TagKey":"k","TagValue":"v"}]}`)
	call("ListResourceTags", `{"KeyId":"`+kid+`"}`)
	call("UntagResource", `{"KeyId":"`+kid+`"}`)
}

func TestKMSHTTPProvenOps(t *testing.T) {
	p := New(spitest.Deps(t))
	if n := len(p.Operations()); n != 54 {
		t.Fatalf("kms Operations() %d want 54", n)
	}
}

func TestBootedServerKMSExtraOps(t *testing.T) {
	cfg := config.Default()
	cfg.Services = []string{"aws.kms"}
	cfg.Seed = "kms-extra"
	rt, err := rtpkg.Boot(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(rt.Handler())
	defer ts.Close()
	auth := "AWS4-HMAC-SHA256 Credential=test/20200101/us-east-1/kms/aws4_request, SignedHeaders=host, Signature=00"
	soft := func(op, body string) string {
		t.Helper()
		req, _ := http.NewRequest(http.MethodPost, ts.URL+"/", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/x-amz-json-1.1")
		req.Header.Set("X-Amz-Target", "TrentService."+op)
		req.Header.Set("Authorization", auth)
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		raw, _ := io.ReadAll(res.Body)
		res.Body.Close()
		if res.Header.Get("x-mirror-fidelity") != "emulate" {
			t.Fatalf("%s fidelity %q %s", op, res.Header.Get("x-mirror-fidelity"), raw)
		}
		if res.StatusCode >= 500 {
			t.Fatalf("%s %d %s", op, res.StatusCode, raw)
		}
		if res.StatusCode >= 300 {
			return string(raw)
		}
		return string(raw)
	}
	hard := func(op, body string) string {
		t.Helper()
		req, _ := http.NewRequest(http.MethodPost, ts.URL+"/", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/x-amz-json-1.1")
		req.Header.Set("X-Amz-Target", "TrentService."+op)
		req.Header.Set("Authorization", auth)
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		raw, _ := io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode >= 300 || res.Header.Get("x-mirror-fidelity") != "emulate" {
			t.Fatalf("%s %d %s %s", op, res.StatusCode, res.Header.Get("x-mirror-fidelity"), raw)
		}
		return string(raw)
	}
	created := hard("CreateKey", `{}`)
	var wrap map[string]any
	_ = json.Unmarshal([]byte(created), &wrap)
	md, _ := wrap["KeyMetadata"].(map[string]any)
	kid, _ := md["KeyId"].(string)
	if kid == "" {
		t.Fatalf("create %s", created)
	}
	hard("CreateAlias", `{"AliasName":"alias/bootx","TargetKeyId":"`+kid+`"}`)
	listed := hard("ListAliases", `{}`)
	if !strings.Contains(listed, "alias/bootx") {
		t.Fatalf("list aliases %s", listed)
	}
	hard("DeleteAlias", `{"AliasName":"alias/bootx"}`)
	gone := hard("ListAliases", `{}`)
	if strings.Contains(gone, "alias/bootx") {
		t.Fatalf("alias still present %s", gone)
	}
	payload := `{"KeyId":"` + kid + `","AliasName":"alias/bootx","TargetKeyId":"` + kid + `","CustomKeyStoreId":"cks1","GrantId":"g1","Message":"aGVsbG8=","Mac":"YQ==","Signature":"YQ==","Plaintext":"aGVsbG8=","CiphertextBlob":"YQ==","DestinationKeyId":"` + kid + `","ReplicaRegion":"us-west-2","Tags":[{"TagKey":"k","TagValue":"v"}],"Policy":"{}","NumberOfBytes":16,"PublicKey":"YQ=="}`
	for _, op := range extraOps() {
		soft(op, payload)
	}
}
