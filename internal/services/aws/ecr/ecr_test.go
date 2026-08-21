package ecr

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/config"
	rtpkg "github.com/tyler-r-kendrick/mirror.cloud/internal/runtime"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spitest"
)

func TestBootedServerECRCreateDescribe(t *testing.T) {
	cfg := config.Default()
	cfg.Services = []string{"aws.api.ecr"}
	cfg.Seed = "ecr-1"
	rt, err := rtpkg.Boot(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(rt.Handler())
	defer ts.Close()
	auth := "AWS4-HMAC-SHA256 Credential=test/20200101/us-east-1/ecr/aws4_request, SignedHeaders=host, Signature=00"
	call := func(op, body string) map[string]any {
		t.Helper()
		req, _ := http.NewRequest(http.MethodPost, ts.URL+"/", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/x-amz-json-1.1")
		req.Header.Set("X-Amz-Target", "AmazonEC2ContainerRegistry_V20150921."+op)
		req.Header.Set("Authorization", auth)
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		raw, _ := io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode >= 300 {
			t.Fatalf("%s %d %s", op, res.StatusCode, raw)
		}
		out := map[string]any{}
		_ = json.Unmarshal(raw, &out)
		return out
	}
	created := call("CreateRepository", `{"repositoryName":"app"}`)
	if created["repository"] == nil {
		t.Fatalf("create %v", created)
	}
	listed := call("DescribeRepositories", `{}`)
	if listed["repositories"] == nil {
		t.Fatalf("list %v", listed)
	}
	img := call("PutImage", `{"repositoryName":"app","imageTag":"v1","imageManifest":"{}"}`)
	if img["image"] == nil {
		t.Fatalf("put %v", img)
	}
	up := call("InitiateLayerUpload", `{"repositoryName":"app"}`)
	uid, _ := up["uploadId"].(string)
	if uid == "" {
		t.Fatalf("upload %v", up)
	}
	call("UploadLayerPart", `{"repositoryName":"app","uploadId":"`+uid+`","partFirstByte":0,"partLastByte":3,"layerPartBlob":"QUJDRA=="}`)
	done := call("CompleteLayerUpload", `{"repositoryName":"app","uploadId":"`+uid+`"}`)
	if done["layerDigest"] == nil {
		t.Fatalf("complete %v", done)
	}
	call("BatchCheckLayerAvailability", `{"repositoryName":"app","layerDigests":["`+strECR(done["layerDigest"])+`"]}`)
	call("DescribeImages", `{"repositoryName":"app"}`)
	call("PutLifecyclePolicy", `{"repositoryName":"app","lifecyclePolicyText":"{}"}`)
	call("GetLifecyclePolicy", `{"repositoryName":"app"}`)
	call("CreatePullThroughCacheRule", `{"ecrRepositoryPrefix":"p","upstreamRegistryUrl":"public.ecr.aws"}`)
	call("DescribePullThroughCacheRules", `{}`)
	call("ValidatePullThroughCacheRule", `{"ecrRepositoryPrefix":"p"}`)
	call("PutRegistryPolicy", `{"policyText":"{}"}`)
	call("GetRegistryPolicy", `{}`)
	call("DescribeRegistry", `{}`)
	call("StartImageScan", `{"repositoryName":"app","imageId":{"imageTag":"v1"}}`)
	call("DescribeImageScanFindings", `{"repositoryName":"app","imageId":{"imageTag":"v1"}}`)
	call("PutImageTagMutability", `{"repositoryName":"app","imageTagMutability":"IMMUTABLE"}`)
}

func strECR(v any) string { s, _ := v.(string); return s }

func TestECRHTTPProvenOps(t *testing.T) {
	p := New(spitest.Deps(t))
	if n := len(p.Operations()); n != 58 {
		t.Fatalf("ecr Operations() %d want 58", n)
	}
}

func TestBootedServerECRExtraOps(t *testing.T) {
	cfg := config.Default()
	cfg.Services = []string{"aws.api.ecr"}
	cfg.Seed = "ecr-extra"
	rt, err := rtpkg.Boot(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(rt.Handler())
	defer ts.Close()
	auth := "AWS4-HMAC-SHA256 Credential=test/20200101/us-east-1/ecr/aws4_request, SignedHeaders=host, Signature=00"
	soft := func(op, body string) string {
		t.Helper()
		req, _ := http.NewRequest(http.MethodPost, ts.URL+"/", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/x-amz-json-1.1")
		req.Header.Set("X-Amz-Target", "AmazonEC2ContainerRegistry_V20150921."+op)
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
		return string(raw)
	}
	hard := func(op, body string) string {
		t.Helper()
		req, _ := http.NewRequest(http.MethodPost, ts.URL+"/", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/x-amz-json-1.1")
		req.Header.Set("X-Amz-Target", "AmazonEC2ContainerRegistry_V20150921."+op)
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
	hard("CreateRepository", `{"repositoryName":"bootrepo"}`)
	hard("PutLifecyclePolicy", `{"repositoryName":"bootrepo","lifecyclePolicyText":"{\"rules\":[]}"}`)
	got := hard("GetLifecyclePolicy", `{"repositoryName":"bootrepo"}`)
	if !strings.Contains(got, "rules") && !strings.Contains(got, "lifecyclePolicyText") {
		t.Fatalf("get lc %s", got)
	}
	hard("DeleteLifecyclePolicy", `{"repositoryName":"bootrepo"}`)
	gone := hard("GetLifecyclePolicy", `{"repositoryName":"bootrepo"}`)
	if strings.Contains(gone, "rules") {
		t.Fatalf("lc still present %s", gone)
	}
	payload := `{"repositoryName":"bootrepo","uploadId":"u1","layerDigest":"sha256:abc","layerDigests":["sha256:abc"],"ecrRepositoryPrefix":"p","prefix":"p","policyText":"{}","name":"BASIC_SCANNING_TYPE_UNCHANGED","value":"v","imageTag":"v1","imageId":{"imageTag":"v1"},"lifecyclePolicyText":"{}","principalArn":"arn:aws:iam::000000000000:root"}`
	for _, op := range extraOps() {
		soft(op, payload)
	}
}
