package eks

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

func TestBootedServerEKSCreateList(t *testing.T) {
	cfg := config.Default()
	cfg.Services = []string{"aws.eks"}
	cfg.Seed = "eks-1"
	rt, err := rtpkg.Boot(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(rt.Handler())
	defer ts.Close()
	auth := "AWS4-HMAC-SHA256 Credential=test/20200101/us-east-1/eks/aws4_request, SignedHeaders=host, Signature=00"
	do := func(method, path, body string) map[string]any {
		t.Helper()
		var rdr io.Reader
		if body != "" {
			rdr = strings.NewReader(body)
		}
		req, _ := http.NewRequest(method, ts.URL+path, rdr)
		req.Header.Set("Authorization", auth)
		req.Header.Set("Content-Type", "application/json")
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		raw, _ := io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode >= 300 {
			t.Fatalf("%s %s %d %s", method, path, res.StatusCode, raw)
		}
		out := map[string]any{}
		_ = json.Unmarshal(raw, &out)
		return out
	}
	created := do(http.MethodPost, "/clusters", `{"name":"prod","roleArn":"arn:aws:iam::000000000000:role/eks"}`)
	if created["cluster"] == nil {
		t.Fatalf("create %v", created)
	}
	listed := do(http.MethodGet, "/clusters", "")
	if listed["clusters"] == nil {
		t.Fatalf("list %v", listed)
	}
	got := do(http.MethodGet, "/clusters/prod", "")
	if got["cluster"] == nil {
		t.Fatalf("describe %v", got)
	}
	addon := do(http.MethodPost, "/?Action=CreateAddon", `{"clusterName":"prod","addonName":"vpc-cni"}`)
	if addon["addon"] == nil {
		t.Fatalf("addon %v", addon)
	}
	gotA := do(http.MethodPost, "/?Action=DescribeAddon", `{"clusterName":"prod","addonName":"vpc-cni"}`)
	if gotA["addon"] == nil {
		t.Fatalf("describe addon %v", gotA)
	}
	do(http.MethodPost, "/?Action=ListAddons", `{"clusterName":"prod"}`)
	do(http.MethodPost, "/?Action=CreateAccessEntry", `{"clusterName":"prod","principalArn":"arn:aws:iam::000000000000:role/r"}`)
	do(http.MethodPost, "/?Action=ListAccessEntries", `{"clusterName":"prod"}`)
	do(http.MethodPost, "/?Action=AssociateAccessPolicy", `{"clusterName":"prod","principalArn":"arn:aws:iam::000000000000:role/r","policyArn":"arn:aws:eks::aws:cluster-access-policy/AmazonEKSAdminPolicy"}`)
	do(http.MethodPost, "/?Action=DescribeClusterVersions", `{}`)
	do(http.MethodPost, "/?Action=UpdateClusterVersion", `{"name":"prod","version":"1.30"}`)
	do(http.MethodPost, "/?Action=CreatePodIdentityAssociation", `{"clusterName":"prod","namespace":"default","serviceAccount":"sa"}`)
	do(http.MethodPost, "/?Action=ListPodIdentityAssociations", `{"clusterName":"prod"}`)
	do(http.MethodPost, "/?Action=StartInsightsRefresh", `{"clusterName":"prod"}`)
}

func TestEKSHTTPProvenOps(t *testing.T) {
	p := New(spitest.Deps(t))
	if n := len(p.Operations()); n != 70 {
		t.Fatalf("eks Operations() %d want 70", n)
	}
}

func TestBootedServerEKSExtraOps(t *testing.T) {
	cfg := config.Default()
	cfg.Services = []string{"aws.eks"}
	cfg.Seed = "eks-extra"
	rt, err := rtpkg.Boot(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(rt.Handler())
	defer ts.Close()
	auth := "AWS4-HMAC-SHA256 Credential=test/20200101/us-east-1/eks/aws4_request, SignedHeaders=host, Signature=00"
	soft := func(op, body string) string {
		t.Helper()
		req, _ := http.NewRequest(http.MethodPost, ts.URL+"/?Action="+op, strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
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
		req, _ := http.NewRequest(http.MethodPost, ts.URL+"/?Action="+op, strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
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
	created := hard("CreateAddon", `{"clusterName":"bootc","addonName":"bootaddon"}`)
	if !strings.Contains(created, "bootaddon") {
		t.Fatalf("create addon %s", created)
	}
	got := hard("DescribeAddon", `{"clusterName":"bootc","addonName":"bootaddon"}`)
	if !strings.Contains(got, "bootaddon") {
		t.Fatalf("describe addon %s", got)
	}
	hard("DeleteAddon", `{"clusterName":"bootc","addonName":"bootaddon"}`)
	listed := hard("ListAddons", `{"clusterName":"bootc"}`)
	if strings.Contains(listed, "bootaddon") {
		t.Fatalf("addon still present %s", listed)
	}
	payload := `{"clusterName":"bootc","name":"bootc","addonName":"bootaddon","principalArn":"arn:aws:iam::000000000000:role/r","policyArn":"arn:aws:eks::aws:cluster-access-policy/AmazonEKSAdminPolicy","namespace":"default","serviceAccount":"sa","capabilityName":"cap","associationId":"a1","id":"id1","version":"1.30","nodegroupName":"ng1"}`
	for _, op := range extraOps() {
		soft(op, payload)
	}
}
