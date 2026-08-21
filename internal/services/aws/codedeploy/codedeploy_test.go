package codedeploy

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

func TestCodeDeployHTTPProvenOps(t *testing.T) {
	p := New(spitest.Deps(t))
	if n := len(p.Operations()); n != 12 {
		t.Fatalf("codedeploy Operations() %d want 12", n)
	}
}

func TestBootedServerCodeDeployCreateGetDelete(t *testing.T) {
	cfg := config.Default()
	cfg.Services = []string{"aws.codedeploy"}
	cfg.Seed = "cd-1"
	rt, err := rtpkg.Boot(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(rt.Handler())
	defer ts.Close()
	auth := "AWS4-HMAC-SHA256 Credential=test/20200101/us-east-1/codedeploy/aws4_request, SignedHeaders=host, Signature=00"
	call := func(op, body string) map[string]any {
		t.Helper()
		req, _ := http.NewRequest(http.MethodPost, ts.URL+"/", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/x-amz-json-1.1")
		req.Header.Set("X-Amz-Target", "CodeDeploy_20141006."+op)
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
		if res.Header.Get("x-mirror-fidelity") != "emulate" {
			t.Fatalf("fidelity %q", res.Header.Get("x-mirror-fidelity"))
		}
		out := map[string]any{}
		_ = json.Unmarshal(raw, &out)
		return out
	}
	created := call("CreateApplication", `{"applicationName":"app1"}`)
	if created["applicationId"] == nil {
		t.Fatalf("create %v", created)
	}
	got := call("GetApplication", `{"applicationName":"app1"}`)
	app, _ := got["application"].(map[string]any)
	if app["applicationName"] != "app1" {
		t.Fatalf("get %v", got)
	}
	listed := call("ListApplications", `{}`)
	raw, _ := json.Marshal(listed)
	if !strings.Contains(string(raw), "app1") {
		t.Fatalf("list %s", raw)
	}
	call("CreateDeploymentGroup", `{"applicationName":"app1","deploymentGroupName":"dg1"}`)
	dg := call("GetDeploymentGroup", `{"applicationName":"app1","deploymentGroupName":"dg1"}`)
	if dg["deploymentGroupInfo"] == nil {
		t.Fatalf("dg %v", dg)
	}
	dep := call("CreateDeployment", `{"applicationName":"app1","deploymentGroupName":"dg1"}`)
	id, _ := dep["deploymentId"].(string)
	if id == "" {
		t.Fatalf("deploy %v", dep)
	}
	call("GetDeployment", `{"deploymentId":"`+id+`"}`)
	call("StopDeployment", `{"deploymentId":"`+id+`"}`)
	call("DeleteDeploymentGroup", `{"applicationName":"app1","deploymentGroupName":"dg1"}`)
	call("DeleteApplication", `{"applicationName":"app1"}`)
	gone := call("ListApplications", `{}`)
	raw, _ = json.Marshal(gone)
	if strings.Contains(string(raw), `"app1"`) {
		t.Fatalf("still present %s", raw)
	}
}
