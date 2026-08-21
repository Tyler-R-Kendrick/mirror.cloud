package sagemaker

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

func TestSageMakerHTTPProvenOps(t *testing.T) {
	p := New(spitest.Deps(t))
	if n := len(p.Operations()); n != 15 {
		t.Fatalf("sagemaker Operations() %d want 15", n)
	}
}

func TestBootedServerSageMakerCreateGetDelete(t *testing.T) {
	cfg := config.Default()
	cfg.Services = []string{"aws.sagemaker"}
	cfg.Seed = "sm-1"
	rt, err := rtpkg.Boot(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(rt.Handler())
	defer ts.Close()
	auth := "AWS4-HMAC-SHA256 Credential=test/20200101/us-east-1/sagemaker/aws4_request, SignedHeaders=host, Signature=00"
	call := func(op, body string) map[string]any {
		t.Helper()
		req, _ := http.NewRequest(http.MethodPost, ts.URL+"/", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/x-amz-json-1.1")
		req.Header.Set("X-Amz-Target", "SageMaker."+op)
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
	created := call("CreateNotebookInstance", `{"NotebookInstanceName":"nb1","InstanceType":"ml.t2.medium"}`)
	if created["NotebookInstanceArn"] == nil {
		t.Fatalf("create %v", created)
	}
	got := call("DescribeNotebookInstance", `{"NotebookInstanceName":"nb1"}`)
	if got["NotebookInstanceName"] != "nb1" {
		t.Fatalf("describe %v", got)
	}
	call("CreateModel", `{"ModelName":"m1","ExecutionRoleArn":"arn:aws:iam::000000000000:role/sm"}`)
	ep := call("CreateEndpoint", `{"EndpointName":"e1","EndpointConfigName":"ec1"}`)
	if ep["EndpointArn"] == nil {
		t.Fatalf("endpoint %v", ep)
	}
	call("DeleteEndpoint", `{"EndpointName":"e1"}`)
	call("DeleteNotebookInstance", `{"NotebookInstanceName":"nb1"}`)
	listed := call("ListNotebookInstances", `{}`)
	raw, _ := json.Marshal(listed)
	if strings.Contains(string(raw), `"NotebookInstanceName":"nb1"`) {
		t.Fatalf("still present %s", raw)
	}
}
