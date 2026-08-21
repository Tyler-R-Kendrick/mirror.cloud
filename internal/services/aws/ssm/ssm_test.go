package ssm

import (
	"context"
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

func TestSecureStringRoundTripAndPath(t *testing.T) {
	p := &Pack{deps: spitest.Deps(t)}
	ctx := context.Background()
	id := spi.Identity{Account: "a", Region: "r"}
	_, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "PutParameter", Input: map[string]any{"Name": "/app/db", "Value": "secret", "Type": "SecureString"}})
	if err != nil {
		t.Fatal(err)
	}
	got, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "GetParameter", Input: map[string]any{"Name": "/app/db"}})
	if err != nil {
		t.Fatal(err)
	}
	param := got.Output["Parameter"].(map[string]any)
	if param["Value"] != "secret" {
		t.Fatalf("decoded %v", param)
	}
	_, _ = p.Invoke(ctx, &spi.Request{Identity: id, Operation: "PutParameter", Input: map[string]any{"Name": "/app/x", "Value": "1", "Type": "String"}})
	list, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "GetParametersByPath", Input: map[string]any{"Path": "/app/"}})
	if err != nil {
		t.Fatal(err)
	}
	if len(list.Output["Parameters"].([]any)) != 2 {
		t.Fatalf("%v", list.Output)
	}
	act, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateActivation", Input: map[string]any{"DefaultInstanceName": "i"}})
	if err != nil {
		t.Fatal(err)
	}
	if act.Output == nil {
		t.Fatalf("activation %v", act)
	}
}

func TestSSMHTTPProvenOps(t *testing.T) {
	p := New(spitest.Deps(t))
	if n := len(p.Operations()); n != 64+len(extraOps()) {
		t.Fatalf("ssm Operations() %d want %d", n, 64+len(extraOps()))
	}
}

func TestBootedServerSSMExtraActivation(t *testing.T) {
	cfg := config.Default()
	cfg.Services = []string{"aws.ssm"}
	cfg.Seed = "ssm-extra"
	rt, err := rtpkg.Boot(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(rt.Handler())
	defer ts.Close()
	auth := "AWS4-HMAC-SHA256 Credential=test/20200101/us-east-1/ssm/aws4_request, SignedHeaders=host, Signature=00"
	call := func(op, body string) map[string]any {
		t.Helper()
		req, _ := http.NewRequest(http.MethodPost, ts.URL+"/", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/x-amz-json-1.1")
		req.Header.Set("X-Amz-Target", "AmazonSSM."+op)
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
	created := call("CreateActivation", `{"DefaultInstanceName":"i1"}`)
	id, _ := created["ActivationId"].(string)
	if id == "" {
		id, _ = created["ActivationId"].(string)
	}
	listed := call("DescribeActivations", `{}`)
	raw, _ := json.Marshal(listed)
	if !strings.Contains(string(raw), "i1") && id == "" {
		t.Fatalf("activation %v list %v", created, listed)
	}
	if id == "" {
		id = "i1"
	}
	call("DeleteActivation", `{"ActivationId":"`+id+`"}`)
	gone := call("DescribeActivations", `{}`)
	graw, _ := json.Marshal(gone)
	if strings.Contains(string(graw), `"ActivationId":"`+id+`"`) {
		t.Fatalf("activation still present %s", graw)
	}
	for _, op := range extraOps() {
		out := call(op, `{"DefaultInstanceName":"i1","ActivationId":"a1","Name":"n","WindowId":"w","InstanceId":"i-1"}`)
		if out == nil {
			t.Fatalf("%s nil", op)
		}
	}
}
