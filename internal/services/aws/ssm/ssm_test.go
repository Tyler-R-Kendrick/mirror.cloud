package ssm_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/bundled"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/config"
	rtpkg "github.com/tyler-r-kendrick/mirror.cloud/internal/runtime"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spitest"
)

func newPack(t *testing.T) spi.BehaviorPack {
	t.Helper()
	p, err := bundled.New("aws.ssm", spitest.Deps(t))
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func TestSecureStringRoundTripAndPath(t *testing.T) {
	p := newPack(t)
	ctx := context.Background()
	id := spi.Identity{Account: "a", Region: "r"}
	_, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "PutParameter", Input: map[string]any{"Name": "/app/db", "Value": "secret", "Type": "SecureString"}})
	if err != nil {
		t.Fatal(err)
	}
	value := func(decrypt bool) any {
		got, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "GetParameter", Input: map[string]any{"Name": "/app/db", "WithDecryption": decrypt}})
		if err != nil {
			t.Fatal(err)
		}
		return got.Output["Parameter"].(map[string]any)["Value"]
	}
	if v := value(true); v != "secret" {
		t.Fatalf("decrypted %v", v)
	}
	if v := value(false); v == "secret" {
		t.Fatalf("a SecureString read without WithDecryption answered plaintext")
	}
	_, _ = p.Invoke(ctx, &spi.Request{Identity: id, Operation: "PutParameter", Input: map[string]any{"Name": "/app/x", "Value": "1", "Type": "String"}})
	_, _ = p.Invoke(ctx, &spi.Request{Identity: id, Operation: "PutParameter", Input: map[string]any{"Name": "/app/deep/y", "Value": "1", "Type": "String"}})
	list, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "GetParametersByPath", Input: map[string]any{"Path": "/app/"}})
	if err != nil {
		t.Fatal(err)
	}
	if len(list.Output["Parameters"].([]any)) != 2 {
		t.Fatalf("%v", list.Output)
	}
}

func TestSSMHTTPProvenOps(t *testing.T) {
	if n := len(newPack(t).Operations()); n != 64 {
		t.Fatalf("ssm Operations() %d want 64", n)
	}
}

// The pack answered 88 further operations from one echoing
// key-value store; they are mock tier now, and say so.
func TestBootedServerSSMExtrasAreNotEmulate(t *testing.T) {
	cfg := config.Default()
	cfg.Services = []string{"aws.ssm"}
	cfg.Seed = "ssm-extra"
	rt, err := rtpkg.Boot(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(rt.Handler())
	defer ts.Close()
	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/", strings.NewReader(`{"DefaultInstanceName":"i1","IamRole":"r"}`))
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", "AmazonSSM.CreateActivation")
	req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=test/20200101/us-east-1/ssm/aws4_request, SignedHeaders=host, Signature=00")
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	if res.Header.Get("x-mirror-fidelity") == "emulate" {
		t.Fatal("CreateActivation claims emulate")
	}
}
