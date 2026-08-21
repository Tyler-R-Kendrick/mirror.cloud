package iotdata

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

func TestIoTDataHTTPProvenOps(t *testing.T) {
	p := New(spitest.Deps(t))
	if n := len(p.Operations()); n != 6 {
		t.Fatalf("iotdata Operations() %d want 6", n)
	}
}

func TestBootedServerIoTDataCreateGetDelete(t *testing.T) {
	cfg := config.Default()
	cfg.Services = []string{"aws.iot-data"}
	cfg.Seed = "iotd-1"
	rt, err := rtpkg.Boot(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(rt.Handler())
	defer ts.Close()
	auth := "AWS4-HMAC-SHA256 Credential=test/20200101/us-east-1/iotdata/aws4_request, SignedHeaders=host, Signature=00"
	call := func(op, body string) (map[string]any, int) {
		t.Helper()
		req, _ := http.NewRequest(http.MethodPost, ts.URL+"/", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/x-amz-json-1.1")
		req.Header.Set("X-Amz-Target", "IotDataPlane."+op)
		req.Header.Set("Authorization", auth)
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		raw, _ := io.ReadAll(res.Body)
		res.Body.Close()
		if res.Header.Get("x-mirror-fidelity") != "emulate" {
			t.Fatalf("fidelity %q", res.Header.Get("x-mirror-fidelity"))
		}
		out := map[string]any{}
		_ = json.Unmarshal(raw, &out)
		if res.StatusCode >= 300 && op != "GetThingShadow" {
			t.Fatalf("%s %d %s", op, res.StatusCode, raw)
		}
		return out, res.StatusCode
	}
	up, code := call("UpdateThingShadow", `{"thingName":"t1","payload":"{\"state\":{\"desired\":{\"x\":1}}}"}`)
	if code >= 300 {
		t.Fatalf("update %d %v", code, up)
	}
	got, code := call("GetThingShadow", `{"thingName":"t1"}`)
	if code >= 300 || got["thingName"] != "t1" {
		t.Fatalf("get %d %v", code, got)
	}
	call("DeleteThingShadow", `{"thingName":"t1"}`)
	_, code = call("GetThingShadow", `{"thingName":"t1"}`)
	if code < 300 {
		t.Fatalf("still present %d", code)
	}
}
