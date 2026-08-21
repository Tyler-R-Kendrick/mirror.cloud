package applicationautoscaling

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/config"
	rtpkg "github.com/tyler-r-kendrick/mirror.cloud/internal/runtime"
)

func TestBootedServerAppAutoScalingTarget(t *testing.T) {
	cfg := config.Default()
	cfg.Services = []string{"aws.application-autoscaling"}
	cfg.Seed = "aas-1"
	rt, err := rtpkg.Boot(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(rt.Handler())
	defer ts.Close()
	auth := "AWS4-HMAC-SHA256 Credential=test/20200101/us-east-1/application-autoscaling/aws4_request, SignedHeaders=host, Signature=00"
	call := func(op, body string) map[string]any {
		t.Helper()
		req, _ := http.NewRequest(http.MethodPost, ts.URL+"/", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/x-amz-json-1.1")
		req.Header.Set("X-Amz-Target", "AnyScaleFrontendService."+op)
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
	reg := call("RegisterScalableTarget", `{"ServiceNamespace":"ecs","ResourceId":"service/c/s","ScalableDimension":"ecs:service:DesiredCount","MinCapacity":1,"MaxCapacity":4}`)
	if reg["ScalableTargetARN"] == nil {
		t.Fatalf("register %v", reg)
	}
	listed := call("DescribeScalableTargets", `{"ServiceNamespace":"ecs"}`)
	if listed["ScalableTargets"] == nil {
		t.Fatalf("describe %v", listed)
	}
}
