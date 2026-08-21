package mediaconvert

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

func TestMediaConvertHTTPProvenOps(t *testing.T) {
	p := New(spitest.Deps(t))
	if n := len(p.Operations()); n != 12 {
		t.Fatalf("mediaconvert Operations() %d want 12", n)
	}
}

func TestBootedServerMediaConvertCreateGetDelete(t *testing.T) {
	cfg := config.Default()
	cfg.Services = []string{"aws.mediaconvert"}
	cfg.Seed = "mc-1"
	rt, err := rtpkg.Boot(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(rt.Handler())
	defer ts.Close()
	auth := "AWS4-HMAC-SHA256 Credential=test/20200101/us-east-1/mediaconvert/aws4_request, SignedHeaders=host, Signature=00"
	call := func(op, body string) map[string]any {
		t.Helper()
		req, _ := http.NewRequest(http.MethodPost, ts.URL+"/", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/x-amz-json-1.1")
		req.Header.Set("X-Amz-Target", "MediaConvert."+op)
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
	created := call("CreateQueue", `{"Name":"q1"}`)
	if created["Queue"] == nil {
		t.Fatalf("create %v", created)
	}
	got := call("GetQueue", `{"Name":"q1"}`)
	q, _ := got["Queue"].(map[string]any)
	if q["Name"] != "q1" {
		t.Fatalf("get %v", got)
	}
	job := call("CreateJob", `{"Queue":"q1","Role":"arn:aws:iam::000000000000:role/mc"}`)
	j, _ := job["Job"].(map[string]any)
	id, _ := j["Id"].(string)
	if id == "" || j["Status"] != "COMPLETE" {
		t.Fatalf("job %v", job)
	}
	gj := call("GetJob", `{"Id":"`+id+`"}`)
	if gj["Job"] == nil {
		t.Fatalf("get job %v", gj)
	}
	call("DeleteQueue", `{"Name":"q1"}`)
	listed := call("ListQueues", `{}`)
	raw, _ := json.Marshal(listed)
	if strings.Contains(string(raw), `"Name":"q1"`) {
		t.Fatalf("still present %s", raw)
	}
}
