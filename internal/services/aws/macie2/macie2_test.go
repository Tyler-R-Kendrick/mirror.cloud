package macie2

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

func TestMacie2HTTPProvenOps(t *testing.T) {
	p := New(spitest.Deps(t))
	if n := len(p.Operations()); n != 6 {
		t.Fatalf("macie2 Operations() %d want 6", n)
	}
}

func TestBootedServerMacie2CreateGetDelete(t *testing.T) {
	cfg := config.Default()
	cfg.Services = []string{"aws.macie2"}
	cfg.Seed = "macie-1"
	rt, err := rtpkg.Boot(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(rt.Handler())
	defer ts.Close()
	auth := "AWS4-HMAC-SHA256 Credential=test/20200101/us-east-1/macie2/aws4_request, SignedHeaders=host, Signature=00"
	call := func(op, body string) map[string]any {
		t.Helper()
		req, _ := http.NewRequest(http.MethodPost, ts.URL+"/", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/x-amz-json-1.1")
		req.Header.Set("X-Amz-Target", "Macie2."+op)
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
	call("EnableMacie", `{}`)
	got := call("GetMacieSession", `{}`)
	if got["status"] != "ENABLED" {
		t.Fatalf("session %v", got)
	}
	job := call("CreateClassificationJob", `{"name":"job1","jobType":"ONE_TIME"}`)
	id, _ := job["jobId"].(string)
	if id == "" {
		t.Fatalf("job %v", job)
	}
	desc := call("DescribeClassificationJob", `{"jobId":"`+id+`"}`)
	if desc["name"] != "job1" {
		t.Fatalf("describe %v", desc)
	}
	call("DisableMacie", `{}`)
	listed := call("ListClassificationJobs", `{}`)
	raw, _ := json.Marshal(listed)
	if !strings.Contains(string(raw), id) {
		t.Fatalf("jobs gone %s", raw)
	}
}
