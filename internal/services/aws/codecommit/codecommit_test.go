package codecommit

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

func TestCodeCommitHTTPProvenOps(t *testing.T) {
	p := New(spitest.Deps(t))
	if n := len(p.Operations()); n != 12 {
		t.Fatalf("codecommit Operations() %d want 12", n)
	}
}

func TestBootedServerCodeCommitCreateGetDelete(t *testing.T) {
	cfg := config.Default()
	cfg.Services = []string{"aws.codecommit"}
	cfg.Seed = "cc-1"
	rt, err := rtpkg.Boot(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(rt.Handler())
	defer ts.Close()
	auth := "AWS4-HMAC-SHA256 Credential=test/20200101/us-east-1/codecommit/aws4_request, SignedHeaders=host, Signature=00"
	call := func(op, body string) map[string]any {
		t.Helper()
		req, _ := http.NewRequest(http.MethodPost, ts.URL+"/", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/x-amz-json-1.1")
		req.Header.Set("X-Amz-Target", "CodeCommit_20150413."+op)
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
	created := call("CreateRepository", `{"repositoryName":"r1"}`)
	if created["repositoryMetadata"] == nil {
		t.Fatalf("create %v", created)
	}
	got := call("GetRepository", `{"repositoryName":"r1"}`)
	md, _ := got["repositoryMetadata"].(map[string]any)
	if md["repositoryName"] != "r1" {
		t.Fatalf("get %v", got)
	}
	listed := call("ListRepositories", `{}`)
	raw, _ := json.Marshal(listed)
	if !strings.Contains(string(raw), "r1") {
		t.Fatalf("list %s", raw)
	}
	call("CreateBranch", `{"repositoryName":"r1","branchName":"main"}`)
	br := call("GetBranch", `{"repositoryName":"r1","branchName":"main"}`)
	if br["branch"] == nil {
		t.Fatalf("branch %v", br)
	}
	call("PutFile", `{"repositoryName":"r1","filePath":"README.md","fileContent":"hi"}`)
	f := call("GetFile", `{"repositoryName":"r1","filePath":"README.md"}`)
	if f["filePath"] != "README.md" {
		t.Fatalf("file %v", f)
	}
	call("DeleteFile", `{"repositoryName":"r1","filePath":"README.md"}`)
	call("DeleteBranch", `{"repositoryName":"r1","branchName":"main"}`)
	call("DeleteRepository", `{"repositoryName":"r1"}`)
	gone := call("ListRepositories", `{}`)
	raw, _ = json.Marshal(gone)
	if strings.Contains(string(raw), `"repositoryName":"r1"`) {
		t.Fatalf("still present %s", raw)
	}
}
