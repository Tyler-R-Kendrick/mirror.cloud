package backup

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

func TestBackupHTTPProvenOps(t *testing.T) {
	p := New(spitest.Deps(t))
	if n := len(p.Operations()); n != 11 {
		t.Fatalf("backup Operations() %d want 11", n)
	}
}

func TestBootedServerBackupVault(t *testing.T) {
	cfg := config.Default()
	cfg.Services = []string{"aws.backup"}
	cfg.Seed = "bk-1"
	rt, err := rtpkg.Boot(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(rt.Handler())
	defer ts.Close()
	auth := "AWS4-HMAC-SHA256 Credential=test/20200101/us-east-1/backup/aws4_request, SignedHeaders=host, Signature=00"
	call := func(op, body string) map[string]any {
		t.Helper()
		req, _ := http.NewRequest(http.MethodPost, ts.URL+"/", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/x-amz-json-1.1")
		req.Header.Set("X-Amz-Target", "AWSBackup."+op)
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
	created := call("CreateBackupVault", `{"BackupVaultName":"v1"}`)
	if created["BackupVaultName"] != "v1" {
		t.Fatalf("create %v", created)
	}
	got := call("DescribeBackupVault", `{"BackupVaultName":"v1"}`)
	if got["BackupVaultName"] != "v1" {
		t.Fatalf("get %v", got)
	}
	listed := call("ListBackupVaults", `{}`)
	raw, _ := json.Marshal(listed)
	if !strings.Contains(string(raw), "v1") {
		t.Fatalf("list %s", raw)
	}
	call("DeleteBackupVault", `{"BackupVaultName":"v1"}`)
	gone := call("ListBackupVaults", `{}`)
	raw, _ = json.Marshal(gone)
	if strings.Contains(string(raw), `"v1"`) {
		t.Fatalf("still present %s", raw)
	}
}
