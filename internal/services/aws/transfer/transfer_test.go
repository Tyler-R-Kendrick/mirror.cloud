package transfer

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

func TestTransferHTTPProvenOps(t *testing.T) {
	p := New(spitest.Deps(t))
	if n := len(p.Operations()); n != 12 {
		t.Fatalf("transfer Operations() %d want 12", n)
	}
}

func TestBootedServerTransferServerUser(t *testing.T) {
	cfg := config.Default()
	cfg.Services = []string{"aws.transfer"}
	cfg.Seed = "xfer-1"
	rt, err := rtpkg.Boot(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(rt.Handler())
	defer ts.Close()
	auth := "AWS4-HMAC-SHA256 Credential=test/20200101/us-east-1/transfer/aws4_request, SignedHeaders=host, Signature=00"
	call := func(op, body string) map[string]any {
		t.Helper()
		req, _ := http.NewRequest(http.MethodPost, ts.URL+"/", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/x-amz-json-1.1")
		req.Header.Set("X-Amz-Target", "TransferService."+op)
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
	created := call("CreateServer", `{"Domain":"S3","Protocols":["SFTP"]}`)
	sid, _ := created["ServerId"].(string)
	if sid == "" {
		t.Fatalf("create %v", created)
	}
	got := call("DescribeServer", `{"ServerId":"`+sid+`"}`)
	if got["Server"] == nil {
		t.Fatalf("describe %v", got)
	}
	call("CreateUser", `{"ServerId":"`+sid+`","UserName":"alice","Role":"arn:aws:iam::000000000000:role/x"}`)
	user := call("DescribeUser", `{"ServerId":"`+sid+`","UserName":"alice"}`)
	if user["User"] == nil {
		t.Fatalf("user %v", user)
	}
	listed := call("ListServers", `{}`)
	raw, _ := json.Marshal(listed)
	if !strings.Contains(string(raw), sid) {
		t.Fatalf("list %s", raw)
	}
	call("DeleteUser", `{"ServerId":"`+sid+`","UserName":"alice"}`)
	call("DeleteServer", `{"ServerId":"`+sid+`"}`)
	gone := call("ListServers", `{}`)
	raw, _ = json.Marshal(gone)
	if strings.Contains(string(raw), `"`+sid+`"`) {
		t.Fatalf("server still present %s", raw)
	}
}
