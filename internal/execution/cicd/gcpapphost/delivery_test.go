package gcpapphost_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/execution/cicd/delivery"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/execution/cicd/gcpapphost"
)

// GCP-APPHOST-DELIVERY: Cloud Build → staging → promote → rollback.
func TestGCP_APPHOST_DELIVERY(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	prior := []byte("prior-" + t.Name())
	nonce := "app-" + t.Name()

	yaml := []byte(`
steps:
- id: gen
  script: echo -n '` + nonce + `' > index.html
`)
	r, err := gcpapphost.BuildAndStage(ctx, yaml, dir, "index.html")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(r.Close)
	if r.Build.Status != "SUCCESS" || r.Staging == nil {
		t.Fatalf("%#v", r.Build)
	}
	if err := r.Staging.VerifyNonce("index.html", nonce); err != nil {
		t.Fatal(err)
	}
	// Production empty until promote.
	if r.Production != nil {
		t.Fatal("production early")
	}
	if err := r.Promote(); err != nil {
		t.Fatal(err)
	}
	if err := r.Production.VerifyNonce("index.html", nonce); err != nil {
		t.Fatal(err)
	}
	if err := r.Rollback(prior); err != nil {
		t.Fatal(err)
	}
	if err := delivery.VerifyNonceFile(r.ProdDir, "index.html", string(prior)); err != nil {
		t.Fatal(err)
	}

	// Failed build cannot promote.
	failYAML := []byte("steps:\n- id: x\n  script: exit 1\n")
	failDir := t.TempDir()
	fr, err := gcpapphost.BuildAndStage(ctx, failYAML, failDir, "index.html")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(fr.Close)
	if fr.Build.Status == "SUCCESS" {
		t.Fatal("want failure")
	}
	if err := fr.Promote(); err == nil {
		t.Fatal("promote on failed build")
	}
	_ = os.RemoveAll(filepath.Join(dir, "unused"))
}
