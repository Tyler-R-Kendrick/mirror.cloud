package gcpdeploy_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/execution/cicd/delivery"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/execution/cicd/gcpdeploy"
)

// GCP-CLOUDDEPLOY-DELIVERY: stage → approval gate → promote → rollback.
func TestGCP_CLOUDDEPLOY_DELIVERY(t *testing.T) {
	p, err := gcpdeploy.New("")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(p.Close)

	prior := []byte("prior-" + t.Name())
	if err := seedProd(p, prior); err != nil {
		t.Fatal(err)
	}

	nonce := "cd-" + t.Name()
	if err := p.Stage(map[string][]byte{"index.html": []byte(nonce)}); err != nil {
		t.Fatal(err)
	}
	if p.Phase != gcpdeploy.PhasePending {
		t.Fatalf("phase %q", p.Phase)
	}
	if err := p.Staging.VerifyNonce("index.html", nonce); err != nil {
		t.Fatal(err)
	}
	if err := p.Promote(); err == nil {
		t.Fatal("promote without approve")
	}

	if err := p.Approve(); err != nil {
		t.Fatal(err)
	}
	if p.Phase != gcpdeploy.PhaseApproved {
		t.Fatalf("phase %q", p.Phase)
	}
	if err := p.Promote(); err != nil {
		t.Fatal(err)
	}
	if p.Phase != gcpdeploy.PhaseProduction {
		t.Fatalf("phase %q", p.Phase)
	}
	if err := p.Production.VerifyNonce("index.html", nonce); err != nil {
		t.Fatal(err)
	}

	if err := p.Rollback(nil); err != nil {
		t.Fatal(err)
	}
	if err := delivery.VerifyNonceFile(p.ProdDir, "index.html", string(prior)); err != nil {
		t.Fatal(err)
	}

	// Reject blocks promote.
	p2, err := gcpdeploy.New("")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(p2.Close)
	if err := p2.Stage(map[string][]byte{"index.html": []byte("nope")}); err != nil {
		t.Fatal(err)
	}
	if err := p2.Reject(); err != nil {
		t.Fatal(err)
	}
	if err := p2.Promote(); err == nil {
		t.Fatal("promote after reject")
	}
	if err := p2.Approve(); err == nil {
		t.Fatal("approve after reject")
	}

	// Optional Cloud Build compose path.
	ctx := context.Background()
	dir := t.TempDir()
	buildNonce := "build-" + t.Name()
	yaml := []byte("steps:\n- id: gen\n  script: echo -n '" + buildNonce + "' > index.html\n")
	p3, err := gcpdeploy.New("")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(p3.Close)
	if err := p3.BuildAndStage(ctx, yaml, dir, "index.html"); err != nil {
		t.Fatal(err)
	}
	if err := p3.Approve(); err != nil {
		t.Fatal(err)
	}
	if err := p3.Promote(); err != nil {
		t.Fatal(err)
	}
	if err := p3.Production.VerifyNonce("index.html", buildNonce); err != nil {
		t.Fatal(err)
	}
	_ = os.RemoveAll(filepath.Join(dir, "unused"))
}

func seedProd(p *gcpdeploy.Pipeline, body []byte) error {
	if err := p.Stage(map[string][]byte{"index.html": body}); err != nil {
		return err
	}
	if err := p.Approve(); err != nil {
		return err
	}
	return p.Promote()
}
