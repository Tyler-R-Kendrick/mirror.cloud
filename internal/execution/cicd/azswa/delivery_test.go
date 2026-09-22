package azswa_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/execution/cicd/azswa"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/execution/cicd/delivery"
)

// AZ-SWA-PREVIEW: preview vs production isolation; closing preview leaves production.
func TestAZ_SWA_PREVIEW(t *testing.T) {
	pub, err := azswa.New("")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pub.Close)

	prevNonce := "swa-prev-" + t.Name()
	prodNonce := "swa-prod-" + t.Name()
	prev, err := pub.Publish(azswa.Preview, map[string][]byte{"index.html": []byte(prevNonce)})
	if err != nil {
		t.Fatal(err)
	}
	prod, err := pub.Publish(azswa.Production, map[string][]byte{"index.html": []byte(prodNonce)})
	if err != nil {
		t.Fatal(err)
	}
	if err := prev.VerifyNonce("index.html", prevNonce); err != nil {
		t.Fatal(err)
	}
	if err := prod.VerifyNonce("index.html", prodNonce); err != nil {
		t.Fatal(err)
	}
	if err := delivery.VerifyNonceFile(pub.Root(azswa.Preview), "index.html", prodNonce); err == nil {
		t.Fatal("preview leaked prod")
	}

	if err := pub.ClosePreview(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(pub.Root(azswa.Preview), "index.html")); !os.IsNotExist(err) {
		t.Fatalf("preview still present: %v", err)
	}
	if err := delivery.VerifyNonceFile(pub.Root(azswa.Production), "index.html", prodNonce); err != nil {
		t.Fatal("production removed with preview:", err)
	}
}
