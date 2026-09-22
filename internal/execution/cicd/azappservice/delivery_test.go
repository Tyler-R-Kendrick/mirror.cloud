package azappservice_test

import (
	"archive/zip"
	"bytes"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/execution/cicd/azappservice"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/execution/cicd/delivery"
)

// AZ-APPSERVICE-DELIVERY: files map + ZIP extract → loopback StaticSite.
func TestAZ_APPSERVICE_DELIVERY(t *testing.T) {
	app, err := azappservice.New("host-app", "")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(app.Close)

	filesNonce := "files-" + t.Name()
	site, err := app.Deploy(map[string][]byte{"index.html": []byte(filesNonce)})
	if err != nil {
		t.Fatal(err)
	}
	if err := site.VerifyNonce("index.html", filesNonce); err != nil {
		t.Fatal(err)
	}
	if err := delivery.VerifyNonceFile(app.Root(), "index.html", filesNonce); err != nil {
		t.Fatal(err)
	}

	zipNonce := "zip-" + t.Name()
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	w, err := zw.Create("index.html")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write([]byte(zipNonce)); err != nil {
		t.Fatal(err)
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}

	zsite, err := app.DeployZip(buf.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	if err := zsite.VerifyNonce("index.html", zipNonce); err != nil {
		t.Fatal(err)
	}
	if err := delivery.VerifyNonceFile(app.Root(), "index.html", filesNonce); err == nil {
		t.Fatal("files nonce survived zip deploy")
	}
	if app.Site() != zsite {
		t.Fatal("Site() mismatch after DeployZip")
	}
	if _, err := app.DeployZip(nil); err == nil {
		t.Fatal("empty zip")
	}
}
