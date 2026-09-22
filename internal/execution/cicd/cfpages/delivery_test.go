package cfpages_test

import (
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/execution/cicd/cfpages"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/execution/cicd/delivery"
)

// CF-PAGES-DELIVERY: mirror-owned Pages publisher keeps preview vs production roots apart.
func TestCF_PAGES_DELIVERY(t *testing.T) {
	pub, err := cfpages.New("")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pub.Close)

	previewNonce := "nonce-cf-preview-" + t.Name()
	prodNonce := "nonce-cf-prod-" + t.Name()

	prev, err := pub.Publish(cfpages.Preview, map[string][]byte{"index.html": []byte(previewNonce)})
	if err != nil {
		t.Fatal(err)
	}
	prod, err := pub.Publish(cfpages.Production, map[string][]byte{"index.html": []byte(prodNonce)})
	if err != nil {
		t.Fatal(err)
	}

	if err := prev.VerifyNonce("index.html", previewNonce); err != nil {
		t.Fatal(err)
	}
	if err := prod.VerifyNonce("index.html", prodNonce); err != nil {
		t.Fatal(err)
	}
	if err := delivery.VerifyNonceFile(pub.Root(cfpages.Preview), "index.html", previewNonce); err != nil {
		t.Fatal(err)
	}
	if err := delivery.VerifyNonceFile(pub.Root(cfpages.Production), "index.html", prodNonce); err != nil {
		t.Fatal(err)
	}
	if err := delivery.VerifyNonceFile(pub.Root(cfpages.Preview), "index.html", prodNonce); err == nil {
		t.Fatal("preview leaked production nonce")
	}
	if err := delivery.VerifyNonceFile(pub.Root(cfpages.Production), "index.html", previewNonce); err == nil {
		t.Fatal("production leaked preview nonce")
	}
	if pub.Site(cfpages.Preview) == nil || pub.Site(cfpages.Production) == nil {
		t.Fatal("missing live sites")
	}
}
