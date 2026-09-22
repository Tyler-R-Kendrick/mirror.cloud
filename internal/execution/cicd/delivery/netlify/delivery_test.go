package netlify_test

import (
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/execution/cicd/delivery/netlify"
)

func TestHOST_NETLIFY_DELIVERY(t *testing.T) {
	site, err := netlify.CreateSite("host-netlify")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(site.Close)

	draftNonce := "draft-" + t.Name()
	prodNonce := "prod-" + t.Name()

	draft, err := site.Deploy(map[string][]byte{"index.html": []byte(draftNonce)})
	if err != nil {
		t.Fatal(err)
	}
	if err := draft.Site.VerifyNonce("index.html", draftNonce); err != nil {
		t.Fatal(err)
	}
	if site.Production() != nil {
		t.Fatal("draft flipped production")
	}

	prod, err := site.Deploy(map[string][]byte{"index.html": []byte(prodNonce)})
	if err != nil {
		t.Fatal(err)
	}
	if err := site.Publish(prod.ID); err != nil {
		t.Fatal(err)
	}
	if err := site.Production().VerifyNonce("index.html", prodNonce); err != nil {
		t.Fatal(err)
	}

	other, err := site.Deploy(map[string][]byte{"index.html": []byte("other")})
	if err != nil {
		t.Fatal(err)
	}
	if err := site.Production().VerifyNonce("index.html", prodNonce); err != nil {
		t.Fatal("new draft mutated production:", err)
	}
	bad := site.FailDeploy()
	if err := site.Publish(bad.ID); err == nil {
		t.Fatal("publish failed deploy")
	}
	inc := site.IncompleteDeploy()
	if err := site.Publish(inc.ID); err == nil {
		t.Fatal("publish incomplete deploy")
	}
	_ = other
}
