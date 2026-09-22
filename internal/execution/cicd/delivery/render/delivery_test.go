package render_test

import (
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/execution/cicd/delivery/render"
)

// HOST-RENDER-DELIVERY: Site/Service create → static Deploy → status → Redeploy.
func TestHOST_RENDER_DELIVERY(t *testing.T) {
	svc, err := render.CreateSite("host-render")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(svc.Close)

	if svc.Status() != render.StatusCreated || svc.Live() != nil {
		t.Fatalf("want created empty, got status=%q live=%v", svc.Status(), svc.Live())
	}

	nonce := "render-" + t.Name()
	d, err := svc.Deploy(map[string][]byte{"index.html": []byte(nonce)})
	if err != nil {
		t.Fatal(err)
	}
	if d.Status != render.StatusLive || d.Site == nil {
		t.Fatalf("deploy %#v", d)
	}
	if svc.Status() != render.StatusLive || svc.LiveID() != d.ID {
		t.Fatalf("service status=%q liveID=%q", svc.Status(), svc.LiveID())
	}
	if err := svc.Live().VerifyNonce("index.html", nonce); err != nil {
		t.Fatal(err)
	}
	if svc.DeployStatus(d.ID) != render.StatusLive {
		t.Fatalf("deploy status %q", svc.DeployStatus(d.ID))
	}

	fail := svc.FailDeploy()
	if fail.Status != render.StatusFailed || fail.Site != nil {
		t.Fatalf("fail %#v", fail)
	}
	if svc.LiveID() != d.ID || svc.Status() != render.StatusLive {
		t.Fatal("failed deploy flipped live")
	}
	if err := svc.Live().VerifyNonce("index.html", nonce); err != nil {
		t.Fatal("failed deploy mutated live:", err)
	}

	rd, err := svc.Redeploy()
	if err != nil {
		t.Fatal(err)
	}
	if rd.ID == d.ID {
		t.Fatal("redeploy reused deploy id")
	}
	if svc.LiveID() != rd.ID || svc.Status() != render.StatusLive {
		t.Fatalf("after redeploy liveID=%q status=%q", svc.LiveID(), svc.Status())
	}
	if err := svc.Live().VerifyNonce("index.html", nonce); err != nil {
		t.Fatal(err)
	}

	web, err := render.CreateService("host-web", render.KindWebService)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(web.Close)
	if web.Kind != render.KindWebService {
		t.Fatalf("kind %q", web.Kind)
	}
	if _, err := web.Redeploy(); err == nil {
		t.Fatal("redeploy with no prior deploy")
	}
}
