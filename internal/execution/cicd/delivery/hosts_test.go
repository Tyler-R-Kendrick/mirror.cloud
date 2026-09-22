package delivery_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/bundled"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/execution/cicd/delivery"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/execution/cicd/delivery/netlify"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spitest"
)

// HOST-*-DELIVERY: admit via native B-IR when present, publish nonce via
// mirror-owned StaticSite, observe status, verify bytes (HTTP + file).
func TestHOST_DELIVERY(t *testing.T) {
	type host struct {
		id      string // HOST-NETLIFY-DELIVERY
		service string // empty => mirror-owned binding only
		admit   func(t *testing.T, pack spi.BehaviorPack, svc string, id spi.Identity) string
	}
	cases := []host{
		// HOST-NETLIFY-DELIVERY: see TestHOST_NETLIFY_DELIVERY (site/deploy adapter).
		// HOST-RENDER-DELIVERY: see render.TestHOST_RENDER_DELIVERY (service/deploy/redeploy).
		{
			id:      "HOST-RAILWAY-DELIVERY",
			service: "railway.graphql",
			admit: func(t *testing.T, pack spi.BehaviorPack, svc string, id spi.Identity) string {
				t.Helper()
				out := invoke(t, pack, svc, id, "projectCreate", map[string]any{
					"input": map[string]any{"name": "host-rail"},
				})
				name, _ := out["name"].(string)
				if name != "host-rail" || out["id"] == nil {
					t.Fatalf("railway projectCreate %#v", out)
				}
				return "created:" + fmt.Sprint(out["id"])
			},
		},
		{
			id:      "HOST-DO-DELIVERY", // alias of HOST-DIGITALOCEAN-DELIVERY
			service: "digitalocean.v2",
			admit: func(t *testing.T, pack spi.BehaviorPack, svc string, id spi.Identity) string {
				t.Helper()
				out := invoke(t, pack, svc, id, "DropletsCreate", map[string]any{
					"name": "host-do", "region": "nyc3", "size": "s-1vcpu-1gb", "image": "ubuntu-24-04-x64",
				})
				drop, _ := out["droplet"].(map[string]any)
				if drop == nil || drop["status"] != "active" {
					t.Fatalf("DropletsCreate %#v", out)
				}
				return "active:" + fmt.Sprint(drop["id"])
			},
		},
		{
			id:      "HOST-FLY-DELIVERY",
			service: "fly.machines",
			admit: func(t *testing.T, pack spi.BehaviorPack, svc string, id spi.Identity) string {
				t.Helper()
				created := invoke(t, pack, svc, id, "AppsCreate", map[string]any{
					"name": "host-fly", "org_slug": "personal",
				})
				if created["id"] == nil {
					t.Fatalf("AppsCreate %#v", created)
				}
				shown := invoke(t, pack, svc, id, "AppsShow", map[string]any{"app_name": "host-fly"})
				if shown["name"] != "host-fly" || shown["status"] == nil {
					t.Fatalf("AppsShow %#v", shown)
				}
				return "status:" + fmt.Sprint(shown["status"])
			},
		},
		{
			id:      "HOST-HOSTINGER-DELIVERY",
			service: "hostinger.api",
			admit: func(t *testing.T, pack spi.BehaviorPack, svc string, id spi.Identity) string {
				t.Helper()
				order := invoke(t, pack, svc, id, "DomainsPurchaseNewDomainV1", map[string]any{
					"domain": "host-delivery.test", "item_id": "hostingercom-domain",
				})
				if order["status"] != "completed" {
					t.Fatalf("purchase %#v", order)
				}
				details := invoke(t, pack, svc, id, "DomainsGetDomainDetailsV1", map[string]any{
					"domain": "host-delivery.test",
				})
				if details["status"] != "active" {
					t.Fatalf("details %#v", details)
				}
				return "active:" + fmt.Sprint(details["domain"])
			},
		},
		{
			id:      "HOST-HETZNER-DELIVERY",
			service: "hetzner.v1",
			admit: func(t *testing.T, pack spi.BehaviorPack, svc string, id spi.Identity) string {
				t.Helper()
				created := invoke(t, pack, svc, id, "CreateServer", map[string]any{
					"name": "host-hz", "server_type": "cx22", "image": "ubuntu-24.04",
				})
				srv, _ := created["server"].(map[string]any)
				if srv == nil || srv["status"] != "running" {
					t.Fatalf("CreateServer %#v", created)
				}
				return "running:" + fmt.Sprint(srv["id"])
			},
		},
	}

	for _, c := range cases {
		c := c
		t.Run(c.id, func(t *testing.T) {
			nonce := "nonce-" + c.id
			var status string
			if c.service != "" {
				deps := spitest.Deps(t)
				pack, err := bundled.New(c.service, deps)
				if err != nil {
					t.Fatal(err)
				}
				id := spi.Identity{Account: "000000000000", Region: "us-east-1"}
				status = c.admit(t, pack, c.service, id)
				if status == "" {
					t.Fatal("empty admit status")
				}
			} else {
				status = "mirror-owned-binding-only"
			}

			// Mirror-owned StaticSite: none of these B-IRs serve static bytes.
			site, err := delivery.Publish(map[string][]byte{"index.html": []byte(nonce)})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = site.Close() })

			if err := site.VerifyNonce("index.html", nonce); err != nil {
				t.Fatalf("http verify (%s): %v", status, err)
			}
			if err := delivery.VerifyNonceFile(site.Root, "index.html", nonce); err != nil {
				t.Fatalf("file verify: %v", err)
			}

			src := t.TempDir()
			mustWrite(t, filepath.Join(src, "index.html"), nonce)
			dirSite, err := delivery.PublishDir(src)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = dirSite.Close() })
			if err := delivery.VerifyNonceHTTP(dirSite.URL, "index.html", nonce); err != nil {
				t.Fatal(err)
			}

			fileOnly, err := delivery.BindDir(site.Root)
			if err != nil {
				t.Fatal(err)
			}
			if err := fileOnly.VerifyNonce("index.html", nonce); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// HOST-NETLIFY-DELIVERY: local Site Create → draft Deploy → publish production
// pointer; draft URL ≠ prod; failed/incomplete deploy does not flip production.
func TestHOST_NETLIFY_DELIVERY(t *testing.T) {
	site, err := netlify.CreateSite("host-netlify")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(site.Close)

	draftNonce := "nonce-draft-" + t.Name()
	prodNonce := "nonce-prod-" + t.Name()

	draft, err := site.Deploy(map[string][]byte{"index.html": []byte(draftNonce)})
	if err != nil {
		t.Fatal(err)
	}
	if draft.Status != netlify.StatusReady || draft.Site == nil || !draft.Draft {
		t.Fatalf("draft %#v", draft)
	}
	if err := draft.Site.VerifyNonce("index.html", draftNonce); err != nil {
		t.Fatal(err)
	}
	if site.Production() != nil || site.ProductionID() != "" {
		t.Fatal("production set before Publish")
	}

	toProd, err := site.Deploy(map[string][]byte{"index.html": []byte(prodNonce)})
	if err != nil {
		t.Fatal(err)
	}
	if err := site.Publish(toProd.ID); err != nil {
		t.Fatal(err)
	}
	prod := site.Production()
	if prod == nil || site.ProductionID() != toProd.ID {
		t.Fatalf("production pointer %#v id=%q", prod, site.ProductionID())
	}
	if err := prod.VerifyNonce("index.html", prodNonce); err != nil {
		t.Fatal(err)
	}
	if prod.URL == "" || draft.Site.URL == "" || prod.URL == draft.Site.URL {
		t.Fatalf("want distinct URLs prod=%q draft=%q", prod.URL, draft.Site.URL)
	}
	if prod.URL == toProd.Site.URL {
		t.Fatal("production URL must not reuse deploy-specific URL")
	}
	if err := draft.Site.VerifyNonce("index.html", draftNonce); err != nil {
		t.Fatal("draft mutated after publish:", err)
	}
	if err := delivery.VerifyNonceFile(draft.Site.Root, "index.html", prodNonce); err == nil {
		t.Fatal("draft root leaked production nonce")
	}

	fail := site.FailDeploy()
	if fail.Status != netlify.StatusError || fail.Site != nil {
		t.Fatalf("fail deploy %#v", fail)
	}
	if err := site.Publish(fail.ID); err == nil {
		t.Fatal("Publish on error deploy")
	}
	if err := site.Production().VerifyNonce("index.html", prodNonce); err != nil {
		t.Fatal("failed deploy flipped production:", err)
	}

	inc := site.IncompleteDeploy()
	if inc.Status != netlify.StatusBuilding || inc.Site != nil {
		t.Fatalf("incomplete %#v", inc)
	}
	if err := site.Publish(inc.ID); err == nil {
		t.Fatal("Publish on incomplete deploy")
	}
	if err := site.Production().VerifyNonce("index.html", prodNonce); err != nil {
		t.Fatal("incomplete deploy flipped production:", err)
	}
	if site.ProductionID() != toProd.ID {
		t.Fatalf("production id drifted to %q", site.ProductionID())
	}
}

func invoke(t *testing.T, pack spi.BehaviorPack, service string, id spi.Identity, op string, in map[string]any) map[string]any {
	t.Helper()
	res, err := pack.Invoke(context.Background(), &spi.Request{
		ServiceID: service,
		Identity:  id,
		Operation: op,
		Input:     in,
	})
	if err != nil {
		t.Fatalf("%s: %v", op, err)
	}
	return res.Output
}

func mustWrite(t *testing.T, path, body string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
}
