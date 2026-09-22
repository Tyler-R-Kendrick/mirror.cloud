package cfbuilds_test

import (
	"io"
	"net/http"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/execution/cicd/cfbuilds"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/execution/cicd/delivery"
)

// CF-DELIVERY: Workers Builds stand-in — preview upload, Activate switches live HTTP,
// failed build leaves production unchanged; generation fence blocks stale Activate.
func TestCF_DELIVERY(t *testing.T) {
	w, err := cfbuilds.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(w.Close)

	prior := "nonce-prior-" + t.Name()
	next := "nonce-next-" + t.Name()
	w.SeedProduction(prior)

	if err := delivery.VerifyNonceHTTP(w.ProdURL, "", prior); err != nil {
		t.Fatal(err)
	}

	preview, err := w.Build(moduleReturning(next))
	if err != nil {
		t.Fatal(err)
	}
	if !preview.Ready || preview.Status != "success" {
		t.Fatalf("preview %#v", preview)
	}
	if err := delivery.VerifyNonceHTTP(w.ProdURL, "", prior); err != nil {
		t.Fatal("production changed before Activate:", err)
	}
	if err := delivery.VerifyNonceHTTP(w.PreviewURL, "", next); err != nil {
		t.Fatal(err)
	}
	if w.PreviewID() == w.ActiveID() && w.ActiveID() != "" {
		t.Fatal("preview id leaked into active before Activate")
	}

	if err := w.Activate(preview.ID); err != nil {
		t.Fatal(err)
	}
	if err := delivery.VerifyNonceHTTP(w.ProdURL, "", next); err != nil {
		t.Fatal(err)
	}
	if got := get(t, w.ProdURL); got != next {
		t.Fatalf("prod body %q want %q", got, next)
	}

	fail, err := w.Build(`export default { fetch() { return new Response("__BUILD_FAIL__"); } }`)
	if err != nil {
		t.Fatal(err)
	}
	if fail.Ready || fail.Status != "failure" {
		t.Fatalf("want failed build, got %#v", fail)
	}
	if err := delivery.VerifyNonceHTTP(w.ProdURL, "", next); err != nil {
		t.Fatal("failed build mutated production:", err)
	}
	if err := w.Activate(fail.ID); err == nil {
		t.Fatal("Activate on failed version must error")
	}

	// Newer ready version, then stale Activate of the prior ready version.
	newerBody := "nonce-newer-" + t.Name()
	newer, err := w.Build(moduleReturning(newerBody))
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Activate(newer.ID); err != nil {
		t.Fatal(err)
	}
	if err := w.Activate(preview.ID); err == nil {
		t.Fatal("stale Activate must fail generation fence")
	}
	if err := delivery.VerifyNonceHTTP(w.ProdURL, "", newerBody); err != nil {
		t.Fatal(err)
	}
}

func moduleReturning(body string) string {
	return `export default { async fetch() { return new Response("` + body + `"); } }`
}

func get(t *testing.T, base string) string {
	t.Helper()
	res, err := http.Get(base)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	b, err := io.ReadAll(res.Body)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}
