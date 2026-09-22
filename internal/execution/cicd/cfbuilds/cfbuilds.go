// Package cfbuilds is a local Workers Builds stand-in: build → version upload →
// Activate. Live HTTP serves Response bodies from a generation-fenced version
// store (net/http), not workerd/miniflare. Distinct from @cloudflare/ci and Pages.
package cfbuilds

import (
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync"
)

// Version is one uploaded Worker revision (preview until Activate).
type Version struct {
	ID         string
	Generation int64
	Module     string
	Body       string
	Ready      bool
	Status     string // success | failure
}

// Worker hosts preview and production loopback endpoints.
type Worker struct {
	mu         sync.Mutex
	seq        uint64
	gen        int64 // bumps on each successful ready upload
	activeGen  int64 // production fence; Activate must not go backwards
	activeID   string
	previewID  string
	versions   map[string]*Version
	prodBody   string
	prevBody   string
	ProdURL    string
	PreviewURL string
	prodLn     net.Listener
	prevLn     net.Listener
}

// New starts production + preview listeners serving the version store.
func New() (*Worker, error) {
	w := &Worker{
		versions: make(map[string]*Version),
		prodBody: "unset",
	}
	prodLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	prevLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		_ = prodLn.Close()
		return nil, err
	}
	w.prodLn, w.prevLn = prodLn, prevLn
	w.ProdURL = "http://" + prodLn.Addr().String() + "/"
	w.PreviewURL = "http://" + prevLn.Addr().String() + "/"
	go http.Serve(prodLn, http.HandlerFunc(func(rw http.ResponseWriter, _ *http.Request) {
		w.mu.Lock()
		body := w.prodBody
		w.mu.Unlock()
		_, _ = rw.Write([]byte(body))
	}))
	go http.Serve(prevLn, http.HandlerFunc(func(rw http.ResponseWriter, _ *http.Request) {
		w.mu.Lock()
		body := w.prevBody
		w.mu.Unlock()
		_, _ = rw.Write([]byte(body))
	}))
	return w, nil
}

// Close stops listeners.
func (w *Worker) Close() {
	if w == nil {
		return
	}
	if w.prodLn != nil {
		_ = w.prodLn.Close()
	}
	if w.prevLn != nil {
		_ = w.prevLn.Close()
	}
}

// SeedProduction sets the live production body (prior deploy) without a preview.
func (w *Worker) SeedProduction(body string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.prodBody = body
	w.activeGen = 0
}

// Build compiles a minimal module Worker from source and uploads a preview version.
// Failed builds (empty source, or source containing "__BUILD_FAIL__") publish no ready version.
func (w *Worker) Build(module string) (*Version, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.seq++
	id := fmt.Sprintf("ver-%d", w.seq)
	v := &Version{ID: id, Module: module, Status: "failure"}

	if strings.TrimSpace(module) == "" || strings.Contains(module, "__BUILD_FAIL__") {
		w.versions[id] = v
		return v, nil
	}
	body, ok := extractBody(module)
	if !ok {
		w.versions[id] = v
		return v, fmt.Errorf("cfbuilds: no Response body in module")
	}
	w.gen++
	v.Generation = w.gen
	v.Body = body
	v.Ready = true
	v.Status = "success"
	w.versions[id] = v
	w.previewID = id
	w.prevBody = body
	return v, nil
}

// Activate promotes a ready version to production. Stale generations cannot overwrite newer ones.
func (w *Worker) Activate(versionID string) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	v, ok := w.versions[versionID]
	if !ok {
		return fmt.Errorf("cfbuilds: version %q not found", versionID)
	}
	if !v.Ready || v.Status != "success" {
		return fmt.Errorf("cfbuilds: version %q not ready", versionID)
	}
	if v.Generation < w.activeGen {
		return fmt.Errorf("cfbuilds: stale generation %d (active %d)", v.Generation, w.activeGen)
	}
	w.activeID = v.ID
	w.activeGen = v.Generation
	w.prodBody = v.Body
	return nil
}

// Version returns a copy of a stored version.
func (w *Worker) Version(id string) (Version, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	v, ok := w.versions[id]
	if !ok {
		return Version{}, false
	}
	return *v, true
}

// ActiveID is the production version id (empty until Activate).
func (w *Worker) ActiveID() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.activeID
}

// PreviewID is the latest successful upload (may differ from production).
func (w *Worker) PreviewID() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.previewID
}

func extractBody(module string) (string, bool) {
	// Find new Response("...") or new Response('...')
	i := strings.Index(module, "new Response(")
	if i < 0 {
		i = strings.Index(module, "new Response (")
	}
	if i < 0 {
		return "", false
	}
	rest := module[i:]
	j := strings.IndexAny(rest, `"'`)
	if j < 0 {
		return "", false
	}
	q := rest[j]
	rest = rest[j+1:]
	k := strings.IndexByte(rest, q)
	if k < 0 {
		return "", false
	}
	return rest[:k], true
}
