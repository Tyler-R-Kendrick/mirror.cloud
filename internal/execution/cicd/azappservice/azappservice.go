// Package azappservice is a mirror-owned Azure App Service ZIP/static deploy stand-in.
// Accepts a files map or ZIP bytes, extracts, serves on loopback — not ARM / Kudu / container parity.
package azappservice

import (
	"archive/zip"
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/execution/cicd/delivery"
)

// App is one local App Service-ish site.
type App struct {
	mu   sync.Mutex
	Name string
	base string
	root string
	site *delivery.Site
}

// New creates an app under a temp (or given) base directory.
func New(name, base string) (*App, error) {
	if name == "" {
		name = "app"
	}
	if base == "" {
		dir, err := os.MkdirTemp("", "mirror-azappservice-")
		if err != nil {
			return nil, err
		}
		base = dir
	}
	root := filepath.Join(base, "wwwroot")
	if err := os.MkdirAll(root, 0o755); err != nil {
		return nil, err
	}
	return &App{Name: name, base: base, root: root}, nil
}

// Deploy writes a files map to wwwroot and serves it.
func (a *App) Deploy(files map[string][]byte) (*delivery.Site, error) {
	if a == nil {
		return nil, fmt.Errorf("azappservice: nil app")
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if err := a.resetRoot(); err != nil {
		return nil, err
	}
	for name, body := range files {
		if err := writeRel(a.root, name, body); err != nil {
			return nil, err
		}
	}
	return a.listen()
}

// DeployZip extracts ZIP bytes into wwwroot and serves them.
func (a *App) DeployZip(zipBytes []byte) (*delivery.Site, error) {
	if a == nil {
		return nil, fmt.Errorf("azappservice: nil app")
	}
	if len(zipBytes) == 0 {
		return nil, fmt.Errorf("azappservice: empty zip")
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if err := a.resetRoot(); err != nil {
		return nil, err
	}
	zr, err := zip.NewReader(bytes.NewReader(zipBytes), int64(len(zipBytes)))
	if err != nil {
		return nil, err
	}
	for _, f := range zr.File {
		if err := extractZipFile(a.root, f); err != nil {
			return nil, err
		}
	}
	return a.listen()
}

// Site returns the current loopback site (nil until Deploy/DeployZip).
func (a *App) Site() *delivery.Site {
	if a == nil {
		return nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.site
}

// Root is the wwwroot path.
func (a *App) Root() string {
	if a == nil {
		return ""
	}
	return a.root
}

// Close stops the listener.
func (a *App) Close() {
	if a == nil {
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.site != nil {
		_ = a.site.Close()
		a.site = nil
	}
}

func (a *App) resetRoot() error {
	if a.site != nil {
		_ = a.site.Close()
		a.site = nil
	}
	_ = os.RemoveAll(a.root)
	return os.MkdirAll(a.root, 0o755)
}

func (a *App) listen() (*delivery.Site, error) {
	site, err := delivery.ServeDir(a.root)
	if err != nil {
		return nil, err
	}
	a.site = site
	return site, nil
}

func writeRel(root, name string, body []byte) error {
	if filepath.IsAbs(name) || strings.Contains(name, "..") {
		return fmt.Errorf("azappservice: bad path %q", name)
	}
	path := filepath.Join(root, name)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	return os.WriteFile(path, body, 0o644)
}

func extractZipFile(root string, f *zip.File) error {
	name := f.Name
	if filepath.IsAbs(name) || strings.Contains(name, "..") {
		return fmt.Errorf("azappservice: bad zip path %q", name)
	}
	target := filepath.Join(root, name)
	if f.FileInfo().IsDir() {
		return os.MkdirAll(target, 0o755)
	}
	if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
		return err
	}
	rc, err := f.Open()
	if err != nil {
		return err
	}
	defer rc.Close()
	out, err := os.OpenFile(target, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, f.Mode())
	if err != nil {
		return err
	}
	defer out.Close()
	_, err = io.Copy(out, rc)
	return err
}
