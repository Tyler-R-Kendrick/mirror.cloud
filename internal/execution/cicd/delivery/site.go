// Package delivery is a tiny local static publish root for CI/CD fixtures.
// Not provider parity — HTTP listener or file root only.
package delivery

import (
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

// Site is one published document root (preview or production).
type Site struct {
	Root string
	URL  string // http://127.0.0.1:port/ when listening; empty for file-only

	ln   net.Listener
	srv  *http.Server
	once sync.Once
	keep bool // true when Root was caller-owned (BindDir / ServeDir)
}

// StaticSite is the fixture name used in capability docs.
type StaticSite = Site

// Publish writes files into a temp root and serves them on loopback.
func Publish(files map[string][]byte) (*Site, error) {
	root, err := os.MkdirTemp("", "mirror-cicd-site-")
	if err != nil {
		return nil, err
	}
	for name, body := range files {
		if err := writeRel(root, name, body); err != nil {
			_ = os.RemoveAll(root)
			return nil, err
		}
	}
	return listen(root, false)
}

// PublishDir copies src into a temp root and serves it on loopback.
func PublishDir(src string) (*Site, error) {
	root, err := os.MkdirTemp("", "mirror-cicd-site-")
	if err != nil {
		return nil, err
	}
	if err := copyTree(src, root); err != nil {
		_ = os.RemoveAll(root)
		return nil, err
	}
	return listen(root, false)
}

// ServeDir serves an existing directory in place over loopback.
func ServeDir(dir string) (*Site, error) {
	st, err := os.Stat(dir)
	if err != nil {
		return nil, err
	}
	if !st.IsDir() {
		return nil, fmt.Errorf("delivery: not a directory: %s", dir)
	}
	return listen(dir, true)
}

// BindDir returns a file-only root (no HTTP listener).
func BindDir(dir string) (*Site, error) {
	st, err := os.Stat(dir)
	if err != nil {
		return nil, err
	}
	if !st.IsDir() {
		return nil, fmt.Errorf("delivery: not a directory: %s", dir)
	}
	return &Site{Root: dir, keep: true}, nil
}

func listen(root string, keep bool) (*Site, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	s := &Site{Root: root, URL: "http://" + ln.Addr().String() + "/", ln: ln, keep: keep}
	s.srv = &http.Server{Handler: http.FileServer(http.Dir(root))}
	go func() { _ = s.srv.Serve(ln) }()
	return s, nil
}

// Close stops the listener and removes a temp root.
func (s *Site) Close() error {
	if s == nil {
		return nil
	}
	var err error
	s.once.Do(func() {
		if s.srv != nil {
			err = s.srv.Close()
		}
		if s.Root != "" && !s.keep {
			_ = os.RemoveAll(s.Root)
		}
	})
	return err
}

// Get fetches path from the site (e.g. "/index.html") when listening.
func (s *Site) Get(path string) ([]byte, error) {
	if s == nil {
		return nil, fmt.Errorf("delivery: nil site")
	}
	if s.URL == "" {
		return nil, fmt.Errorf("delivery: file-only site; use ReadFile")
	}
	path = strings.TrimPrefix(path, "/")
	res, err := http.Get(s.URL + path)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	if res.StatusCode != 200 {
		return nil, fmt.Errorf("delivery: status %d", res.StatusCode)
	}
	return io.ReadAll(res.Body)
}

// ReadFile reads a relative path from the document root.
func (s *Site) ReadFile(rel string) ([]byte, error) {
	if s == nil {
		return nil, fmt.Errorf("delivery: nil site")
	}
	path, err := safeJoin(s.Root, rel)
	if err != nil {
		return nil, err
	}
	return os.ReadFile(path)
}

// VerifyNonce checks published bytes equal nonce via HTTP (if URL set) else file.
func (s *Site) VerifyNonce(rel, nonce string) error {
	if s == nil {
		return fmt.Errorf("delivery: nil site")
	}
	if s.URL != "" {
		return VerifyNonceHTTP(s.URL, rel, nonce)
	}
	return VerifyNonceFile(s.Root, rel, nonce)
}

// VerifyNonceHTTP GETs baseURL+rel and compares body to nonce.
func VerifyNonceHTTP(baseURL, rel, nonce string) error {
	rel = strings.TrimPrefix(rel, "/")
	base := strings.TrimRight(baseURL, "/") + "/"
	res, err := http.Get(base + rel)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	body, err := io.ReadAll(res.Body)
	if err != nil {
		return err
	}
	if res.StatusCode != 200 {
		return fmt.Errorf("delivery: status %d body %q", res.StatusCode, body)
	}
	if string(body) != nonce {
		return fmt.Errorf("delivery: http nonce %q want %q", body, nonce)
	}
	return nil
}

// VerifyNonceFile reads root/rel and compares bytes to nonce.
func VerifyNonceFile(root, rel, nonce string) error {
	path, err := safeJoin(root, rel)
	if err != nil {
		return err
	}
	body, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	if string(body) != nonce {
		return fmt.Errorf("delivery: file nonce %q want %q", body, nonce)
	}
	return nil
}

func writeRel(root, name string, body []byte) error {
	if filepath.IsAbs(name) || strings.Contains(name, "..") {
		return fmt.Errorf("delivery: bad path %q", name)
	}
	path := filepath.Join(root, name)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	return os.WriteFile(path, body, 0o644)
}

func safeJoin(root, rel string) (string, error) {
	rel = strings.TrimPrefix(filepath.Clean("/"+rel), "/")
	if rel == ".." || strings.HasPrefix(rel, "../") {
		return "", fmt.Errorf("delivery: bad path %q", rel)
	}
	path := filepath.Join(root, rel)
	if !strings.HasPrefix(path, filepath.Clean(root)+string(os.PathSeparator)) && path != filepath.Clean(root) {
		return "", fmt.Errorf("delivery: path escapes root")
	}
	return path, nil
}

func copyTree(src, dst string) error {
	return filepath.Walk(src, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}
		target := filepath.Join(dst, rel)
		if info.IsDir() {
			return os.MkdirAll(target, 0o755)
		}
		body, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
			return err
		}
		return os.WriteFile(target, body, info.Mode().Perm())
	})
}
