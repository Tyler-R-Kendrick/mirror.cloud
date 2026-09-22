// Package azacr is a dockerless ACR Tasks stand-in: config + image blob file.
package azacr

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

// Registry is one local image namespace.
type Registry struct {
	mu   sync.Mutex
	Root string
	Log  string
}

func New(root string) (*Registry, error) {
	if root == "" {
		dir, err := os.MkdirTemp("", "mirror-azacr-")
		if err != nil {
			return nil, err
		}
		root = dir
	}
	if err := os.MkdirAll(filepath.Join(root, "repos"), 0o755); err != nil {
		return nil, err
	}
	return &Registry{Root: root}, nil
}

// QuickTask writes source bytes as an "image" blob under repo:tag and records a task log.
func (r *Registry) QuickTask(repo, tag string, source []byte) (digest string, err error) {
	if r == nil {
		return "", fmt.Errorf("azacr: nil registry")
	}
	if len(source) == 0 {
		return "", fmt.Errorf("azacr: empty source")
	}
	if repo == "" || tag == "" || strings.Contains(repo, "..") || strings.Contains(tag, "..") || filepath.IsAbs(repo) {
		return "", fmt.Errorf("azacr: bad repo/tag")
	}
	sum := sha256.Sum256(source)
	digest = "sha256:" + hex.EncodeToString(sum[:])
	dir := filepath.Join(r.Root, "repos", repo)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", err
	}
	path := filepath.Join(dir, tag+".blob")
	if err := os.WriteFile(path, source, 0o644); err != nil {
		return "", err
	}
	r.mu.Lock()
	r.Log += fmt.Sprintf("quick %s:%s %s\n", repo, tag, digest)
	r.mu.Unlock()
	_ = os.WriteFile(filepath.Join(r.Root, "last-task.log"), []byte(r.Log), 0o644)
	return digest, nil
}

// GetBlob reads repo:tag bytes.
func (r *Registry) GetBlob(repo, tag string) ([]byte, error) {
	return os.ReadFile(filepath.Join(r.Root, "repos", repo, tag+".blob"))
}
