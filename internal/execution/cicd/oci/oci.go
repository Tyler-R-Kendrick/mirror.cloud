// Package oci is a dockerless local image registry stand-in (bytes + digest).
package oci

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

// Registry stores image blobs by name:tag under Root.
type Registry struct {
	mu   sync.Mutex
	Root string
}

func New(root string) (*Registry, error) {
	if root == "" {
		dir, err := os.MkdirTemp("", "mirror-oci-")
		if err != nil {
			return nil, err
		}
		root = dir
	}
	if err := os.MkdirAll(filepath.Join(root, "blobs"), 0o755); err != nil {
		return nil, err
	}
	return &Registry{Root: root}, nil
}

func splitRef(ref string) (name, tag string, err error) {
	name, tag, ok := strings.Cut(ref, ":")
	if !ok || name == "" || tag == "" || strings.Contains(name, "/") && strings.HasPrefix(name, "/") {
		// allow path-like names without leading slash issues
	}
	if !ok || name == "" || tag == "" {
		return "", "", fmt.Errorf("oci: bad ref %q (want name:tag)", ref)
	}
	if strings.Contains(name, "..") || strings.Contains(tag, "..") {
		return "", "", fmt.Errorf("oci: bad ref %q", ref)
	}
	return name, tag, nil
}

// Push stores bytes and returns sha256 digest.
func (r *Registry) Push(ref string, layer []byte) (digest string, err error) {
	if r == nil {
		return "", fmt.Errorf("oci: nil registry")
	}
	name, tag, err := splitRef(ref)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(layer)
	digest = "sha256:" + hex.EncodeToString(sum[:])
	r.mu.Lock()
	defer r.mu.Unlock()
	dir := filepath.Join(r.Root, "repos", name)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", err
	}
	blob := filepath.Join(r.Root, "blobs", hex.EncodeToString(sum[:]))
	if err := os.WriteFile(blob, layer, 0o644); err != nil {
		return "", err
	}
	meta := digest + "\n" + blob + "\n"
	if err := os.WriteFile(filepath.Join(dir, tag+".ref"), []byte(meta), 0o644); err != nil {
		return "", err
	}
	return digest, nil
}

// Pull returns layer bytes for ref.
func (r *Registry) Pull(ref string) ([]byte, string, error) {
	if r == nil {
		return nil, "", fmt.Errorf("oci: nil registry")
	}
	name, tag, err := splitRef(ref)
	if err != nil {
		return nil, "", err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	meta, err := os.ReadFile(filepath.Join(r.Root, "repos", name, tag+".ref"))
	if err != nil {
		return nil, "", err
	}
	parts := strings.Split(strings.TrimSpace(string(meta)), "\n")
	if len(parts) < 2 {
		return nil, "", fmt.Errorf("oci: corrupt ref")
	}
	body, err := os.ReadFile(parts[1])
	if err != nil {
		return nil, "", err
	}
	return body, parts[0], nil
}
