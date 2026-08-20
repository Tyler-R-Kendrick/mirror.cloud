// Package blobs implements BlobStore (memory).
package blobs

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"sync"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

// Memory is an in-memory BlobStore.
type Memory struct {
	mu   sync.Mutex
	data map[string][]byte
}

// NewMemory returns an empty blob store.
func NewMemory() *Memory { return &Memory{data: map[string][]byte{}} }

func hash(b []byte) spi.BlobInfo {
	m := md5.Sum(b)
	s := sha256.Sum256(b)
	return spi.BlobInfo{Size: int64(len(b)), MD5: hex.EncodeToString(m[:]), SHA256: hex.EncodeToString(s[:])}
}

func (m *Memory) Put(_ context.Context, key string, r io.Reader) (spi.BlobInfo, error) {
	b, err := io.ReadAll(r)
	if err != nil {
		return spi.BlobInfo{}, err
	}
	m.mu.Lock()
	m.data[key] = b
	m.mu.Unlock()
	return hash(b), nil
}

func (m *Memory) Get(_ context.Context, key string) (io.ReadSeekCloser, spi.BlobInfo, error) {
	m.mu.Lock()
	b, ok := m.data[key]
	m.mu.Unlock()
	if !ok {
		return nil, spi.BlobInfo{}, fmt.Errorf("blob not found: %s", key)
	}
	return nopCloser{bytes.NewReader(b)}, hash(b), nil
}

func (m *Memory) Stat(_ context.Context, key string) (spi.BlobInfo, error) {
	m.mu.Lock()
	b, ok := m.data[key]
	m.mu.Unlock()
	if !ok {
		return spi.BlobInfo{}, fmt.Errorf("blob not found: %s", key)
	}
	return hash(b), nil
}

func (m *Memory) Delete(_ context.Context, key string) error {
	m.mu.Lock()
	delete(m.data, key)
	m.mu.Unlock()
	return nil
}

func (m *Memory) Snapshot(_ context.Context, w io.Writer) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	enc := json.NewEncoder(w)
	return enc.Encode(m.data)
}

func (m *Memory) Restore(_ context.Context, r io.Reader) error {
	var data map[string][]byte
	if err := json.NewDecoder(r).Decode(&data); err != nil {
		return err
	}
	m.mu.Lock()
	m.data = data
	m.mu.Unlock()
	return nil
}

type nopCloser struct{ *bytes.Reader }

func (nopCloser) Close() error { return nil }
