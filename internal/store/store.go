// Package store implements account+region namespaced structured state.
package store

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

// Memory is an in-memory Store.
type Memory struct {
	mu      sync.Mutex
	data    map[string]map[string][]byte // scope -> key -> val, collection encoded in key
	lockSHA string
}

// NewMemory returns an empty in-memory store.
func NewMemory(lockSHA string) *Memory {
	return &Memory{data: map[string]map[string][]byte{}, lockSHA: lockSHA}
}

func scopeKey(account, region string) string { return account + "\x1f" + region }

func (m *Memory) Scope(account, region string) spi.Scope {
	return &scope{m: m, account: account, region: region}
}

func (m *Memory) Scopes(context.Context) ([]spi.Identity, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	keys := make([]string, 0, len(m.data))
	for key := range m.data {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	out := make([]spi.Identity, 0, len(keys))
	for _, key := range keys {
		account, region, _ := strings.Cut(key, "\x1f")
		out = append(out, spi.Identity{Account: account, Region: region})
	}
	return out, nil
}

type scope struct {
	m               *Memory
	account, region string
}

func (s *scope) Collection(name string) spi.Collection {
	return &coll{s: s, name: name}
}

type coll struct {
	s    *scope
	name string
}

func (c *coll) full(key string) string { return c.name + "\x1f" + key }

func (c *coll) Get(_ context.Context, key string) ([]byte, bool, error) {
	c.s.m.mu.Lock()
	defer c.s.m.mu.Unlock()
	sk := scopeKey(c.s.account, c.s.region)
	v, ok := c.s.m.data[sk][c.full(key)]
	if !ok {
		return nil, false, nil
	}
	out := make([]byte, len(v))
	copy(out, v)
	return out, true, nil
}

func (c *coll) Put(_ context.Context, key string, val []byte) error {
	c.s.m.mu.Lock()
	defer c.s.m.mu.Unlock()
	sk := scopeKey(c.s.account, c.s.region)
	if c.s.m.data[sk] == nil {
		c.s.m.data[sk] = map[string][]byte{}
	}
	cp := make([]byte, len(val))
	copy(cp, val)
	c.s.m.data[sk][c.full(key)] = cp
	return nil
}

func (c *coll) Delete(_ context.Context, key string) error {
	c.s.m.mu.Lock()
	defer c.s.m.mu.Unlock()
	sk := scopeKey(c.s.account, c.s.region)
	delete(c.s.m.data[sk], c.full(key))
	return nil
}

func (c *coll) List(_ context.Context, prefix, after string, limit int) ([]spi.KV, bool, error) {
	c.s.m.mu.Lock()
	defer c.s.m.mu.Unlock()
	return c.listLocked(prefix, after, limit)
}

func (c *coll) listLocked(prefix, after string, limit int) ([]spi.KV, bool, error) {
	sk := scopeKey(c.s.account, c.s.region)
	p := c.full(prefix)
	var keys []string
	for k := range c.s.m.data[sk] {
		if strings.HasPrefix(k, p) {
			naked := strings.TrimPrefix(k, c.name+"\x1f")
			if after == "" || naked > after {
				keys = append(keys, naked)
			}
		}
	}
	sort.Strings(keys)
	more := false
	if limit > 0 && len(keys) > limit {
		more = true
		keys = keys[:limit]
	}
	out := make([]spi.KV, 0, len(keys))
	for _, k := range keys {
		v := c.s.m.data[sk][c.full(k)]
		cp := make([]byte, len(v))
		copy(cp, v)
		out = append(out, spi.KV{Key: k, Value: cp})
	}
	return out, more, nil
}

func (c *coll) Txn(_ context.Context, fn func(spi.Tx) error) error {
	c.s.m.mu.Lock()
	defer c.s.m.mu.Unlock()
	return fn(&tx{c: c})
}

type tx struct{ c *coll }

func (t *tx) Get(key string) ([]byte, bool, error) {
	sk := scopeKey(t.c.s.account, t.c.s.region)
	v, ok := t.c.s.m.data[sk][t.c.full(key)]
	if !ok {
		return nil, false, nil
	}
	cp := make([]byte, len(v))
	copy(cp, v)
	return cp, true, nil
}

func (t *tx) Put(key string, val []byte) error {
	sk := scopeKey(t.c.s.account, t.c.s.region)
	if t.c.s.m.data[sk] == nil {
		t.c.s.m.data[sk] = map[string][]byte{}
	}
	cp := make([]byte, len(val))
	copy(cp, val)
	t.c.s.m.data[sk][t.c.full(key)] = cp
	return nil
}

func (t *tx) Delete(key string) error {
	sk := scopeKey(t.c.s.account, t.c.s.region)
	delete(t.c.s.m.data[sk], t.c.full(key))
	return nil
}

func (t *tx) List(prefix, after string, limit int) ([]spi.KV, bool, error) {
	return t.c.listLocked(prefix, after, limit)
}

func (m *Memory) Snapshot(_ context.Context, w io.Writer) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	tw := tar.NewWriter(w)
	defer tw.Close()
	meta, _ := json.Marshal(map[string]string{"lockSHA": m.lockSHA})
	if err := writeTar(tw, "manifest.json", meta); err != nil {
		return err
	}
	body, err := json.Marshal(m.data)
	if err != nil {
		return err
	}
	return writeTar(tw, "store.json", body)
}

func (m *Memory) Restore(_ context.Context, r io.Reader) error {
	tr := tar.NewReader(r)
	files := map[string][]byte{}
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		b, err := io.ReadAll(tr)
		if err != nil {
			return err
		}
		files[hdr.Name] = b
	}
	var man struct {
		LockSHA string `json:"lockSHA"`
	}
	if err := json.Unmarshal(files["manifest.json"], &man); err != nil {
		return err
	}
	if m.lockSHA != "" && man.LockSHA != "" && man.LockSHA != m.lockSHA {
		return fmt.Errorf("snapshot spec-lock hash %s does not match current %s", man.LockSHA, m.lockSHA)
	}
	var data map[string]map[string][]byte
	if err := json.Unmarshal(files["store.json"], &data); err != nil {
		return err
	}
	m.mu.Lock()
	m.data = data
	m.mu.Unlock()
	return nil
}

func (m *Memory) Close() error { return nil }

func writeTar(tw *tar.Writer, name string, body []byte) error {
	if err := tw.WriteHeader(&tar.Header{Name: name, Mode: 0o644, Size: int64(len(body))}); err != nil {
		return err
	}
	_, err := tw.Write(body)
	return err
}

// SnapshotBytes is a helper for tests.
func SnapshotBytes(s spi.Store) ([]byte, error) {
	var buf bytes.Buffer
	if err := s.Snapshot(context.Background(), &buf); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
