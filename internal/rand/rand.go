// Package rand is the only source of randomness. All methods are
// deterministic given the seed and the call sequence.
package rand

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"sync"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

// Seeded is a deterministic Rand.
type Seeded struct {
	mu   sync.Mutex
	ctr  uint64
	seed [32]byte
}

// New returns a Rand derived from seed.
func New(seed string) *Seeded {
	s := &Seeded{seed: sha256.Sum256([]byte(seed))}
	return s
}

func (s *Seeded) next() []byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ctr++
	var buf [40]byte
	copy(buf[:32], s.seed[:])
	binary.BigEndian.PutUint64(buf[32:], s.ctr)
	sum := sha256.Sum256(buf[:])
	return sum[:]
}

func (s *Seeded) Intn(n int) int {
	if n <= 0 {
		return 0
	}
	b := s.next()
	return int(binary.BigEndian.Uint64(b[:8]) % uint64(n))
}

func (s *Seeded) Bytes(n int) []byte {
	out := make([]byte, n)
	off := 0
	for off < n {
		b := s.next()
		copy(out[off:], b)
		off += len(b)
	}
	return out[:n]
}

func (s *Seeded) Hex(n int) string {
	return hex.EncodeToString(s.Bytes((n + 1) / 2))[:n]
}

func (s *Seeded) UUID() string {
	b := s.Bytes(16)
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

func (s *Seeded) Derive(key string) spi.Rand {
	h := sha256.New()
	h.Write(s.seed[:])
	h.Write([]byte(key))
	var next Seeded
	copy(next.seed[:], h.Sum(nil))
	return &next
}

var _ spi.Rand = (*Seeded)(nil)
