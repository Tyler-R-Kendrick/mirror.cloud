// Package idgen produces request IDs from Rand.
package idgen

import "github.com/tyler-r-kendrick/mirror.cloud/internal/spi"

// Next returns a 16-hex request ID.
func Next(r spi.Rand) string { return r.Hex(16) }
