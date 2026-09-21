//go:build !miniflare

package miniflare_test

import "os"

func liveRequested() bool {
	return os.Getenv("MIRROR_MINIFLARE") == "1"
}
