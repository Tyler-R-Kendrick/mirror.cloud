// Package shellstep runs one /bin/sh -c command via process.Run (trusted-dev).
package shellstep

import (
	"context"
	"io"
	"os"
	"strings"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/execution/process"
)

// Run executes command in dir with env (nil → scrubbed os.Environ).
func Run(ctx context.Context, dir, command string, env []string, w io.Writer) (int, error) {
	if env == nil {
		env = process.ScrubEnv(os.Environ())
	}
	return process.Run(ctx, process.Config{
		Path: "/bin/sh",
		Args: []string{"-c", command},
		Dir:  dir,
		Env:  env,
	}, w)
}

// MergeEnv copies base, replaces keys present in overlay, then scrubs.
func MergeEnv(base []string, overlay map[string]string) []string {
	if len(overlay) == 0 {
		return process.ScrubEnv(base)
	}
	drop := make(map[string]struct{}, len(overlay))
	for k := range overlay {
		drop[k] = struct{}{}
	}
	out := make([]string, 0, len(base)+len(overlay))
	for _, e := range base {
		k, _, _ := strings.Cut(e, "=")
		if _, ok := drop[k]; ok {
			continue
		}
		out = append(out, e)
	}
	for k, v := range overlay {
		out = append(out, k+"="+v)
	}
	return process.ScrubEnv(out)
}

// BaseEnv is MergeEnv(os.Environ(), overlay).
func BaseEnv(overlay map[string]string) []string {
	return MergeEnv(os.Environ(), overlay)
}
