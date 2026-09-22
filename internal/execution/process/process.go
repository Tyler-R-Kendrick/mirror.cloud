// Package process supervises an explicit child executable (no shell).
package process

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"sync"
	"time"
)

// Config starts one child. Path+Args are argv; never passed through a shell.
type Config struct {
	Path string
	Args []string
	Dir  string
	// Env is the base environment. nil means os.Environ(). Always scrubbed.
	Env []string

	// Stdio, when true, attaches pipes; Proc.Stdin/Stdout are set.
	Stdio bool

	// ReadyFile, if set, must appear before Start returns.
	ReadyFile string
	// ReadyURL, if set, must return HTTP 2xx before Start returns (loopback OK).
	ReadyURL string
	// ReadyTimeout bounds readiness polling. Zero defaults to 5s.
	ReadyTimeout time.Duration
}

// Proc is a started child. Close is idempotent and cancels the process.
type Proc struct {
	cancel context.CancelFunc
	done   <-chan error

	// Stdin/Stdout set when Config.Stdio is true. Closed by Close.
	Stdin  io.WriteCloser
	Stdout io.ReadCloser

	mu     sync.Mutex
	closed bool
}

// ScrubEnv drops AWS_*, CF_API*, HTTP_PROXY, NODE_OPTIONS, and OTEL* entries.
func ScrubEnv(env []string) []string {
	out := make([]string, 0, len(env))
	for _, e := range env {
		key, _, _ := strings.Cut(e, "=")
		if scrubKey(key) {
			continue
		}
		out = append(out, e)
	}
	return out
}

func scrubKey(key string) bool {
	uk := strings.ToUpper(key)
	switch {
	case strings.HasPrefix(uk, "AWS_"):
		return true
	case strings.HasPrefix(uk, "CF_API"):
		return true
	case strings.HasPrefix(uk, "CLOUDFLARE_API"):
		return true
	case uk == "HTTP_PROXY" || uk == "HTTPS_PROXY" || uk == "ALL_PROXY" || uk == "NO_PROXY":
		return true
	case uk == "NODE_OPTIONS":
		return true
	case strings.HasPrefix(uk, "OTEL"):
		return true
	}
	return false
}

// Start launches cfg.Path with cfg.Args. Parent cancel or Close kills the child.
func Start(parent context.Context, cfg Config) (*Proc, error) {
	if cfg.Path == "" {
		return nil, fmt.Errorf("process: empty Path")
	}
	ctx, cancel := context.WithCancel(parent)
	cmd := exec.CommandContext(ctx, cfg.Path, cfg.Args...)
	cmd.Dir = cfg.Dir
	base := cfg.Env
	if base == nil {
		base = os.Environ()
	}
	cmd.Env = ScrubEnv(base)

	var stdin io.WriteCloser
	var stdout io.ReadCloser
	if cfg.Stdio {
		var err error
		stdin, err = cmd.StdinPipe()
		if err != nil {
			cancel()
			return nil, err
		}
		stdout, err = cmd.StdoutPipe()
		if err != nil {
			cancel()
			return nil, err
		}
		cmd.Stderr = io.Discard
	}

	if err := cmd.Start(); err != nil {
		cancel()
		return nil, err
	}
	done := make(chan error, 1)
	go func() {
		done <- cmd.Wait()
		close(done)
	}()

	p := &Proc{cancel: cancel, done: done, Stdin: stdin, Stdout: stdout}
	if err := p.waitReady(ctx, cfg); err != nil {
		_ = p.Close()
		return nil, err
	}
	return p, nil
}

func (p *Proc) waitReady(ctx context.Context, cfg Config) error {
	if cfg.ReadyFile == "" && cfg.ReadyURL == "" {
		return nil
	}
	timeout := cfg.ReadyTimeout
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	deadline, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	tick := time.NewTicker(20 * time.Millisecond)
	defer tick.Stop()
	for {
		if cfg.ReadyFile != "" {
			if _, err := os.Stat(cfg.ReadyFile); err == nil {
				return nil
			}
		}
		if cfg.ReadyURL != "" {
			req, err := http.NewRequestWithContext(deadline, http.MethodGet, cfg.ReadyURL, nil)
			if err == nil {
				res, err := http.DefaultClient.Do(req)
				if err == nil {
					res.Body.Close()
					if res.StatusCode >= 200 && res.StatusCode < 300 {
						return nil
					}
				}
			}
		}
		select {
		case <-deadline.Done():
			return fmt.Errorf("process: readiness timeout: %w", deadline.Err())
		case err := <-p.done:
			if err != nil {
				return fmt.Errorf("process: exited before ready: %w", err)
			}
			return fmt.Errorf("process: exited before ready")
		case <-tick.C:
		}
	}
}

// Close cancels the child and waits for exit. Safe to call more than once.
func (p *Proc) Close() error {
	if p == nil {
		return nil
	}
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}
	p.closed = true
	p.mu.Unlock()

	if p.Stdin != nil {
		_ = p.Stdin.Close()
	}
	p.cancel()
	<-p.done
	return nil
}

// Run executes Path+Args to completion. Combined stdout/stderr go to w when non-nil.
// Returns the process exit code. Context cancel kills the child. Start failures return err.
func Run(ctx context.Context, cfg Config, w io.Writer) (int, error) {
	if cfg.Path == "" {
		return -1, fmt.Errorf("process: empty Path")
	}
	cmd := exec.CommandContext(ctx, cfg.Path, cfg.Args...)
	cmd.Dir = cfg.Dir
	base := cfg.Env
	if base == nil {
		base = os.Environ()
	}
	cmd.Env = ScrubEnv(base)
	if w != nil {
		lw := &lockedWriter{w: w}
		cmd.Stdout = lw
		cmd.Stderr = lw
	}
	err := cmd.Run()
	if err == nil {
		return 0, nil
	}
	var ee *exec.ExitError
	if errors.As(err, &ee) {
		return ee.ExitCode(), nil
	}
	return -1, err
}

type lockedWriter struct {
	mu sync.Mutex
	w  io.Writer
}

func (l *lockedWriter) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.w.Write(p)
}
