package execution

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/execution/process"
)

// CelldPin is the on-disk pin beside tools/celld-runtime.
type CelldPin struct {
	Name      string                   `json:"name"`
	Version   string                   `json:"version"`
	Tag       string                   `json:"tag"`
	Upstream  string                   `json:"upstream"`
	Artifacts map[string]CelldArtifact `json:"artifacts"`
}

// CelldArtifact is one OS/arch release binary.
type CelldArtifact struct {
	Asset      string `json:"asset"`
	URL        string `json:"url"`
	GzipSHA256 string `json:"gzip_sha256"`
	BinPath    string `json:"bin_path"`
	BinSHA256  string `json:"bin_sha256"`
}

// CelldStartSpec names the pinned binary and a Wrangler/celld project dir.
type CelldStartSpec struct {
	RuntimeDir    string // tools/celld-runtime
	ProjectDir    string // fixture or caller project
	Host          string
	Port          int
	ReadyDeadline time.Duration
}

// CelldSession supervises `celld dev` for one local project.
type CelldSession struct {
	mu       sync.Mutex
	cmd      *exec.Cmd
	baseURL  string
	identity string
	closed   bool
	waitErr  error
}

// CelldAvailable verifies PIN.json, binary digest, and --version.
func CelldAvailable(spec CelldStartSpec) error {
	pinPath := filepath.Join(spec.RuntimeDir, "PIN.json")
	raw, err := os.ReadFile(pinPath)
	if err != nil {
		return fmt.Errorf("celld: read PIN: %w (run scripts/provision-celld.sh)", err)
	}
	var pin CelldPin
	if err := json.Unmarshal(raw, &pin); err != nil {
		return fmt.Errorf("celld: parse PIN: %w", err)
	}
	key, err := celldArchKey()
	if err != nil {
		return err
	}
	art, ok := pin.Artifacts[key]
	if !ok {
		return fmt.Errorf("celld: PIN has no artifact for %s", key)
	}
	bin := filepath.Join(spec.RuntimeDir, art.BinPath)
	sum, err := fileSHA256(bin)
	if err != nil {
		return fmt.Errorf("celld: binary: %w (run scripts/provision-celld.sh)", err)
	}
	if !strings.EqualFold(sum, art.BinSHA256) {
		return fmt.Errorf("celld: bin digest %s != pinned %s", sum, art.BinSHA256)
	}
	out, err := exec.Command(bin, "--version").CombinedOutput()
	if err != nil {
		return fmt.Errorf("celld: --version: %w (%s)", err, out)
	}
	if !strings.Contains(string(out), pin.Version) {
		return fmt.Errorf("celld: version %q does not contain pinned %s", strings.TrimSpace(string(out)), pin.Version)
	}
	if spec.ProjectDir != "" {
		if _, err := os.Stat(spec.ProjectDir); err != nil {
			return fmt.Errorf("celld: project: %w", err)
		}
	}
	if _, err := celldEsbuild(spec.RuntimeDir); err != nil {
		return err
	}
	return nil
}

func celldEsbuild(runtimeDir string) (string, error) {
	if p := os.Getenv("CELLD_ESBUILD"); p != "" {
		if _, err := os.Stat(p); err != nil {
			return "", fmt.Errorf("celld: CELLD_ESBUILD: %w", err)
		}
		return p, nil
	}
	cand := filepath.Join(runtimeDir, "node_modules", ".bin", "esbuild")
	if _, err := os.Stat(cand); err != nil {
		return "", fmt.Errorf("celld: esbuild missing (npm ci in tools/celld-runtime or set CELLD_ESBUILD)")
	}
	return cand, nil
}

func celldArchKey() (string, error) {
	switch runtime.GOOS + "/" + runtime.GOARCH {
	case "linux/arm64":
		return "linux-aarch64", nil
	case "linux/amd64":
		return "linux-amd64", nil
	default:
		return "", fmt.Errorf("celld: unsupported platform %s/%s", runtime.GOOS, runtime.GOARCH)
	}
}

func fileSHA256(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// CelldStart runs `celld dev --no-watch` on the fixture/project.
func CelldStart(ctx context.Context, spec CelldStartSpec) (*CelldSession, error) {
	if err := CelldAvailable(spec); err != nil {
		return nil, err
	}
	pinRaw, _ := os.ReadFile(filepath.Join(spec.RuntimeDir, "PIN.json"))
	var pin CelldPin
	_ = json.Unmarshal(pinRaw, &pin)
	key, _ := celldArchKey()
	bin := filepath.Join(spec.RuntimeDir, pin.Artifacts[key].BinPath)
	host := spec.Host
	if host == "" {
		host = "127.0.0.1"
	}
	port := spec.Port
	if port == 0 {
		ln, err := net.Listen("tcp", host+":0")
		if err != nil {
			return nil, err
		}
		port = ln.Addr().(*net.TCPAddr).Port
		_ = ln.Close()
	}
	deadline := spec.ReadyDeadline
	if deadline == 0 {
		deadline = 45 * time.Second
	}
	args := []string{"dev", spec.ProjectDir, "--host", host, "--port", fmt.Sprint(port), "--no-watch", "--clean"}
	esbuild, err := celldEsbuild(spec.RuntimeDir)
	if err != nil {
		return nil, err
	}
	cmd := exec.CommandContext(ctx, bin, args...)
	cmd.Dir = spec.ProjectDir
	cmd.Env = process.ScrubEnv([]string{
		"PATH=" + os.Getenv("PATH"),
		"HOME=" + os.Getenv("HOME"),
		"CELLD_ESBUILD=" + esbuild,
	})
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	cmd.Stderr = cmd.Stdout
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("celld: start: %w", err)
	}
	s := &CelldSession{
		cmd:      cmd,
		baseURL:  fmt.Sprintf("http://%s:%d", host, port),
		identity: "celld/" + pin.Version,
	}
	readyCtx, cancel := context.WithTimeout(ctx, deadline)
	defer cancel()
	if err := waitHTTPReady(readyCtx, s.baseURL+"/"); err != nil {
		_ = s.Close()
		slurp, _ := io.ReadAll(io.LimitReader(stdout, 8<<10))
		return nil, fmt.Errorf("celld: ready: %w\n%s", err, slurp)
	}
	go io.Copy(io.Discard, stdout)
	return s, nil
}

func waitHTTPReady(ctx context.Context, url string) error {
	var last error
	for {
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		resp, err := http.DefaultClient.Do(req)
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode > 0 {
				return nil
			}
		} else {
			last = err
		}
		select {
		case <-ctx.Done():
			if last != nil {
				return last
			}
			return ctx.Err()
		case <-time.After(200 * time.Millisecond):
		}
	}
}

// Identity is "celld/<pinned-version>".
func (s *CelldSession) Identity() string { return s.identity }

// BaseURL is the Worker listener.
func (s *CelldSession) BaseURL() string { return s.baseURL }

// Fetch hits the local Worker.
func (s *CelldSession) Fetch(ctx context.Context, method, path string, body io.Reader) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, method, s.baseURL+path, body)
	if err != nil {
		return nil, err
	}
	return http.DefaultClient.Do(req)
}

// Close reaps the celld process group.
func (s *CelldSession) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return s.waitErr
	}
	s.closed = true
	if s.cmd == nil || s.cmd.Process == nil {
		return nil
	}
	pid := s.cmd.Process.Pid
	_ = syscall.Kill(-pid, syscall.SIGTERM)
	done := make(chan error, 1)
	go func() { done <- s.cmd.Wait() }()
	select {
	case err := <-done:
		s.waitErr = err
	case <-time.After(5 * time.Second):
		_ = syscall.Kill(-pid, syscall.SIGKILL)
		s.waitErr = s.cmd.Wait()
	}
	return nil
}
