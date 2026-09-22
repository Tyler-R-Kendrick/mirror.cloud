package execution

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"
)

// MiniflareSession supervises one helper process (tools/cloudflare-runtime)
// and speaks its versioned v1 control protocol. It is constructed only when
// the miniflare execution profile is selected: nothing in the native binary
// path references it, and Close is safe to call twice.
//
// Process discipline (mirrored by the tests):
//   - the executable is an explicit path resolved by the caller, never a
//     PATH search performed here and never a shell string;
//   - the environment is a scrubbed minimal set plus the session token, so
//     inherited provider credentials, proxy variables and telemetry hooks
//     cannot reach the helper;
//   - readiness is the helper's own describe over authenticated loopback,
//     bounded by a deadline -- not a sleep and not a user's HTTP response;
//   - Close asks the helper to dispose its Miniflare environment, then
//     signals it, then waits, so workerd children are reaped.
type MiniflareSession struct {
	mu       sync.Mutex
	cmd      *exec.Cmd
	baseURL  string
	token    string
	identity string // backend name+version from describe
	closed   bool
	waitErr  error
}

// MiniflareStartSpec is everything Start needs; the caller resolves paths
// and proves the package exists before asking for a session.
type MiniflareStartSpec struct {
	// NodeBin is the explicit node executable (absolute path or a name the
	// caller verified). Required.
	NodeBin string
	// HelperDir is the tools/cloudflare-runtime directory containing
	// session.mjs and node_modules. Required.
	HelperDir string
	// ReadyDeadline bounds the wait for the helper's first describe.
	ReadyDeadline time.Duration
	// ExtraEnv, if any, is appended AFTER the scrub (caller's choice, never
	// inherited implicitly).
	ExtraEnv []string
	// OfflineNetNS runs the helper under `unshare -n` with loopback only
	// (packet-level isolation). Requires CAP_SYS_ADMIN / unshare permission.
	OfflineNetNS bool
}

// scrubEnv builds the helper's environment from scratch: PATH (node resolves
// its own workerd relative to its install), HOME (node cache dirs), and
// nothing else. No HTTP_PROXY, no npm telemetry, no provider credentials
// ride along to a process that only serves loopback.
func scrubEnv(extra []string) []string {
	env := []string{
		"PATH=" + os.Getenv("PATH"),
		"HOME=" + os.Getenv("HOME"),
		"NODE_TELEMETRY_DISABLED=1",
		"npm_config_update_notifier=false",
	}
	return append(env, extra...)
}

// MiniflareAvailable reports whether the optional runtime is provisioned:
// node resolvable and the helper's node_modules present. Callers fail fast
// with a diagnostic naming the missing capability instead of silently
// selecting another backend.
func MiniflareAvailable(spec MiniflareStartSpec) error {
	if spec.NodeBin == "" {
		return fmt.Errorf("miniflare: NodeBin not configured")
	}
	if _, err := exec.LookPath(spec.NodeBin); err != nil {
		return fmt.Errorf("miniflare: node not resolvable at %q: %w", spec.NodeBin, err)
	}
	if spec.HelperDir == "" {
		return fmt.Errorf("miniflare: HelperDir not configured")
	}
	for _, need := range []string{"session.mjs", filepath.Join("node_modules", "miniflare")} {
		if _, err := os.Stat(filepath.Join(spec.HelperDir, need)); err != nil {
			return fmt.Errorf("miniflare: missing %s under %s (run npm install there): %w",
				need, spec.HelperDir, err)
		}
	}
	return nil
}

// MiniflareToken generates the per-session bearer credential with crypto
// strength. It exists only in memory and in the child's environment; it is
// never journaled, never persisted, and never derived from spi.Rand -- the
// deterministic stream is for reproducible fixtures, not secrets.
func MiniflareToken() (string, error) {
	buf := make([]byte, 32)
	if _, err := rand.Read(buf); err != nil {
		return "", fmt.Errorf("miniflare: token entropy: %w", err)
	}
	return base64.RawURLEncoding.EncodeToString(buf), nil
}

// MiniflareStart spawns the helper, waits for authenticated readiness, and
// answers a live session. On any failure it cleans the partial process and
// returns a typed error; no orphan survives Start.
func MiniflareStart(ctx context.Context, spec MiniflareStartSpec) (*MiniflareSession, error) {
	if err := MiniflareAvailable(spec); err != nil {
		return nil, err
	}
	token, err := MiniflareToken()
	if err != nil {
		return nil, err
	}
	deadline := spec.ReadyDeadline
	if deadline <= 0 {
		deadline = 30 * time.Second
	}
	readyCtx, cancel := context.WithTimeout(ctx, deadline)
	defer cancel()

	// The helper's lifetime is NOT the readiness context: CommandContext
	// would SIGKILL the child the moment readyCtx is cancelled (i.e. the
	// instant Start returns). Lifecycle belongs to Close; readiness is only
	// a bounded wait on the ready line below.
	var cmd *exec.Cmd
	if spec.OfflineNetNS {
		// Empty network namespace + loopback only: packet-level offline.
		nodeBin := spec.NodeBin
		if abs, err := exec.LookPath(nodeBin); err == nil {
			nodeBin = abs
		}
		script := fmt.Sprintf("ip link set lo up 2>/dev/null || true; cd %q && exec %q session.mjs", spec.HelperDir, nodeBin)
		cmd = exec.Command("unshare", "-n", "--", "bash", "-c", script)
	} else {
		cmd = exec.Command(spec.NodeBin, "session.mjs")
	}
	cmd.Dir = spec.HelperDir
	cmd.Env = scrubEnv(append([]string{"MIRROR_SESSION_TOKEN=" + token}, spec.ExtraEnv...))
	// New process group: Close can signal the whole tree (node -> workerd)
	// without racing the parent's stdio.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, fmt.Errorf("miniflare: stdout pipe: %w", err)
	}
	var stderr strings.Builder
	cmd.Stderr = &stderr

	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("miniflare: spawn: %w", err)
	}
	s := &MiniflareSession{cmd: cmd, token: token}

	// The helper prints exactly one JSON line when its loopback listener is
	// up: {ready, port, protocol}. Read it with the readiness deadline; a
	// helper that never prints (bad install, bad syntax) fails Start.
	readyLine := make(chan string, 1)
	go func() {
		sc := bufio.NewScanner(stdout)
		if sc.Scan() {
			readyLine <- sc.Text()
		}
		close(readyLine)
	}()
	var port int
	select {
	case line, ok := <-readyLine:
		if !ok {
			s.cleanupAfterFailedStart()
			return nil, fmt.Errorf("miniflare: helper exited before ready: %s", strings.TrimSpace(stderr.String()))
		}
		var hello struct {
			Ready    bool `json:"ready"`
			Port     int  `json:"port"`
			Protocol int  `json:"protocol"`
		}
		if err := json.Unmarshal([]byte(line), &hello); err != nil || !hello.Ready || hello.Protocol != 1 {
			s.cleanupAfterFailedStart()
			return nil, fmt.Errorf("miniflare: bad ready line %q", line)
		}
		port = hello.Port
	case <-readyCtx.Done():
		s.cleanupAfterFailedStart()
		return nil, fmt.Errorf("miniflare: readiness deadline exceeded: %s", strings.TrimSpace(stderr.String()))
	}
	// Keep draining stdout so the child never blocks on a full pipe.
	go func() {
		sc := bufio.NewScanner(stdout)
		for sc.Scan() {
		}
	}()

	s.baseURL = fmt.Sprintf("http://127.0.0.1:%d", port)
	// Authenticated describe is the real readiness probe: the listener is
	// up AND the token works AND protocol matches.
	desc, err := s.Describe(ctx)
	if err != nil {
		_ = s.Close()
		return nil, fmt.Errorf("miniflare: describe after ready: %w", err)
	}
	if fmt.Sprint(desc["protocol"]) != "1" {
		_ = s.Close()
		return nil, fmt.Errorf("miniflare: helper protocol %v, want 1", desc["protocol"])
	}
	s.identity = fmt.Sprintf("%v/%v", desc["backend"], desc["version"])
	return s, nil
}

func (s *MiniflareSession) cleanupAfterFailedStart() {
	if s.cmd != nil && s.cmd.Process != nil {
		_ = syscall.Kill(-s.cmd.Process.Pid, syscall.SIGKILL)
		_, _ = s.cmd.Process.Wait()
	}
}

// Identity answers "backend/version" as the helper reported it.
func (s *MiniflareSession) Identity() string { return s.identity }

// Describe calls the helper's authenticated describe endpoint.
func (s *MiniflareSession) Describe(ctx context.Context) (map[string]any, error) {
	return s.roundTrip(ctx, http.MethodGet, "/v1/describe", nil)
}

// Apply materializes a binding graph. The graph is caller-normalized JSON;
// the helper re-validates its own safety constraints on top.
func (s *MiniflareSession) Apply(ctx context.Context, graph any) (map[string]any, error) {
	return s.roundTrip(ctx, http.MethodPost, "/v1/apply", graph)
}

// Call runs one closed-set control action (kv.*, worker.dispatch).
func (s *MiniflareSession) Call(ctx context.Context, action string, args map[string]any) (map[string]any, error) {
	body := map[string]any{"action": action}
	for k, v := range args {
		body[k] = v
	}
	return s.roundTrip(ctx, http.MethodPost, "/v1/call", body)
}

// Quiesce refuses new application work and reports stable state.
func (s *MiniflareSession) Quiesce(ctx context.Context) (map[string]any, error) {
	return s.roundTrip(ctx, http.MethodPost, "/v1/quiesce", map[string]any{})
}

// Close disposes the helper cleanly, then signals the process group if it
// lingers. Safe on a closed session and on a session whose Start failed.
func (s *MiniflareSession) Close() error {
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
	// Best-effort polite close: the helper disposes Miniflare, reaps
	// workerd, and exits 0 on its own.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	_, _ = s.roundTrip(ctx, http.MethodPost, "/v1/close", map[string]any{})
	cancel()
	done := make(chan error, 1)
	go func() { done <- s.cmd.Wait() }()
	select {
	case err := <-done:
		s.waitErr = err
		return nil
	case <-time.After(3 * time.Second):
		_ = syscall.Kill(-pid, syscall.SIGTERM)
	}
	select {
	case err := <-done:
		s.waitErr = err
	case <-time.After(3 * time.Second):
		_ = syscall.Kill(-pid, syscall.SIGKILL)
		s.waitErr = s.cmd.Wait()
	}
	return nil
}

func (s *MiniflareSession) roundTrip(ctx context.Context, method, path string, payload any) (map[string]any, error) {
	var body io.Reader
	if payload != nil {
		raw, err := json.Marshal(payload)
		if err != nil {
			return nil, fmt.Errorf("miniflare: marshal %s: %w", path, err)
		}
		body = strings.NewReader(string(raw))
	}
	req, err := http.NewRequestWithContext(ctx, method, s.baseURL+path, body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+s.token)
	if payload != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, &Failure{Class: ClassUnavailable, Action: path,
			Detail: "control request failed: " + err.Error()}
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 64*1024*1024+1))
	if err != nil {
		return nil, fmt.Errorf("miniflare: read %s: %w", path, err)
	}
	if len(raw) > 64*1024*1024 {
		return nil, fmt.Errorf("miniflare: %s answer exceeds 64MiB control bound", path)
	}
	var out map[string]any
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, fmt.Errorf("miniflare: %s non-JSON answer (%d): %w", path, resp.StatusCode, err)
	}
	if resp.StatusCode != http.StatusOK {
		ec, _ := out["error"].(map[string]any)
		code, _ := ec["code"].(string)
		msg, _ := ec["message"].(string)
		return out, &Failure{
			Class:  controlClass(code, resp.StatusCode),
			Action: path,
			Detail: fmt.Sprintf("%s: %s", code, msg),
		}
	}
	return out, nil
}

// controlClass maps the helper's closed error-code vocabulary onto the
// boundary taxonomy. Anything unrecognized from a 5xx reads as unavailable
// (the backend itself failed); a 4xx reads as validation (the request was
// refused before it ran).
func controlClass(code string, status int) Class {
	switch code {
	case "unknown_action", "unsupported":
		return ClassUnsupported
	case "namespace_absent":
		return ClassAbsent
	case "not_applied", "quiesced":
		return ClassUnavailable
	case "invalid_call", "invalid_graph", "invalid_json":
		return ClassValidation
	}
	if status >= 500 {
		return ClassUnavailable
	}
	return ClassValidation
}

// SnapshotKV exports every applied KV namespace as portable JSON
// {kv: {ns: {key: base64}}, generation}.
// Kill SIGKILLs the helper process group immediately. Used by CF-CRASH tests
// to prove mid-invocation failure maps to ClassUnavailable; not for polite Close.
func (s *MiniflareSession) Kill() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cmd == nil || s.cmd.Process == nil {
		return
	}
	s.closed = true
	_ = syscall.Kill(-s.cmd.Process.Pid, syscall.SIGKILL)
	s.waitErr = s.cmd.Wait()
}

func (s *MiniflareSession) SnapshotKV(ctx context.Context) (map[string]any, error) {
	return s.Call(ctx, "snapshot", map[string]any{})
}

// RestoreKV loads a prior SnapshotKV payload into the live namespaces.
func (s *MiniflareSession) RestoreKV(ctx context.Context, snap map[string]any) (map[string]any, error) {
	kv, _ := snap["kv"].(map[string]any)
	if kv == nil {
		kv = snap
	}
	return s.Call(ctx, "restore", map[string]any{"kv": kv})
}
