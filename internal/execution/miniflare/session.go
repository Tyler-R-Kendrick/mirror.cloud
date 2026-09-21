// Package miniflare is a CapabilitySession for an optional Miniflare/workerd backend.
// It never downloads packages or runs npx; callers must preprovision tools/cloudflare-runtime.
package miniflare

import (
	"bufio"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/execution/process"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

const protocolVersion = 1

// Session implements spi.CapabilitySession for Miniflare via the Node helper.
//
// Provisioning requires Node, helper.mjs, and tools/cloudflare-runtime/node_modules
// (from npm ci). Workerd/Miniflare fields are legacy aliases; prefer Node+Helper.
type Session struct {
	Workerd   string // optional legacy; unused when Helper is set
	Node      string // path to node binary
	Miniflare string // legacy: treated as Helper when Helper empty
	Helper    string // path to helper.mjs

	mu      sync.Mutex
	proc    *process.Proc
	scan    *bufio.Scanner
	nextID  atomic.Uint64
	started bool
	version string
}

func (s *Session) helperPath() string {
	if s == nil {
		return ""
	}
	if s.Helper != "" {
		return s.Helper
	}
	return s.Miniflare
}

// provisioned reports whether node, helper, and miniflare node_modules are present.
func (s *Session) provisioned() bool {
	if s == nil || s.Node == "" {
		return false
	}
	helper := s.helperPath()
	if helper == "" {
		return false
	}
	if _, err := os.Stat(s.Node); err != nil {
		return false
	}
	if _, err := os.Stat(helper); err != nil {
		return false
	}
	modules := filepath.Join(filepath.Dir(helper), "node_modules", "miniflare")
	if _, err := os.Stat(modules); err != nil {
		return false
	}
	return true
}

type wireReq struct {
	V         int    `json:"v"`
	ID        string `json:"id"`
	Op        string `json:"op"`
	Key       string `json:"key,omitempty"`
	ValueB64  string `json:"value_b64,omitempty"`
	Method    string `json:"method,omitempty"`
	KVBinding string `json:"kv_binding,omitempty"`
}

type wireErr struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

type wireResp struct {
	V      int            `json:"v"`
	ID     string         `json:"id"`
	OK     bool           `json:"ok"`
	Op     string         `json:"op,omitempty"`
	Result map[string]any `json:"result,omitempty"`
	Error  *wireErr       `json:"error,omitempty"`
}

// Start launches the helper and creates a Miniflare instance with one KV binding.
// Does not download or run npx.
func (s *Session) Start(ctx context.Context) error {
	if !s.provisioned() {
		return &spi.CapabilityError{
			Kind:    spi.CapErrUnavailable,
			Message: "miniflare helper/node/node_modules not provisioned; run npm ci in tools/cloudflare-runtime (Start does not download or run npx)",
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.started {
		return nil
	}
	helper := s.helperPath()
	dir := filepath.Dir(helper)
	p, err := process.Start(ctx, process.Config{
		Path:  s.Node,
		Args:  []string{helper},
		Dir:   dir,
		Stdio: true,
		Env:   os.Environ(), // scrubbed inside process.Start
	})
	if err != nil {
		return &spi.CapabilityError{
			Kind:    spi.CapErrUnavailable,
			Message: fmt.Sprintf("helper spawn: %v", err),
		}
	}
	s.proc = p
	s.scan = bufio.NewScanner(p.Stdout)
	s.scan.Buffer(make([]byte, 0, 64*1024), 16*1024*1024)

	// Consume ready banner.
	if !s.scan.Scan() {
		_ = p.Close()
		s.proc = nil
		return &spi.CapabilityError{
			Kind:    spi.CapErrUnavailable,
			Message: "helper produced no ready line",
		}
	}
	var ready wireResp
	if err := json.Unmarshal(s.scan.Bytes(), &ready); err != nil || ready.Op != "ready" {
		_ = p.Close()
		s.proc = nil
		return &spi.CapabilityError{
			Kind:    spi.CapErrUnavailable,
			Message: "helper ready handshake failed",
		}
	}

	if _, err := s.roundTripLocked(ctx, wireReq{Op: "start", KVBinding: "KV"}); err != nil {
		_ = p.Close()
		s.proc = nil
		s.scan = nil
		return err
	}
	s.started = true
	s.version = "3.20250718.3"
	return nil
}

// Close stops the helper. Idempotent.
func (s *Session) Close(context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.proc == nil {
		s.started = false
		return nil
	}
	if s.started {
		_, _ = s.roundTripLocked(context.Background(), wireReq{Op: "stop"})
	}
	err := s.proc.Close()
	s.proc = nil
	s.scan = nil
	s.started = false
	return err
}

func (s *Session) roundTripLocked(ctx context.Context, req wireReq) (map[string]any, error) {
	if s.proc == nil || s.proc.Stdin == nil || s.scan == nil {
		return nil, &spi.CapabilityError{Kind: spi.CapErrUnavailable, Message: "helper not running"}
	}
	req.V = protocolVersion
	if req.ID == "" {
		req.ID = fmt.Sprintf("%d", s.nextID.Add(1))
	}
	line, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if _, err := s.proc.Stdin.Write(append(line, '\n')); err != nil {
		return nil, &spi.CapabilityError{Kind: spi.CapErrUnavailable, Message: fmt.Sprintf("helper write: %v", err)}
	}
	if !s.scan.Scan() {
		return nil, &spi.CapabilityError{Kind: spi.CapErrUnavailable, Message: "helper closed stdout"}
	}
	var resp wireResp
	if err := json.Unmarshal(s.scan.Bytes(), &resp); err != nil {
		return nil, &spi.CapabilityError{Kind: spi.CapErrUnavailable, Message: "helper bad json"}
	}
	if resp.V != protocolVersion {
		return nil, &spi.CapabilityError{Kind: spi.CapErrValidation, Message: "protocol mismatch"}
	}
	if !resp.OK {
		kind := spi.CapErrUnavailable
		msg := "helper error"
		if resp.Error != nil {
			msg = resp.Error.Message
			switch resp.Error.Code {
			case "validation":
				kind = spi.CapErrValidation
			case "unsupported":
				kind = spi.CapErrUnsupported
			case "absent":
				kind = spi.CapErrAbsent
			case "unavailable":
				kind = spi.CapErrUnavailable
			}
		}
		return nil, &spi.CapabilityError{Kind: kind, Capability: req.Op, Message: msg}
	}
	return resp.Result, nil
}

func (s *Session) callHelper(ctx context.Context, req wireReq) (map[string]any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.started {
		return nil, &spi.CapabilityError{
			Kind:    spi.CapErrUnavailable,
			Message: "session not started",
		}
	}
	return s.roundTripLocked(ctx, req)
}

// Describe reports backend "miniflare". Lists kv.put/kv.get when provisioned.
func (s *Session) Describe(context.Context) (spi.BackendIdentity, []spi.CapabilityDescriptor, error) {
	id := spi.BackendIdentity{Name: "miniflare", Version: s.version}
	if !s.provisioned() {
		return id, nil, nil
	}
	req := "miniflare>=3.20250718.3"
	caps := []spi.CapabilityDescriptor{
		{
			Name:               "kv.put",
			InputSchema:        "tag:kv.put.in",
			OutputSchema:       "tag:kv.put.out",
			ResourceKind:       "kv_namespace",
			BodyMode:           spi.BodyNone,
			FailureClass:       "kv",
			EffectClass:        spi.EffectWrite,
			BackendRequirement: req,
		},
		{
			Name:               "kv.get",
			InputSchema:        "tag:kv.get.in",
			OutputSchema:       "tag:kv.get.out",
			ResourceKind:       "kv_namespace",
			BodyMode:           spi.BodyNone,
			FailureClass:       "kv",
			EffectClass:        spi.EffectRead,
			BackendRequirement: req,
		},
	}
	return id, caps, nil
}

// Call dispatches kv.put / kv.get through the helper. Other actions are unsupported.
func (s *Session) Call(ctx context.Context, req spi.CapabilityCall) (*spi.CapabilityResult, error) {
	if !s.provisioned() {
		return nil, &spi.CapabilityError{
			Kind:       spi.CapErrUnavailable,
			Capability: req.Action,
			Message:    "miniflare helper/node/node_modules not provisioned; run npm ci in tools/cloudflare-runtime (Start does not download or run npx)",
		}
	}
	if !s.started {
		return nil, &spi.CapabilityError{
			Kind:       spi.CapErrUnavailable,
			Capability: req.Action,
			Message:    "session not started",
		}
	}
	key, _ := req.Args["key"].(string)
	if key == "" {
		key = req.Resource.ID
	}
	switch req.Action {
	case "kv.put":
		b64, err := valueB64(req.Args)
		if err != nil {
			return nil, &spi.CapabilityError{Kind: spi.CapErrValidation, Capability: "kv.put", Message: err.Error()}
		}
		out, err := s.callHelper(ctx, wireReq{Op: "kv_put", Key: key, ValueB64: b64})
		if err != nil {
			return nil, err
		}
		return &spi.CapabilityResult{Output: map[string]any{"ok": true, "backend": "miniflare", "result": out}}, nil
	case "kv.get":
		out, err := s.callHelper(ctx, wireReq{Op: "kv_get", Key: key})
		if err != nil {
			return nil, err
		}
		val := ""
		if v, ok := out["value_b64"].(string); ok {
			raw, decErr := base64.StdEncoding.DecodeString(v)
			if decErr != nil {
				return nil, &spi.CapabilityError{Kind: spi.CapErrUnavailable, Capability: "kv.get", Message: "bad b64"}
			}
			val = string(raw)
		}
		return &spi.CapabilityResult{Output: map[string]any{
			"value":     val,
			"value_b64": out["value_b64"],
			"backend":   "miniflare",
		}}, nil
	case "worker.fetch":
		// Optional coherence path: Worker put/get via dispatchFetch.
		method, _ := req.Args["method"].(string)
		if method == "" {
			method = "GET"
		}
		b64, _ := req.Args["value_b64"].(string)
		if b64 == "" {
			if v, ok := req.Args["value"].(string); ok {
				b64 = base64.StdEncoding.EncodeToString([]byte(v))
			}
		}
		out, err := s.callHelper(ctx, wireReq{Op: "worker_fetch", Key: key, Method: method, ValueB64: b64})
		if err != nil {
			return nil, err
		}
		return &spi.CapabilityResult{Output: out}, nil
	default:
		return nil, &spi.CapabilityError{
			Kind:       spi.CapErrUnsupported,
			Capability: req.Action,
			Message:    "miniflare session has no configured capability descriptors for this action",
		}
	}
}

func valueB64(args map[string]any) (string, error) {
	if args == nil {
		return "", fmt.Errorf("missing value")
	}
	if v, ok := args["value_b64"].(string); ok {
		return v, nil
	}
	switch v := args["value"].(type) {
	case string:
		return base64.StdEncoding.EncodeToString([]byte(v)), nil
	case []byte:
		return base64.StdEncoding.EncodeToString(v), nil
	default:
		return "", fmt.Errorf("value or value_b64 required")
	}
}

// Ensure Session satisfies CapabilitySession.
var _ spi.CapabilitySession = (*Session)(nil)
