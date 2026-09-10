// Package v1 is the emulate-tier Hetzner Cloud API v1 servers+SSH keys control plane.
package v1

import (
	"context"
	"encoding/json"
	"strconv"
	"strings"
	"sync"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func init() {
	registry.Register(registry.Factory{ServiceID: "hetzner.v1", Tier: model.TierEmulate, New: func(d spi.Deps) (spi.BehaviorPack, error) {
		return New(d), nil
	}})
}

// Pack implements hetzner.v1.
type Pack struct {
	deps spi.Deps
	// ponytail: process-wide lock; per-account locks if concurrent create throughput matters.
	mu sync.Mutex
}

// New constructs the pack.
func New(d spi.Deps) *Pack { return &Pack{deps: d} }

func (p *Pack) ServiceID() string { return "hetzner.v1" }
func (p *Pack) Tier() model.Tier  { return model.TierEmulate }
func (p *Pack) Operations() []string {
	return []string{"CreateServer", "ListServers", "GetServer", "DeleteServer", "CreateSSHKey", "ListSSHKeys", "GetSSHKey", "DeleteSSHKey"}
}

func (p *Pack) col(req *spi.Request, n string) spi.Collection {
	acct := req.Identity.Account
	if acct == "" {
		acct = "000000000000"
	}
	reg := req.Identity.Region
	if reg == "" {
		reg = "us-east-1"
	}
	return p.deps.Store.Scope(acct, reg).Collection(n)
}

func (p *Pack) Invoke(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if req.Input == nil {
		req.Input = map[string]any{}
	}
	if req.HTTP != nil {
		hydrate(req)
	}
	switch req.Operation {
	case "CreateServer":
		return p.createServer(ctx, req)
	case "ListServers":
		return p.list(ctx, req, "hzsrv", "servers")
	case "GetServer":
		return p.getServer(ctx, req)
	case "DeleteServer":
		return p.deleteServer(ctx, req)
	case "CreateSSHKey":
		return p.createSSHKey(ctx, req)
	case "ListSSHKeys":
		return p.list(ctx, req, "hzkey", "ssh_keys")
	case "GetSSHKey":
		return p.getSSHKey(ctx, req)
	case "DeleteSSHKey":
		return p.deleteSSHKey(ctx, req)
	default:
		return nil, spi.NotImplemented("hetzner.v1", req.Operation, "emulate")
	}
}

func (p *Pack) createServer(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	name := str(req.Input["name"])
	if name == "" {
		return nil, hzFault("invalid_input", "server name is required", 400)
	}
	if _, exists, _ := p.col(req, "hzsname").Get(ctx, name); exists {
		return nil, hzFault("uniqueness_error", "server name already used", 409)
	}
	id := p.deps.Rand.Intn(1<<30) + 1
	rec := map[string]any{"id": id, "name": name, "status": "running", "server_type": str(req.Input["server_type"]), "image": str(req.Input["image"])}
	_ = p.col(req, "hzsrv").Put(ctx, strconv.Itoa(id), mustJSON(rec))
	_ = p.col(req, "hzsname").Put(ctx, name, []byte(strconv.Itoa(id)))
	return &spi.Response{Output: map[string]any{"_wrap": "server", "server": rec}}, nil
}

func (p *Pack) getServer(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	id := str(req.Input["id"])
	b, ok, _ := p.col(req, "hzsrv").Get(ctx, id)
	if !ok {
		return nil, hzFault("not_found", "Server not found", 404)
	}
	var rec map[string]any
	_ = json.Unmarshal(b, &rec)
	return &spi.Response{Output: map[string]any{"_wrap": "server", "server": rec}}, nil
}

func (p *Pack) deleteServer(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	got, err := p.getServer(ctx, req)
	if err != nil {
		return nil, err
	}
	rec, _ := got.Output["server"].(map[string]any)
	_ = p.col(req, "hzsrv").Delete(ctx, str(req.Input["id"]))
	_ = p.col(req, "hzsname").Delete(ctx, str(rec["name"]))
	return &spi.Response{Status: 200}, nil
}

func (p *Pack) createSSHKey(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	name := str(req.Input["name"])
	pub := str(req.Input["public_key"])
	if name == "" {
		return nil, hzFault("invalid_input", "ssh key name is required", 400)
	}
	if pub == "" {
		return nil, hzFault("invalid_input", "public_key is required", 400)
	}
	fp := pub
	if _, exists, _ := p.col(req, "hzfp").Get(ctx, fp); exists {
		return nil, hzFault("uniqueness_error", "SSH key fingerprint already used", 409)
	}
	id := p.deps.Rand.Intn(1<<30) + 1
	rec := map[string]any{"id": id, "name": name, "fingerprint": fp, "public_key": pub}
	_ = p.col(req, "hzkey").Put(ctx, strconv.Itoa(id), mustJSON(rec))
	_ = p.col(req, "hzfp").Put(ctx, fp, []byte(strconv.Itoa(id)))
	return &spi.Response{Output: map[string]any{"_wrap": "ssh_key", "ssh_key": rec}}, nil
}

func (p *Pack) getSSHKey(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	id := str(req.Input["id"])
	b, ok, _ := p.col(req, "hzkey").Get(ctx, id)
	if !ok {
		return nil, hzFault("not_found", "SSH key not found", 404)
	}
	var rec map[string]any
	_ = json.Unmarshal(b, &rec)
	return &spi.Response{Output: map[string]any{"_wrap": "ssh_key", "ssh_key": rec}}, nil
}

func (p *Pack) deleteSSHKey(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	got, err := p.getSSHKey(ctx, req)
	if err != nil {
		return nil, err
	}
	rec, _ := got.Output["ssh_key"].(map[string]any)
	_ = p.col(req, "hzkey").Delete(ctx, str(req.Input["id"]))
	_ = p.col(req, "hzfp").Delete(ctx, str(rec["fingerprint"]))
	return &spi.Response{Status: 200}, nil
}

func (p *Pack) list(ctx context.Context, req *spi.Request, col, key string) (*spi.Response, error) {
	kvs, _, _ := p.col(req, col).List(ctx, "", "", 0)
	var items []any
	for _, kv := range kvs {
		var rec map[string]any
		if json.Unmarshal(kv.Value, &rec) == nil {
			items = append(items, rec)
		}
	}
	if items == nil {
		items = []any{}
	}
	return &spi.Response{Output: map[string]any{"_list": items, "_wrap": key}}, nil
}

func hydrate(req *spi.Request) {
	parts := strings.Split(strings.Trim(req.HTTP.URL.Path, "/"), "/")
	if len(parts) >= 3 && parts[0] == "v1" && (parts[1] == "servers" || parts[1] == "ssh_keys") {
		req.Input["id"] = parts[2]
	}
}

func hzFault(code, msg string, status int) *spi.Fault {
	return &spi.Fault{Code: code, Message: msg, HTTPStatus: status, Fault: "client"}
}

func mustJSON(v any) []byte {
	b, _ := json.Marshal(v)
	return b
}

func str(v any) string {
	s, _ := v.(string)
	return s
}
