package v1

import (
	"context"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/golden"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spitest"
)

func TestHetznerV1Characterization(t *testing.T) {
	p := New(spitest.Deps(t))
	ctx := context.Background()
	id := spi.Identity{Account: "000000000000", Region: "us-east-1"}
	inv := func(op string, in map[string]any) any {
		t.Helper()
		res, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: op, Input: in})
		if err != nil {
			f := err.(*spi.Fault)
			return map[string]any{"error": f.Code, "status": f.HTTPStatus, "message": f.Message}
		}
		return map[string]any{"status": res.Status, "output": res.Output}
	}
	create := inv("CreateServer", map[string]any{"name": "web", "server_type": "cx22", "image": "ubuntu-24.04"})
	sid := ""
	if m, ok := create.(map[string]any); ok {
		if out, ok := m["output"].(map[string]any); ok {
			if srv, ok := out["server"].(map[string]any); ok {
				sid = strID(srv["id"])
			}
		}
	}
	ssh := inv("CreateSSHKey", map[string]any{"name": "laptop", "public_key": "ssh-ed25519 AAAA"})
	kid := ""
	if m, ok := ssh.(map[string]any); ok {
		if out, ok := m["output"].(map[string]any); ok {
			if key, ok := out["ssh_key"].(map[string]any); ok {
				kid = strID(key["id"])
			}
		}
	}
	golden.AssertJSON(t, map[string]any{
		"create":    create,
		"get":       inv("GetServer", map[string]any{"id": sid}),
		"list":      inv("ListServers", nil),
		"duplicate": inv("CreateServer", map[string]any{"name": "web"}),
		"empty":     inv("CreateServer", map[string]any{}),
		"delete":    inv("DeleteServer", map[string]any{"id": sid}),
		"missing":   inv("GetServer", map[string]any{"id": sid}),
		"ssh":       ssh,
		"get_ssh":   inv("GetSSHKey", map[string]any{"id": kid}),
		"keys":      inv("ListSSHKeys", nil),
		"dup_fp":    inv("CreateSSHKey", map[string]any{"name": "other", "public_key": "ssh-ed25519 AAAA"}),
		"nokey":     inv("GetSSHKey", map[string]any{"id": "missing"}),
	})
}
