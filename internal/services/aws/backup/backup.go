// Package backup stores vaults, plans, and jobs (no real snapshot copy).
package backup

import (
	"context"
	"encoding/json"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func init() {
	registry.Register(registry.Factory{ServiceID: "aws.backup", Tier: model.TierEmulate, New: func(d spi.Deps) (spi.BehaviorPack, error) {
		return New(d), nil
	}})
}

// Pack implements Backup-lite.
type Pack struct{ deps spi.Deps }

// New constructs the pack.
func New(d spi.Deps) *Pack { return &Pack{deps: d} }

func (p *Pack) ServiceID() string { return "aws.backup" }
func (p *Pack) Tier() model.Tier  { return model.TierEmulate }
func (p *Pack) Operations() []string {
	return []string{
		"CreateBackupVault", "DescribeBackupVault", "ListBackupVaults", "DeleteBackupVault",
		"CreateBackupPlan", "GetBackupPlan", "ListBackupPlans", "DeleteBackupPlan",
		"StartBackupJob", "DescribeBackupJob", "ListBackupJobs",
	}
}

func (p *Pack) col(req *spi.Request, n string) spi.Collection {
	return p.deps.Store.Scope(req.Identity.Account, req.Identity.Region).Collection(n)
}

func (p *Pack) Invoke(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	switch req.Operation {
	case "CreateBackupVault":
		name := first(req.Input, "BackupVaultName")
		arn := "arn:aws:backup:" + req.Identity.Region + ":" + req.Identity.Account + ":backup-vault:" + name
		rec := map[string]any{"BackupVaultName": name, "BackupVaultArn": arn}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "bv").Put(ctx, name, b)
		return &spi.Response{Output: rec}, nil
	case "DescribeBackupVault":
		return get(ctx, p.col(req, "bv"), first(req.Input, "BackupVaultName"))
	case "ListBackupVaults":
		return listCol(ctx, p.col(req, "bv"), "BackupVaultList")
	case "DeleteBackupVault":
		_ = p.col(req, "bv").Delete(ctx, first(req.Input, "BackupVaultName"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "CreateBackupPlan":
		id := p.deps.Rand.Hex(8)
		in, _ := req.Input["BackupPlan"].(map[string]any)
		name := first(in, "BackupPlanName")
		arn := "arn:aws:backup:" + req.Identity.Region + ":" + req.Identity.Account + ":backup-plan:" + id
		rec := map[string]any{"BackupPlanId": id, "BackupPlanArn": arn, "BackupPlanName": name, "BackupPlan": in}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "bp").Put(ctx, id, b)
		return &spi.Response{Output: map[string]any{"BackupPlanId": id, "BackupPlanArn": arn}}, nil
	case "GetBackupPlan":
		return get(ctx, p.col(req, "bp"), first(req.Input, "BackupPlanId"))
	case "ListBackupPlans":
		return listCol(ctx, p.col(req, "bp"), "BackupPlansList")
	case "DeleteBackupPlan":
		_ = p.col(req, "bp").Delete(ctx, first(req.Input, "BackupPlanId"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "StartBackupJob":
		id := p.deps.Rand.Hex(16)
		rec := map[string]any{
			"BackupJobId": id, "State": "COMPLETED",
			"BackupVaultName": first(req.Input, "BackupVaultName"),
			"ResourceArn":     first(req.Input, "ResourceArn"),
		}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "bj").Put(ctx, id, b)
		return &spi.Response{Output: map[string]any{"BackupJobId": id}}, nil
	case "DescribeBackupJob":
		return get(ctx, p.col(req, "bj"), first(req.Input, "BackupJobId"))
	case "ListBackupJobs":
		return listCol(ctx, p.col(req, "bj"), "BackupJobs")
	default:
		return nil, spi.NotImplemented("aws.backup", req.Operation, "emulate")
	}
}

func get(ctx context.Context, c spi.Collection, id string) (*spi.Response, error) {
	b, ok, _ := c.Get(ctx, id)
	if !ok {
		return nil, &spi.Fault{Code: "ResourceNotFoundException", HTTPStatus: 400, Fault: "client"}
	}
	var rec map[string]any
	_ = json.Unmarshal(b, &rec)
	return &spi.Response{Output: rec}, nil
}

func listCol(ctx context.Context, c spi.Collection, key string) (*spi.Response, error) {
	kvs, _, _ := c.List(ctx, "", "", 0)
	var items []any
	for _, kv := range kvs {
		var rec map[string]any
		_ = json.Unmarshal(kv.Value, &rec)
		items = append(items, rec)
	}
	return &spi.Response{Output: map[string]any{key: items}}, nil
}

func first(in map[string]any, keys ...string) string {
	if in == nil {
		return ""
	}
	for _, k := range keys {
		if s, ok := in[k].(string); ok && s != "" {
			return s
		}
	}
	return ""
}
