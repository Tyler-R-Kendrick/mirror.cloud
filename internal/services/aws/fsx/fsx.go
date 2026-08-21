// Package fsx stores file system and backup records (no Lustre/Windows FS).
package fsx

import (
	"context"
	"encoding/json"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func init() {
	registry.Register(registry.Factory{ServiceID: "aws.fsx", Tier: model.TierEmulate, New: func(d spi.Deps) (spi.BehaviorPack, error) {
		return New(d), nil
	}})
}

// Pack implements FSx-lite.
type Pack struct{ deps spi.Deps }

// New constructs the pack.
func New(d spi.Deps) *Pack { return &Pack{deps: d} }

func (p *Pack) ServiceID() string { return "aws.fsx" }
func (p *Pack) Tier() model.Tier  { return model.TierEmulate }
func (p *Pack) Operations() []string {
	return []string{
		"CreateFileSystem", "DescribeFileSystems", "UpdateFileSystem", "DeleteFileSystem",
		"CreateBackup", "DescribeBackups", "DeleteBackup",
	}
}

func (p *Pack) col(req *spi.Request, n string) spi.Collection {
	return p.deps.Store.Scope(req.Identity.Account, req.Identity.Region).Collection(n)
}

func (p *Pack) Invoke(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	switch req.Operation {
	case "CreateFileSystem":
		id := "fs-" + p.deps.Rand.Hex(8)
		rec := map[string]any{
			"FileSystemId": id, "Lifecycle": "AVAILABLE", "FileSystemType": first(req.Input, "FileSystemType"),
			"StorageCapacity": req.Input["StorageCapacity"], "SubnetIds": req.Input["SubnetIds"],
		}
		if rec["FileSystemType"] == "" {
			rec["FileSystemType"] = "LUSTRE"
		}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "fsxfs").Put(ctx, id, b)
		return &spi.Response{Output: map[string]any{"FileSystem": rec}}, nil
	case "DescribeFileSystems":
		return listOrGet(ctx, p.col(req, "fsxfs"), firstID(req.Input, "FileSystemId", "FileSystemIds"), "FileSystems")
	case "UpdateFileSystem":
		id := first(req.Input, "FileSystemId")
		b, ok, _ := p.col(req, "fsxfs").Get(ctx, id)
		if !ok {
			return nil, &spi.Fault{Code: "FileSystemNotFound", HTTPStatus: 400, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		if v, ok := req.Input["StorageCapacity"]; ok {
			rec["StorageCapacity"] = v
		}
		nb, _ := json.Marshal(rec)
		_ = p.col(req, "fsxfs").Put(ctx, id, nb)
		return &spi.Response{Output: map[string]any{"FileSystem": rec}}, nil
	case "DeleteFileSystem":
		_ = p.col(req, "fsxfs").Delete(ctx, first(req.Input, "FileSystemId"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "CreateBackup":
		id := "backup-" + p.deps.Rand.Hex(8)
		rec := map[string]any{"BackupId": id, "Lifecycle": "AVAILABLE", "FileSystemId": first(req.Input, "FileSystemId")}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "fsxbk").Put(ctx, id, b)
		return &spi.Response{Output: map[string]any{"Backup": rec}}, nil
	case "DescribeBackups":
		return listOrGet(ctx, p.col(req, "fsxbk"), firstID(req.Input, "BackupId", "BackupIds"), "Backups")
	case "DeleteBackup":
		_ = p.col(req, "fsxbk").Delete(ctx, first(req.Input, "BackupId"))
		return &spi.Response{Output: map[string]any{}}, nil
	default:
		return nil, spi.NotImplemented("aws.fsx", req.Operation, "emulate")
	}
}

func listOrGet(ctx context.Context, c spi.Collection, want, key string) (*spi.Response, error) {
	if want != "" {
		b, ok, _ := c.Get(ctx, want)
		if !ok {
			return &spi.Response{Output: map[string]any{key: []any{}}}, nil
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		return &spi.Response{Output: map[string]any{key: []any{rec}}}, nil
	}
	kvs, _, _ := c.List(ctx, "", "", 0)
	var items []any
	for _, kv := range kvs {
		var rec map[string]any
		_ = json.Unmarshal(kv.Value, &rec)
		items = append(items, rec)
	}
	return &spi.Response{Output: map[string]any{key: items}}, nil
}

func firstID(in map[string]any, scalar, list string) string {
	if s := first(in, scalar); s != "" {
		return s
	}
	if arr, ok := in[list].([]any); ok && len(arr) > 0 {
		if s, ok := arr[0].(string); ok {
			return s
		}
	}
	return ""
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
