// Package efs stores file systems and mount targets (no NFS server).
package efs

import (
	"context"
	"encoding/json"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func init() {
	registry.Register(registry.Factory{ServiceID: "aws.elasticfilesystem", Tier: model.TierEmulate, New: func(d spi.Deps) (spi.BehaviorPack, error) {
		return New(d), nil
	}})
}

// Pack implements EFS-lite.
type Pack struct{ deps spi.Deps }

// New constructs the pack.
func New(d spi.Deps) *Pack { return &Pack{deps: d} }

func (p *Pack) ServiceID() string { return "aws.elasticfilesystem" }
func (p *Pack) Tier() model.Tier  { return model.TierEmulate }
func (p *Pack) Operations() []string {
	return []string{
		"CreateFileSystem", "DescribeFileSystems", "DeleteFileSystem",
		"CreateMountTarget", "DescribeMountTargets", "DeleteMountTarget",
		"CreateAccessPoint", "DescribeAccessPoints", "DeleteAccessPoint",
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
			"FileSystemId": id, "LifeCycleState": "available", "OwnerId": req.Identity.Account,
			"FileSystemArn": "arn:aws:elasticfilesystem:" + req.Identity.Region + ":" + req.Identity.Account + ":file-system/" + id,
			"CreationToken": first(req.Input, "CreationToken"),
		}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "efsfs").Put(ctx, id, b)
		return &spi.Response{Output: rec}, nil
	case "DescribeFileSystems":
		return listOrGet(ctx, p.col(req, "efsfs"), first(req.Input, "FileSystemId"), "FileSystems")
	case "DeleteFileSystem":
		_ = p.col(req, "efsfs").Delete(ctx, first(req.Input, "FileSystemId"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "CreateMountTarget":
		id := "fsmt-" + p.deps.Rand.Hex(8)
		fs := first(req.Input, "FileSystemId")
		rec := map[string]any{"MountTargetId": id, "FileSystemId": fs, "LifeCycleState": "available", "SubnetId": first(req.Input, "SubnetId")}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "efsmt").Put(ctx, id, b)
		return &spi.Response{Output: rec}, nil
	case "DescribeMountTargets":
		return listOrGet(ctx, p.col(req, "efsmt"), first(req.Input, "MountTargetId"), "MountTargets")
	case "DeleteMountTarget":
		_ = p.col(req, "efsmt").Delete(ctx, first(req.Input, "MountTargetId"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "CreateAccessPoint":
		id := "fsap-" + p.deps.Rand.Hex(8)
		rec := map[string]any{"AccessPointId": id, "FileSystemId": first(req.Input, "FileSystemId"), "LifeCycleState": "available"}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "efsap").Put(ctx, id, b)
		return &spi.Response{Output: rec}, nil
	case "DescribeAccessPoints":
		return listOrGet(ctx, p.col(req, "efsap"), first(req.Input, "AccessPointId"), "AccessPoints")
	case "DeleteAccessPoint":
		_ = p.col(req, "efsap").Delete(ctx, first(req.Input, "AccessPointId"))
		return &spi.Response{Output: map[string]any{}}, nil
	default:
		return nil, spi.NotImplemented("aws.elasticfilesystem", req.Operation, "emulate")
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
