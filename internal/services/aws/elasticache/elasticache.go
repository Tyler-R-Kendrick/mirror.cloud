// Package elasticache emulates cache cluster control-plane records (no Redis/Memcached server).
package elasticache

import (
	"context"
	"encoding/json"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func init() {
	registry.Register(registry.Factory{ServiceID: "aws.elasticache", Tier: model.TierEmulate, New: func(d spi.Deps) (spi.BehaviorPack, error) {
		return New(d), nil
	}})
}

// Pack implements ElastiCache-lite.
type Pack struct{ deps spi.Deps }

// New constructs the pack.
func New(d spi.Deps) *Pack { return &Pack{deps: d} }

func (p *Pack) ServiceID() string { return "aws.elasticache" }
func (p *Pack) Tier() model.Tier  { return model.TierEmulate }
func (p *Pack) Operations() []string {
	core := []string{
		"CreateCacheCluster", "DescribeCacheClusters", "DeleteCacheCluster", "ModifyCacheCluster",
		"CreateReplicationGroup", "DescribeReplicationGroups", "DeleteReplicationGroup",
		"CreateCacheSubnetGroup", "DescribeCacheSubnetGroups", "DeleteCacheSubnetGroup",
		"CreateSnapshot", "DescribeSnapshots", "DeleteSnapshot",
		"AddTagsToResource", "ListTagsForResource", "RemoveTagsFromResource",
	}
	return append(core, extraOps()...)
}

func (p *Pack) col(req *spi.Request, n string) spi.Collection {
	return p.deps.Store.Scope(req.Identity.Account, req.Identity.Region).Collection(n)
}

func (p *Pack) Invoke(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	switch req.Operation {
	case "CreateCacheCluster":
		id := first(req.Input, "CacheClusterId")
		rec := map[string]any{"CacheClusterId": id, "CacheClusterStatus": "available", "Engine": first(req.Input, "Engine"), "CacheNodeType": first(req.Input, "CacheNodeType")}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "cache").Put(ctx, id, b)
		return &spi.Response{Output: map[string]any{"CacheCluster": rec}}, nil
	case "DescribeCacheClusters":
		return listOrGet(ctx, p.col(req, "cache"), first(req.Input, "CacheClusterId"), "CacheClusters")
	case "ModifyCacheCluster":
		return modify(ctx, p.col(req, "cache"), first(req.Input, "CacheClusterId"), req.Input, "CacheCluster")
	case "DeleteCacheCluster":
		id := first(req.Input, "CacheClusterId")
		_ = p.col(req, "cache").Delete(ctx, id)
		return &spi.Response{Output: map[string]any{"CacheCluster": map[string]any{"CacheClusterId": id, "CacheClusterStatus": "deleting"}}}, nil
	case "CreateReplicationGroup":
		id := first(req.Input, "ReplicationGroupId")
		rec := map[string]any{"ReplicationGroupId": id, "Status": "available"}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "rg").Put(ctx, id, b)
		return &spi.Response{Output: map[string]any{"ReplicationGroup": rec}}, nil
	case "DescribeReplicationGroups":
		return listOrGet(ctx, p.col(req, "rg"), first(req.Input, "ReplicationGroupId"), "ReplicationGroups")
	case "DeleteReplicationGroup":
		id := first(req.Input, "ReplicationGroupId")
		_ = p.col(req, "rg").Delete(ctx, id)
		return &spi.Response{Output: map[string]any{"ReplicationGroup": map[string]any{"ReplicationGroupId": id, "Status": "deleting"}}}, nil
	case "CreateCacheSubnetGroup":
		id := first(req.Input, "CacheSubnetGroupName")
		rec := map[string]any{"CacheSubnetGroupName": id}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "csubnet").Put(ctx, id, b)
		return &spi.Response{Output: map[string]any{"CacheSubnetGroup": rec}}, nil
	case "DescribeCacheSubnetGroups":
		return listOrGet(ctx, p.col(req, "csubnet"), first(req.Input, "CacheSubnetGroupName"), "CacheSubnetGroups")
	case "DeleteCacheSubnetGroup":
		_ = p.col(req, "csubnet").Delete(ctx, first(req.Input, "CacheSubnetGroupName"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "CreateSnapshot":
		id := first(req.Input, "SnapshotName")
		rec := map[string]any{"SnapshotName": id, "CacheClusterId": first(req.Input, "CacheClusterId")}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "csnap").Put(ctx, id, b)
		return &spi.Response{Output: map[string]any{"Snapshot": rec}}, nil
	case "DescribeSnapshots":
		return listOrGet(ctx, p.col(req, "csnap"), first(req.Input, "SnapshotName"), "Snapshots")
	case "DeleteSnapshot":
		_ = p.col(req, "csnap").Delete(ctx, first(req.Input, "SnapshotName"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "AddTagsToResource":
		b, _ := json.Marshal(req.Input["Tags"])
		_ = p.col(req, "ctags").Put(ctx, first(req.Input, "ResourceName"), b)
		return &spi.Response{Output: map[string]any{}}, nil
	case "ListTagsForResource":
		b, ok, _ := p.col(req, "ctags").Get(ctx, first(req.Input, "ResourceName"))
		var tags any = []any{}
		if ok {
			_ = json.Unmarshal(b, &tags)
		}
		return &spi.Response{Output: map[string]any{"TagList": tags}}, nil
	case "RemoveTagsFromResource":
		_ = p.col(req, "ctags").Delete(ctx, first(req.Input, "ResourceName"))
		return &spi.Response{Output: map[string]any{}}, nil
	default:
		return p.extra(ctx, req)
	}
}

func listOrGet(ctx context.Context, c spi.Collection, id, listKey string) (*spi.Response, error) {
	if id != "" {
		b, ok, _ := c.Get(ctx, id)
		if !ok {
			return nil, &spi.Fault{Code: "CacheClusterNotFound", HTTPStatus: 404, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		return &spi.Response{Output: map[string]any{listKey: []any{rec}}}, nil
	}
	kvs, _, _ := c.List(ctx, "", "", 0)
	var items []any
	for _, kv := range kvs {
		var rec map[string]any
		_ = json.Unmarshal(kv.Value, &rec)
		items = append(items, rec)
	}
	return &spi.Response{Output: map[string]any{listKey: items}}, nil
}

func modify(ctx context.Context, c spi.Collection, id string, in map[string]any, outKey string) (*spi.Response, error) {
	b, ok, _ := c.Get(ctx, id)
	rec := map[string]any{}
	if ok {
		_ = json.Unmarshal(b, &rec)
	}
	for k, v := range in {
		rec[k] = v
	}
	nb, _ := json.Marshal(rec)
	_ = c.Put(ctx, id, nb)
	return &spi.Response{Output: map[string]any{outKey: rec}}, nil
}

func first(in map[string]any, keys ...string) string {
	for _, k := range keys {
		if s, ok := in[k].(string); ok && s != "" {
			return s
		}
	}
	return ""
}
