package elasticache

import (
	"context"
	"encoding/json"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func extraOps() []string {
	return []string{
		"AuthorizeCacheSecurityGroupIngress", "BatchApplyUpdateAction", "BatchStopUpdateAction", "CompleteMigration",
		"CopyServerlessCacheSnapshot", "CopySnapshot", "CreateCacheParameterGroup", "CreateCacheSecurityGroup",
		"CreateGlobalReplicationGroup", "CreateServerlessCache", "CreateServerlessCacheSnapshot", "CreateUser",
		"CreateUserGroup", "DecreaseNodeGroupsInGlobalReplicationGroup", "DecreaseReplicaCount",
		"DeleteCacheParameterGroup", "DeleteCacheSecurityGroup", "DeleteGlobalReplicationGroup",
		"DeleteServerlessCache", "DeleteServerlessCacheSnapshot", "DeleteUser", "DeleteUserGroup",
		"DescribeCacheEngineVersions", "DescribeCacheParameterGroups", "DescribeCacheParameters",
		"DescribeCacheSecurityGroups", "DescribeEngineDefaultParameters", "DescribeEvents",
		"DescribeGlobalReplicationGroups", "DescribeReservedCacheNodes", "DescribeReservedCacheNodesOfferings",
		"DescribeServerlessCacheSnapshots", "DescribeServerlessCaches", "DescribeServiceUpdates",
		"DescribeUpdateActions", "DescribeUserGroups", "DescribeUsers", "DisassociateGlobalReplicationGroup",
		"ExportServerlessCacheSnapshot", "FailoverGlobalReplicationGroup",
		"IncreaseNodeGroupsInGlobalReplicationGroup", "IncreaseReplicaCount", "ListAllowedNodeTypeModifications",
		"ModifyCacheParameterGroup", "ModifyCacheSubnetGroup", "ModifyGlobalReplicationGroup",
		"ModifyReplicationGroup", "ModifyReplicationGroupShardConfiguration", "ModifyServerlessCache",
		"ModifyUser", "ModifyUserGroup", "PurchaseReservedCacheNodesOffering",
		"RebalanceSlotsInGlobalReplicationGroup", "RebootCacheCluster", "ResetCacheParameterGroup",
		"RevokeCacheSecurityGroupIngress", "StartMigration", "TestFailover", "TestMigration",
	}
}

func (p *Pack) extra(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	op := req.Operation
	switch op {
	case "CreateUser", "ModifyUser":
		return p.putWrap(ctx, req, "ecuser", first(req.Input, "UserId"), "User", map[string]any{"Status": "active"})
	case "DescribeUsers":
		return listOrGet(ctx, p.col(req, "ecuser"), first(req.Input, "UserId"), "Users")
	case "DeleteUser":
		_ = p.col(req, "ecuser").Delete(ctx, first(req.Input, "UserId"))
		return &spi.Response{Output: map[string]any{"UserId": first(req.Input, "UserId"), "Status": "deleting"}}, nil
	case "CreateUserGroup", "ModifyUserGroup":
		return p.putWrap(ctx, req, "ecug", first(req.Input, "UserGroupId"), "UserGroup", map[string]any{"Status": "active"})
	case "DescribeUserGroups":
		return listOrGet(ctx, p.col(req, "ecug"), first(req.Input, "UserGroupId"), "UserGroups")
	case "DeleteUserGroup":
		_ = p.col(req, "ecug").Delete(ctx, first(req.Input, "UserGroupId"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "CreateServerlessCache", "ModifyServerlessCache":
		return p.putWrap(ctx, req, "ecsls", first(req.Input, "ServerlessCacheName"), "ServerlessCache", map[string]any{"Status": "available"})
	case "DescribeServerlessCaches":
		return listOrGet(ctx, p.col(req, "ecsls"), first(req.Input, "ServerlessCacheName"), "ServerlessCaches")
	case "DeleteServerlessCache":
		_ = p.col(req, "ecsls").Delete(ctx, first(req.Input, "ServerlessCacheName"))
		return &spi.Response{Output: map[string]any{"ServerlessCache": map[string]any{"Status": "deleting"}}}, nil
	case "CreateServerlessCacheSnapshot":
		return p.putWrap(ctx, req, "ecslssnap", first(req.Input, "ServerlessCacheSnapshotName", "SnapshotName"), "ServerlessCacheSnapshot", map[string]any{"Status": "available"})
	case "CopyServerlessCacheSnapshot":
		src := first(req.Input, "SourceServerlessCacheSnapshotName", "SourceSnapshotName")
		dst := first(req.Input, "TargetServerlessCacheSnapshotName", "TargetSnapshotName")
		b, ok, _ := p.col(req, "ecslssnap").Get(ctx, src)
		rec := map[string]any{"ServerlessCacheSnapshotName": dst, "Status": "available"}
		if ok {
			_ = json.Unmarshal(b, &rec)
			rec["ServerlessCacheSnapshotName"] = dst
		}
		nb, _ := json.Marshal(rec)
		_ = p.col(req, "ecslssnap").Put(ctx, dst, nb)
		return &spi.Response{Output: map[string]any{"ServerlessCacheSnapshot": rec}}, nil
	case "DescribeServerlessCacheSnapshots":
		return listOrGet(ctx, p.col(req, "ecslssnap"), first(req.Input, "ServerlessCacheSnapshotName", "SnapshotName"), "ServerlessCacheSnapshots")
	case "DeleteServerlessCacheSnapshot":
		_ = p.col(req, "ecslssnap").Delete(ctx, first(req.Input, "ServerlessCacheSnapshotName", "SnapshotName"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "ExportServerlessCacheSnapshot":
		return &spi.Response{Output: map[string]any{"ServerlessCacheSnapshot": map[string]any{"Status": "exporting"}}}, nil
	case "CreateGlobalReplicationGroup", "ModifyGlobalReplicationGroup":
		return p.putWrap(ctx, req, "ecgrg", first(req.Input, "GlobalReplicationGroupId"), "GlobalReplicationGroup", map[string]any{"Status": "available"})
	case "DescribeGlobalReplicationGroups":
		return listOrGet(ctx, p.col(req, "ecgrg"), first(req.Input, "GlobalReplicationGroupId"), "GlobalReplicationGroups")
	case "DeleteGlobalReplicationGroup", "DisassociateGlobalReplicationGroup":
		_ = p.col(req, "ecgrg").Delete(ctx, first(req.Input, "GlobalReplicationGroupId"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "IncreaseNodeGroupsInGlobalReplicationGroup", "DecreaseNodeGroupsInGlobalReplicationGroup",
		"RebalanceSlotsInGlobalReplicationGroup", "FailoverGlobalReplicationGroup":
		return modify(ctx, p.col(req, "ecgrg"), first(req.Input, "GlobalReplicationGroupId"), map[string]any{"LastAction": op}, "GlobalReplicationGroup")
	case "CreateCacheParameterGroup", "ModifyCacheParameterGroup", "ResetCacheParameterGroup":
		return p.putWrap(ctx, req, "ecpg", first(req.Input, "CacheParameterGroupName"), "CacheParameterGroup", map[string]any{})
	case "DescribeCacheParameterGroups":
		return listOrGet(ctx, p.col(req, "ecpg"), first(req.Input, "CacheParameterGroupName"), "CacheParameterGroups")
	case "DescribeCacheParameters", "DescribeEngineDefaultParameters":
		return &spi.Response{Output: map[string]any{"Parameters": []any{map[string]any{"ParameterName": "maxmemory-policy", "ParameterValue": "volatile-lru"}}}}, nil
	case "DeleteCacheParameterGroup":
		_ = p.col(req, "ecpg").Delete(ctx, first(req.Input, "CacheParameterGroupName"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "CreateCacheSecurityGroup":
		return p.putWrap(ctx, req, "ecsg", first(req.Input, "CacheSecurityGroupName"), "CacheSecurityGroup", map[string]any{})
	case "DescribeCacheSecurityGroups":
		return listOrGet(ctx, p.col(req, "ecsg"), first(req.Input, "CacheSecurityGroupName"), "CacheSecurityGroups")
	case "DeleteCacheSecurityGroup":
		_ = p.col(req, "ecsg").Delete(ctx, first(req.Input, "CacheSecurityGroupName"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "AuthorizeCacheSecurityGroupIngress", "RevokeCacheSecurityGroupIngress":
		return modify(ctx, p.col(req, "ecsg"), first(req.Input, "CacheSecurityGroupName"), map[string]any{"EC2SecurityGroupName": first(req.Input, "EC2SecurityGroupName"), "LastAction": op}, "CacheSecurityGroup")
	case "CopySnapshot":
		src, dst := first(req.Input, "SourceSnapshotName"), first(req.Input, "TargetSnapshotName")
		b, ok, _ := p.col(req, "csnap").Get(ctx, src)
		rec := map[string]any{"SnapshotName": dst}
		if ok {
			_ = json.Unmarshal(b, &rec)
			rec["SnapshotName"] = dst
		}
		nb, _ := json.Marshal(rec)
		_ = p.col(req, "csnap").Put(ctx, dst, nb)
		return &spi.Response{Output: map[string]any{"Snapshot": rec}}, nil
	case "ModifyReplicationGroup", "ModifyReplicationGroupShardConfiguration":
		return modify(ctx, p.col(req, "rg"), first(req.Input, "ReplicationGroupId"), req.Input, "ReplicationGroup")
	case "IncreaseReplicaCount", "DecreaseReplicaCount":
		n := req.Input["NewReplicaCount"]
		return modify(ctx, p.col(req, "rg"), first(req.Input, "ReplicationGroupId"), map[string]any{"Replicas": n, "LastAction": op}, "ReplicationGroup")
	case "ModifyCacheSubnetGroup":
		return modify(ctx, p.col(req, "csubnet"), first(req.Input, "CacheSubnetGroupName"), req.Input, "CacheSubnetGroup")
	case "RebootCacheCluster":
		return modify(ctx, p.col(req, "cache"), first(req.Input, "CacheClusterId"), map[string]any{"CacheClusterStatus": "rebooting"}, "CacheCluster")
	case "StartMigration", "CompleteMigration", "TestMigration", "TestFailover":
		id := first(req.Input, "ReplicationGroupId", "CacheClusterId")
		return modify(ctx, p.col(req, "rg"), id, map[string]any{"Migration": op}, "ReplicationGroup")
	case "BatchApplyUpdateAction", "BatchStopUpdateAction":
		b, _ := json.Marshal(map[string]any{"Action": op, "ServiceUpdateName": first(req.Input, "ServiceUpdateName")})
		_ = p.col(req, "ecupd").Put(ctx, first(req.Input, "ServiceUpdateName")+op, b)
		return &spi.Response{Output: map[string]any{"ProcessedUpdateActions": []any{}, "UnprocessedUpdateActions": []any{}}}, nil
	case "DescribeUpdateActions":
		return listOrGet(ctx, p.col(req, "ecupd"), "", "UpdateActions")
	case "DescribeServiceUpdates":
		return &spi.Response{Output: map[string]any{"ServiceUpdates": []any{}}}, nil
	case "DescribeCacheEngineVersions":
		return &spi.Response{Output: map[string]any{"CacheEngineVersions": []any{map[string]any{"Engine": "redis", "EngineVersion": "7.0"}}}}, nil
	case "DescribeEvents":
		return &spi.Response{Output: map[string]any{"Events": []any{}}}, nil
	case "DescribeReservedCacheNodes":
		return listOrGet(ctx, p.col(req, "ecreserved"), first(req.Input, "ReservedCacheNodeId"), "ReservedCacheNodes")
	case "DescribeReservedCacheNodesOfferings":
		return &spi.Response{Output: map[string]any{"ReservedCacheNodesOfferings": []any{map[string]any{"ReservedCacheNodesOfferingId": "off-1", "CacheNodeType": "cache.t3.micro", "Duration": 31536000}}}}, nil
	case "PurchaseReservedCacheNodesOffering":
		id := p.deps.Rand.Hex(8)
		rec := map[string]any{"ReservedCacheNodeId": id, "ReservedCacheNodesOfferingId": first(req.Input, "ReservedCacheNodesOfferingId"), "State": "payment-pending"}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "ecreserved").Put(ctx, id, b)
		return &spi.Response{Output: map[string]any{"ReservedCacheNode": rec}}, nil
	case "ListAllowedNodeTypeModifications":
		return &spi.Response{Output: map[string]any{"ScaleUpModifications": []any{"cache.t3.small"}, "ScaleDownModifications": []any{"cache.t3.micro"}}}, nil
	default:
		return nil, spi.NotImplemented("aws.elasticache", op, "emulate")
	}
}

func (p *Pack) putWrap(ctx context.Context, req *spi.Request, col, id, wrap string, extra map[string]any) (*spi.Response, error) {
	if id == "" {
		id = p.deps.Rand.Hex(8)
	}
	rec := map[string]any{}
	for k, v := range req.Input {
		rec[k] = v
	}
	for k, v := range extra {
		rec[k] = v
	}
	b, _ := json.Marshal(rec)
	_ = p.col(req, col).Put(ctx, id, b)
	return &spi.Response{Output: map[string]any{wrap: rec}}, nil
}
