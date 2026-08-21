// Package rds emulates instance/cluster/snapshot CRUD (not a real database engine).
package rds

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func init() {
	registry.Register(registry.Factory{ServiceID: "aws.rds", Tier: model.TierEmulate, New: func(d spi.Deps) (spi.BehaviorPack, error) {
		return New(d), nil
	}})
}

// Pack implements RDS-lite.
type Pack struct{ deps spi.Deps }

// New constructs the pack.
func New(d spi.Deps) *Pack { return &Pack{deps: d} }

func (p *Pack) ServiceID() string { return "aws.rds" }
func (p *Pack) Tier() model.Tier  { return model.TierEmulate }
func (p *Pack) Operations() []string {
	core := []string{
		"CreateDBInstance", "DescribeDBInstances", "ModifyDBInstance", "DeleteDBInstance", "RebootDBInstance",
		"StartDBInstance", "StopDBInstance", "CreateDBInstanceReadReplica", "PromoteReadReplica",
		"RestoreDBInstanceFromDBSnapshot",
		"CreateDBCluster", "DescribeDBClusters", "ModifyDBCluster", "DeleteDBCluster", "FailoverDBCluster",
		"RestoreDBClusterFromSnapshot",
		"CreateDBSnapshot", "DescribeDBSnapshots", "DeleteDBSnapshot", "CopyDBSnapshot",
		"CreateDBClusterSnapshot", "DescribeDBClusterSnapshots", "DeleteDBClusterSnapshot",
		"CreateDBSubnetGroup", "DescribeDBSubnetGroups", "DeleteDBSubnetGroup",
		"CreateDBParameterGroup", "DescribeDBParameterGroups", "DeleteDBParameterGroup",
		"ModifyDBParameterGroup", "ResetDBParameterGroup", "DescribeDBParameters",
		"CreateDBClusterParameterGroup", "DescribeDBClusterParameterGroups", "DeleteDBClusterParameterGroup",
		"CreateOptionGroup", "DescribeOptionGroups", "DeleteOptionGroup",
		"AddRoleToDBInstance", "RemoveRoleFromDBInstance",
		"CreateEventSubscription", "DescribeEventSubscriptions", "DeleteEventSubscription",
		"AddTagsToResource", "RemoveTagsFromResource", "ListTagsForResource",
	}
	return append(core, extraOps()...)
}

func (p *Pack) col(req *spi.Request, n string) spi.Collection {
	return p.deps.Store.Scope(req.Identity.Account, req.Identity.Region).Collection(n)
}

func (p *Pack) Invoke(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	switch req.Operation {
	case "CreateDBInstance":
		id := first(req.Input, "DBInstanceIdentifier")
		port := 3306
		if strings.Contains(strings.ToLower(first(req.Input, "Engine")), "postgres") {
			port = 5432
		}
		rec := map[string]any{
			"DBInstanceIdentifier": id, "DBInstanceStatus": "available",
			"Engine": first(req.Input, "Engine"), "DBInstanceClass": first(req.Input, "DBInstanceClass"),
			"MasterUsername": first(req.Input, "MasterUsername"),
			"Endpoint": map[string]any{
				"Address": id + "." + req.Identity.Region + ".rds.amazonaws.com", "Port": port,
			},
		}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "dbinst").Put(ctx, id, b)
		return &spi.Response{Output: map[string]any{"DBInstance": rec}}, nil
	case "DescribeDBInstances":
		return p.listOrGet(ctx, req, "dbinst", "DBInstanceIdentifier", "DBInstances")
	case "ModifyDBInstance":
		return p.modify(ctx, req, "dbinst", "DBInstanceIdentifier", "DBInstance")
	case "DeleteDBInstance":
		id := first(req.Input, "DBInstanceIdentifier")
		b, _, _ := p.col(req, "dbinst").Get(ctx, id)
		_ = p.col(req, "dbinst").Delete(ctx, id)
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		if rec == nil {
			rec = map[string]any{"DBInstanceIdentifier": id}
		}
		rec["DBInstanceStatus"] = "deleting"
		return &spi.Response{Output: map[string]any{"DBInstance": rec}}, nil
	case "RebootDBInstance":
		return p.modify(ctx, req, "dbinst", "DBInstanceIdentifier", "DBInstance")
	case "StartDBInstance", "StopDBInstance":
		id := first(req.Input, "DBInstanceIdentifier")
		b, ok, _ := p.col(req, "dbinst").Get(ctx, id)
		rec := map[string]any{"DBInstanceIdentifier": id}
		if ok {
			_ = json.Unmarshal(b, &rec)
		}
		if req.Operation == "StopDBInstance" {
			rec["DBInstanceStatus"] = "stopped"
		} else {
			rec["DBInstanceStatus"] = "available"
		}
		nb, _ := json.Marshal(rec)
		_ = p.col(req, "dbinst").Put(ctx, id, nb)
		return &spi.Response{Output: map[string]any{"DBInstance": rec}}, nil
	case "CreateDBInstanceReadReplica":
		id := first(req.Input, "DBInstanceIdentifier")
		src := first(req.Input, "SourceDBInstanceIdentifier")
		b, _, _ := p.col(req, "dbinst").Get(ctx, src)
		rec := map[string]any{}
		_ = json.Unmarshal(b, &rec)
		rec["DBInstanceIdentifier"] = id
		rec["ReadReplicaSourceDBInstanceIdentifier"] = src
		rec["DBInstanceStatus"] = "available"
		nb, _ := json.Marshal(rec)
		_ = p.col(req, "dbinst").Put(ctx, id, nb)
		return &spi.Response{Output: map[string]any{"DBInstance": rec}}, nil
	case "PromoteReadReplica":
		id := first(req.Input, "DBInstanceIdentifier")
		b, ok, _ := p.col(req, "dbinst").Get(ctx, id)
		rec := map[string]any{"DBInstanceIdentifier": id}
		if ok {
			_ = json.Unmarshal(b, &rec)
		}
		delete(rec, "ReadReplicaSourceDBInstanceIdentifier")
		nb, _ := json.Marshal(rec)
		_ = p.col(req, "dbinst").Put(ctx, id, nb)
		return &spi.Response{Output: map[string]any{"DBInstance": rec}}, nil
	case "RestoreDBInstanceFromDBSnapshot":
		id := first(req.Input, "DBInstanceIdentifier")
		snap := first(req.Input, "DBSnapshotIdentifier")
		b, _, _ := p.col(req, "dbsnap").Get(ctx, snap)
		var snapRec map[string]any
		_ = json.Unmarshal(b, &snapRec)
		rec := map[string]any{
			"DBInstanceIdentifier": id, "DBInstanceStatus": "available",
			"Engine": snapRec["Engine"], "Endpoint": map[string]any{
				"Address": id + "." + req.Identity.Region + ".rds.amazonaws.com", "Port": 3306,
			},
		}
		nb, _ := json.Marshal(rec)
		_ = p.col(req, "dbinst").Put(ctx, id, nb)
		return &spi.Response{Output: map[string]any{"DBInstance": rec}}, nil
	case "CreateDBCluster":
		id := first(req.Input, "DBClusterIdentifier")
		rec := map[string]any{
			"DBClusterIdentifier": id, "Status": "available", "Engine": first(req.Input, "Engine"),
			"Endpoint": id + ".cluster-" + req.Identity.Region + ".rds.amazonaws.com",
		}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "dbcluster").Put(ctx, id, b)
		return &spi.Response{Output: map[string]any{"DBCluster": rec}}, nil
	case "DescribeDBClusters":
		return p.listOrGet(ctx, req, "dbcluster", "DBClusterIdentifier", "DBClusters")
	case "ModifyDBCluster":
		return p.modify(ctx, req, "dbcluster", "DBClusterIdentifier", "DBCluster")
	case "FailoverDBCluster":
		return p.modify(ctx, req, "dbcluster", "DBClusterIdentifier", "DBCluster")
	case "RestoreDBClusterFromSnapshot":
		id := first(req.Input, "DBClusterIdentifier")
		rec := map[string]any{
			"DBClusterIdentifier": id, "Status": "available", "Engine": first(req.Input, "Engine"),
			"Endpoint": id + ".cluster-" + req.Identity.Region + ".rds.amazonaws.com",
		}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "dbcluster").Put(ctx, id, b)
		return &spi.Response{Output: map[string]any{"DBCluster": rec}}, nil
	case "DeleteDBCluster":
		id := first(req.Input, "DBClusterIdentifier")
		_ = p.col(req, "dbcluster").Delete(ctx, id)
		return &spi.Response{Output: map[string]any{"DBCluster": map[string]any{"DBClusterIdentifier": id, "Status": "deleting"}}}, nil
	case "CreateDBSnapshot":
		id := first(req.Input, "DBSnapshotIdentifier")
		rec := map[string]any{"DBSnapshotIdentifier": id, "DBInstanceIdentifier": first(req.Input, "DBInstanceIdentifier"), "Status": "available"}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "dbsnap").Put(ctx, id, b)
		return &spi.Response{Output: map[string]any{"DBSnapshot": rec}}, nil
	case "DescribeDBSnapshots":
		return p.listOrGet(ctx, req, "dbsnap", "DBSnapshotIdentifier", "DBSnapshots")
	case "DeleteDBSnapshot":
		id := first(req.Input, "DBSnapshotIdentifier")
		_ = p.col(req, "dbsnap").Delete(ctx, id)
		return &spi.Response{Output: map[string]any{"DBSnapshot": map[string]any{"DBSnapshotIdentifier": id}}}, nil
	case "CopyDBSnapshot":
		src, dst := first(req.Input, "SourceDBSnapshotIdentifier"), first(req.Input, "TargetDBSnapshotIdentifier")
		b, _, _ := p.col(req, "dbsnap").Get(ctx, src)
		rec := map[string]any{"DBSnapshotIdentifier": dst, "Status": "available"}
		_ = json.Unmarshal(b, &rec)
		rec["DBSnapshotIdentifier"] = dst
		nb, _ := json.Marshal(rec)
		_ = p.col(req, "dbsnap").Put(ctx, dst, nb)
		return &spi.Response{Output: map[string]any{"DBSnapshot": rec}}, nil
	case "CreateDBClusterSnapshot":
		id := first(req.Input, "DBClusterSnapshotIdentifier")
		rec := map[string]any{"DBClusterSnapshotIdentifier": id, "DBClusterIdentifier": first(req.Input, "DBClusterIdentifier"), "Status": "available"}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "dbcsnap").Put(ctx, id, b)
		return &spi.Response{Output: map[string]any{"DBClusterSnapshot": rec}}, nil
	case "DescribeDBClusterSnapshots":
		return p.listOrGet(ctx, req, "dbcsnap", "DBClusterSnapshotIdentifier", "DBClusterSnapshots")
	case "DeleteDBClusterSnapshot":
		id := first(req.Input, "DBClusterSnapshotIdentifier")
		_ = p.col(req, "dbcsnap").Delete(ctx, id)
		return &spi.Response{Output: map[string]any{"DBClusterSnapshot": map[string]any{"DBClusterSnapshotIdentifier": id}}}, nil
	case "CreateDBSubnetGroup":
		id := first(req.Input, "DBSubnetGroupName")
		rec := map[string]any{"DBSubnetGroupName": id, "DBSubnetGroupDescription": first(req.Input, "DBSubnetGroupDescription")}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "dbsubnet").Put(ctx, id, b)
		return &spi.Response{Output: map[string]any{"DBSubnetGroup": rec}}, nil
	case "DescribeDBSubnetGroups":
		return p.listOrGet(ctx, req, "dbsubnet", "DBSubnetGroupName", "DBSubnetGroups")
	case "DeleteDBSubnetGroup":
		_ = p.col(req, "dbsubnet").Delete(ctx, first(req.Input, "DBSubnetGroupName"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "CreateDBParameterGroup":
		id := first(req.Input, "DBParameterGroupName")
		rec := map[string]any{"DBParameterGroupName": id, "DBParameterGroupFamily": first(req.Input, "DBParameterGroupFamily")}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "dbpg").Put(ctx, id, b)
		return &spi.Response{Output: map[string]any{"DBParameterGroup": rec}}, nil
	case "DescribeDBParameterGroups":
		return p.listOrGet(ctx, req, "dbpg", "DBParameterGroupName", "DBParameterGroups")
	case "DeleteDBParameterGroup":
		_ = p.col(req, "dbpg").Delete(ctx, first(req.Input, "DBParameterGroupName"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "ModifyDBParameterGroup", "ResetDBParameterGroup":
		name := first(req.Input, "DBParameterGroupName")
		b, _ := json.Marshal(req.Input)
		_ = p.col(req, "dbpg-params").Put(ctx, name, b)
		return &spi.Response{Output: map[string]any{}}, nil
	case "DescribeDBParameters":
		name := first(req.Input, "DBParameterGroupName")
		b, ok, _ := p.col(req, "dbpg-params").Get(ctx, name)
		params := []any{}
		if ok {
			var rec map[string]any
			_ = json.Unmarshal(b, &rec)
			params = append(params, rec)
		}
		return &spi.Response{Output: map[string]any{"Parameters": params}}, nil
	case "CreateDBClusterParameterGroup":
		id := first(req.Input, "DBClusterParameterGroupName")
		rec := map[string]any{"DBClusterParameterGroupName": id, "DBParameterGroupFamily": first(req.Input, "DBParameterGroupFamily")}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "dbcpg").Put(ctx, id, b)
		return &spi.Response{Output: map[string]any{"DBClusterParameterGroup": rec}}, nil
	case "DescribeDBClusterParameterGroups":
		return p.listOrGet(ctx, req, "dbcpg", "DBClusterParameterGroupName", "DBClusterParameterGroups")
	case "DeleteDBClusterParameterGroup":
		_ = p.col(req, "dbcpg").Delete(ctx, first(req.Input, "DBClusterParameterGroupName"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "CreateOptionGroup":
		id := first(req.Input, "OptionGroupName")
		rec := map[string]any{"OptionGroupName": id, "EngineName": first(req.Input, "EngineName"), "MajorEngineVersion": first(req.Input, "MajorEngineVersion")}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "dbog").Put(ctx, id, b)
		return &spi.Response{Output: map[string]any{"OptionGroup": rec}}, nil
	case "DescribeOptionGroups":
		return p.listOrGet(ctx, req, "dbog", "OptionGroupName", "OptionGroupsList")
	case "DeleteOptionGroup":
		_ = p.col(req, "dbog").Delete(ctx, first(req.Input, "OptionGroupName"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "AddRoleToDBInstance":
		id, arn := first(req.Input, "DBInstanceIdentifier"), first(req.Input, "RoleArn")
		b, _ := json.Marshal(map[string]any{"RoleArn": arn})
		_ = p.col(req, "dbrole").Put(ctx, id+":"+arn, b)
		return &spi.Response{Output: map[string]any{}}, nil
	case "RemoveRoleFromDBInstance":
		_ = p.col(req, "dbrole").Delete(ctx, first(req.Input, "DBInstanceIdentifier")+":"+first(req.Input, "RoleArn"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "CreateEventSubscription":
		id := first(req.Input, "SubscriptionName")
		rec := map[string]any{"CustSubscriptionId": id, "SnsTopicArn": first(req.Input, "SnsTopicArn"), "Status": "active"}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "dbev").Put(ctx, id, b)
		return &spi.Response{Output: map[string]any{"EventSubscription": rec}}, nil
	case "DescribeEventSubscriptions":
		return p.listOrGet(ctx, req, "dbev", "SubscriptionName", "EventSubscriptionsList")
	case "DeleteEventSubscription":
		_ = p.col(req, "dbev").Delete(ctx, first(req.Input, "SubscriptionName"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "AddTagsToResource":
		b, _ := json.Marshal(req.Input["Tags"])
		_ = p.col(req, "rdstags").Put(ctx, first(req.Input, "ResourceName"), b)
		return &spi.Response{Output: map[string]any{}}, nil
	case "ListTagsForResource":
		b, ok, _ := p.col(req, "rdstags").Get(ctx, first(req.Input, "ResourceName"))
		var tags any = []any{}
		if ok {
			_ = json.Unmarshal(b, &tags)
		}
		return &spi.Response{Output: map[string]any{"TagList": tags}}, nil
	case "RemoveTagsFromResource":
		_ = p.col(req, "rdstags").Delete(ctx, first(req.Input, "ResourceName"))
		return &spi.Response{Output: map[string]any{}}, nil
	default:
		return p.extra(ctx, req)
	}
}

func (p *Pack) listOrGet(ctx context.Context, req *spi.Request, col, idKey, listKey string) (*spi.Response, error) {
	id := first(req.Input, idKey)
	if id != "" {
		b, ok, _ := p.col(req, col).Get(ctx, id)
		if !ok {
			return nil, &spi.Fault{Code: "DBInstanceNotFound", HTTPStatus: 404, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		return &spi.Response{Output: map[string]any{listKey: []any{rec}}}, nil
	}
	kvs, _, _ := p.col(req, col).List(ctx, "", "", 0)
	var items []any
	for _, kv := range kvs {
		var rec map[string]any
		_ = json.Unmarshal(kv.Value, &rec)
		items = append(items, rec)
	}
	return &spi.Response{Output: map[string]any{listKey: items}}, nil
}

func (p *Pack) modify(ctx context.Context, req *spi.Request, col, idKey, outKey string) (*spi.Response, error) {
	id := first(req.Input, idKey)
	b, ok, _ := p.col(req, col).Get(ctx, id)
	rec := map[string]any{}
	if ok {
		_ = json.Unmarshal(b, &rec)
	}
	for k, v := range req.Input {
		if k == idKey {
			continue
		}
		rec[k] = v
	}
	nb, _ := json.Marshal(rec)
	_ = p.col(req, col).Put(ctx, id, nb)
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
