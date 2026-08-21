// Package neptune stores Neptune clusters and instances (no Gremlin/SPARQL engine).
package neptune

import (
	"context"
	"encoding/json"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func init() {
	registry.Register(registry.Factory{ServiceID: "aws.neptune", Tier: model.TierEmulate, New: func(d spi.Deps) (spi.BehaviorPack, error) {
		return New(d), nil
	}})
}

// Pack implements Neptune-lite.
type Pack struct{ deps spi.Deps }

// New constructs the pack.
func New(d spi.Deps) *Pack { return &Pack{deps: d} }

func (p *Pack) ServiceID() string { return "aws.neptune" }
func (p *Pack) Tier() model.Tier  { return model.TierEmulate }
func (p *Pack) Operations() []string {
	return []string{
		"CreateDBCluster", "DescribeDBClusters", "ModifyDBCluster", "DeleteDBCluster", "FailoverDBCluster",
		"CreateDBInstance", "DescribeDBInstances", "DeleteDBInstance",
		"CreateDBSubnetGroup", "DescribeDBSubnetGroups", "DeleteDBSubnetGroup",
		"CreateDBClusterSnapshot", "DescribeDBClusterSnapshots", "DeleteDBClusterSnapshot",
	}
}

func (p *Pack) col(req *spi.Request, n string) spi.Collection {
	return p.deps.Store.Scope(req.Identity.Account, req.Identity.Region).Collection(n)
}

func (p *Pack) Invoke(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	switch req.Operation {
	case "CreateDBCluster":
		id := first(req.Input, "DBClusterIdentifier")
		if id == "" {
			return nil, &spi.Fault{Code: "InvalidParameterValue", HTTPStatus: 400, Fault: "client"}
		}
		rec := map[string]any{
			"DBClusterIdentifier": id, "Status": "available", "Engine": "neptune",
			"EngineVersion": first(req.Input, "EngineVersion"),
			"Endpoint":      id + "." + req.Identity.Region + ".neptune.amazonaws.com", "Port": 8182,
		}
		if rec["EngineVersion"] == "" {
			rec["EngineVersion"] = "1.2.1.0"
		}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "npcl").Put(ctx, id, b)
		return &spi.Response{Output: map[string]any{"DBCluster": rec}}, nil
	case "DescribeDBClusters":
		return listOrGet(ctx, p.col(req, "npcl"), first(req.Input, "DBClusterIdentifier"), "DBClusters")
	case "ModifyDBCluster":
		id := first(req.Input, "DBClusterIdentifier")
		b, ok, _ := p.col(req, "npcl").Get(ctx, id)
		if !ok {
			return nil, &spi.Fault{Code: "DBClusterNotFoundFault", HTTPStatus: 404, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		if v := first(req.Input, "BackupRetentionPeriod"); v != "" {
			rec["BackupRetentionPeriod"] = v
		}
		nb, _ := json.Marshal(rec)
		_ = p.col(req, "npcl").Put(ctx, id, nb)
		return &spi.Response{Output: map[string]any{"DBCluster": rec}}, nil
	case "DeleteDBCluster":
		id := first(req.Input, "DBClusterIdentifier")
		b, _, _ := p.col(req, "npcl").Get(ctx, id)
		_ = p.col(req, "npcl").Delete(ctx, id)
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		if rec == nil {
			rec = map[string]any{"DBClusterIdentifier": id}
		}
		rec["Status"] = "deleting"
		return &spi.Response{Output: map[string]any{"DBCluster": rec}}, nil
	case "FailoverDBCluster":
		id := first(req.Input, "DBClusterIdentifier")
		b, ok, _ := p.col(req, "npcl").Get(ctx, id)
		if !ok {
			return nil, &spi.Fault{Code: "DBClusterNotFoundFault", HTTPStatus: 404, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		return &spi.Response{Output: map[string]any{"DBCluster": rec}}, nil
	case "CreateDBInstance":
		id := first(req.Input, "DBInstanceIdentifier")
		rec := map[string]any{
			"DBInstanceIdentifier": id, "DBInstanceStatus": "available", "Engine": "neptune",
			"DBClusterIdentifier": first(req.Input, "DBClusterIdentifier"),
			"DBInstanceClass":     first(req.Input, "DBInstanceClass"),
			"Endpoint":            map[string]any{"Address": id + "." + req.Identity.Region + ".neptune.amazonaws.com", "Port": 8182},
		}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "npinst").Put(ctx, id, b)
		return &spi.Response{Output: map[string]any{"DBInstance": rec}}, nil
	case "DescribeDBInstances":
		return listOrGet(ctx, p.col(req, "npinst"), first(req.Input, "DBInstanceIdentifier"), "DBInstances")
	case "DeleteDBInstance":
		id := first(req.Input, "DBInstanceIdentifier")
		_ = p.col(req, "npinst").Delete(ctx, id)
		return &spi.Response{Output: map[string]any{"DBInstance": map[string]any{"DBInstanceIdentifier": id, "DBInstanceStatus": "deleting"}}}, nil
	case "CreateDBSubnetGroup":
		name := first(req.Input, "DBSubnetGroupName")
		rec := map[string]any{"DBSubnetGroupName": name, "DBSubnetGroupDescription": first(req.Input, "DBSubnetGroupDescription")}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "npsg").Put(ctx, name, b)
		return &spi.Response{Output: map[string]any{"DBSubnetGroup": rec}}, nil
	case "DescribeDBSubnetGroups":
		return listOrGet(ctx, p.col(req, "npsg"), first(req.Input, "DBSubnetGroupName"), "DBSubnetGroups")
	case "DeleteDBSubnetGroup":
		_ = p.col(req, "npsg").Delete(ctx, first(req.Input, "DBSubnetGroupName"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "CreateDBClusterSnapshot":
		id := first(req.Input, "DBClusterSnapshotIdentifier")
		rec := map[string]any{"DBClusterSnapshotIdentifier": id, "DBClusterIdentifier": first(req.Input, "DBClusterIdentifier"), "Status": "available"}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "npsnap").Put(ctx, id, b)
		return &spi.Response{Output: map[string]any{"DBClusterSnapshot": rec}}, nil
	case "DescribeDBClusterSnapshots":
		return listOrGet(ctx, p.col(req, "npsnap"), first(req.Input, "DBClusterSnapshotIdentifier"), "DBClusterSnapshots")
	case "DeleteDBClusterSnapshot":
		_ = p.col(req, "npsnap").Delete(ctx, first(req.Input, "DBClusterSnapshotIdentifier"))
		return &spi.Response{Output: map[string]any{}}, nil
	default:
		return nil, spi.NotImplemented("aws.neptune", req.Operation, "emulate")
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
	for _, k := range keys {
		if s, ok := in[k].(string); ok && s != "" {
			return s
		}
	}
	return ""
}
