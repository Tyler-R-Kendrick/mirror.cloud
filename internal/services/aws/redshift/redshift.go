// Package redshift emulates cluster control-plane records (not a SQL engine).
package redshift

import (
	"context"
	"encoding/json"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func init() {
	registry.Register(registry.Factory{ServiceID: "aws.redshift", Tier: model.TierEmulate, New: func(d spi.Deps) (spi.BehaviorPack, error) {
		return New(d), nil
	}})
}

// Pack implements Redshift-lite.
type Pack struct{ deps spi.Deps }

// New constructs the pack.
func New(d spi.Deps) *Pack { return &Pack{deps: d} }

func (p *Pack) ServiceID() string { return "aws.redshift" }
func (p *Pack) Tier() model.Tier  { return model.TierEmulate }
func (p *Pack) Operations() []string {
	core := []string{
		"CreateCluster", "DescribeClusters", "ModifyCluster", "DeleteCluster", "RebootCluster",
		"PauseCluster", "ResumeCluster", "ResizeCluster", "RestoreFromClusterSnapshot",
		"CreateClusterSnapshot", "DescribeClusterSnapshots", "DeleteClusterSnapshot", "CopyClusterSnapshot",
		"CreateClusterSubnetGroup", "DescribeClusterSubnetGroups", "DeleteClusterSubnetGroup", "ModifyClusterSubnetGroup",
		"CreateClusterParameterGroup", "DescribeClusterParameterGroups", "DeleteClusterParameterGroup",
		"ModifyClusterParameterGroup", "ResetClusterParameterGroup", "DescribeClusterParameters",
		"EnableSnapshotCopy", "DisableSnapshotCopy",
		"CreateSnapshotCopyGrant", "DescribeSnapshotCopyGrants", "DeleteSnapshotCopyGrant",
		"CreateEventSubscription", "DescribeEventSubscriptions", "DeleteEventSubscription",
		"GetClusterCredentials", "ModifyClusterIamRoles",
		"CreateTags", "DescribeTags", "DeleteTags",
	}
	return append(core, extraOps()...)
}

func (p *Pack) col(req *spi.Request, n string) spi.Collection {
	return p.deps.Store.Scope(req.Identity.Account, req.Identity.Region).Collection(n)
}

func (p *Pack) Invoke(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	switch req.Operation {
	case "CreateCluster":
		id := first(req.Input, "ClusterIdentifier")
		rec := map[string]any{
			"ClusterIdentifier": id, "ClusterStatus": "available",
			"NodeType": first(req.Input, "NodeType"), "MasterUsername": first(req.Input, "MasterUsername"),
			"Endpoint": map[string]any{"Address": id + "." + req.Identity.Region + ".redshift.amazonaws.com", "Port": 5439},
		}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "rscluster").Put(ctx, id, b)
		return &spi.Response{Output: map[string]any{"Cluster": rec}}, nil
	case "DescribeClusters":
		return listOrGet(ctx, p.col(req, "rscluster"), first(req.Input, "ClusterIdentifier"), "Clusters")
	case "ModifyCluster", "RebootCluster", "PauseCluster", "ResumeCluster":
		id := first(req.Input, "ClusterIdentifier")
		b, ok, _ := p.col(req, "rscluster").Get(ctx, id)
		rec := map[string]any{"ClusterIdentifier": id}
		if ok {
			_ = json.Unmarshal(b, &rec)
		}
		if req.Operation == "PauseCluster" {
			rec["ClusterStatus"] = "paused"
		} else {
			rec["ClusterStatus"] = "available"
		}
		nb, _ := json.Marshal(rec)
		_ = p.col(req, "rscluster").Put(ctx, id, nb)
		return &spi.Response{Output: map[string]any{"Cluster": rec}}, nil
	case "DeleteCluster":
		id := first(req.Input, "ClusterIdentifier")
		_ = p.col(req, "rscluster").Delete(ctx, id)
		return &spi.Response{Output: map[string]any{"Cluster": map[string]any{"ClusterIdentifier": id, "ClusterStatus": "deleting"}}}, nil
	case "CreateClusterSnapshot":
		id := first(req.Input, "SnapshotIdentifier")
		rec := map[string]any{"SnapshotIdentifier": id, "ClusterIdentifier": first(req.Input, "ClusterIdentifier"), "Status": "available"}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "rssnap").Put(ctx, id, b)
		return &spi.Response{Output: map[string]any{"Snapshot": rec}}, nil
	case "DescribeClusterSnapshots":
		return listOrGet(ctx, p.col(req, "rssnap"), first(req.Input, "SnapshotIdentifier"), "Snapshots")
	case "DeleteClusterSnapshot":
		_ = p.col(req, "rssnap").Delete(ctx, first(req.Input, "SnapshotIdentifier"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "CreateClusterSubnetGroup":
		id := first(req.Input, "ClusterSubnetGroupName")
		rec := map[string]any{"ClusterSubnetGroupName": id}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "rssubnet").Put(ctx, id, b)
		return &spi.Response{Output: map[string]any{"ClusterSubnetGroup": rec}}, nil
	case "DescribeClusterSubnetGroups":
		return listOrGet(ctx, p.col(req, "rssubnet"), first(req.Input, "ClusterSubnetGroupName"), "ClusterSubnetGroups")
	case "DeleteClusterSubnetGroup":
		_ = p.col(req, "rssubnet").Delete(ctx, first(req.Input, "ClusterSubnetGroupName"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "CreateClusterParameterGroup":
		id := first(req.Input, "ParameterGroupName")
		rec := map[string]any{"ParameterGroupName": id, "ParameterGroupFamily": first(req.Input, "ParameterGroupFamily")}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "rspg").Put(ctx, id, b)
		return &spi.Response{Output: map[string]any{"ClusterParameterGroup": rec}}, nil
	case "DescribeClusterParameterGroups":
		return listOrGet(ctx, p.col(req, "rspg"), first(req.Input, "ParameterGroupName"), "ParameterGroups")
	case "DeleteClusterParameterGroup":
		_ = p.col(req, "rspg").Delete(ctx, first(req.Input, "ParameterGroupName"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "CreateTags":
		b, _ := json.Marshal(req.Input["Tags"])
		_ = p.col(req, "rstags").Put(ctx, first(req.Input, "ResourceName"), b)
		return &spi.Response{Output: map[string]any{}}, nil
	case "DescribeTags":
		b, ok, _ := p.col(req, "rstags").Get(ctx, first(req.Input, "ResourceName"))
		var tags any = []any{}
		if ok {
			_ = json.Unmarshal(b, &tags)
		}
		return &spi.Response{Output: map[string]any{"TaggedResources": []any{map[string]any{"Tags": tags}}}}, nil
	case "DeleteTags":
		_ = p.col(req, "rstags").Delete(ctx, first(req.Input, "ResourceName"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "ResizeCluster":
		id := first(req.Input, "ClusterIdentifier")
		b, ok, _ := p.col(req, "rscluster").Get(ctx, id)
		rec := map[string]any{"ClusterIdentifier": id, "ClusterStatus": "resizing"}
		if ok {
			_ = json.Unmarshal(b, &rec)
			rec["ClusterStatus"] = "resizing"
		}
		if n := first(req.Input, "NodeType"); n != "" {
			rec["NodeType"] = n
		}
		nb, _ := json.Marshal(rec)
		_ = p.col(req, "rscluster").Put(ctx, id, nb)
		return &spi.Response{Output: map[string]any{"Cluster": rec}}, nil
	case "RestoreFromClusterSnapshot":
		id := first(req.Input, "ClusterIdentifier")
		rec := map[string]any{
			"ClusterIdentifier": id, "ClusterStatus": "available",
			"Endpoint": map[string]any{"Address": id + "." + req.Identity.Region + ".redshift.amazonaws.com", "Port": 5439},
		}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "rscluster").Put(ctx, id, b)
		return &spi.Response{Output: map[string]any{"Cluster": rec}}, nil
	case "CopyClusterSnapshot":
		src, dst := first(req.Input, "SourceSnapshotIdentifier"), first(req.Input, "TargetSnapshotIdentifier")
		b, _, _ := p.col(req, "rssnap").Get(ctx, src)
		rec := map[string]any{"SnapshotIdentifier": dst, "Status": "available"}
		_ = json.Unmarshal(b, &rec)
		rec["SnapshotIdentifier"] = dst
		nb, _ := json.Marshal(rec)
		_ = p.col(req, "rssnap").Put(ctx, dst, nb)
		return &spi.Response{Output: map[string]any{"Snapshot": rec}}, nil
	case "ModifyClusterSubnetGroup":
		id := first(req.Input, "ClusterSubnetGroupName")
		rec := map[string]any{"ClusterSubnetGroupName": id}
		if b, ok, _ := p.col(req, "rssubnet").Get(ctx, id); ok {
			_ = json.Unmarshal(b, &rec)
		}
		nb, _ := json.Marshal(rec)
		_ = p.col(req, "rssubnet").Put(ctx, id, nb)
		return &spi.Response{Output: map[string]any{"ClusterSubnetGroup": rec}}, nil
	case "ModifyClusterParameterGroup", "ResetClusterParameterGroup":
		name := first(req.Input, "ParameterGroupName")
		b, _ := json.Marshal(req.Input)
		_ = p.col(req, "rspg-params").Put(ctx, name, b)
		return &spi.Response{Output: map[string]any{}}, nil
	case "DescribeClusterParameters":
		name := first(req.Input, "ParameterGroupName")
		b, ok, _ := p.col(req, "rspg-params").Get(ctx, name)
		params := []any{}
		if ok {
			var rec map[string]any
			_ = json.Unmarshal(b, &rec)
			params = append(params, rec)
		}
		return &spi.Response{Output: map[string]any{"Parameters": params}}, nil
	case "EnableSnapshotCopy":
		id := first(req.Input, "ClusterIdentifier")
		b, ok, _ := p.col(req, "rscluster").Get(ctx, id)
		rec := map[string]any{"ClusterIdentifier": id}
		if ok {
			_ = json.Unmarshal(b, &rec)
		}
		rec["ClusterSnapshotCopyStatus"] = map[string]any{"DestinationRegion": first(req.Input, "DestinationRegion")}
		nb, _ := json.Marshal(rec)
		_ = p.col(req, "rscluster").Put(ctx, id, nb)
		return &spi.Response{Output: map[string]any{"Cluster": rec}}, nil
	case "DisableSnapshotCopy":
		id := first(req.Input, "ClusterIdentifier")
		b, ok, _ := p.col(req, "rscluster").Get(ctx, id)
		rec := map[string]any{"ClusterIdentifier": id}
		if ok {
			_ = json.Unmarshal(b, &rec)
		}
		delete(rec, "ClusterSnapshotCopyStatus")
		nb, _ := json.Marshal(rec)
		_ = p.col(req, "rscluster").Put(ctx, id, nb)
		return &spi.Response{Output: map[string]any{"Cluster": rec}}, nil
	case "CreateSnapshotCopyGrant":
		n := first(req.Input, "SnapshotCopyGrantName")
		rec := map[string]any{"SnapshotCopyGrantName": n, "KmsKeyId": first(req.Input, "KmsKeyId")}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "rscopygrant").Put(ctx, n, b)
		return &spi.Response{Output: map[string]any{"SnapshotCopyGrant": rec}}, nil
	case "DescribeSnapshotCopyGrants":
		return listOrGet(ctx, p.col(req, "rscopygrant"), first(req.Input, "SnapshotCopyGrantName"), "SnapshotCopyGrants")
	case "DeleteSnapshotCopyGrant":
		_ = p.col(req, "rscopygrant").Delete(ctx, first(req.Input, "SnapshotCopyGrantName"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "CreateEventSubscription":
		n := first(req.Input, "SubscriptionName")
		rec := map[string]any{"CustSubscriptionId": n, "SnsTopicArn": first(req.Input, "SnsTopicArn"), "Status": "active"}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "rsev").Put(ctx, n, b)
		return &spi.Response{Output: map[string]any{"EventSubscription": rec}}, nil
	case "DescribeEventSubscriptions":
		return listOrGet(ctx, p.col(req, "rsev"), first(req.Input, "SubscriptionName"), "EventSubscriptionsList")
	case "DeleteEventSubscription":
		_ = p.col(req, "rsev").Delete(ctx, first(req.Input, "SubscriptionName"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "GetClusterCredentials":
		user := first(req.Input, "DbUser")
		return &spi.Response{Output: map[string]any{
			"DbUser": "IAM:" + user, "DbPassword": p.deps.Rand.Derive("rs:" + user).Hex(16),
			"Expiration": "2099-01-01T00:00:00Z",
		}}, nil
	case "ModifyClusterIamRoles":
		id := first(req.Input, "ClusterIdentifier")
		b, ok, _ := p.col(req, "rscluster").Get(ctx, id)
		rec := map[string]any{"ClusterIdentifier": id}
		if ok {
			_ = json.Unmarshal(b, &rec)
		}
		rec["IamRoles"] = req.Input["AddIamRoles"]
		nb, _ := json.Marshal(rec)
		_ = p.col(req, "rscluster").Put(ctx, id, nb)
		return &spi.Response{Output: map[string]any{"Cluster": rec}}, nil
	default:
		return p.extra(ctx, req)
	}
}

func listOrGet(ctx context.Context, c spi.Collection, id, listKey string) (*spi.Response, error) {
	if id != "" {
		b, ok, _ := c.Get(ctx, id)
		if !ok {
			return nil, &spi.Fault{Code: "ClusterNotFound", HTTPStatus: 404, Fault: "client"}
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

func first(in map[string]any, keys ...string) string {
	for _, k := range keys {
		if s, ok := in[k].(string); ok && s != "" {
			return s
		}
	}
	return ""
}
