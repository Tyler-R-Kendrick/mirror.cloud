package rds

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

// extraOps remaining RDS ops served as control-plane KV.
// leftoverOps are remaining Smithy operations served as control-plane KV.
// ponytail: no real engine, proxy, or global replication; upgrade is per-op AWS shapes.
func ExtraOps() []string { return extraOps() }

func extraOps() []string {
	return []string{
		"AddRoleToDBCluster",
		"AddSourceIdentifierToSubscription",
		"ApplyPendingMaintenanceAction",
		"AuthorizeDBSecurityGroupIngress",
		"BacktrackDBCluster",
		"CancelExportTask",
		"CopyDBClusterParameterGroup",
		"CopyDBClusterSnapshot",
		"CopyDBParameterGroup",
		"CopyOptionGroup",
		"CreateBlueGreenDeployment",
		"CreateCustomDBEngineVersion",
		"CreateDBClusterEndpoint",
		"CreateDBProxy",
		"CreateDBProxyEndpoint",
		"CreateDBSecurityGroup",
		"CreateDBShardGroup",
		"CreateGlobalCluster",
		"CreateIntegration",
		"CreateTenantDatabase",
		"DeleteBlueGreenDeployment",
		"DeleteCustomDBEngineVersion",
		"DeleteDBClusterAutomatedBackup",
		"DeleteDBClusterEndpoint",
		"DeleteDBInstanceAutomatedBackup",
		"DeleteDBProxy",
		"DeleteDBProxyEndpoint",
		"DeleteDBSecurityGroup",
		"DeleteDBShardGroup",
		"DeleteGlobalCluster",
		"DeleteIntegration",
		"DeleteTenantDatabase",
		"DeregisterDBProxyTargets",
		"DescribeAccountAttributes",
		"DescribeBlueGreenDeployments",
		"DescribeCertificates",
		"DescribeDBClusterAutomatedBackups",
		"DescribeDBClusterBacktracks",
		"DescribeDBClusterEndpoints",
		"DescribeDBClusterParameters",
		"DescribeDBClusterSnapshotAttributes",
		"DescribeDBEngineVersions",
		"DescribeDBInstanceAutomatedBackups",
		"DescribeDBLogFiles",
		"DescribeDBMajorEngineVersions",
		"DescribeDBProxies",
		"DescribeDBProxyEndpoints",
		"DescribeDBProxyTargetGroups",
		"DescribeDBProxyTargets",
		"DescribeDBRecommendations",
		"DescribeDBSecurityGroups",
		"DescribeDBShardGroups",
		"DescribeDBSnapshotAttributes",
		"DescribeDBSnapshotTenantDatabases",
		"DescribeEngineDefaultClusterParameters",
		"DescribeEngineDefaultParameters",
		"DescribeEventCategories",
		"DescribeEvents",
		"DescribeExportTasks",
		"DescribeGlobalClusters",
		"DescribeIntegrations",
		"DescribeOptionGroupOptions",
		"DescribeOrderableDBInstanceOptions",
		"DescribePendingMaintenanceActions",
		"DescribeReservedDBInstances",
		"DescribeReservedDBInstancesOfferings",
		"DescribeServerlessV2PlatformVersions",
		"DescribeSourceRegions",
		"DescribeTenantDatabases",
		"DescribeValidDBInstanceModifications",
		"DisableHttpEndpoint",
		"DownloadDBLogFilePortion",
		"EnableHttpEndpoint",
		"FailoverGlobalCluster",
		"ModifyActivityStream",
		"ModifyCertificates",
		"ModifyCurrentDBClusterCapacity",
		"ModifyCustomDBEngineVersion",
		"ModifyDBClusterEndpoint",
		"ModifyDBClusterParameterGroup",
		"ModifyDBClusterSnapshotAttribute",
		"ModifyDBProxy",
		"ModifyDBProxyEndpoint",
		"ModifyDBProxyTargetGroup",
		"ModifyDBRecommendation",
		"ModifyDBShardGroup",
		"ModifyDBSnapshot",
		"ModifyDBSnapshotAttribute",
		"ModifyDBSubnetGroup",
		"ModifyEventSubscription",
		"ModifyGlobalCluster",
		"ModifyIntegration",
		"ModifyOptionGroup",
		"ModifyTenantDatabase",
		"PromoteReadReplicaDBCluster",
		"PurchaseReservedDBInstancesOffering",
		"RebootDBCluster",
		"RebootDBShardGroup",
		"RegisterDBProxyTargets",
		"RemoveFromGlobalCluster",
		"RemoveRoleFromDBCluster",
		"RemoveSourceIdentifierFromSubscription",
		"ResetDBClusterParameterGroup",
		"RestoreDBClusterFromS3",
		"RestoreDBClusterToPointInTime",
		"RestoreDBInstanceFromS3",
		"RestoreDBInstanceToPointInTime",
		"RevokeDBSecurityGroupIngress",
		"StartActivityStream",
		"StartDBCluster",
		"StartDBInstanceAutomatedBackupsReplication",
		"StartExportTask",
		"StopActivityStream",
		"StopDBCluster",
		"StopDBInstanceAutomatedBackupsReplication",
		"SwitchoverBlueGreenDeployment",
		"SwitchoverGlobalCluster",
		"SwitchoverReadReplica",
	}
}

func (p *Pack) extra(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	op := req.Operation
	kind, idKey, listKey, wrap := extraShape(op)
	id := first(req.Input, idKey,
		"DBInstanceIdentifier", "DBClusterIdentifier", "GlobalClusterIdentifier", "DBProxyName",
		"BlueGreenDeploymentIdentifier", "IntegrationIdentifier", "DBSecurityGroupName",
		"OptionGroupName", "DBParameterGroupName", "DBClusterParameterGroupName",
		"DBSnapshotIdentifier", "DBClusterSnapshotIdentifier", "ExportTaskIdentifier",
		"SubscriptionName", "ResourceName", "Target", "SourceIdentifier")
	switch {
	case isExtraWrite(op):
		if id == "" {
			id = p.deps.Rand.Hex(8)
		}
		rec := map[string]any{}
		for k, v := range req.Input {
			rec[k] = v
		}
		if idKey != "" {
			if _, ok := rec[idKey]; !ok {
				rec[idKey] = id
			}
		}
		if op == "StartActivityStream" || op == "EnableHttpEndpoint" {
			rec["Status"] = "available"
		}
		p.lput(ctx, req, kind+":"+id, rec)
		out := map[string]any{}
		if wrap != "" {
			out[wrap] = rec
		} else {
			out = rec
		}
		if idKey != "" {
			out[idKey] = id
		}
		return &spi.Response{Output: out}, nil
	case strings.HasPrefix(op, "Delete") || strings.HasPrefix(op, "Remove") || strings.HasPrefix(op, "Deregister") ||
		strings.HasPrefix(op, "Cancel") || strings.HasPrefix(op, "Revoke") || strings.HasPrefix(op, "Disable") ||
		strings.HasPrefix(op, "Stop"):
		if id != "" {
			_ = p.col(req, "rdsl").Delete(ctx, kind+":"+id)
		}
		return &spi.Response{Output: map[string]any{}}, nil
	case strings.HasPrefix(op, "Describe") || op == "DownloadDBLogFilePortion":
		if op == "DownloadDBLogFilePortion" {
			return &spi.Response{Output: map[string]any{"LogFileData": "", "AdditionalDataPending": false}}, nil
		}
		if id != "" {
			if rec, ok := p.lget(ctx, req, kind+":"+id); ok {
				return &spi.Response{Output: map[string]any{listKey: []any{rec}}}, nil
			}
		}
		return p.llist(ctx, req, kind+":", listKey)
	default:
		if rec, ok := p.lget(ctx, req, kind+":"+id); ok {
			if wrap != "" {
				return &spi.Response{Output: map[string]any{wrap: rec}}, nil
			}
			return &spi.Response{Output: rec}, nil
		}
		out := map[string]any{}
		if wrap != "" {
			out[wrap] = map[string]any{}
		}
		return &spi.Response{Output: out}, nil
	}
}

func (p *Pack) lput(ctx context.Context, req *spi.Request, key string, rec any) {
	b, _ := json.Marshal(rec)
	_ = p.col(req, "rdsl").Put(ctx, key, b)
}

func (p *Pack) lget(ctx context.Context, req *spi.Request, key string) (map[string]any, bool) {
	b, ok, _ := p.col(req, "rdsl").Get(ctx, key)
	if !ok {
		return nil, false
	}
	var rec map[string]any
	_ = json.Unmarshal(b, &rec)
	return rec, true
}

func (p *Pack) llist(ctx context.Context, req *spi.Request, pfx, outKey string) (*spi.Response, error) {
	kvs, _, _ := p.col(req, "rdsl").List(ctx, pfx, "", 0)
	items := make([]any, 0, len(kvs))
	for _, kv := range kvs {
		var rec map[string]any
		_ = json.Unmarshal(kv.Value, &rec)
		items = append(items, rec)
	}
	return &spi.Response{Output: map[string]any{outKey: items}}, nil
}

func isExtraWrite(op string) bool {
	for _, p := range []string{"Create", "Copy", "Modify", "Restore", "Start", "Switchover", "Promote", "Purchase",
		"Authorize", "Apply", "Backtrack", "Enable", "Failover", "Reboot", "Register", "Add", "Reset"} {
		if strings.HasPrefix(op, p) {
			return true
		}
	}
	return false
}

func extraShape(op string) (kind, idKey, listKey, wrap string) {
	switch {
	case strings.Contains(op, "BlueGreen"):
		return "lbg", "BlueGreenDeploymentIdentifier", "BlueGreenDeployments", "BlueGreenDeployment"
	case strings.Contains(op, "GlobalCluster"):
		return "lgc", "GlobalClusterIdentifier", "GlobalClusters", "GlobalCluster"
	case strings.Contains(op, "Prox"):
		return "lproxy", "DBProxyName", "DBProxies", "DBProxy"
	case strings.Contains(op, "Integration"):
		return "lint", "IntegrationIdentifier", "Integrations", "Integration"
	case strings.Contains(op, "Tenant"):
		return "lten", "TenantDatabaseName", "TenantDatabases", "TenantDatabase"
	case strings.Contains(op, "ShardGroup"):
		return "lshard", "DBShardGroupIdentifier", "DBShardGroups", "DBShardGroup"
	case strings.Contains(op, "SecurityGroup"):
		return "lsg", "DBSecurityGroupName", "DBSecurityGroups", "DBSecurityGroup"
	case strings.Contains(op, "CustomDBEngineVersion"):
		return "lcve", "EngineVersion", "DBEngineVersions", ""
	case strings.Contains(op, "ClusterEndpoint"):
		return "lcend", "DBClusterEndpointIdentifier", "DBClusterEndpoints", "DBClusterEndpoint"
	case strings.Contains(op, "Export"):
		return "lexp", "ExportTaskIdentifier", "ExportTasks", "ExportTask"
	case strings.Contains(op, "ActivityStream"):
		return "lact", "ResourceArn", "ActivityStreams", ""
	case strings.Contains(op, "HttpEndpoint"):
		return "lhttp", "ResourceArn", "HttpEndpoints", ""
	case strings.Contains(op, "Certificate"):
		return "lcert", "CertificateIdentifier", "Certificates", ""
	case strings.Contains(op, "Recommendation"):
		return "lrec", "RecommendationId", "DBRecommendations", ""
	case strings.Contains(op, "Reserved"):
		return "lres", "ReservedDBInstanceId", "ReservedDBInstances", "ReservedDBInstance"
	case strings.Contains(op, "Event"):
		return "lev", "SubscriptionName", "Events", "EventSubscription"
	case strings.Contains(op, "Option"):
		return "log", "OptionGroupName", "OptionGroupsList", "OptionGroup"
	case strings.Contains(op, "AutomatedBackup"):
		return "lab", "DBInstanceAutomatedBackupsArn", "DBInstanceAutomatedBackups", ""
	case strings.Contains(op, "Backtrack"):
		return "lbt", "DBClusterIdentifier", "DBClusterBacktracks", ""
	case strings.Contains(op, "PendingMaintenance"):
		return "lpm", "ResourceIdentifier", "PendingMaintenanceActions", ""
	case strings.Contains(op, "EngineVersion") || strings.Contains(op, "Orderable") || strings.Contains(op, "Serverless") || strings.Contains(op, "SourceRegion") || strings.Contains(op, "AccountAttribute") || strings.Contains(op, "ValidDB"):
		return "lmeta", "Engine", "DBEngineVersions", ""
	case strings.Contains(op, "LogFile"):
		return "llog", "DBInstanceIdentifier", "DescribeDBLogFiles", ""
	case strings.Contains(op, "SnapshotAttribute"):
		return "lsattr", "DBSnapshotIdentifier", "DBSnapshotAttributesResult", "DBSnapshotAttributesResult"
	case strings.Contains(op, "Parameter"):
		return "lpg", "DBParameterGroupName", "Parameters", ""
	case strings.Contains(op, "SubnetGroup"):
		return "lsub", "DBSubnetGroupName", "DBSubnetGroups", "DBSubnetGroup"
	case strings.Contains(op, "Snapshot"):
		return "lsnap", "DBSnapshotIdentifier", "DBSnapshots", "DBSnapshot"
	case strings.Contains(op, "Cluster"):
		return "lcl", "DBClusterIdentifier", "DBClusters", "DBCluster"
	default:
		return "lmisc", "DBInstanceIdentifier", "DBInstances", "DBInstance"
	}
}
