package redshift

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

// extraOps remaining Redshift ops served as control-plane KV.
// leftoverOps are remaining Smithy operations served as control-plane KV.
// ponytail: no SQL engine, HSM, or partner integration; upgrade is per-op AWS shapes.
func ExtraOps() []string { return extraOps() }

func extraOps() []string {
	return []string{
		"AcceptReservedNodeExchange",
		"AddPartner",
		"AssociateDataShareConsumer",
		"AuthorizeClusterSecurityGroupIngress",
		"AuthorizeDataShare",
		"AuthorizeEndpointAccess",
		"AuthorizeSnapshotAccess",
		"BatchDeleteClusterSnapshots",
		"BatchModifyClusterSnapshots",
		"CancelResize",
		"CreateAuthenticationProfile",
		"CreateClusterSecurityGroup",
		"CreateCustomDomainAssociation",
		"CreateEndpointAccess",
		"CreateHsmClientCertificate",
		"CreateHsmConfiguration",
		"CreateIntegration",
		"CreateQev2IdcApplication",
		"CreateRedshiftIdcApplication",
		"CreateScheduledAction",
		"CreateSnapshotSchedule",
		"CreateUsageLimit",
		"DeauthorizeDataShare",
		"DeleteAuthenticationProfile",
		"DeleteClusterSecurityGroup",
		"DeleteCustomDomainAssociation",
		"DeleteEndpointAccess",
		"DeleteHsmClientCertificate",
		"DeleteHsmConfiguration",
		"DeleteIntegration",
		"DeletePartner",
		"DeleteQev2IdcApplication",
		"DeleteRedshiftIdcApplication",
		"DeleteResourcePolicy",
		"DeleteScheduledAction",
		"DeleteSnapshotSchedule",
		"DeleteUsageLimit",
		"DeregisterNamespace",
		"DescribeAccountAttributes",
		"DescribeAuthenticationProfiles",
		"DescribeClusterDbRevisions",
		"DescribeClusterSecurityGroups",
		"DescribeClusterTracks",
		"DescribeClusterVersions",
		"DescribeCustomDomainAssociations",
		"DescribeDataShares",
		"DescribeDataSharesForConsumer",
		"DescribeDataSharesForProducer",
		"DescribeDefaultClusterParameters",
		"DescribeEndpointAccess",
		"DescribeEndpointAuthorization",
		"DescribeEventCategories",
		"DescribeEvents",
		"DescribeHsmClientCertificates",
		"DescribeHsmConfigurations",
		"DescribeInboundIntegrations",
		"DescribeIntegrations",
		"DescribeLoggingStatus",
		"DescribeNodeConfigurationOptions",
		"DescribeOrderableClusterOptions",
		"DescribePartners",
		"DescribeQev2IdcApplications",
		"DescribeRedshiftIdcApplications",
		"DescribeReservedNodeExchangeStatus",
		"DescribeReservedNodeOfferings",
		"DescribeReservedNodes",
		"DescribeResize",
		"DescribeScheduledActions",
		"DescribeSnapshotSchedules",
		"DescribeStorage",
		"DescribeTableRestoreStatus",
		"DescribeUsageLimits",
		"DisableLogging",
		"DisassociateDataShareConsumer",
		"EnableLogging",
		"FailoverPrimaryCompute",
		"GetClusterCredentialsWithIAM",
		"GetIdentityCenterAuthToken",
		"GetReservedNodeExchangeConfigurationOptions",
		"GetReservedNodeExchangeOfferings",
		"GetResourcePolicy",
		"ListRecommendations",
		"ModifyAquaConfiguration",
		"ModifyAuthenticationProfile",
		"ModifyClusterDbRevision",
		"ModifyClusterMaintenance",
		"ModifyClusterSnapshot",
		"ModifyClusterSnapshotSchedule",
		"ModifyCustomDomainAssociation",
		"ModifyEndpointAccess",
		"ModifyEventSubscription",
		"ModifyIntegration",
		"ModifyLakehouseConfiguration",
		"ModifyQev2IdcApplication",
		"ModifyRedshiftIdcApplication",
		"ModifyScheduledAction",
		"ModifySnapshotCopyRetentionPeriod",
		"ModifySnapshotSchedule",
		"ModifyUsageLimit",
		"PurchaseReservedNodeOffering",
		"PutResourcePolicy",
		"RegisterNamespace",
		"RejectDataShare",
		"RestoreTableFromClusterSnapshot",
		"RevokeClusterSecurityGroupIngress",
		"RevokeEndpointAccess",
		"RevokeSnapshotAccess",
		"RotateEncryptionKey",
		"UpdatePartnerStatus",
	}
}

func (p *Pack) extra(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	op := req.Operation
	kind, idKey, listKey, wrap := extraShape(op)
	id := first(req.Input, idKey, "ClusterIdentifier", "SnapshotIdentifier", "ResourceArn",
		"ScheduledActionName", "UsageLimitId", "IntegrationArn", "NamespaceArn", "PartnerName")
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
		if op == "GetClusterCredentialsWithIAM" {
			rec["DbUser"] = "IAM:" + first(req.Input, "DbUser")
			rec["DbPassword"] = p.deps.Rand.Derive("rsiam:" + id).Hex(16)
		}
		if op == "GetIdentityCenterAuthToken" {
			rec["AuthToken"] = p.deps.Rand.Derive("idc:" + id).Hex(16)
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
	case strings.HasPrefix(op, "Delete") || strings.HasPrefix(op, "Revoke") || strings.HasPrefix(op, "Reject") ||
		strings.HasPrefix(op, "Deauthorize") || strings.HasPrefix(op, "Deregister") || strings.HasPrefix(op, "Disassociate") ||
		strings.HasPrefix(op, "Cancel") || strings.HasPrefix(op, "Disable") || strings.HasPrefix(op, "BatchDelete"):
		if id != "" {
			_ = p.col(req, "rsl").Delete(ctx, kind+":"+id)
		}
		return &spi.Response{Output: map[string]any{}}, nil
	case strings.HasPrefix(op, "Describe") || strings.HasPrefix(op, "List"):
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
		if op == "GetClusterCredentialsWithIAM" {
			out["DbUser"] = "IAM:" + first(req.Input, "DbUser")
			out["DbPassword"] = p.deps.Rand.Derive("rsiam:" + first(req.Input, "ClusterIdentifier")).Hex(16)
		}
		if op == "GetIdentityCenterAuthToken" {
			out["AuthToken"] = p.deps.Rand.Derive("idc").Hex(16)
		}
		if op == "DescribeLoggingStatus" || op == "DescribeStorage" || op == "DescribeResize" {
			out["Status"] = "available"
		}
		return &spi.Response{Output: out}, nil
	}
}

func (p *Pack) lput(ctx context.Context, req *spi.Request, key string, rec any) {
	b, _ := json.Marshal(rec)
	_ = p.col(req, "rsl").Put(ctx, key, b)
}

func (p *Pack) lget(ctx context.Context, req *spi.Request, key string) (map[string]any, bool) {
	b, ok, _ := p.col(req, "rsl").Get(ctx, key)
	if !ok {
		return nil, false
	}
	var rec map[string]any
	_ = json.Unmarshal(b, &rec)
	return rec, true
}

func (p *Pack) llist(ctx context.Context, req *spi.Request, pfx, outKey string) (*spi.Response, error) {
	kvs, _, _ := p.col(req, "rsl").List(ctx, pfx, "", 0)
	items := make([]any, 0, len(kvs))
	for _, kv := range kvs {
		var rec map[string]any
		_ = json.Unmarshal(kv.Value, &rec)
		items = append(items, rec)
	}
	return &spi.Response{Output: map[string]any{outKey: items}}, nil
}

func isExtraWrite(op string) bool {
	for _, p := range []string{"Create", "Modify", "Put", "Register", "Authorize", "Associate", "Accept", "Add",
		"Enable", "Failover", "Purchase", "Restore", "Rotate", "Update", "BatchModify"} {
		if strings.HasPrefix(op, p) {
			return true
		}
	}
	return false
}

func extraShape(op string) (kind, idKey, listKey, wrap string) {
	switch {
	case strings.Contains(op, "DataShare"):
		return "lds", "DataShareArn", "DataShares", "DataShare"
	case strings.Contains(op, "ReservedNode"):
		return "lrn", "ReservedNodeId", "ReservedNodes", "ReservedNode"
	case strings.Contains(op, "AuthenticationProfile"):
		return "lauth", "AuthenticationProfileName", "AuthenticationProfiles", "AuthenticationProfile"
	case strings.Contains(op, "SecurityGroup"):
		return "lsg", "ClusterSecurityGroupName", "ClusterSecurityGroups", "ClusterSecurityGroup"
	case strings.Contains(op, "CustomDomain"):
		return "lcd", "CustomDomainName", "Associations", "CustomDomainAssociation"
	case strings.Contains(op, "Endpoint"):
		return "lep", "EndpointName", "EndpointAccessList", "EndpointAccess"
	case strings.Contains(op, "HsmClient"):
		return "lhsmc", "HsmClientCertificateIdentifier", "HsmClientCertificates", "HsmClientCertificate"
	case strings.Contains(op, "HsmConfiguration"):
		return "lhsm", "HsmConfigurationIdentifier", "HsmConfigurations", "HsmConfiguration"
	case strings.Contains(op, "Integration"):
		return "lint", "IntegrationArn", "Integrations", "Integration"
	case strings.Contains(op, "IdcApplication") || strings.Contains(op, "Qev2"):
		return "lidc", "IdcApplicationArn", "IdcApplications", "IdcApplication"
	case strings.Contains(op, "ScheduledAction"):
		return "lsa", "ScheduledActionName", "ScheduledActions", "ScheduledAction"
	case strings.Contains(op, "SnapshotSchedule"):
		return "lss", "ScheduleIdentifier", "SnapshotSchedules", "SnapshotSchedule"
	case strings.Contains(op, "UsageLimit"):
		return "lul", "UsageLimitId", "UsageLimits", "UsageLimit"
	case strings.Contains(op, "Partner"):
		return "lpart", "PartnerName", "Partners", "PartnerIntegrationInfo"
	case strings.Contains(op, "ResourcePolicy"):
		return "lrpol", "ResourceArn", "ResourcePolicy", "ResourcePolicy"
	case strings.Contains(op, "Namespace"):
		return "lns", "NamespaceArn", "Namespaces", ""
	case strings.Contains(op, "Logging"):
		return "llog", "ClusterIdentifier", "LoggingStatus", ""
	case strings.Contains(op, "Recommendation"):
		return "lrec", "RecommendationId", "Recommendations", ""
	default:
		return "lmisc", "ClusterIdentifier", "Clusters", "Cluster"
	}
}
