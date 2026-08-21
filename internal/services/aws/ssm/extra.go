package ssm

import (
	"context"
	"strings"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

// leftoverOps are remaining Smithy operations served as control-plane KV.
// ponytail: no SSM agent, inventory engine, or patch scan; upgrade is per-op AWS shapes.
func ExtraOps() []string { return extraOps() }

func extraOps() []string {
	return []string{
		"AssociateOpsItemRelatedItem",
		"CancelMaintenanceWindowExecution",
		"CreateActivation",
		"CreateAssociationBatch",
		"CreateCloudConnector",
		"CreateOpsMetadata",
		"DeleteActivation",
		"DeleteCloudConnector",
		"DeleteInventory",
		"DeleteOpsMetadata",
		"DeleteResourcePolicy",
		"DeregisterManagedInstance",
		"DeregisterPatchBaselineForPatchGroup",
		"DescribeActivations",
		"DescribeAssociationExecutionTargets",
		"DescribeAssociationExecutions",
		"DescribeAutomationStepExecutions",
		"DescribeAvailablePatches",
		"DescribeDocumentPermission",
		"DescribeEffectiveInstanceAssociations",
		"DescribeEffectivePatchesForPatchBaseline",
		"DescribeInstanceAssociationsStatus",
		"DescribeInstanceInformation",
		"DescribeInstancePatchStates",
		"DescribeInstancePatchStatesForPatchGroup",
		"DescribeInstancePatches",
		"DescribeInstanceProperties",
		"DescribeInventoryDeletions",
		"DescribeMaintenanceWindowExecutionTaskInvocations",
		"DescribeMaintenanceWindowExecutionTasks",
		"DescribeMaintenanceWindowExecutions",
		"DescribeMaintenanceWindowSchedule",
		"DescribeMaintenanceWindowsForTarget",
		"DescribePatchGroupState",
		"DescribePatchGroups",
		"DescribePatchProperties",
		"DescribeSessions",
		"DisassociateOpsItemRelatedItem",
		"GetAccessToken",
		"GetCalendarState",
		"GetCloudConnector",
		"GetConnectionStatus",
		"GetDeployablePatchSnapshotForInstance",
		"GetExecutionPreview",
		"GetInventory",
		"GetInventorySchema",
		"GetMaintenanceWindowExecution",
		"GetMaintenanceWindowExecutionTask",
		"GetMaintenanceWindowExecutionTaskInvocation",
		"GetMaintenanceWindowTask",
		"GetOpsMetadata",
		"GetOpsSummary",
		"GetPatchBaselineForPatchGroup",
		"GetResourcePolicies",
		"ListAssociationVersions",
		"ListCloudConnectors",
		"ListComplianceItems",
		"ListComplianceSummaries",
		"ListDocumentMetadataHistory",
		"ListInventoryEntries",
		"ListNodes",
		"ListNodesSummary",
		"ListOpsItemEvents",
		"ListOpsItemRelatedItems",
		"ListOpsMetadata",
		"ListResourceComplianceSummaries",
		"ModifyDocumentPermission",
		"PutComplianceItems",
		"PutInventory",
		"PutResourcePolicy",
		"RegisterPatchBaselineForPatchGroup",
		"ResumeSession",
		"SendAutomationSignal",
		"StartAccessRequest",
		"StartAssociationsOnce",
		"StartChangeRequestExecution",
		"StartExecutionPreview",
		"StartSession",
		"TerminateSession",
		"UpdateAssociationStatus",
		"UpdateCloudConnector",
		"UpdateDocumentMetadata",
		"UpdateMaintenanceWindowTarget",
		"UpdateMaintenanceWindowTask",
		"UpdateManagedInstanceRole",
		"UpdateOpsMetadata",
		"UpdateResourceDataSync",
		"ValidateCloudConnector",
	}
}

func (p *Pack) extra(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	op := req.Operation
	kind, idKey, listKey, wrap := extraShape(op)
	id := first(req.Input, idKey, "Name", "OpsItemId", "SessionId", "ActivationId", "ResourceArn", "InstanceId", "BaselineId", "WindowId", "AssociationId")
	switch {
	case isExtraWrite(op):
		if id == "" {
			id = p.deps.Rand.Hex(8)
		}
		rec := map[string]any{}
		for k, v := range req.Input {
			rec[k] = v
		}
		if _, ok := rec[idKey]; !ok && idKey != "" {
			rec[idKey] = id
		}
		if op == "CreateActivation" {
			rec["ActivationId"] = id
			rec["ActivationCode"] = p.deps.Rand.Derive("act:" + id).Hex(8)
		}
		if op == "StartSession" {
			rec["SessionId"] = id
			rec["TokenValue"] = p.deps.Rand.Derive("sess:" + id).Hex(16)
			rec["StreamUrl"] = "wss://ssmmessages." + req.Identity.Region + ".amazonaws.com/v1/data-channel/" + id
		}
		p.put(ctx, req, kind+":"+id, rec)
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
	case strings.HasPrefix(op, "Delete") || strings.HasPrefix(op, "Cancel") || strings.HasPrefix(op, "Terminate") ||
		strings.HasPrefix(op, "Deregister") || strings.HasPrefix(op, "Disassociate") || strings.HasPrefix(op, "Stop"):
		if id != "" {
			_ = p.col(req).Delete(ctx, kind+":"+id)
		}
		return &spi.Response{Output: map[string]any{}}, nil
	case strings.HasPrefix(op, "Describe") || strings.HasPrefix(op, "List"):
		if id != "" {
			if rec, ok := p.get(ctx, req, kind+":"+id); ok {
				return &spi.Response{Output: map[string]any{listKey: []any{rec}}}, nil
			}
		}
		return p.listPref(ctx, req, kind+":", listKey)
	default: // Get*, Validate*, Modify*, Resume*, GetCalendarState, GetAccessToken, GetConnectionStatus
		if rec, ok := p.get(ctx, req, kind+":"+id); ok {
			if wrap != "" {
				return &spi.Response{Output: map[string]any{wrap: rec}}, nil
			}
			return &spi.Response{Output: rec}, nil
		}
		out := map[string]any{}
		if wrap != "" {
			out[wrap] = map[string]any{}
		}
		if op == "GetCalendarState" {
			out["State"] = "OPEN"
		}
		if op == "GetConnectionStatus" {
			out["Status"] = "notconnected"
			out["Target"] = first(req.Input, "Target")
		}
		if op == "GetAccessToken" {
			out["AccessToken"] = p.deps.Rand.Derive("tok").Hex(16)
		}
		if op == "GetInventorySchema" {
			out["Schemas"] = []any{}
		}
		if op == "GetInventory" {
			out["Entities"] = []any{}
		}
		if op == "ValidateCloudConnector" {
			out["Status"] = "SUCCESS"
		}
		return &spi.Response{Output: out}, nil
	}
}

func isExtraWrite(op string) bool {
	for _, p := range []string{"Create", "Put", "Register", "Start", "Send", "Update", "Associate", "Modify", "Resume"} {
		if strings.HasPrefix(op, p) {
			return true
		}
	}
	return false
}

func extraShape(op string) (kind, idKey, listKey, wrap string) {
	switch {
	case strings.Contains(op, "Activation"):
		return "lact", "ActivationId", "ActivationList", ""
	case strings.Contains(op, "CloudConnector"):
		return "lcc", "CloudConnectorId", "CloudConnectorList", "CloudConnector"
	case strings.Contains(op, "OpsMetadata"):
		return "lmeta", "OpsMetadataArn", "OpsMetadataList", "OpsMetadata"
	case strings.Contains(op, "Session") || op == "GetConnectionStatus":
		return "lsess", "SessionId", "Sessions", ""
	case strings.Contains(op, "Inventory"):
		return "linv", "InstanceId", "Entities", ""
	case strings.Contains(op, "Compliance"):
		return "lcomp", "ResourceId", "ComplianceItems", ""
	case strings.Contains(op, "PatchGroup") || strings.Contains(op, "PatchBaselineForPatchGroup") || op == "DescribePatchGroups" || op == "DescribePatchGroupState":
		return "lpg", "PatchGroup", "Mappings", ""
	case strings.Contains(op, "AvailablePatch") || strings.Contains(op, "PatchProperties") || strings.Contains(op, "EffectivePatches"):
		return "lpatch", "BaselineId", "Patches", ""
	case strings.Contains(op, "MaintenanceWindowExecution") || strings.Contains(op, "WindowExecution"):
		return "lmwx", "WindowExecutionId", "WindowExecutions", ""
	case strings.Contains(op, "MaintenanceWindow") || strings.Contains(op, "WindowTask") || strings.Contains(op, "WindowTarget"):
		return "lmw", "WindowId", "WindowIdentities", ""
	case strings.Contains(op, "Association"):
		return "lassoc", "AssociationId", "AssociationExecutions", "AssociationDescription"
	case strings.Contains(op, "Automation") || strings.Contains(op, "ExecutionPreview") || strings.Contains(op, "ChangeRequest"):
		return "lauto", "AutomationExecutionId", "AutomationExecutionMetadataList", "AutomationExecution"
	case strings.Contains(op, "OpsItem") || op == "GetOpsSummary":
		return "lops", "OpsItemId", "OpsItemSummaries", ""
	case strings.Contains(op, "ResourcePolicy"):
		return "lrpol", "PolicyId", "Policies", ""
	case strings.Contains(op, "Instance"):
		return "linst", "InstanceId", "InstanceInformationList", ""
	case strings.Contains(op, "Document"):
		return "ldoc", "Name", "DocumentIdentifierList", ""
	case strings.Contains(op, "Node"):
		return "lnode", "NodeId", "Nodes", ""
	case op == "GetAccessToken" || op == "StartAccessRequest":
		return "laccess", "AccessRequestId", "AccessRequestList", ""
	case op == "GetCalendarState":
		return "lcal", "CalendarName", "Calendars", ""
	case strings.Contains(op, "ResourceDataSync"):
		return "rdsync", "SyncName", "ResourceDataSyncItems", ""
	default:
		return "lmisc", "Name", "Items", ""
	}
}
