package logs

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

// extraOps are remaining Smithy Logs ops: deliveries, export/import, anomaly detectors, lookup tables.
// ponytail: no Insights engine, live tail, or S3 table integrations; upgrade is per-op AWS shapes.
func extraOps() []string {
	return []string{
		"AssociateSourceToS3TableIntegration", "CancelExportTask", "CancelImportTask",
		"CreateDelivery", "CreateExportTask", "CreateImportTask", "CreateLogAnomalyDetector",
		"CreateLookupTable", "CreateScheduledQuery",
		"DeleteAccountPolicy", "DeleteDataProtectionPolicy", "DeleteDelivery", "DeleteDeliveryDestination",
		"DeleteDeliveryDestinationPolicy", "DeleteDeliverySource", "DeleteIndexPolicy", "DeleteIntegration",
		"DeleteLogAnomalyDetector", "DeleteLookupTable", "DeleteScheduledQuery", "DeleteSyslogConfiguration",
		"DeleteTransformer",
		"DescribeAccountPolicies", "DescribeConfigurationTemplates", "DescribeDeliveries",
		"DescribeDeliveryDestinations", "DescribeDeliverySources", "DescribeExportTasks", "DescribeFieldIndexes",
		"DescribeImportTaskBatches", "DescribeImportTasks", "DescribeIndexPolicies", "DescribeLookupTables",
		"DescribeQueries",
		"DisassociateSourceFromS3TableIntegration",
		"GetDataProtectionPolicy", "GetDelivery", "GetDeliveryDestination", "GetDeliveryDestinationPolicy",
		"GetDeliverySource", "GetIntegration", "GetLogAnomalyDetector", "GetLogFields", "GetLogGroupFields",
		"GetLogObject", "GetLogRecord", "GetLookupTable", "GetScheduledQuery", "GetScheduledQueryHistory",
		"GetStorageTierPolicy", "GetTransformer",
		"ListAggregateLogGroupSummaries", "ListAnomalies", "ListIntegrations", "ListLogAnomalyDetectors",
		"ListLogGroups", "ListLogGroupsForQuery", "ListScheduledQueries", "ListSourcesForS3TableIntegration",
		"ListSyslogConfigurations", "ListTagsForResource",
		"PutAccountPolicy", "PutBearerTokenAuthentication", "PutDataProtectionPolicy", "PutDeliveryDestination",
		"PutDeliveryDestinationPolicy", "PutDeliverySource", "PutDestinationPolicy", "PutIndexPolicy",
		"PutIntegration", "PutLogGroupDeletionProtection", "PutStorageTierPolicy", "PutSyslogConfiguration",
		"PutTransformer",
		"StartLiveTail", "TagResource", "TestMetricFilter", "TestTransformer", "UntagResource",
		"UpdateAnomaly", "UpdateDeliveryConfiguration", "UpdateLogAnomalyDetector", "UpdateLookupTable",
		"UpdateScheduledQuery",
	}
}

func (p *Pack) extra(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	op := req.Operation
	kind, idKey, listKey, wrap := extraShape(op)
	id := first(req.Input, idKey, "logGroupName", "LogGroupName", "name", "Name", "taskId", "deliveryId")
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
		if op == "CreateExportTask" || op == "CreateImportTask" {
			rec["taskId"] = id
			rec["status"] = "COMPLETED"
		}
		if op == "StartLiveTail" {
			rec["sessionId"] = id
		}
		if op == "TestMetricFilter" || op == "TestTransformer" {
			rec["matches"] = []any{}
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
	case strings.HasPrefix(op, "Delete") || strings.HasPrefix(op, "Cancel") || strings.HasPrefix(op, "Untag") ||
		strings.HasPrefix(op, "Disassociate"):
		if id != "" {
			_ = p.col(req, "logsl").Delete(ctx, kind+":"+id)
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
		return &spi.Response{Output: out}, nil
	}
}

func (p *Pack) lput(ctx context.Context, req *spi.Request, key string, rec any) {
	b, _ := json.Marshal(rec)
	_ = p.col(req, "logsl").Put(ctx, key, b)
}

func (p *Pack) lget(ctx context.Context, req *spi.Request, key string) (map[string]any, bool) {
	b, ok, _ := p.col(req, "logsl").Get(ctx, key)
	if !ok {
		return nil, false
	}
	var rec map[string]any
	_ = json.Unmarshal(b, &rec)
	return rec, true
}

func (p *Pack) llist(ctx context.Context, req *spi.Request, pfx, outKey string) (*spi.Response, error) {
	kvs, _, _ := p.col(req, "logsl").List(ctx, pfx, "", 0)
	items := make([]any, 0, len(kvs))
	for _, kv := range kvs {
		var rec map[string]any
		_ = json.Unmarshal(kv.Value, &rec)
		items = append(items, rec)
	}
	return &spi.Response{Output: map[string]any{outKey: items}}, nil
}

func isExtraWrite(op string) bool {
	for _, p := range []string{"Create", "Put", "Update", "Tag", "Associate", "Start", "Test"} {
		if strings.HasPrefix(op, p) && !strings.HasPrefix(op, "Untag") {
			return true
		}
	}
	return false
}

func extraShape(op string) (kind, idKey, listKey, wrap string) {
	switch {
	case strings.Contains(op, "Delivery"):
		return "ldel", "deliveryId", "deliveries", "delivery"
	case strings.Contains(op, "Export"):
		return "lexp", "taskId", "exportTasks", "exportTask"
	case strings.Contains(op, "Import"):
		return "limp", "importId", "importTasks", "importTask"
	case strings.Contains(op, "Anomaly"):
		return "lanom", "anomalyDetectorArn", "anomalyDetectors", "anomalyDetector"
	case strings.Contains(op, "LookupTable"):
		return "llut", "lookupTableName", "lookupTables", "lookupTable"
	case strings.Contains(op, "ScheduledQuery"):
		return "lsq", "scheduledQueryArn", "scheduledQueries", "scheduledQuery"
	case strings.Contains(op, "Integration"):
		return "lint", "integrationName", "integrations", "integration"
	case strings.Contains(op, "Transformer"):
		return "ltx", "logGroupIdentifier", "transformers", "transformer"
	case strings.Contains(op, "Syslog"):
		return "lsys", "syslogConfigurationName", "syslogConfigurations", ""
	case strings.Contains(op, "Index"):
		return "lidx", "logGroupIdentifier", "indexPolicies", ""
	case strings.Contains(op, "DataProtection"):
		return "ldp", "logGroupIdentifier", "dataProtectionPolicy", ""
	case strings.Contains(op, "AccountPolicy"):
		return "lacct", "policyName", "accountPolicies", ""
	case strings.Contains(op, "StorageTier"):
		return "ltier", "logGroupIdentifier", "storageTierPolicy", ""
	default:
		return "lmisc", "name", "logGroups", ""
	}
}
