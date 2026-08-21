package monitoring

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func (p *Pack) extra(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	op := req.Operation
	switch {
	case op == "PutDashboard":
		name := first(req.Input, "DashboardName")
		b, _ := json.Marshal(req.Input)
		_ = p.col(req, "cwdash").Put(ctx, name, b)
		return &spi.Response{Output: map[string]any{"DashboardValidationMessages": []any{}}}, nil
	case op == "GetDashboard":
		return p.getWrap(ctx, req, "cwdash", first(req.Input, "DashboardName"), "")
	case op == "ListDashboards":
		return p.listWrap(ctx, req, "cwdash", "DashboardEntries")
	case op == "DeleteDashboards":
		for _, n := range asSlice(req.Input["DashboardNames"]) {
			_ = p.col(req, "cwdash").Delete(ctx, str(n))
		}
		return &spi.Response{Output: map[string]any{}}, nil
	case op == "PutAnomalyDetector":
		return p.putID(ctx, req, "cwanom", first(req.Input, "MetricName", "Namespace"))
	case op == "DescribeAnomalyDetectors":
		return p.listWrap(ctx, req, "cwanom", "AnomalyDetectors")
	case op == "DeleteAnomalyDetector":
		_ = p.col(req, "cwanom").Delete(ctx, first(req.Input, "MetricName", "Namespace"))
		return &spi.Response{Output: map[string]any{}}, nil
	case op == "PutInsightRule" || op == "PutManagedInsightRules":
		return p.putID(ctx, req, "cwins", first(req.Input, "RuleName", "Name"))
	case op == "DescribeInsightRules" || op == "ListManagedInsightRules":
		return p.listWrap(ctx, req, "cwins", "InsightRules")
	case op == "DeleteInsightRules":
		for _, n := range asSlice(req.Input["RuleNames"]) {
			_ = p.col(req, "cwins").Delete(ctx, str(n))
		}
		return &spi.Response{Output: map[string]any{}}, nil
	case op == "EnableInsightRules" || op == "DisableInsightRules":
		return p.flagRules(ctx, req, "cwins", "RuleNames", op[:6] == "Enable")
	case op == "GetInsightRuleReport":
		return &spi.Response{Output: map[string]any{"KeyLabels": []any{}, "AggregationStatistic": "Sum", "AggregateValue": 0}}, nil
	case op == "PutMetricStream":
		return p.putID(ctx, req, "cwstream", first(req.Input, "Name"))
	case op == "GetMetricStream":
		return p.getWrap(ctx, req, "cwstream", first(req.Input, "Name"), "")
	case op == "ListMetricStreams":
		return p.listWrap(ctx, req, "cwstream", "Entries")
	case op == "DeleteMetricStream":
		_ = p.col(req, "cwstream").Delete(ctx, first(req.Input, "Name"))
		return &spi.Response{Output: map[string]any{}}, nil
	case op == "StartMetricStreams" || op == "StopMetricStreams":
		return p.flagRules(ctx, req, "cwstream", "Names", strings.HasPrefix(op, "Start"))
	case op == "PutAlarmMuteRule":
		return p.putID(ctx, req, "cwmute", first(req.Input, "AlarmName", "Name"))
	case op == "GetAlarmMuteRule":
		return p.getWrap(ctx, req, "cwmute", first(req.Input, "AlarmName", "Name"), "")
	case op == "ListAlarmMuteRules":
		return p.listWrap(ctx, req, "cwmute", "MuteRules")
	case op == "DeleteAlarmMuteRule":
		_ = p.col(req, "cwmute").Delete(ctx, first(req.Input, "AlarmName", "Name"))
		return &spi.Response{Output: map[string]any{}}, nil
	case op == "PutCompositeAlarm" || op == "PutLogAlarm":
		name := first(req.Input, "AlarmName")
		b, _ := json.Marshal(req.Input)
		_ = p.col(req, "alarms").Put(ctx, name, b)
		return &spi.Response{Output: map[string]any{}}, nil
	case op == "SetAlarmState":
		name := first(req.Input, "AlarmName")
		b, ok, _ := p.col(req, "alarms").Get(ctx, name)
		rec := map[string]any{"AlarmName": name}
		if ok {
			_ = json.Unmarshal(b, &rec)
		}
		rec["StateValue"] = req.Input["StateValue"]
		rec["StateReason"] = req.Input["StateReason"]
		nb, _ := json.Marshal(rec)
		_ = p.col(req, "alarms").Put(ctx, name, nb)
		hb, _ := json.Marshal(map[string]any{"AlarmName": name, "HistoryItemType": "StateUpdate", "HistorySummary": req.Input["StateValue"]})
		_ = p.col(req, "cwhist").Put(ctx, name+"/"+p.deps.Rand.Hex(4), hb)
		return &spi.Response{Output: map[string]any{}}, nil
	case op == "EnableAlarmActions" || op == "DisableAlarmActions":
		return p.flagRules(ctx, req, "alarms", "AlarmNames", strings.HasPrefix(op, "Enable"))
	case op == "DescribeAlarmHistory":
		return p.listWrap(ctx, req, "cwhist", "AlarmHistoryItems")
	case op == "DescribeAlarmsForMetric":
		name := first(req.Input, "MetricName")
		kvs, _, _ := p.col(req, "alarms").List(ctx, "", "", 0)
		var out []any
		for _, kv := range kvs {
			var m map[string]any
			_ = json.Unmarshal(kv.Value, &m)
			if name == "" || str(m["MetricName"]) == name {
				out = append(out, m)
			}
		}
		return &spi.Response{Output: map[string]any{"MetricAlarms": out}}, nil
	case op == "DescribeAlarmContributors":
		return &spi.Response{Output: map[string]any{"AlarmContributors": []any{}}}, nil
	case op == "GetMetricData":
		resp, err := p.Invoke(ctx, &spi.Request{Identity: req.Identity, HTTP: req.HTTP, Operation: "GetMetricStatistics", Input: req.Input})
		if err != nil {
			return nil, err
		}
		return &spi.Response{Output: map[string]any{"MetricDataResults": []any{map[string]any{"Id": "m1", "Label": first(req.Input, "MetricName"), "Values": []any{resp.Output["Datapoints"]}}}}}, nil
	case op == "GetMetricWidgetImage":
		return &spi.Response{Output: map[string]any{"MetricWidgetImage": "iVBORw0KGgo="}}, nil
	case op == "GetDataset" || op == "AssociateDatasetKmsKey" || op == "DisassociateDatasetKmsKey":
		if strings.HasPrefix(op, "Associate") || strings.HasPrefix(op, "Disassociate") {
			return p.putID(ctx, req, "cwds", first(req.Input, "Name", "DatasetName"))
		}
		return p.getWrap(ctx, req, "cwds", first(req.Input, "Name", "DatasetName"), "")
	case op == "StartOTelEnrichment" || op == "StopOTelEnrichment" || op == "GetOTelEnrichment":
		if op != "GetOTelEnrichment" {
			return p.putID(ctx, req, "cwotel", first(req.Input, "Name"))
		}
		return p.getWrap(ctx, req, "cwotel", first(req.Input, "Name"), "")
	case op == "TagResource":
		b, _ := json.Marshal(req.Input["Tags"])
		_ = p.col(req, "cwtags").Put(ctx, first(req.Input, "ResourceARN", "ResourceArn"), b)
		return &spi.Response{Output: map[string]any{}}, nil
	case op == "UntagResource":
		_ = p.col(req, "cwtags").Delete(ctx, first(req.Input, "ResourceARN", "ResourceArn"))
		return &spi.Response{Output: map[string]any{}}, nil
	case op == "ListTagsForResource":
		b, ok, _ := p.col(req, "cwtags").Get(ctx, first(req.Input, "ResourceARN", "ResourceArn"))
		var tags any = []any{}
		if ok {
			_ = json.Unmarshal(b, &tags)
		}
		return &spi.Response{Output: map[string]any{"Tags": tags}}, nil
	default:
		return nil, spi.NotImplemented("aws.monitoring", op, "emulate")
	}
}

func (p *Pack) putID(ctx context.Context, req *spi.Request, col, id string) (*spi.Response, error) {
	if id == "" {
		id = p.deps.Rand.Hex(8)
	}
	b, _ := json.Marshal(req.Input)
	_ = p.col(req, col).Put(ctx, id, b)
	return &spi.Response{Output: map[string]any{"Name": id}}, nil
}

func (p *Pack) getWrap(ctx context.Context, req *spi.Request, col, id, wrap string) (*spi.Response, error) {
	b, ok, _ := p.col(req, col).Get(ctx, id)
	if !ok {
		return &spi.Response{Output: map[string]any{}}, nil
	}
	var rec map[string]any
	_ = json.Unmarshal(b, &rec)
	if wrap == "" {
		return &spi.Response{Output: rec}, nil
	}
	return &spi.Response{Output: map[string]any{wrap: rec}}, nil
}

func (p *Pack) listWrap(ctx context.Context, req *spi.Request, col, key string) (*spi.Response, error) {
	kvs, _, _ := p.col(req, col).List(ctx, "", "", 0)
	var out []any
	for _, kv := range kvs {
		var rec map[string]any
		_ = json.Unmarshal(kv.Value, &rec)
		out = append(out, rec)
	}
	return &spi.Response{Output: map[string]any{key: out}}, nil
}

func (p *Pack) flagRules(ctx context.Context, req *spi.Request, col, namesKey string, on bool) (*spi.Response, error) {
	names := asSlice(req.Input[namesKey])
	if len(names) == 0 {
		if n := first(req.Input, "Name", "AlarmName"); n != "" {
			names = []any{n}
		}
	}
	for _, n := range names {
		id := str(n)
		b, ok, _ := p.col(req, col).Get(ctx, id)
		rec := map[string]any{}
		if ok {
			_ = json.Unmarshal(b, &rec)
		}
		rec["Enabled"] = on
		nb, _ := json.Marshal(rec)
		_ = p.col(req, col).Put(ctx, id, nb)
	}
	return &spi.Response{Output: map[string]any{}}, nil
}
