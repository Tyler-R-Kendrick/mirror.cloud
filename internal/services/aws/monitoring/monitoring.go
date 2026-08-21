// Package monitoring is CloudWatch metrics-lite (in-memory datapoints, not AWS-compatible aggregation).
package monitoring

import (
	"context"
	"encoding/json"
	"strconv"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func init() {
	registry.Register(registry.Factory{ServiceID: "aws.monitoring", Tier: model.TierEmulate, New: func(d spi.Deps) (spi.BehaviorPack, error) {
		return New(d), nil
	}})
}

// Pack implements CloudWatch metrics.
type Pack struct{ deps spi.Deps }

// New constructs the pack.
func New(d spi.Deps) *Pack { return &Pack{deps: d} }

func (p *Pack) ServiceID() string { return "aws.monitoring" }
func (p *Pack) Tier() model.Tier  { return model.TierEmulate }
func (p *Pack) Operations() []string {
	core := []string{"PutMetricData", "ListMetrics", "GetMetricStatistics", "PutMetricAlarm", "DescribeAlarms", "DeleteAlarms",
		"AssociateDatasetKmsKey", "DeleteAlarmMuteRule", "DeleteAnomalyDetector", "DeleteDashboards",
		"DeleteInsightRules", "DeleteMetricStream", "DescribeAlarmContributors", "DescribeAlarmHistory",
		"DescribeAlarmsForMetric", "DescribeAnomalyDetectors", "DescribeInsightRules", "DisableAlarmActions",
		"DisableInsightRules", "DisassociateDatasetKmsKey", "EnableAlarmActions", "EnableInsightRules",
		"GetAlarmMuteRule", "GetDashboard", "GetDataset", "GetInsightRuleReport",
		"GetMetricData", "GetMetricStream", "GetMetricWidgetImage", "GetOTelEnrichment",
		"ListAlarmMuteRules", "ListDashboards", "ListManagedInsightRules", "ListMetricStreams",
		"ListTagsForResource", "PutAlarmMuteRule", "PutAnomalyDetector", "PutCompositeAlarm",
		"PutDashboard", "PutInsightRule", "PutLogAlarm", "PutManagedInsightRules",
		"PutMetricStream", "SetAlarmState", "StartMetricStreams", "StartOTelEnrichment",
		"StopMetricStreams", "StopOTelEnrichment", "TagResource", "UntagResource"}
	return core
}

func (p *Pack) col(req *spi.Request, n string) spi.Collection {
	return p.deps.Store.Scope(req.Identity.Account, req.Identity.Region).Collection(n)
}

func (p *Pack) Invoke(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	switch req.Operation {
	case "PutMetricData":
		ns := first(req.Input, "Namespace")
		metrics := asSlice(req.Input["MetricData"])
		for i, m := range metrics {
			mm, _ := m.(map[string]any)
			name := str(mm["MetricName"])
			key := ns + "/" + name + "/" + strconv.Itoa(p.next(ctx, req, ns+"/"+name))
			b, _ := json.Marshal(mm)
			_ = p.col(req, "metrics").Put(ctx, key, b)
			_ = i
		}
		return &spi.Response{Output: map[string]any{}}, nil
	case "ListMetrics":
		ns := first(req.Input, "Namespace")
		kvs, _, _ := p.col(req, "metrics").List(ctx, ns, "", 0)
		seen := map[string]bool{}
		var out []any
		for _, kv := range kvs {
			var m map[string]any
			_ = json.Unmarshal(kv.Value, &m)
			name := str(m["MetricName"])
			id := ns + "|" + name
			if seen[id] {
				continue
			}
			seen[id] = true
			out = append(out, map[string]any{"Namespace": ns, "MetricName": name})
		}
		if ns == "" {
			kvs, _, _ = p.col(req, "metrics").List(ctx, "", "", 0)
			out = nil
			seen = map[string]bool{}
			for _, kv := range kvs {
				var m map[string]any
				_ = json.Unmarshal(kv.Value, &m)
				name := str(m["MetricName"])
				pref := kv.Key
				nspace := pref
				if i := indexSlash(pref); i >= 0 {
					nspace = pref[:i]
				}
				id := nspace + "|" + name
				if seen[id] {
					continue
				}
				seen[id] = true
				out = append(out, map[string]any{"Namespace": nspace, "MetricName": name})
			}
		}
		return &spi.Response{Output: map[string]any{"Metrics": out}}, nil
	case "GetMetricStatistics":
		ns, name := first(req.Input, "Namespace"), first(req.Input, "MetricName")
		kvs, _, _ := p.col(req, "metrics").List(ctx, ns+"/"+name+"/", "", 0)
		var pts []any
		var sum, mn, mx, n float64
		firstv := true
		for _, kv := range kvs {
			var m map[string]any
			_ = json.Unmarshal(kv.Value, &m)
			v := asFloat(m["Value"])
			if vs := asSlice(m["Values"]); len(vs) > 0 {
				v = asFloat(vs[0])
			}
			sum += v
			n++
			if firstv || v < mn {
				mn = v
			}
			if firstv || v > mx {
				mx = v
			}
			firstv = false
		}
		if n > 0 {
			pts = append(pts, map[string]any{"SampleCount": n, "Sum": sum, "Minimum": mn, "Maximum": mx, "Average": sum / n, "Timestamp": "2020-01-01T00:00:00Z"})
		}
		return &spi.Response{Output: map[string]any{"Label": name, "Datapoints": pts}}, nil
	case "PutMetricAlarm":
		name := first(req.Input, "AlarmName")
		b, _ := json.Marshal(req.Input)
		_ = p.col(req, "alarms").Put(ctx, name, b)
		return &spi.Response{Output: map[string]any{}}, nil
	case "DescribeAlarms":
		kvs, _, _ := p.col(req, "alarms").List(ctx, "", "", 0)
		var as []any
		for _, kv := range kvs {
			var m map[string]any
			_ = json.Unmarshal(kv.Value, &m)
			as = append(as, m)
		}
		return &spi.Response{Output: map[string]any{"MetricAlarms": as}}, nil
	case "DeleteAlarms":
		for _, n := range asSlice(req.Input["AlarmNames"]) {
			_ = p.col(req, "alarms").Delete(ctx, str(n))
		}
		return &spi.Response{Output: map[string]any{}}, nil
	default:
		return p.extra(ctx, req)
	}
}

func (p *Pack) next(ctx context.Context, req *spi.Request, key string) int {
	b, ok, _ := p.col(req, "metricseq").Get(ctx, key)
	n := 0
	if ok {
		n, _ = strconv.Atoi(string(b))
	}
	_ = p.col(req, "metricseq").Put(ctx, key, []byte(strconv.Itoa(n+1)))
	return n
}

func first(in map[string]any, keys ...string) string {
	for _, k := range keys {
		if s := str(in[k]); s != "" {
			return s
		}
	}
	return ""
}

func str(v any) string {
	s, _ := v.(string)
	return s
}

func asSlice(v any) []any {
	a, _ := v.([]any)
	return a
}

func asFloat(v any) float64 {
	switch t := v.(type) {
	case float64:
		return t
	case int:
		return float64(t)
	case json.Number:
		f, _ := t.Float64()
		return f
	case string:
		f, _ := strconv.ParseFloat(t, 64)
		return f
	}
	return 0
}

func indexSlash(s string) int {
	for i, c := range s {
		if c == '/' {
			return i
		}
	}
	return -1
}
