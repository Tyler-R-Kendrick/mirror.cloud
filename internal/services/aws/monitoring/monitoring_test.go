package monitoring

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/config"
	rtpkg "github.com/tyler-r-kendrick/mirror.cloud/internal/runtime"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spitest"
)

func TestBootedServerCloudWatchPutGet(t *testing.T) {
	cfg := config.Default()
	cfg.Services = []string{"aws.monitoring"}
	cfg.Seed = "cw-1"
	rt, err := rtpkg.Boot(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(rt.Handler())
	defer ts.Close()
	auth := "AWS4-HMAC-SHA256 Credential=test/20200101/us-east-1/monitoring/aws4_request, SignedHeaders=host, Signature=00"
	call := func(op, body string) map[string]any {
		t.Helper()
		req, _ := http.NewRequest(http.MethodPost, ts.URL+"/", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/x-amz-json-1.0")
		req.Header.Set("X-Amz-Target", "GraniteServiceVersion20100801."+op)
		req.Header.Set("Authorization", auth)
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		raw, _ := io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode >= 300 {
			t.Fatalf("%s %d %s", op, res.StatusCode, raw)
		}
		out := map[string]any{}
		_ = json.Unmarshal(raw, &out)
		return out
	}
	call("PutMetricData", `{"Namespace":"N","MetricData":[{"MetricName":"Latency","Value":7}]}`)
	listed := call("ListMetrics", `{"Namespace":"N"}`)
	if listed["Metrics"] == nil {
		t.Fatalf("list %v", listed)
	}
	got := call("GetMetricStatistics", `{"Namespace":"N","MetricName":"Latency"}`)
	pts, _ := got["Datapoints"].([]any)
	if len(pts) < 1 {
		t.Fatalf("stats %v", got)
	}
	call("PutMetricAlarm", `{"AlarmName":"a","MetricName":"Latency","Namespace":"N","Threshold":1,"ComparisonOperator":"GreaterThanThreshold"}`)
	al := call("DescribeAlarms", `{}`)
	if al["MetricAlarms"] == nil {
		t.Fatalf("alarms %v", al)
	}
	call("PutDashboard", `{"DashboardName":"d1","DashboardBody":"{\"widgets\":[]}"}`)
	dash := call("GetDashboard", `{"DashboardName":"d1"}`)
	if dash["DashboardName"] != "d1" && dash["DashboardBody"] == nil {
		t.Fatalf("dash %v", dash)
	}
	dashes := call("ListDashboards", `{}`)
	if dashes["DashboardEntries"] == nil {
		t.Fatalf("list dash %v", dashes)
	}
	call("DeleteDashboards", `{"DashboardNames":["d1"]}`)
	call("PutAnomalyDetector", `{"Namespace":"N","MetricName":"Latency"}`)
	call("DescribeAnomalyDetectors", `{}`)
	call("DeleteAnomalyDetector", `{"Namespace":"N","MetricName":"Latency"}`)
	call("PutInsightRule", `{"RuleName":"r1","RuleDefinition":"{}"}`)
	call("DescribeInsightRules", `{}`)
	call("EnableInsightRules", `{"RuleNames":["r1"]}`)
	call("GetInsightRuleReport", `{"RuleName":"r1"}`)
	call("DisableInsightRules", `{"RuleNames":["r1"]}`)
	call("DeleteInsightRules", `{"RuleNames":["r1"]}`)
	call("PutManagedInsightRules", `{"ManagedRules":[{"TemplateName":"t"}]}`)
	call("ListManagedInsightRules", `{}`)
	call("PutMetricStream", `{"Name":"s1"}`)
	call("GetMetricStream", `{"Name":"s1"}`)
	call("ListMetricStreams", `{}`)
	call("StartMetricStreams", `{"Names":["s1"]}`)
	call("StopMetricStreams", `{"Names":["s1"]}`)
	call("DeleteMetricStream", `{"Name":"s1"}`)
	call("PutAlarmMuteRule", `{"AlarmName":"a"}`)
	call("GetAlarmMuteRule", `{"AlarmName":"a"}`)
	call("ListAlarmMuteRules", `{}`)
	call("DeleteAlarmMuteRule", `{"AlarmName":"a"}`)
	call("PutCompositeAlarm", `{"AlarmName":"c1","AlarmRule":"ALARM(a)"}`)
	call("PutLogAlarm", `{"AlarmName":"l1"}`)
	call("SetAlarmState", `{"AlarmName":"a","StateValue":"ALARM","StateReason":"test"}`)
	call("EnableAlarmActions", `{"AlarmNames":["a"]}`)
	call("DisableAlarmActions", `{"AlarmNames":["a"]}`)
	call("DescribeAlarmHistory", `{}`)
	call("DescribeAlarmsForMetric", `{"MetricName":"Latency"}`)
	call("DescribeAlarmContributors", `{"AlarmName":"a"}`)
	call("GetMetricData", `{"Namespace":"N","MetricName":"Latency"}`)
	call("GetMetricWidgetImage", `{"MetricWidget":"{}"}`)
	call("AssociateDatasetKmsKey", `{"Name":"ds","KmsKey":"arn:k"}`)
	call("GetDataset", `{"Name":"ds"}`)
	call("DisassociateDatasetKmsKey", `{"Name":"ds"}`)
	call("StartOTelEnrichment", `{"Name":"o1"}`)
	call("GetOTelEnrichment", `{"Name":"o1"}`)
	call("StopOTelEnrichment", `{"Name":"o1"}`)
	call("TagResource", `{"ResourceARN":"arn:a","Tags":[{"Key":"k","Value":"v"}]}`)
	call("ListTagsForResource", `{"ResourceARN":"arn:a"}`)
	call("UntagResource", `{"ResourceARN":"arn:a"}`)
}

func TestMonitoringHTTPProvenOps(t *testing.T) {
	p := New(spitest.Deps(t))
	if n := len(p.Operations()); n != 50 {
		t.Fatalf("monitoring Operations() %d want 50", n)
	}
}
