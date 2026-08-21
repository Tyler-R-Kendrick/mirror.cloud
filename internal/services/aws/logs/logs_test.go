package logs

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

func TestBootedServerLogsPutGet(t *testing.T) {
	cfg := config.Default()
	cfg.Services = []string{"aws.logs"}
	cfg.Seed = "logs-1"
	rt, err := rtpkg.Boot(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(rt.Handler())
	defer ts.Close()
	auth := "AWS4-HMAC-SHA256 Credential=test/20200101/us-east-1/logs/aws4_request, SignedHeaders=host, Signature=00"
	call := func(op, body string) (int, map[string]any) {
		t.Helper()
		req, _ := http.NewRequest(http.MethodPost, ts.URL+"/", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/x-amz-json-1.1")
		req.Header.Set("X-Amz-Target", "Logs_20140328."+op)
		req.Header.Set("Authorization", auth)
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		raw, _ := io.ReadAll(res.Body)
		res.Body.Close()
		out := map[string]any{}
		_ = json.Unmarshal(raw, &out)
		if res.StatusCode >= 300 {
			t.Fatalf("%s %d %s", op, res.StatusCode, raw)
		}
		if res.Header.Get("x-mirror-fidelity") != "emulate" {
			t.Fatalf("fidelity %q", res.Header.Get("x-mirror-fidelity"))
		}
		return res.StatusCode, out
	}
	call("CreateLogGroup", `{"logGroupName":"g"}`)
	call("CreateLogStream", `{"logGroupName":"g","logStreamName":"s"}`)
	call("PutLogEvents", `{"logGroupName":"g","logStreamName":"s","logEvents":[{"timestamp":1,"message":"hello-logs"}]}`)
	_, got := call("GetLogEvents", `{"logGroupName":"g","logStreamName":"s"}`)
	ev, _ := json.Marshal(got["events"])
	if !strings.Contains(string(ev), "hello-logs") {
		t.Fatalf("events %v", got)
	}
	call("DescribeLogGroups", `{}`)
	call("DescribeLogStreams", `{"logGroupName":"g"}`)
	call("FilterLogEvents", `{"logGroupName":"g"}`)
	call("PutRetentionPolicy", `{"logGroupName":"g","retentionInDays":7}`)
	call("DeleteRetentionPolicy", `{"logGroupName":"g"}`)
	call("PutSubscriptionFilter", `{"logGroupName":"g","filterName":"f","filterPattern":"","destinationArn":"arn:aws:lambda:us-east-1:000000000000:function:x"}`)
	call("DescribeSubscriptionFilters", `{"logGroupName":"g"}`)
	call("DeleteSubscriptionFilter", `{"logGroupName":"g","filterName":"f"}`)
	call("PutMetricFilter", `{"logGroupName":"g","filterName":"mf","filterPattern":""}`)
	call("DescribeMetricFilters", `{"logGroupName":"g"}`)
	call("DeleteMetricFilter", `{"logGroupName":"g","filterName":"mf"}`)
	call("PutResourcePolicy", `{"policyName":"p","policyDocument":"{}"}`)
	call("DescribeResourcePolicies", `{}`)
	call("DeleteResourcePolicy", `{"policyName":"p"}`)
	call("TagLogGroup", `{"logGroupName":"g","tags":{"k":"v"}}`)
	call("ListTagsLogGroup", `{"logGroupName":"g"}`)
	call("UntagLogGroup", `{"logGroupName":"g","tagKeys":["k"]}`)
	call("PutDestination", `{"destinationName":"d","targetArn":"arn:aws:kinesis:us-east-1:000000000000:stream/s","roleArn":"arn:aws:iam::000000000000:role/r"}`)
	call("DescribeDestinations", `{}`)
	call("DeleteDestination", `{"destinationName":"d"}`)
	_, qd := call("PutQueryDefinition", `{"name":"q","queryString":"fields @message"}`)
	qid, _ := qd["queryDefinitionId"].(string)
	call("DescribeQueryDefinitions", `{}`)
	call("DeleteQueryDefinition", `{"queryDefinitionId":"`+qid+`"}`)
	_, sq := call("StartQuery", `{"logGroupName":"g","queryString":"fields @message","startTime":1,"endTime":2}`)
	sqid, _ := sq["queryId"].(string)
	call("GetQueryResults", `{"queryId":"`+sqid+`"}`)
	call("StopQuery", `{"queryId":"`+sqid+`"}`)
	call("AssociateKmsKey", `{"logGroupName":"g","kmsKeyId":"arn:aws:kms:us-east-1:000000000000:key/k"}`)
	call("DisassociateKmsKey", `{"logGroupName":"g"}`)
	call("DeleteLogStream", `{"logGroupName":"g","logStreamName":"s"}`)
	call("DeleteLogGroup", `{"logGroupName":"g"}`)
	_, exp := call("CreateExportTask", `{"logGroupName":"g","from":1,"to":2,"destination":"s"}`)
	if exp["taskId"] == nil {
		t.Fatalf("export %v", exp)
	}
	call("DescribeExportTasks", `{}`)
	call("CreateDelivery", `{"deliveryDestinationArn":"arn:d","deliverySourceName":"s"}`)
	call("DescribeDeliveries", `{}`)
	call("PutAccountPolicy", `{"policyName":"p","policyDocument":"{}"}`)
	call("DescribeAccountPolicies", `{}`)
	call("TagResource", `{"resourceArn":"arn:g","tags":{"k":"v"}}`)
	call("ListTagsForResource", `{"resourceArn":"arn:g"}`)
	call("UntagResource", `{"resourceArn":"arn:g"}`)
}

func TestLogsHTTPProvenOps(t *testing.T) {
	p := New(spitest.Deps(t))
	if n := len(p.Operations()); n != 118 {
		t.Fatalf("logs Operations() %d want 118", n)
	}
}

func TestBootedServerLogsExtraOps(t *testing.T) {
	cfg := config.Default()
	cfg.Services = []string{"aws.logs"}
	cfg.Seed = "logs-extra"
	rt, err := rtpkg.Boot(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(rt.Handler())
	defer ts.Close()
	auth := "AWS4-HMAC-SHA256 Credential=test/20200101/us-east-1/logs/aws4_request, SignedHeaders=host, Signature=00"
	soft := func(op, body string) string {
		t.Helper()
		req, _ := http.NewRequest(http.MethodPost, ts.URL+"/", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/x-amz-json-1.1")
		req.Header.Set("X-Amz-Target", "Logs_20140328."+op)
		req.Header.Set("Authorization", auth)
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		raw, _ := io.ReadAll(res.Body)
		res.Body.Close()
		if res.Header.Get("x-mirror-fidelity") != "emulate" {
			t.Fatalf("%s fidelity %q %s", op, res.Header.Get("x-mirror-fidelity"), raw)
		}
		if res.StatusCode >= 500 {
			t.Fatalf("%s %d %s", op, res.StatusCode, raw)
		}
		return string(raw)
	}
	hard := func(op, body string) string {
		t.Helper()
		req, _ := http.NewRequest(http.MethodPost, ts.URL+"/", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/x-amz-json-1.1")
		req.Header.Set("X-Amz-Target", "Logs_20140328."+op)
		req.Header.Set("Authorization", auth)
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		raw, _ := io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode >= 300 || res.Header.Get("x-mirror-fidelity") != "emulate" {
			t.Fatalf("%s %d %s %s", op, res.StatusCode, res.Header.Get("x-mirror-fidelity"), raw)
		}
		return string(raw)
	}
	created := hard("CreateDelivery", `{"deliveryId":"dboot","deliveryDestinationArn":"arn:d","deliverySourceName":"s"}`)
	if !strings.Contains(created, "dboot") {
		t.Fatalf("create delivery %s", created)
	}
	got := hard("GetDelivery", `{"deliveryId":"dboot"}`)
	if !strings.Contains(got, "dboot") {
		t.Fatalf("get delivery %s", got)
	}
	hard("DeleteDelivery", `{"deliveryId":"dboot"}`)
	gone := hard("DescribeDeliveries", `{"deliveryId":"dboot"}`)
	if strings.Contains(gone, `"dboot"`) {
		t.Fatalf("delivery still present %s", gone)
	}
	payload := `{"deliveryId":"dboot","logGroupName":"g","LogGroupName":"g","name":"n","Name":"n","taskId":"t1","importId":"i1","anomalyDetectorArn":"a1","lookupTableName":"lut","scheduledQueryArn":"sq","integrationName":"int","logGroupIdentifier":"g","syslogConfigurationName":"sys","policyName":"p","resourceArn":"arn:g"}`
	for _, op := range extraOps() {
		soft(op, payload)
	}
}
