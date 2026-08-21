package events

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

	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/sqs"
)

func TestBootedServerEventBridgePutEvents(t *testing.T) {
	cfg := config.Default()
	cfg.Services = []string{"aws.events", "aws.sqs"}
	cfg.Seed = "ev-1"
	rt, err := rtpkg.Boot(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(rt.Handler())
	defer ts.Close()
	auth := "AWS4-HMAC-SHA256 Credential=test/20200101/us-east-1/events/aws4_request, SignedHeaders=host, Signature=00"
	call := func(op, body string) (int, map[string]any) {
		t.Helper()
		req, _ := http.NewRequest(http.MethodPost, ts.URL+"/", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/x-amz-json-1.1")
		req.Header.Set("X-Amz-Target", "AWSEvents."+op)
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
		return res.StatusCode, out
	}
	call("PutRule", `{"Name":"r","EventPattern":"{}"}`)
	call("PutTargets", `{"Rule":"r","Targets":[{"Id":"1","Arn":"arn:aws:sqs:us-east-1:000000000000:q"}]}`)
	_, listed := call("ListRules", `{}`)
	if listed["Rules"] == nil {
		t.Fatalf("list %v", listed)
	}
	call("PutEvents", `{"Entries":[{"Source":"app","DetailType":"x","Detail":"{}"}]}`)
	_, bus := call("CreateEventBus", `{"Name":"custom"}`)
	if bus["EventBusArn"] == nil {
		t.Fatalf("bus %v", bus)
	}
	_, got := call("DescribeEventBus", `{"Name":"custom"}`)
	if got["Name"] != "custom" && got["EventBusArn"] == nil {
		t.Fatalf("describe bus %v", got)
	}
	call("PutPermission", `{"EventBusName":"custom","StatementId":"s1","Action":"events:PutEvents","Principal":"*"}`)
	call("RemovePermission", `{"EventBusName":"custom","StatementId":"s1"}`)
	call("CreateArchive", `{"ArchiveName":"a1","EventSourceArn":"arn:aws:events:us-east-1:000000000000:event-bus/custom"}`)
	call("DescribeArchive", `{"ArchiveName":"a1"}`)
	call("ListArchives", `{}`)
	call("CreateConnection", `{"Name":"c1","AuthorizationType":"API_KEY"}`)
	call("DescribeConnection", `{"Name":"c1"}`)
	call("ListConnections", `{}`)
	call("CreateApiDestination", `{"Name":"d1","ConnectionArn":"arn:c","InvocationEndpoint":"https://example.test"}`)
	call("ListApiDestinations", `{}`)
	call("CreateEndpoint", `{"Name":"e1"}`)
	call("ListEndpoints", `{}`)
	call("CreatePartnerEventSource", `{"Name":"p1"}`)
	call("ActivateEventSource", `{"Name":"p1"}`)
	call("ListEventSources", `{}`)
	call("StartReplay", `{"ReplayName":"r1","EventSourceArn":"arn:a"}`)
	call("DescribeReplay", `{"ReplayName":"r1"}`)
	call("CancelReplay", `{"ReplayName":"r1"}`)
	call("EnableRule", `{"Name":"r"}`)
	call("DescribeRule", `{"Name":"r"}`)
	_, pat := call("TestEventPattern", `{"EventPattern":"{\"source\":[\"app\"]}","Event":"{\"source\":\"app\"}"}`)
	if pat["Result"] != true {
		t.Fatalf("pattern %v", pat)
	}
	call("TagResource", `{"ResourceARN":"arn:r","Tags":[{"Key":"k","Value":"v"}]}`)
	call("ListTagsForResource", `{"ResourceARN":"arn:r"}`)
	call("UntagResource", `{"ResourceARN":"arn:r"}`)
	call("DisableRule", `{"Name":"r"}`)
	call("DeleteArchive", `{"ArchiveName":"a1"}`)
	call("DeleteConnection", `{"Name":"c1"}`)
	call("DeleteApiDestination", `{"Name":"d1"}`)
	call("DeleteEndpoint", `{"Name":"e1"}`)
	call("DeleteEventBus", `{"Name":"custom"}`)
}

func TestEventsHTTPProvenOps(t *testing.T) {
	p := New(spitest.Deps(t))
	if n := len(p.Operations()); n != 57 {
		t.Fatalf("events Operations() %d want 57", n)
	}
}

func TestBootedServerEventsExtraOps(t *testing.T) {
	cfg := config.Default()
	cfg.Services = []string{"aws.events"}
	cfg.Seed = "ev-extra"
	rt, err := rtpkg.Boot(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(rt.Handler())
	defer ts.Close()
	auth := "AWS4-HMAC-SHA256 Credential=test/20200101/us-east-1/events/aws4_request, SignedHeaders=host, Signature=00"
	soft := func(op, body string) string {
		t.Helper()
		req, _ := http.NewRequest(http.MethodPost, ts.URL+"/", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/x-amz-json-1.1")
		req.Header.Set("X-Amz-Target", "AWSEvents."+op)
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
		req.Header.Set("X-Amz-Target", "AWSEvents."+op)
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
	created := hard("CreateEventBus", `{"Name":"bootbus"}`)
	if !strings.Contains(created, "bootbus") {
		t.Fatalf("create bus %s", created)
	}
	got := hard("DescribeEventBus", `{"Name":"bootbus"}`)
	if !strings.Contains(got, "bootbus") {
		t.Fatalf("describe bus %s", got)
	}
	hard("DeleteEventBus", `{"Name":"bootbus"}`)
	listed := hard("ListEventBuses", `{}`)
	if strings.Contains(listed, `"Name":"bootbus"`) {
		t.Fatalf("bus still present %s", listed)
	}
	payload := `{"Name":"bootbus","ArchiveName":"a1","ReplayName":"r1","EventBusName":"bootbus","ResourceARN":"arn:r","StatementId":"s1","TargetArn":"arn:t","EventPattern":"{}","Event":"{}","Entries":[]}`
	for _, op := range extraOps() {
		soft(op, payload)
	}
}
