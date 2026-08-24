package kafka

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/config"
	rtpkg "github.com/tyler-r-kendrick/mirror.cloud/internal/runtime"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spitest"
)

func TestKafkaHTTPProvenOps(t *testing.T) {
	p := New(spitest.Deps(t))
	if n := len(p.Operations()); n != 7 {
		t.Fatalf("kafka Operations() %d want 7", n)
	}
}

func TestKafkaPublishesMessages(t *testing.T) {
	deps := spitest.Deps(t)
	p := New(deps)
	ctx := context.Background()
	identity := spi.Identity{Account: "123456789012", Region: "us-east-1"}
	created, err := p.Invoke(ctx, &spi.Request{Identity: identity, Operation: "CreateCluster", Input: map[string]any{"ClusterName": "source"}})
	if err != nil {
		t.Fatal(err)
	}
	arn := created.Output["ClusterArn"].(string)
	var event map[string]any
	cancel := deps.Bus.Subscribe("kafka", func(_ context.Context, payload []byte) { _ = json.Unmarshal(payload, &event) })
	defer cancel()
	if err := p.Publish(ctx, identity, arn, "events", []byte("one")); err != nil {
		t.Fatal(err)
	}
	if err := deps.Clock.Advance(time.Second); err != nil {
		t.Fatal(err)
	}
	from := deps.Clock.Now()
	if err := p.Publish(ctx, identity, arn, "events", []byte("two")); err != nil {
		t.Fatal(err)
	}
	messages, err := p.Messages(ctx, identity, arn, "events", from)
	message, _ := event["Message"].(map[string]any)
	if err != nil || len(messages) != 1 || string(messages[0].Data) != "two" || event["ClusterARN"] != arn || event["Topic"] != "events" || message["Data"] != "dHdv" {
		t.Fatalf("MSK messages %#v, event %#v, %v", messages, event, err)
	}
	if _, err := p.Invoke(ctx, &spi.Request{Identity: identity, Operation: "DeleteCluster", Input: map[string]any{"ClusterArn": arn}}); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Messages(ctx, identity, arn, "events", time.Time{}); err == nil {
		t.Fatal("deleted MSK cluster retained messages")
	}
}

func TestBootedServerMSKCluster(t *testing.T) {
	cfg := config.Default()
	cfg.Services = []string{"aws.kafka"}
	cfg.Seed = "msk-1"
	rt, err := rtpkg.Boot(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(rt.Handler())
	defer ts.Close()
	auth := "AWS4-HMAC-SHA256 Credential=test/20200101/us-east-1/kafka/aws4_request, SignedHeaders=host, Signature=00"
	call := func(op, body string) map[string]any {
		t.Helper()
		req, _ := http.NewRequest(http.MethodPost, ts.URL+"/", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/x-amz-json-1.1")
		req.Header.Set("X-Amz-Target", "Kafka."+op)
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
		if res.Header.Get("x-mirror-fidelity") != "emulate" {
			t.Fatalf("fidelity %q", res.Header.Get("x-mirror-fidelity"))
		}
		out := map[string]any{}
		_ = json.Unmarshal(raw, &out)
		return out
	}
	created := call("CreateCluster", `{"ClusterName":"c1","BrokerNodeGroupInfo":{"InstanceType":"kafka.t3.small"}}`)
	arn, _ := created["ClusterArn"].(string)
	if arn == "" {
		t.Fatalf("create %v", created)
	}
	got := call("DescribeCluster", `{"ClusterArn":"`+arn+`"}`)
	if got["ClusterInfo"] == nil {
		t.Fatalf("describe %v", got)
	}
	listed := call("ListClusters", `{}`)
	raw, _ := json.Marshal(listed)
	if !strings.Contains(string(raw), "c1") {
		t.Fatalf("list %s", raw)
	}
	call("DeleteCluster", `{"ClusterArn":"`+arn+`"}`)
	gone := call("ListClusters", `{}`)
	raw, _ = json.Marshal(gone)
	if strings.Contains(string(raw), `"c1"`) {
		t.Fatalf("still present %s", raw)
	}
}
