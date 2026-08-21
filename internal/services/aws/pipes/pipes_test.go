package pipes

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/clock"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/config"
	rtpkg "github.com/tyler-r-kendrick/mirror.cloud/internal/runtime"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/sqs"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spitest"
)

func TestPipesHTTPProvenOps(t *testing.T) {
	p := New(spitest.Deps(t))
	defer p.Close()
	if n := len(p.Operations()); n != 10 {
		t.Fatalf("pipes Operations() %d want 10", n)
	}
}

func TestBootedServerPipesCreateGetDelete(t *testing.T) {
	cfg := config.Default()
	cfg.Services = []string{"aws.pipes"}
	cfg.Seed = "pipes-1"
	rt, err := rtpkg.Boot(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer rt.Close()
	ts := httptest.NewServer(rt.Handler())
	defer ts.Close()
	auth := "AWS4-HMAC-SHA256 Credential=test/20200101/us-east-1/pipes/aws4_request, SignedHeaders=host, Signature=00"
	call := func(op, body string) map[string]any {
		t.Helper()
		req, _ := http.NewRequest(http.MethodPost, ts.URL+"/", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/x-amz-json-1.1")
		req.Header.Set("X-Amz-Target", "AWSPipes."+op)
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
	created := call("CreatePipe", `{"Name":"p1","Source":"arn:aws:sqs:us-east-1:000000000000:src","Target":"arn:aws:sqs:us-east-1:000000000000:dst","RoleArn":"arn:aws:iam::000000000000:role/x"}`)
	if created["Name"] != "p1" {
		t.Fatalf("create %v", created)
	}
	got := call("DescribePipe", `{"Name":"p1"}`)
	if got["Name"] != "p1" || got["CurrentState"] != "RUNNING" {
		t.Fatalf("describe %v", got)
	}
	listed := call("ListPipes", `{}`)
	raw, _ := json.Marshal(listed)
	if !strings.Contains(string(raw), "p1") {
		t.Fatalf("list %s", raw)
	}
	stopped := call("StopPipe", `{"Name":"p1"}`)
	if stopped["CurrentState"] != "STOPPED" {
		t.Fatalf("stop %v", stopped)
	}
	call("DeletePipe", `{"Name":"p1"}`)
	gone := call("ListPipes", `{}`)
	raw, _ = json.Marshal(gone)
	if strings.Contains(string(raw), `"Name":"p1"`) {
		t.Fatalf("still present %s", raw)
	}
}

func TestPipesSQSDeliveryStateAndFiltering(t *testing.T) {
	id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
	deps := spitest.Deps(t)
	p := New(deps)
	defer p.Close()
	queue := sqs.New(deps)
	for _, name := range []string{"source", "target", "filtered-source", "filtered-target"} {
		invoke(t, queue, id, "CreateQueue", map[string]any{"QueueName": name})
	}

	pipe := pipeInput("delivery", "source", "target")
	pipe["DesiredState"] = "STOPPED"
	pipe["SourceParameters"] = map[string]any{"SqsQueueParameters": map[string]any{"BatchSize": 2}}
	invoke(t, p, id, "CreatePipe", pipe)
	for _, body := range []string{"one", "two"} {
		invoke(t, queue, id, "SendMessage", map[string]any{"QueueName": "source", "MessageBody": body})
	}
	if got := storedMessages(t, deps, id, "target"); len(got) != 0 {
		t.Fatalf("stopped pipe delivered %d messages", len(got))
	}
	invoke(t, p, id, "StartPipe", map[string]any{"Name": "delivery"})
	eventually(t, func() bool { return len(storedMessages(t, deps, id, "target")) == 2 })
	if got := storedMessages(t, deps, id, "source"); len(got) != 0 {
		t.Fatalf("successful source messages retained: %v", got)
	}
	bodies := map[string]bool{}
	for _, message := range storedMessages(t, deps, id, "target") {
		var record map[string]any
		if err := json.Unmarshal([]byte(message["body"].(string)), &record); err != nil {
			t.Fatal(err)
		}
		bodies[record["body"].(string)] = true
		if record["eventSource"] != "aws:sqs" || record["eventSourceARN"] != queueARN(id, "source") || record["awsRegion"] != id.Region {
			t.Fatalf("source record %#v", record)
		}
	}
	if !bodies["one"] || !bodies["two"] {
		t.Fatalf("delivered bodies %v", bodies)
	}

	filtered := pipeInput("filter", "filtered-source", "filtered-target")
	filtered["DesiredState"] = "STOPPED"
	filtered["FilterCriteria"] = map[string]any{"Filters": []any{map[string]any{"Pattern": `{"body":{"kind":["keep"]}}`}}}
	invoke(t, p, id, "CreatePipe", filtered)
	for _, body := range []string{`{"kind":"keep"}`, `{"kind":"drop"}`} {
		invoke(t, queue, id, "SendMessage", map[string]any{"QueueName": "filtered-source", "MessageBody": body})
	}
	invoke(t, p, id, "StartPipe", map[string]any{"Name": "filter"})
	eventually(t, func() bool {
		return len(storedMessages(t, deps, id, "filtered-source")) == 0 && len(storedMessages(t, deps, id, "filtered-target")) == 1
	})
	var record map[string]any
	if err := json.Unmarshal([]byte(storedMessages(t, deps, id, "filtered-target")[0]["body"].(string)), &record); err != nil {
		t.Fatal(err)
	}
	if record["body"] != `{"kind":"keep"}` {
		t.Fatalf("filtered record %#v", record)
	}
}

func TestPipesRetriesFailedTargetWithoutDeletingSource(t *testing.T) {
	id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
	deps := spitest.Deps(t)
	p := New(deps)
	defer p.Close()
	queue := sqs.New(deps)
	invoke(t, queue, id, "CreateQueue", map[string]any{"QueueName": "source"})
	invoke(t, p, id, "CreatePipe", pipeInput("retry", "source", "late"))
	invoke(t, queue, id, "SendMessage", map[string]any{"QueueName": "source", "MessageBody": "retry-me"})
	eventually(t, func() bool {
		messages := storedMessages(t, deps, id, "source")
		return len(messages) == 1 && messages[0]["receiveCount"] == float64(1)
	})
	invoke(t, queue, id, "CreateQueue", map[string]any{"QueueName": "late"})
	if err := deps.Clock.(*clock.Controllable).Advance(30 * time.Second); err != nil {
		t.Fatal(err)
	}
	eventually(t, func() bool {
		return len(storedMessages(t, deps, id, "source")) == 0 && len(storedMessages(t, deps, id, "late")) == 1
	})
}

func pipeInput(name, source, target string) map[string]any {
	id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
	return map[string]any{
		"Name": name, "Source": queueARN(id, source), "Target": queueARN(id, target),
		"RoleArn": "arn:aws:iam::123456789012:role/pipes",
	}
}

func queueARN(id spi.Identity, name string) string {
	return "arn:aws:sqs:" + id.Region + ":" + id.Account + ":" + name
}

func invoke(t *testing.T, handler spi.Handler, id spi.Identity, operation string, input map[string]any) *spi.Response {
	t.Helper()
	response, err := handler.Invoke(context.Background(), &spi.Request{Identity: id, Operation: operation, Input: input})
	if err != nil {
		t.Fatalf("%s: %v", operation, err)
	}
	return response
}

func storedMessages(t *testing.T, deps spi.Deps, id spi.Identity, queue string) []map[string]any {
	t.Helper()
	kvs, _, err := deps.Store.Scope(id.Account, id.Region).Collection("msgs:"+queue).List(context.Background(), "", "", 0)
	if err != nil {
		t.Fatal(err)
	}
	messages := make([]map[string]any, 0, len(kvs))
	for _, kv := range kvs {
		var message map[string]any
		if err := json.Unmarshal(kv.Value, &message); err != nil {
			t.Fatal(err)
		}
		messages = append(messages, message)
	}
	return messages
}

func eventually(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.After(2 * time.Second)
	for !condition() {
		select {
		case <-deadline:
			t.Fatal("condition not met")
		default:
			time.Sleep(time.Millisecond)
		}
	}
}
