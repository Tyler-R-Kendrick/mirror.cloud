package scheduler

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/clock"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/config"
	rtpkg "github.com/tyler-r-kendrick/mirror.cloud/internal/runtime"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/events"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/sqs"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spitest"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/store"
)

func TestSchedulerHTTPProvenOps(t *testing.T) {
	p := New(spitest.Deps(t))
	defer p.Close()
	if n := len(p.Operations()); n != 9 {
		t.Fatalf("scheduler Operations() %d want 9", n)
	}
}

type deadlineRaceClock struct {
	*clock.Controllable
	advanced atomic.Bool
}

func (c *deadlineRaceClock) jump() {
	if c.advanced.CompareAndSwap(false, true) {
		_ = c.Advance(2 * time.Minute)
	}
}

func (c *deadlineRaceClock) After(d time.Duration) <-chan time.Time {
	c.jump()
	return c.Controllable.After(d)
}

func (c *deadlineRaceClock) AfterUntil(at time.Time) <-chan time.Time {
	c.jump()
	return c.Controllable.AfterUntil(at)
}

func TestSchedulerWaitsForAbsoluteDeadline(t *testing.T) {
	ctx := context.Background()
	id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
	deps := spitest.Deps(t)
	deps.Clock = &deadlineRaceClock{Controllable: clock.NewControllable()}
	queue := sqs.New(deps)
	if _, err := queue.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateQueue", Input: map[string]any{"QueueName": "jobs"}}); err != nil {
		t.Fatal(err)
	}
	arn := "arn:aws:scheduler:us-east-1:123456789012:schedule/default/once"
	rec, fault := (&Pack{deps: deps}).scheduleRecord(scheduleInput("once", "at(1970-01-01T00:01:00)", time.Time{}, "jobs", "work"), "once", "default", arn)
	if fault != nil {
		t.Fatal(fault)
	}
	if err := putRecord(ctx, deps.Store.Scope(id.Account, id.Region).Collection("sch:default"), "once", rec); err != nil {
		t.Fatal(err)
	}
	p := New(deps)
	defer p.Close()
	deadline := time.After(time.Second)
	for {
		if got := queueBodies(t, deps, id, "jobs"); len(got) == 1 && got[0] == "work" {
			break
		}
		select {
		case <-deadline:
			t.Fatal("absolute deadline was not delivered")
		default:
			time.Sleep(time.Millisecond)
		}
	}
}

func TestBootedServerSchedulerCreateGetDelete(t *testing.T) {
	cfg := config.Default()
	cfg.Services = []string{"aws.scheduler"}
	cfg.Seed = "sch-1"
	rt, err := rtpkg.Boot(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer rt.Close()
	ts := httptest.NewServer(rt.Handler())
	defer ts.Close()
	auth := "AWS4-HMAC-SHA256 Credential=test/20200101/us-east-1/scheduler/aws4_request, SignedHeaders=host, Signature=00"
	call := func(op, body string) map[string]any {
		t.Helper()
		req, _ := http.NewRequest(http.MethodPost, ts.URL+"/", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/x-amz-json-1.0")
		req.Header.Set("X-Amz-Target", "Scheduler."+op)
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
	created := call("CreateSchedule", `{"Name":"s1","ScheduleExpression":"rate(1 minute)","FlexibleTimeWindow":{"Mode":"OFF"},"Target":{"Arn":"arn:aws:sqs:us-east-1:000000000000:q","RoleArn":"arn:aws:iam::000000000000:role/test"}}`)
	if created["ScheduleArn"] == nil {
		t.Fatalf("create %v", created)
	}
	got := call("GetSchedule", `{"Name":"s1"}`)
	if got["Name"] != "s1" {
		t.Fatalf("get %v", got)
	}
	listed := call("ListSchedules", `{}`)
	raw, _ := json.Marshal(listed)
	if !strings.Contains(string(raw), `"s1"`) {
		t.Fatalf("list %s", raw)
	}
	call("DeleteSchedule", `{"Name":"s1"}`)
	gone := call("ListSchedules", `{}`)
	raw, _ = json.Marshal(gone)
	if strings.Contains(string(raw), `"Name":"s1"`) {
		t.Fatalf("still present %s", raw)
	}
}

func TestChangeRecordIfUnchangedRejectsStaleWorkerState(t *testing.T) {
	ctx := context.Background()
	collection := spitest.Deps(t).Store.Scope("123456789012", "us-east-1").Collection("sch:default")
	original := map[string]any{"Name": "job", "State": "ENABLED"}
	if err := putRecord(ctx, collection, "job", original); err != nil {
		t.Fatal(err)
	}
	expected, _, _ := collection.Get(ctx, "job")
	if err := collection.Delete(ctx, "job"); err != nil {
		t.Fatal(err)
	}
	if err := changeRecordIfUnchanged(ctx, collection, "job", expected, map[string]any{"Name": "job", "State": "DISABLED"}); err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := collection.Get(ctx, "job"); ok {
		t.Fatal("stale worker resurrected deleted schedule")
	}

	if err := putRecord(ctx, collection, "job", original); err != nil {
		t.Fatal(err)
	}
	expected, _, _ = collection.Get(ctx, "job")
	fresh := map[string]any{"Name": "job", "State": "DISABLED"}
	if err := putRecord(ctx, collection, "job", fresh); err != nil {
		t.Fatal(err)
	}
	if err := changeRecordIfUnchanged(ctx, collection, "job", expected, nil); err != nil {
		t.Fatal(err)
	}
	got, ok, err := getRecord(ctx, collection, "job")
	if err != nil || !ok || got["State"] != "DISABLED" {
		t.Fatalf("stale worker deleted replacement: %v, %v, %v", got, ok, err)
	}
}

func TestSchedulerDeliversRestoredSchedule(t *testing.T) {
	ctx := context.Background()
	id := spi.Identity{Account: "123456789012", Region: "us-west-2"}
	deps := spitest.Deps(t)
	queue := sqs.New(deps)
	if _, err := queue.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateQueue", Input: map[string]any{"QueueName": "jobs"}}); err != nil {
		t.Fatal(err)
	}
	p := New(deps)
	start := deps.Clock.Now().Add(time.Minute)
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateSchedule", Input: scheduleInput("once", "at(1970-01-01T00:01:00)", start, "jobs", "work")}); err != nil {
		t.Fatal(err)
	}
	if got := queueBodies(t, deps, id, "jobs"); len(got) != 0 {
		t.Fatalf("delivered early: %v", got)
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}

	snapshot, err := store.SnapshotBytes(deps.Store)
	if err != nil {
		t.Fatal(err)
	}
	restored := spitest.Deps(t)
	if err := restored.Store.Restore(ctx, bytes.NewReader(snapshot)); err != nil {
		t.Fatal(err)
	}
	p = New(restored)
	defer p.Close()
	if err := restored.Clock.(*clock.Controllable).Advance(time.Minute); err != nil {
		t.Fatal(err)
	}
	eventually(t, func() bool {
		got := queueBodies(t, restored, id, "jobs")
		return len(got) == 1 && got[0] == "work"
	})
	eventually(t, func() bool {
		_, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "GetSchedule", Input: map[string]any{"Name": "once"}})
		fault, _ := err.(*spi.Fault)
		return fault != nil && fault.Code == "ResourceNotFoundException"
	})
}

func TestSchedulerStateUpdateAndGroups(t *testing.T) {
	ctx := context.Background()
	id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
	deps := spitest.Deps(t)
	p := New(deps)
	defer p.Close()
	queue := sqs.New(deps)
	_, _ = queue.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateQueue", Input: map[string]any{"QueueName": "jobs"}})
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateScheduleGroup", Input: map[string]any{"Name": "batch"}}); err != nil {
		t.Fatal(err)
	}
	bad := scheduleInput("bad", "rate(1 minute)", time.Time{}, "jobs", "not-json")
	bad["Target"].(map[string]any)["Arn"] = "arn:aws:lambda:us-east-1:123456789012:function:job"
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateSchedule", Input: bad}); err == nil {
		t.Fatal("accepted non-JSON Lambda input")
	}
	universal := scheduleInput("universal", "at(1970-01-01T00:10:00)", time.Time{}, "jobs", "validated-at-invocation")
	universal["Target"].(map[string]any)["Arn"] = "arn:aws:scheduler:::aws-sdk:lambda:invoke"
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateSchedule", Input: universal}); err != nil {
		t.Fatalf("universal target input was validated at creation: %v", err)
	}
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "DeleteSchedule", Input: map[string]any{"Name": "universal"}}); err != nil {
		t.Fatal(err)
	}
	input := scheduleInput("disabled", "at(1970-01-01T00:01:00)", time.Time{}, "jobs", "first")
	input["GroupName"], input["State"] = "batch", "DISABLED"
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateSchedule", Input: input}); err != nil {
		t.Fatal(err)
	}
	listed, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "ListSchedules", Input: map[string]any{}})
	if err != nil || len(listed.Output["Schedules"].([]any)) != 1 {
		t.Fatalf("all-group list: %v %v", listed, err)
	}
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "DeleteScheduleGroup", Input: map[string]any{"Name": "batch"}}); err == nil {
		t.Fatal("deleted non-empty group")
	}
	_ = deps.Clock.(*clock.Controllable).Advance(time.Minute)
	runtime.Gosched()
	if got := queueBodies(t, deps, id, "jobs"); len(got) != 0 {
		t.Fatalf("disabled schedule delivered: %v", got)
	}
	input = scheduleInput("disabled", "at(1970-01-01T00:02:00)", time.Time{}, "jobs", "updated")
	input["GroupName"] = "batch"
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "UpdateSchedule", Input: input}); err != nil {
		t.Fatal(err)
	}
	_ = deps.Clock.(*clock.Controllable).Advance(time.Minute)
	eventually(t, func() bool {
		got := queueBodies(t, deps, id, "jobs")
		return len(got) == 1 && got[0] == "updated"
	})
}

func TestSchedulerFlexibleWindowRetryAndDLQ(t *testing.T) {
	var destinationCalls atomic.Int32
	var destinationReady atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		destinationCalls.Add(1)
		if !destinationReady.Load() {
			w.WriteHeader(http.StatusInternalServerError)
		}
	}))
	defer server.Close()
	ctx := context.Background()
	id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
	deps := spitest.Deps(t)
	clock := deps.Clock.(*clock.Controllable)
	p := New(deps)
	defer p.Close()
	eventPack := events.New(deps)
	defer eventPack.Close()
	queue := sqs.New(deps)
	for _, name := range []string{"jobs", "failures"} {
		if _, err := queue.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateQueue", Input: map[string]any{"QueueName": name}}); err != nil {
			t.Fatal(err)
		}
	}
	connection, err := eventPack.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateConnection", Input: map[string]any{
		"Name": "scheduler", "AuthorizationType": "API_KEY", "AuthParameters": map[string]any{"ApiKeyAuthParameters": map[string]any{"ApiKeyName": "X-Key", "ApiKeyValue": "value"}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	destination, err := eventPack.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateApiDestination", Input: map[string]any{
		"Name": "scheduler", "ConnectionArn": connection.Output["ConnectionArn"], "InvocationEndpoint": server.URL, "HttpMethod": "POST",
	}})
	if err != nil {
		t.Fatal(err)
	}

	flexible := scheduleInput("flexible", "at(1970-01-01T00:01:00)", time.Time{}, "jobs", "time=<aws.scheduler.scheduled-time>;attempt=<aws.scheduler.attempt-number>")
	flexible["FlexibleTimeWindow"] = map[string]any{"Mode": "FLEXIBLE", "MaximumWindowInMinutes": 1}
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateSchedule", Input: flexible}); err != nil {
		t.Fatal(err)
	}
	rec := storedSchedule(t, deps, id, "flexible")
	next, ok := inputTime(rec[nextInvocation])
	windowStart := time.Unix(60, 0).UTC()
	if !ok || !next.After(windowStart) || next.After(windowStart.Add(time.Minute)) {
		t.Fatalf("flexible invocation %v", rec[nextInvocation])
	}
	_ = clock.Advance(2 * time.Minute)
	eventually(t, func() bool {
		got := queueBodies(t, deps, id, "jobs")
		return len(got) == 1 && got[0] == "time=1970-01-01T00:01:00Z;attempt=1"
	})

	retryAt := clock.Now().Add(time.Minute)
	retrying := scheduleInput("retry", "at("+retryAt.Format("2006-01-02T15:04:05")+")", time.Time{}, "late", `{"attempt":"<aws.scheduler.attempt-number>"}`)
	retryTarget := retrying["Target"].(map[string]any)
	retryTarget["Arn"] = destination.Output["ApiDestinationArn"]
	retryTarget["RetryPolicy"] = map[string]any{"MaximumEventAgeInSeconds": 60, "MaximumRetryAttempts": 1}
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateSchedule", Input: retrying}); err != nil {
		t.Fatal(err)
	}
	_ = clock.Advance(time.Minute)
	eventually(t, func() bool {
		attempts, _ := integer(storedSchedule(t, deps, id, "retry")[retryAttempts])
		return attempts == 1
	})
	destinationReady.Store(true)
	_ = clock.Advance(2 * time.Second)
	eventually(t, func() bool {
		return destinationCalls.Load() == 2
	})

	dlqAt := clock.Now().Add(time.Minute)
	dead := scheduleInput("dead", "at("+dlqAt.Format("2006-01-02T15:04:05")+")", time.Time{}, "missing", "dead")
	deadTarget := dead["Target"].(map[string]any)
	deadTarget["RetryPolicy"] = map[string]any{"MaximumEventAgeInSeconds": 60, "MaximumRetryAttempts": 0}
	deadTarget["DeadLetterConfig"] = map[string]any{"Arn": "arn:aws:sqs:us-east-1:123456789012:failures"}
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateSchedule", Input: dead}); err != nil {
		t.Fatal(err)
	}
	_ = clock.Advance(time.Minute)
	eventually(t, func() bool { return len(queueBodies(t, deps, id, "failures")) == 1 })
	dlq := storedMessage(t, deps, id, "failures")
	attrs := dlq["attrs"].(map[string]any)
	errorCode := attrs["ERROR_CODE"].(map[string]any)["StringValue"]
	if dlq["body"] != "dead" || errorCode != "AWS.SimpleQueueService.NonExistentQueue" {
		t.Fatalf("DLQ message %#v", dlq)
	}
}

func scheduleInput(name, expression string, start time.Time, queue, payload string) map[string]any {
	input := map[string]any{
		"Name": name, "ScheduleExpression": expression, "ActionAfterCompletion": "DELETE",
		"FlexibleTimeWindow": map[string]any{"Mode": "OFF"},
		"Target": map[string]any{
			"Arn":     "arn:aws:sqs:us-east-1:123456789012:" + queue,
			"RoleArn": "arn:aws:iam::123456789012:role/scheduler", "Input": payload,
		},
	}
	if !start.IsZero() {
		input["StartDate"] = start
	}
	return input
}

func queueBodies(t *testing.T, deps spi.Deps, id spi.Identity, queue string) []string {
	t.Helper()
	kvs, _, err := deps.Store.Scope(id.Account, id.Region).Collection("msgs:"+queue).List(context.Background(), "", "", 0)
	if err != nil {
		t.Fatal(err)
	}
	bodies := make([]string, 0, len(kvs))
	for _, kv := range kvs {
		var message map[string]any
		if err := json.Unmarshal(kv.Value, &message); err != nil {
			t.Fatal(err)
		}
		bodies = append(bodies, message["body"].(string))
	}
	return bodies
}

func storedSchedule(t *testing.T, deps spi.Deps, id spi.Identity, name string) map[string]any {
	t.Helper()
	b, ok, err := deps.Store.Scope(id.Account, id.Region).Collection("sch:default").Get(context.Background(), name)
	if err != nil || !ok {
		t.Fatalf("schedule %s: found=%v err=%v", name, ok, err)
	}
	var rec map[string]any
	if err := json.Unmarshal(b, &rec); err != nil {
		t.Fatal(err)
	}
	return rec
}

func storedMessage(t *testing.T, deps spi.Deps, id spi.Identity, queue string) map[string]any {
	t.Helper()
	kvs, _, err := deps.Store.Scope(id.Account, id.Region).Collection("msgs:"+queue).List(context.Background(), "", "", 1)
	if err != nil || len(kvs) != 1 {
		t.Fatalf("queue %s: messages=%d err=%v", queue, len(kvs), err)
	}
	var message map[string]any
	if err := json.Unmarshal(kvs[0].Value, &message); err != nil {
		t.Fatal(err)
	}
	return message
}

func eventually(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.After(30 * time.Second)
	for !condition() {
		select {
		case <-deadline:
			t.Fatal("condition not met")
		default:
			time.Sleep(time.Millisecond)
		}
	}
}
