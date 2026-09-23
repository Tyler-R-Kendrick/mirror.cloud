// Package scheduler is EventBridge Scheduler's delivery worker: the loop
// behavior/aws/scheduler declares under worker:. Groups and schedules are the
// bundle's; this fires them.
package scheduler

import (
	"bytes"
	"context"
	"encoding/json"
	"math"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/bundled"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/events"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

const (
	nextInvocation = "_mirrorNextInvocation"
	scheduledTime  = "_mirrorScheduledTime"
	retryAttempts  = "_mirrorRetryAttempts"
	retryStarted   = "_mirrorRetryStarted"
)

func init() { bundled.RegisterWorker("aws.scheduler", Start) }

// worker fires the schedules behavior/aws/scheduler stores.
type worker struct {
	deps      spi.Deps
	stop      chan struct{}
	done      chan struct{}
	closeOnce sync.Once
}

// Start runs the delivery loop against deps and answers how to stop it.
func Start(d spi.Deps) func() error {
	p := &worker{deps: d, stop: make(chan struct{}), done: make(chan struct{})}
	if d.Store == nil || d.Clock == nil {
		close(p.done)
		return p.close
	}
	go p.loop()
	return p.close
}

func (p *worker) close() error {
	p.closeOnce.Do(func() { close(p.stop) })
	<-p.done
	return nil
}

// poll bounds how long the loop sleeps without a due schedule: the bundle's
// writes do not wake it, so a schedule created or changed is seen within one
// poll.
//
// ponytail: a poll on the clock. A store change feed the engine could publish
// on write is the upgrade; until then a new schedule waits up to one poll.
const poll = time.Second

func (p *worker) loop() {
	defer close(p.done)
	for {
		wake := p.deps.Clock.Now().Add(poll)
		if next := p.runDue(context.Background()); !next.IsZero() && next.Before(wake) {
			wake = next
		}
		select {
		case <-p.deps.Clock.AfterTime(wake):
		case <-p.stop:
			return
		}
	}
}

func (p *worker) runDue(ctx context.Context) time.Time {
	now := p.deps.Clock.Now()
	var earliest time.Time
	scopes, err := p.deps.Store.Scopes(ctx)
	if err != nil {
		return earliest
	}
	for _, identity := range scopes {
		{
			collection := p.deps.Store.Scope(identity.Account, identity.Region).Collection("sch")
			kvs, _, err := collection.List(ctx, "", "", 0)
			if err != nil {
				continue
			}
			for _, kv := range kvs {
				var rec map[string]any
				if json.Unmarshal(kv.Value, &rec) != nil || stringValue(rec["State"]) == "DISABLED" {
					continue
				}
				scheduled, next := p.recordNext(rec, now)
				if next.IsZero() {
					continue
				}
				if next.After(now) {
					earliest = earlier(earliest, next)
					continue
				}
				target, _ := rec["Target"].(map[string]any)
				attempt, _ := integer(rec[retryAttempts])
				payload := p.targetPayload(rec, target, scheduled, attempt+1)
				if err := events.DeliverTarget(ctx, p.deps, identity, first(target, "Arn", "arn"), target, payload); err != nil {
					if !events.TargetErrorRetryable(err) {
						p.deadLetter(ctx, rec, target, payload, attempt, scheduled, "", err)
					} else if retryAt, retrying := p.retry(ctx, collection, kv.Key, kv.Value, rec, target, payload, scheduled, now, err); retrying {
						earliest = earlier(earliest, retryAt)
						continue
					}
				}
				delete(rec, retryAttempts)
				delete(rec, retryStarted)
				expr, err := parseScheduleExpression(stringValue(rec["ScheduleExpression"]), stringValue(rec["ScheduleExpressionTimezone"]))
				if err != nil {
					continue
				}
				scheduled = expr.after(now)
				if end, ok := inputTime(rec["EndDate"]); !expr.OneTime() && ok && scheduled.After(end) {
					scheduled = time.Time{}
				}
				next = p.withWindow(rec, stringValue(rec["Arn"]), scheduled)
				rec[scheduledTime] = formatTime(scheduled)
				if next.IsZero() && stringValue(rec["ActionAfterCompletion"]) == "DELETE" {
					_ = changeRecordIfUnchanged(ctx, collection, kv.Key, kv.Value, nil)
					continue
				}
				rec[nextInvocation] = formatTime(next)
				_ = changeRecordIfUnchanged(ctx, collection, kv.Key, kv.Value, rec)
				earliest = earlier(earliest, next)
			}
		}
	}
	return earliest
}

// recordNext answers when a schedule is next due (scheduled) and when it
// fires, which a flexible window offsets from that. Both are the worker's
// bookkeeping once it has seen the schedule, and computed from the record
// before.
func (p *worker) recordNext(rec map[string]any, now time.Time) (scheduled, next time.Time) {
	if raw, initialized := rec[nextInvocation]; initialized {
		next, _ = inputTime(raw)
		if scheduled, ok := inputTime(rec[scheduledTime]); ok {
			return scheduled, next
		}
		return next, next
	}
	expr, err := parseScheduleExpression(stringValue(rec["ScheduleExpression"]), stringValue(rec["ScheduleExpressionTimezone"]))
	if err != nil {
		return time.Time{}, time.Time{}
	}
	// The first invocation counts from when the schedule was written, not from
	// when this loop first saw it: a poll that lands late must not skip it.
	written, ok := inputTime(rec["LastModificationDate"])
	if !ok {
		written = now
	}
	start, _ := inputTime(rec["StartDate"])
	scheduled = expr.first(written, start)
	if end, ok := inputTime(rec["EndDate"]); !expr.OneTime() && ok && scheduled.After(end) {
		scheduled = time.Time{}
	}
	return scheduled, p.withWindow(rec, stringValue(rec["Arn"]), scheduled)
}

func (p *worker) withWindow(rec map[string]any, arn string, scheduled time.Time) time.Time {
	if scheduled.IsZero() {
		return scheduled
	}
	window, _ := rec["FlexibleTimeWindow"].(map[string]any)
	if stringValue(window["Mode"]) != "FLEXIBLE" {
		return scheduled
	}
	minutes, _ := integer(window["MaximumWindowInMinutes"])
	seconds := p.deps.Rand.Derive(arn + "|" + scheduled.UTC().Format(time.RFC3339Nano)).Intn(minutes*60 + 1)
	return scheduled.Add(time.Duration(seconds) * time.Second)
}

func (p *worker) targetPayload(rec, target map[string]any, scheduled time.Time, attempt int) []byte {
	payload := first(target, "Input", "input")
	if payload == "" {
		payload = "{}"
	}
	executionID := p.deps.Rand.Derive(stringValue(rec["Arn"]) + "|" + scheduled.String() + "|" + strconv.Itoa(attempt)).Hex(16)
	for from, to := range map[string]string{
		"<aws.scheduler.schedule-arn>":   stringValue(rec["Arn"]),
		"<aws.scheduler.scheduled-time>": scheduled.UTC().Format(time.RFC3339),
		"<aws.scheduler.execution-id>":   executionID,
		"<aws.scheduler.attempt-number>": strconv.Itoa(attempt),
	} {
		payload = strings.ReplaceAll(payload, from, to)
	}
	return []byte(payload)
}

func (p *worker) retry(ctx context.Context, collection spi.Collection, key string, expected []byte, rec, target map[string]any, payload []byte, scheduled, now time.Time, deliveryErr error) (time.Time, bool) {
	policy, _ := target["RetryPolicy"].(map[string]any)
	maxAttempts, ok := integer(policy["MaximumRetryAttempts"])
	if !ok {
		maxAttempts = 185
	}
	maxAge, ok := integer(policy["MaximumEventAgeInSeconds"])
	if !ok {
		maxAge = 86400
	}
	attempts, _ := integer(rec[retryAttempts])
	started, ok := inputTime(rec[retryStarted])
	if !ok {
		started = scheduled
	}
	exhausted := ""
	if !now.Before(started.Add(time.Duration(maxAge) * time.Second)) {
		exhausted = "MaximumEventAgeInSeconds"
	} else if attempts >= maxAttempts {
		exhausted = "MaximumRetryAttempts"
	}
	if exhausted != "" {
		p.deadLetter(ctx, rec, target, payload, attempts, scheduled, exhausted, deliveryErr)
		return time.Time{}, false
	}
	attempts++
	delay := time.Second << min(attempts-1, 8)
	jitter := p.deps.Rand.Derive(stringValue(rec["Arn"]) + "|retry|" + strconv.Itoa(attempts)).Intn(int(delay/time.Second) + 1)
	if jitter == 0 {
		jitter = 1
	}
	retryAt := now.Add(time.Duration(jitter) * time.Second)
	rec[retryAttempts], rec[retryStarted], rec[nextInvocation] = attempts, formatTime(started), formatTime(retryAt)
	_ = changeRecordIfUnchanged(ctx, collection, key, expected, rec)
	return retryAt, true
}

func (p *worker) deadLetter(ctx context.Context, rec, target map[string]any, payload []byte, attempts int, scheduled time.Time, exhausted string, deliveryErr error) {
	config, _ := target["DeadLetterConfig"].(map[string]any)
	arn := first(config, "Arn", "arn")
	if arn == "" {
		return
	}
	code, message := "AWS.Scheduler.InternalServerError", deliveryErr.Error()
	if fault, ok := deliveryErr.(*spi.Fault); ok {
		code = fault.Code
	}
	attributes := map[string]any{}
	values := map[string]string{
		"ERROR_CODE": code, "ERROR_MESSAGE": message, "EXHAUSTED_RETRY_CONDITION": exhausted,
		"IS_PAYLOAD_TRUNCATED": "false", "RETRY_ATTEMPTS": strconv.Itoa(attempts),
		"SCHEDULED_TIME": scheduled.UTC().Format(time.RFC3339), "SCHEDULE_ARN": stringValue(rec["Arn"]),
		"TARGET_ARN": first(target, "Arn", "arn"),
	}
	if exhausted == "" {
		delete(values, "EXHAUSTED_RETRY_CONDITION")
	}
	for key, value := range values {
		attributes[key] = map[string]any{"DataType": "String", "StringValue": value}
	}
	_, _ = bundled.Handler("aws.sqs", p.deps).Invoke(ctx, &spi.Request{Identity: spi.Identity{Account: accountFromARN(arn), Region: regionFromARN(arn)}, Operation: "SendMessage", Input: map[string]any{
		"QueueName": arn[strings.LastIndex(arn, ":")+1:], "MessageBody": string(payload), "MessageAttributes": attributes,
	}})
}

func changeRecordIfUnchanged(ctx context.Context, collection spi.Collection, key string, expected []byte, rec map[string]any) error {
	var next []byte
	var err error
	if rec != nil {
		next, err = json.Marshal(rec)
		if err != nil {
			return err
		}
	}
	return collection.Txn(ctx, func(tx spi.Tx) error {
		current, ok, err := tx.Get(key)
		if err != nil || !ok || !bytes.Equal(current, expected) {
			return err
		}
		if rec == nil {
			return tx.Delete(key)
		}
		return tx.Put(key, next)
	})
}

func first(input map[string]any, keys ...string) string {
	for _, key := range keys {
		if value, ok := input[key].(string); ok && value != "" {
			return value
		}
	}
	return ""
}

func stringValue(value any) string { valueString, _ := value.(string); return valueString }

func integer(value any) (int, bool) {
	switch value := value.(type) {
	case float64:
		if math.Trunc(value) != value {
			return 0, false
		}
		return int(value), true
	case int:
		return value, true
	}
	return 0, false
}

func inputTime(value any) (time.Time, bool) {
	switch value := value.(type) {
	case time.Time:
		return value, true
	case string:
		parsed, err := time.Parse(time.RFC3339Nano, value)
		return parsed, err == nil
	case float64:
		seconds, fraction := math.Modf(value)
		return time.Unix(int64(seconds), int64(fraction*1e9)).UTC(), true
	}
	return time.Time{}, false
}

func formatTime(value time.Time) string {
	if value.IsZero() {
		return ""
	}
	return value.Format(time.RFC3339Nano)
}

func earlier(current, candidate time.Time) time.Time {
	if candidate.IsZero() || (!current.IsZero() && !candidate.Before(current)) {
		return current
	}
	return candidate
}

func regionFromARN(arn string) string {
	parts := strings.SplitN(arn, ":", 6)
	if len(parts) == 6 {
		return parts[3]
	}
	return ""
}

func accountFromARN(arn string) string {
	parts := strings.SplitN(arn, ":", 6)
	if len(parts) == 6 {
		return parts[4]
	}
	return ""
}
