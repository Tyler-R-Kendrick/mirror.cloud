// Package scheduler emulates EventBridge Scheduler groups, schedules, and templated target delivery.
package scheduler

import (
	"context"
	"encoding/json"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/events"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/sqs"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

const (
	nextInvocation = "_mirrorNextInvocation"
	scheduledTime  = "_mirrorScheduledTime"
	retryAttempts  = "_mirrorRetryAttempts"
	retryStarted   = "_mirrorRetryStarted"
)

func init() {
	registry.Register(registry.Factory{ServiceID: "aws.scheduler", Tier: model.TierEmulate, New: func(d spi.Deps) (spi.BehaviorPack, error) {
		return New(d), nil
	}})
}

// Pack implements EventBridge Scheduler.
type Pack struct {
	deps      spi.Deps
	wake      chan struct{}
	stop      chan struct{}
	done      chan struct{}
	closeOnce sync.Once
}

// New constructs the pack and resumes persisted schedules.
func New(d spi.Deps) *Pack {
	p := &Pack{deps: d, wake: make(chan struct{}, 1), stop: make(chan struct{}), done: make(chan struct{})}
	if d.Store == nil || d.Clock == nil {
		close(p.done)
		return p
	}
	go p.loop()
	return p
}

func (p *Pack) ServiceID() string { return "aws.scheduler" }
func (p *Pack) Tier() model.Tier  { return model.TierEmulate }
func (p *Pack) Operations() []string {
	return []string{
		"CreateSchedule", "GetSchedule", "ListSchedules", "UpdateSchedule", "DeleteSchedule",
		"CreateScheduleGroup", "GetScheduleGroup", "ListScheduleGroups", "DeleteScheduleGroup",
	}
}

// Close stops the schedule worker.
func (p *Pack) Close() error {
	p.closeOnce.Do(func() { close(p.stop) })
	<-p.done
	return nil
}

func (p *Pack) col(req *spi.Request, name string) spi.Collection {
	return p.deps.Store.Scope(req.Identity.Account, req.Identity.Region).Collection(name)
}

func (p *Pack) Invoke(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	name := first(req.Input, "Name", "name")
	requestedGroup := first(req.Input, "GroupName", "groupName")
	group := requestedGroup
	if group == "" {
		group = "default"
	}
	schedules := p.col(req, "sch:"+group)
	switch req.Operation {
	case "CreateSchedule":
		if name == "" {
			return nil, validation("Name is required.")
		}
		if group != "default" && !p.groupExists(ctx, req, group) {
			return nil, notFound("Schedule group does not exist.")
		}
		if _, ok, err := schedules.Get(ctx, name); err != nil {
			return nil, err
		} else if ok {
			return nil, conflict("Schedule already exists.")
		}
		arn := "arn:aws:scheduler:" + req.Identity.Region + ":" + req.Identity.Account + ":schedule/" + group + "/" + name
		rec, fault := p.scheduleRecord(req.Input, name, group, arn)
		if fault != nil {
			return nil, fault
		}
		if err := putRecord(ctx, schedules, name, rec); err != nil {
			return nil, err
		}
		p.notify()
		return &spi.Response{Output: map[string]any{"ScheduleArn": arn}}, nil
	case "GetSchedule":
		if name == "" {
			return nil, validation("Name is required.")
		}
		rec, ok, err := getRecord(ctx, schedules, name)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, notFound("Schedule does not exist.")
		}
		return &spi.Response{Output: publicRecord(rec)}, nil
	case "ListSchedules":
		groups := []string{group}
		if requestedGroup == "" {
			groups = p.groups(ctx, req.Identity)
		}
		items := []any{}
		for _, group := range groups {
			kvs, _, err := p.deps.Store.Scope(req.Identity.Account, req.Identity.Region).Collection("sch:"+group).List(ctx, "", "", 0)
			if err != nil {
				return nil, err
			}
			for _, kv := range kvs {
				var rec map[string]any
				if json.Unmarshal(kv.Value, &rec) == nil {
					items = append(items, publicRecord(rec))
				}
			}
		}
		return &spi.Response{Output: map[string]any{"Schedules": items}}, nil
	case "UpdateSchedule":
		if name == "" {
			return nil, validation("Name is required.")
		}
		existing, ok, err := getRecord(ctx, schedules, name)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, notFound("Schedule does not exist.")
		}
		rec, fault := p.scheduleRecord(req.Input, name, group, stringValue(existing["Arn"]))
		if fault != nil {
			return nil, fault
		}
		if err := putRecord(ctx, schedules, name, rec); err != nil {
			return nil, err
		}
		p.notify()
		return &spi.Response{Output: map[string]any{"ScheduleArn": existing["Arn"]}}, nil
	case "DeleteSchedule":
		if name == "" {
			return nil, validation("Name is required.")
		}
		if _, ok, err := schedules.Get(ctx, name); err != nil {
			return nil, err
		} else if !ok {
			return nil, notFound("Schedule does not exist.")
		}
		if err := schedules.Delete(ctx, name); err != nil {
			return nil, err
		}
		p.notify()
		return &spi.Response{Output: map[string]any{}}, nil
	case "CreateScheduleGroup":
		if name == "" || name == "default" {
			return nil, validation("A non-default Name is required.")
		}
		if p.groupExists(ctx, req, name) {
			return nil, conflict("Schedule group already exists.")
		}
		arn := "arn:aws:scheduler:" + req.Identity.Region + ":" + req.Identity.Account + ":schedule-group/" + name
		rec := map[string]any{"Name": name, "Arn": arn, "State": "ACTIVE"}
		if err := putRecord(ctx, p.col(req, "schg"), name, rec); err != nil {
			return nil, err
		}
		return &spi.Response{Output: map[string]any{"ScheduleGroupArn": arn}}, nil
	case "GetScheduleGroup":
		if name == "" {
			return nil, validation("Name is required.")
		}
		if name == "default" {
			return &spi.Response{Output: defaultGroup(req.Identity)}, nil
		}
		rec, ok, err := getRecord(ctx, p.col(req, "schg"), name)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, notFound("Schedule group does not exist.")
		}
		return &spi.Response{Output: rec}, nil
	case "ListScheduleGroups":
		resp, err := listCol(ctx, p.col(req, "schg"), "ScheduleGroups")
		if err != nil {
			return nil, err
		}
		resp.Output["ScheduleGroups"] = append([]any{defaultGroup(req.Identity)}, resp.Output["ScheduleGroups"].([]any)...)
		return resp, nil
	case "DeleteScheduleGroup":
		if name == "" {
			return nil, validation("Name is required.")
		}
		if name == "default" {
			return nil, conflict("The default schedule group cannot be deleted.")
		}
		if !p.groupExists(ctx, req, name) {
			return nil, notFound("Schedule group does not exist.")
		}
		if kvs, _, err := p.col(req, "sch:"+name).List(ctx, "", "", 1); err != nil {
			return nil, err
		} else if len(kvs) != 0 {
			return nil, conflict("Schedule group is not empty.")
		}
		if err := p.col(req, "schg").Delete(ctx, name); err != nil {
			return nil, err
		}
		return &spi.Response{Output: map[string]any{}}, nil
	default:
		return nil, spi.NotImplemented("aws.scheduler", req.Operation, "emulate")
	}
}

func (p *Pack) scheduleRecord(input map[string]any, name, group, arn string) (map[string]any, *spi.Fault) {
	expression := first(input, "ScheduleExpression", "scheduleExpression")
	timezone := first(input, "ScheduleExpressionTimezone", "scheduleExpressionTimezone")
	expr, err := parseScheduleExpression(expression, timezone)
	if err != nil {
		return nil, validation(err.Error())
	}
	target, ok := input["Target"].(map[string]any)
	if !ok || first(target, "Arn", "arn") == "" || !validRoleARN(first(target, "RoleArn", "roleArn")) {
		return nil, validation("Target Arn and RoleArn are required.")
	}
	targetARN := first(target, "Arn", "arn")
	if len(targetARN) > 1600 {
		return nil, validation("Target Arn exceeds 1600 characters.")
	}
	if payload := first(target, "Input", "input"); len(payload) > 256*1024 || (payload != "" && requiresJSON(targetARN) && !json.Valid([]byte(payload))) {
		return nil, validation("Target Input must meet the target format and 256 KB limit.")
	}
	window, ok := input["FlexibleTimeWindow"].(map[string]any)
	mode := first(window, "Mode", "mode")
	if !ok || (mode != "OFF" && mode != "FLEXIBLE") {
		return nil, validation("FlexibleTimeWindow Mode must be OFF or FLEXIBLE.")
	}
	if mode == "FLEXIBLE" {
		minutes, _ := integer(window["MaximumWindowInMinutes"])
		if minutes < 1 || minutes > 1440 {
			return nil, validation("MaximumWindowInMinutes must be between 1 and 1440.")
		}
	}
	state := first(input, "State", "state")
	if state == "" {
		state = "ENABLED"
	}
	if state != "ENABLED" && state != "DISABLED" {
		return nil, validation("State must be ENABLED or DISABLED.")
	}
	action := first(input, "ActionAfterCompletion", "actionAfterCompletion")
	if action != "" && action != "NONE" && action != "DELETE" {
		return nil, validation("ActionAfterCompletion must be NONE or DELETE.")
	}
	if fault := validateReliability(target); fault != nil {
		return nil, fault
	}
	start, _ := inputTime(input["StartDate"])
	end, hasEnd := inputTime(input["EndDate"])
	if hasEnd && !start.IsZero() && end.Before(start) {
		return nil, validation("EndDate must not precede StartDate.")
	}
	rec := clone(input)
	rec["Name"], rec["GroupName"], rec["Arn"], rec["State"] = name, group, arn, state
	scheduled := expr.first(p.deps.Clock.Now(), start)
	if !expr.OneTime() && hasEnd && scheduled.After(end) {
		scheduled = time.Time{}
	}
	rec[nextInvocation] = formatTime(p.withWindow(rec, arn, scheduled))
	rec[scheduledTime] = formatTime(scheduled)
	return rec, nil
}

func (p *Pack) loop() {
	defer close(p.done)
	for {
		next := p.runDue(context.Background())
		if next.IsZero() {
			select {
			case <-p.wake:
			case <-p.stop:
				return
			}
			continue
		}
		delay := next.Sub(p.deps.Clock.Now())
		if delay < 0 {
			delay = 0
		}
		select {
		case <-p.deps.Clock.After(delay):
		case <-p.wake:
		case <-p.stop:
			return
		}
	}
}

func (p *Pack) runDue(ctx context.Context) time.Time {
	now := p.deps.Clock.Now()
	var earliest time.Time
	scopes, err := p.deps.Store.Scopes(ctx)
	if err != nil {
		return earliest
	}
	for _, identity := range scopes {
		for _, group := range p.groups(ctx, identity) {
			collection := p.deps.Store.Scope(identity.Account, identity.Region).Collection("sch:" + group)
			kvs, _, err := collection.List(ctx, "", "", 0)
			if err != nil {
				continue
			}
			for _, kv := range kvs {
				var rec map[string]any
				if json.Unmarshal(kv.Value, &rec) != nil || stringValue(rec["State"]) == "DISABLED" {
					continue
				}
				next := p.recordNext(rec, now)
				if next.IsZero() {
					continue
				}
				if next.After(now) {
					earliest = earlier(earliest, next)
					continue
				}
				target, _ := rec["Target"].(map[string]any)
				scheduled := next
				if stored, ok := inputTime(rec[scheduledTime]); ok {
					scheduled = stored
				}
				attempt, _ := integer(rec[retryAttempts])
				payload := p.targetPayload(rec, target, scheduled, attempt+1)
				if err := events.DeliverTarget(ctx, p.deps, identity, first(target, "Arn", "arn"), target, payload); err != nil {
					if !events.TargetErrorRetryable(err) {
						p.deadLetter(ctx, rec, target, payload, attempt, scheduled, "", err)
					} else if retryAt, retrying := p.retry(ctx, collection, kv.Key, rec, target, payload, scheduled, now, err); retrying {
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
					_ = collection.Delete(ctx, kv.Key)
					continue
				}
				rec[nextInvocation] = formatTime(next)
				_ = putRecord(ctx, collection, kv.Key, rec)
				earliest = earlier(earliest, next)
			}
		}
	}
	return earliest
}

func (p *Pack) recordNext(rec map[string]any, now time.Time) time.Time {
	if raw, initialized := rec[nextInvocation]; initialized {
		next, _ := inputTime(raw)
		return next
	}
	expr, err := parseScheduleExpression(stringValue(rec["ScheduleExpression"]), stringValue(rec["ScheduleExpressionTimezone"]))
	if err != nil {
		return time.Time{}
	}
	start, _ := inputTime(rec["StartDate"])
	return p.withWindow(rec, stringValue(rec["Arn"]), expr.first(now, start))
}

func (p *Pack) withWindow(rec map[string]any, arn string, scheduled time.Time) time.Time {
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

func (p *Pack) targetPayload(rec, target map[string]any, scheduled time.Time, attempt int) []byte {
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

func (p *Pack) retry(ctx context.Context, collection spi.Collection, key string, rec, target map[string]any, payload []byte, scheduled, now time.Time, deliveryErr error) (time.Time, bool) {
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
	_ = putRecord(ctx, collection, key, rec)
	return retryAt, true
}

func (p *Pack) deadLetter(ctx context.Context, rec, target map[string]any, payload []byte, attempts int, scheduled time.Time, exhausted string, deliveryErr error) {
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
	_, _ = sqs.New(p.deps).Invoke(ctx, &spi.Request{Identity: spi.Identity{Account: accountFromARN(arn), Region: regionFromARN(arn)}, Operation: "SendMessage", Input: map[string]any{
		"QueueName": arn[strings.LastIndex(arn, ":")+1:], "MessageBody": string(payload), "MessageAttributes": attributes,
	}})
}

func (p *Pack) notify() {
	select {
	case p.wake <- struct{}{}:
	default:
	}
}

func (p *Pack) groupExists(ctx context.Context, req *spi.Request, name string) bool {
	_, ok, _ := p.col(req, "schg").Get(ctx, name)
	return ok
}

func (p *Pack) groups(ctx context.Context, identity spi.Identity) []string {
	groups := []string{"default"}
	kvs, _, _ := p.deps.Store.Scope(identity.Account, identity.Region).Collection("schg").List(ctx, "", "", 0)
	for _, kv := range kvs {
		groups = append(groups, kv.Key)
	}
	sort.Strings(groups)
	return groups
}

func defaultGroup(identity spi.Identity) map[string]any {
	return map[string]any{
		"Name": "default", "State": "ACTIVE",
		"Arn": "arn:aws:scheduler:" + identity.Region + ":" + identity.Account + ":schedule-group/default",
	}
}

func listCol(ctx context.Context, collection spi.Collection, key string) (*spi.Response, error) {
	kvs, _, err := collection.List(ctx, "", "", 0)
	if err != nil {
		return nil, err
	}
	items := make([]any, 0, len(kvs))
	for _, kv := range kvs {
		var rec map[string]any
		if json.Unmarshal(kv.Value, &rec) == nil {
			items = append(items, rec)
		}
	}
	return &spi.Response{Output: map[string]any{key: items}}, nil
}

func getRecord(ctx context.Context, collection spi.Collection, key string) (map[string]any, bool, error) {
	b, ok, err := collection.Get(ctx, key)
	if err != nil || !ok {
		return nil, ok, err
	}
	var rec map[string]any
	if err := json.Unmarshal(b, &rec); err != nil {
		return nil, false, err
	}
	return rec, true, nil
}

func putRecord(ctx context.Context, collection spi.Collection, key string, rec map[string]any) error {
	b, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	return collection.Put(ctx, key, b)
}

func publicRecord(rec map[string]any) map[string]any {
	out := clone(rec)
	delete(out, nextInvocation)
	delete(out, scheduledTime)
	delete(out, retryAttempts)
	delete(out, retryStarted)
	delete(out, "ClientToken")
	return out
}

func clone(input map[string]any) map[string]any {
	out := make(map[string]any, len(input))
	for key, value := range input {
		out[key] = value
	}
	return out
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

func requiresJSON(arn string) bool {
	if strings.Contains(arn, ":scheduler:::aws-sdk:") {
		return false
	}
	return strings.Contains(arn, ":lambda:") || strings.Contains(arn, ":states:") || strings.Contains(arn, ":events:")
}

func validateReliability(target map[string]any) *spi.Fault {
	if raw, exists := target["RetryPolicy"]; exists {
		policy, ok := raw.(map[string]any)
		if !ok {
			return validation("RetryPolicy must be an object.")
		}
		if raw, exists := policy["MaximumEventAgeInSeconds"]; exists {
			age, ok := integer(raw)
			if !ok || age < 60 || age > 86400 {
				return validation("MaximumEventAgeInSeconds must be between 60 and 86400.")
			}
		}
		if raw, exists := policy["MaximumRetryAttempts"]; exists {
			attempts, ok := integer(raw)
			if !ok || attempts < 0 || attempts > 185 {
				return validation("MaximumRetryAttempts must be between 0 and 185.")
			}
		}
	}
	if raw, exists := target["DeadLetterConfig"]; exists {
		config, ok := raw.(map[string]any)
		arn := first(config, "Arn", "arn")
		if !ok || (arn != "" && (!validSQSARN(arn) || strings.HasSuffix(arn, ".fifo"))) {
			return validation("DeadLetterConfig Arn must identify a standard SQS queue.")
		}
	}
	return nil
}

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

func validation(message string) *spi.Fault {
	return &spi.Fault{Code: "ValidationException", Message: message, HTTPStatus: 400, Fault: "client"}
}

func notFound(message string) *spi.Fault {
	return &spi.Fault{Code: "ResourceNotFoundException", Message: message, HTTPStatus: 400, Fault: "client"}
}

func conflict(message string) *spi.Fault {
	return &spi.Fault{Code: "ConflictException", Message: message, HTTPStatus: 409, Fault: "client"}
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

func validRoleARN(arn string) bool {
	parts := strings.SplitN(arn, ":", 6)
	return len(parts) == 6 && parts[0] == "arn" && validPartition(parts[1]) && parts[2] == "iam" && parts[3] == "" && len(parts[4]) == 12 && allDigits(parts[4]) && strings.HasPrefix(parts[5], "role/") && len(parts[5]) > len("role/")
}

func validSQSARN(arn string) bool {
	parts := strings.SplitN(arn, ":", 6)
	return len(parts) == 6 && parts[0] == "arn" && validPartition(parts[1]) && parts[2] == "sqs" && parts[3] != "" && len(parts[4]) == 12 && allDigits(parts[4]) && parts[5] != ""
}

func validPartition(value string) bool { return value == "aws" || strings.HasPrefix(value, "aws-") }

func allDigits(value string) bool {
	for _, char := range value {
		if char < '0' || char > '9' {
			return false
		}
	}
	return true
}

var _ interface{ Close() error } = (*Pack)(nil)
