// Package events emulates EventBridge rules, schedules, targets, and event ingestion.
package events

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/eventhttp"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/lambda"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/scheduleexpr"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/sns"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/sqs"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/states"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func init() {
	registry.Register(registry.Factory{ServiceID: "aws.events", Tier: model.TierEmulate, New: func(d spi.Deps) (spi.BehaviorPack, error) {
		return New(d), nil
	}})
}

const (
	eventRetryAttempts = "_mirrorRetryAttempts"
	eventRetryStarted  = "_mirrorRetryStarted"
	eventRetryNext     = "_mirrorRetryNext"
	eventRuleNext      = "_mirrorNextInvocation"
)

// Pack implements EventBridge.
type Pack struct {
	deps      spi.Deps
	wake      chan struct{}
	stop      chan struct{}
	done      chan struct{}
	cancelS3  func()
	closeOnce sync.Once
}

// New constructs the pack and resumes persisted target retries.
func New(d spi.Deps) *Pack {
	p := &Pack{deps: d, wake: make(chan struct{}, 1), stop: make(chan struct{}), done: make(chan struct{})}
	if d.Bus != nil {
		p.cancelS3 = d.Bus.Subscribe("events:s3", p.consumeS3)
	}
	if d.Store == nil || d.Clock == nil {
		close(p.done)
		return p
	}
	go p.retryLoop()
	return p
}

// Close stops the target retry worker.
func (p *Pack) Close() error {
	p.closeOnce.Do(func() {
		if p.cancelS3 != nil {
			p.cancelS3()
		}
		close(p.stop)
	})
	<-p.done
	return nil
}

func (p *Pack) consumeS3(ctx context.Context, payload []byte) {
	var event struct {
		Identity spi.Identity   `json:"identity"`
		Entry    map[string]any `json:"entry"`
	}
	if json.Unmarshal(payload, &event) != nil || event.Entry == nil {
		return
	}
	_, _ = p.Invoke(ctx, &spi.Request{Identity: event.Identity, Operation: "PutEvents", Input: map[string]any{"Entries": []any{event.Entry}}})
}

func (p *Pack) ServiceID() string { return "aws.events" }
func (p *Pack) Tier() model.Tier  { return model.TierEmulate }
func (p *Pack) Operations() []string {
	return append([]string{"PutEvents", "PutRule", "PutTargets", "ListRules", "ListTargetsByRule", "DeleteRule", "RemoveTargets"}, extraOps()...)
}

func (p *Pack) col(req *spi.Request, n string) spi.Collection {
	return p.deps.Store.Scope(req.Identity.Account, req.Identity.Region).Collection(n)
}

func (p *Pack) Invoke(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	switch req.Operation {
	case "PutRule":
		name := str(req.Input["Name"])
		if !validEventName(name) {
			return nil, validationFault("Name must be 1-64 characters containing only letters, numbers, period, hyphen, or underscore.")
		}
		if len(str(req.Input["Description"])) > 512 {
			return nil, validationFault("Description exceeds 512 characters.")
		}
		state := str(req.Input["State"])
		if state != "" && state != "ENABLED" && state != "DISABLED" && state != "ENABLED_WITH_ALL_CLOUDTRAIL_MANAGEMENT_EVENTS" {
			return nil, validationFault("State is invalid.")
		}
		pattern, schedule := str(req.Input["EventPattern"]), str(req.Input["ScheduleExpression"])
		if pattern == "" && schedule == "" {
			return nil, &spi.Fault{Code: "InvalidEventPatternException", Message: "EventPattern or ScheduleExpression is required.", HTTPStatus: 400, Fault: "client"}
		}
		if len(pattern) > 4096 || (pattern != "" && !json.Valid([]byte(pattern))) {
			return nil, &spi.Fault{Code: "InvalidEventPatternException", Message: "EventPattern is not valid JSON.", HTTPStatus: 400, Fault: "client"}
		}
		if len(schedule) > 256 {
			return nil, validationFault("ScheduleExpression exceeds 256 characters.")
		}
		bus := eventBus(req.Input)
		if schedule != "" && bus != "default" {
			return nil, validationFault("Scheduled rules are supported only on the default event bus.")
		}
		rec := clone(req.Input)
		if schedule != "" {
			next, err := nextScheduledRule(schedule, p.deps.Clock.Now())
			if err != nil {
				return nil, validationFault(err.Error())
			}
			rec[eventRuleNext] = next.UTC().Format(time.RFC3339Nano)
		}
		if str(rec["State"]) == "" {
			rec["State"] = "ENABLED"
		}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "rules").Put(ctx, eventKey(bus, name), b)
		p.notify()
		path := name
		if bus != "default" {
			path = bus + "/" + name
		}
		arn := "arn:aws:events:" + req.Identity.Region + ":" + req.Identity.Account + ":rule/" + path
		return &spi.Response{Output: map[string]any{"RuleArn": arn}}, nil
	case "ListRules":
		kvs, _, _ := p.col(req, "rules").List(ctx, "", "", 0)
		var rs []any
		for _, kv := range kvs {
			var m map[string]any
			_ = json.Unmarshal(kv.Value, &m)
			if eventBus(m) == eventBus(req.Input) {
				delete(m, eventRuleNext)
				rs = append(rs, m)
			}
		}
		return &spi.Response{Output: map[string]any{"Rules": rs}}, nil
	case "DeleteRule":
		_ = p.col(req, "rules").Delete(ctx, eventKey(eventBus(req.Input), str(req.Input["Name"])))
		return &spi.Response{Output: map[string]any{}}, nil
	case "PutTargets":
		requested := asSlice(req.Input["Targets"])
		if len(requested) < 1 || len(requested) > 10 {
			return nil, validationFault("Targets must contain between 1 and 10 entries.")
		}
		key := eventKey(eventBus(req.Input), str(req.Input["Rule"]))
		if _, ok := p.load(ctx, req, "rules", key); !ok {
			return nil, &spi.Fault{Code: "ResourceNotFoundException", Message: "Rule does not exist.", HTTPStatus: 400, Fault: "client"}
		}
		targets := p.targets(ctx, req, key)
		byID := map[string]int{}
		for i, target := range targets {
			m, _ := target.(map[string]any)
			byID[str(m["Id"])] = i
		}
		var failed []any
		for _, raw := range requested {
			target, _ := raw.(map[string]any)
			id := str(target["Id"])
			if message := validateTarget(target); message != "" {
				failed = append(failed, map[string]any{"TargetId": id, "ErrorCode": "ValidationException", "ErrorMessage": message})
				continue
			}
			if i, ok := byID[id]; ok {
				targets[i] = target
			} else {
				byID[id] = len(targets)
				targets = append(targets, target)
			}
		}
		if len(targets) > 5 {
			return nil, &spi.Fault{Code: "LimitExceededException", Message: "A rule cannot have more than five targets.", HTTPStatus: 400, Fault: "client"}
		}
		b, _ := json.Marshal(targets)
		_ = p.col(req, "targets").Put(ctx, key, b)
		return &spi.Response{Output: map[string]any{"FailedEntryCount": len(failed), "FailedEntries": failed}}, nil
	case "ListTargetsByRule":
		return &spi.Response{Output: map[string]any{"Targets": p.targets(ctx, req, eventKey(eventBus(req.Input), str(req.Input["Rule"])))}}, nil
	case "RemoveTargets":
		key := eventKey(eventBus(req.Input), str(req.Input["Rule"]))
		remove := map[string]bool{}
		for _, id := range asSlice(req.Input["Ids"]) {
			remove[str(id)] = true
		}
		var keep []any
		for _, target := range p.targets(ctx, req, key) {
			m, _ := target.(map[string]any)
			if !remove[str(m["Id"])] {
				keep = append(keep, target)
			}
		}
		b, _ := json.Marshal(keep)
		_ = p.col(req, "targets").Put(ctx, key, b)
		return &spi.Response{Output: map[string]any{"FailedEntryCount": 0, "FailedEntries": []any{}}}, nil
	case "PutEvents":
		entries, _ := req.Input["Entries"].([]any)
		if err := validatePutEvents(entries); err != nil {
			return nil, err
		}
		return p.putEvents(ctx, req, entries), nil
	default:
		return p.extra(ctx, req)
	}
}

func validEventName(value string) bool {
	if len(value) < 1 || len(value) > 64 {
		return false
	}
	for _, r := range value {
		if !((r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || strings.ContainsRune(".-_", r)) {
			return false
		}
	}
	return true
}

func validatePutEvents(entries []any) error {
	if len(entries) < 1 || len(entries) > 10 {
		return validationFault("Entries must contain between 1 and 10 items.")
	}
	total := 0
	for _, raw := range entries {
		entry, _ := raw.(map[string]any)
		if len(str(entry["DetailType"])) > 128 || len(str(entry["TraceHeader"])) > 500 {
			return validationFault("Event entry field exceeds its maximum length.")
		}
		total += len([]byte(str(entry["Source"]))) + len([]byte(str(entry["DetailType"]))) + len([]byte(str(entry["Detail"])))
		if entry["Time"] != nil {
			total += 14
		}
		for _, resource := range asSlice(entry["Resources"]) {
			value := str(resource)
			if len(value) > 2048 {
				return validationFault("Resource exceeds 2048 characters.")
			}
			total += len([]byte(value))
		}
	}
	if total >= 1024*1024 {
		return validationFault("Total event entry size must be less than 1 MB.")
	}
	return nil
}

func validateTarget(target map[string]any) string {
	id, arn := str(target["Id"]), str(target["Arn"])
	if !validEventName(id) || len(arn) < 1 || len(arn) > 1600 {
		return "Id or Arn is invalid."
	}
	inputs := 0
	for _, key := range []string{"Input", "InputPath", "InputTransformer"} {
		if _, specified := target[key]; specified {
			inputs++
		}
	}
	if inputs > 1 {
		return "Input, InputPath, and InputTransformer are mutually exclusive."
	}
	if input, specified := target["Input"]; specified {
		value := str(input)
		if len(value) > 8192 || !json.Valid([]byte(value)) {
			return "Input must be valid JSON and no longer than 8192 characters."
		}
	}
	if len(str(target["InputPath"])) > 256 {
		return "InputPath exceeds 256 characters."
	}
	return validateTargetReliability(target)
}

func (p *Pack) targets(ctx context.Context, req *spi.Request, key string) []any {
	return p.targetsFor(ctx, req.Identity, key)
}

func (p *Pack) targetsFor(ctx context.Context, identity spi.Identity, key string) []any {
	b, ok, _ := p.deps.Store.Scope(identity.Account, identity.Region).Collection("targets").Get(ctx, key)
	if !ok {
		return []any{}
	}
	var targets []any
	_ = json.Unmarshal(b, &targets)
	return targets
}

func eventBus(in map[string]any) string {
	return eventBusName(str(in["EventBusName"]))
}

func eventBusName(bus string) string {
	if _, name, ok := strings.Cut(bus, ":event-bus/"); ok {
		return name
	}
	if bus == "" {
		return "default"
	}
	return bus
}

func eventKey(bus, name string) string { return bus + "\x00" + name }

func eventName(key string) string {
	if _, name, ok := strings.Cut(key, "\x00"); ok {
		return name
	}
	return key
}

func clone(in map[string]any) map[string]any {
	out := make(map[string]any, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func (p *Pack) putEvents(ctx context.Context, req *spi.Request, entries []any) *spi.Response {
	results := make([]any, 0, len(entries))
	failed := 0
	for _, raw := range entries {
		entry, _ := raw.(map[string]any)
		if str(entry["Source"]) == "" || str(entry["DetailType"]) == "" || str(entry["Detail"]) == "" {
			failed++
			results = append(results, map[string]any{"ErrorCode": "ValidationException", "ErrorMessage": "Source, DetailType and Detail are required."})
			continue
		}
		var detail any
		if json.Unmarshal([]byte(str(entry["Detail"])), &detail) != nil {
			failed++
			results = append(results, map[string]any{"ErrorCode": "MalformedDetail", "ErrorMessage": "Detail is not valid JSON."})
			continue
		}
		id := p.deps.Rand.UUID()
		event := map[string]any{
			"version": "0", "id": id, "detail-type": entry["DetailType"], "source": entry["Source"],
			"account": req.Identity.Account, "time": eventTime(entry["Time"], p.deps.Clock.Now()),
			"region": req.Identity.Region, "resources": entry["Resources"], "detail": detail,
		}
		if event["resources"] == nil {
			event["resources"] = []any{}
		}
		p.fanout(ctx, req, event, str(entry["EventBusName"]))
		results = append(results, map[string]any{"EventId": id})
	}
	return &spi.Response{Output: map[string]any{"FailedEntryCount": failed, "Entries": results}}
}

func eventTime(v any, fallback time.Time) string {
	switch t := v.(type) {
	case time.Time:
		return t.UTC().Format(time.RFC3339)
	case string:
		if t != "" {
			return t
		}
	}
	return fallback.UTC().Format(time.RFC3339)
}

func (p *Pack) fanout(ctx context.Context, req *spi.Request, event map[string]any, bus string) {
	raw, _ := json.Marshal(event)
	_ = p.deps.Bus.Publish(ctx, "events", raw)
	kvs, _, _ := p.col(req, "targets").List(ctx, "", "", 0)
	for _, kv := range kvs {
		rule, ok := p.load(ctx, req, "rules", kv.Key)
		if !ok || !ruleMatches(rule, bus, event) {
			continue
		}
		p.deliverRule(ctx, req.Identity, kv.Key, event, raw)
	}
}

func (p *Pack) deliverRule(ctx context.Context, identity spi.Identity, ruleKey string, event map[string]any, raw []byte) {
	for _, target := range p.targetsFor(ctx, identity, ruleKey) {
		m, _ := target.(map[string]any)
		arn := str(m["Arn"])
		if arn == "" {
			continue
		}
		payload := targetPayload(m, event, raw)
		_ = p.deps.Bus.Publish(ctx, "events:"+arn, payload)
		if err := DeliverTarget(ctx, p.deps, identity, arn, m, payload); err != nil {
			p.failedTarget(ctx, identity, ruleKey, m, payload, err)
		}
	}
}

func (p *Pack) failedTarget(ctx context.Context, identity spi.Identity, ruleKey string, target map[string]any, payload []byte, deliveryErr error) {
	rec := map[string]any{
		"RuleArn": ruleARN(identity, ruleKey), "Target": target, "Payload": string(payload),
		eventRetryAttempts: 0, eventRetryStarted: p.deps.Clock.Now().UTC().Format(time.RFC3339Nano),
	}
	if !TargetErrorRetryable(deliveryErr) {
		p.deadLetter(ctx, identity, rec, target, payload, 0, "", deliveryErr)
		return
	}
	key := p.deps.Rand.UUID()
	if _, retrying := p.retryTarget(ctx, identity, key, rec, target, payload, deliveryErr); retrying {
		p.notify()
	}
}

func (p *Pack) retryLoop() {
	defer close(p.done)
	for {
		next := earlierTime(p.runRetries(context.Background()), p.runScheduledRules(context.Background()))
		if next.IsZero() {
			select {
			case <-p.wake:
			case <-p.stop:
				return
			}
			continue
		}
		select {
		case <-p.deps.Clock.AfterUntil(next):
		case <-p.wake:
		case <-p.stop:
			return
		}
	}
}

func (p *Pack) runScheduledRules(ctx context.Context) time.Time {
	now := p.deps.Clock.Now()
	var earliest time.Time
	scopes, err := p.deps.Store.Scopes(ctx)
	if err != nil {
		return earliest
	}
	for _, identity := range scopes {
		collection := p.deps.Store.Scope(identity.Account, identity.Region).Collection("rules")
		kvs, _, err := collection.List(ctx, "", "", 0)
		if err != nil {
			continue
		}
		for _, kv := range kvs {
			var rule map[string]any
			if json.Unmarshal(kv.Value, &rule) != nil || str(rule["State"]) == "DISABLED" || str(rule["ScheduleExpression"]) == "" {
				continue
			}
			next := parsedTime(rule[eventRuleNext])
			if next.IsZero() {
				next, err = nextScheduledRule(str(rule["ScheduleExpression"]), now)
				if err != nil {
					continue
				}
				rule[eventRuleNext] = next.UTC().Format(time.RFC3339Nano)
				body, _ := json.Marshal(rule)
				_ = collection.Put(ctx, kv.Key, body)
			}
			if next.After(now) {
				earliest = earlierTime(earliest, next)
				continue
			}
			event := map[string]any{
				"version": "0", "id": p.deps.Rand.UUID(), "detail-type": "Scheduled Event", "source": "aws.events",
				"account": identity.Account, "time": next.UTC().Format(time.RFC3339), "region": identity.Region,
				"resources": []any{ruleARN(identity, kv.Key)}, "detail": map[string]any{},
			}
			raw, _ := json.Marshal(event)
			_ = p.deps.Bus.Publish(ctx, "events", raw)
			p.deliverRule(ctx, identity, kv.Key, event, raw)
			next, err = nextScheduledRule(str(rule["ScheduleExpression"]), next)
			if err != nil {
				continue
			}
			rule[eventRuleNext] = next.UTC().Format(time.RFC3339Nano)
			body, _ := json.Marshal(rule)
			_ = collection.Put(ctx, kv.Key, body)
			earliest = earlierTime(earliest, next)
		}
	}
	return earliest
}

func nextScheduledRule(raw string, after time.Time) (time.Time, error) {
	expression, err := scheduleexpr.Parse(raw, "UTC")
	if err != nil {
		return time.Time{}, err
	}
	if expression.OneTime() {
		return time.Time{}, errors.New("legacy EventBridge rules support only rate and cron expressions")
	}
	return expression.After(after), nil
}

func (p *Pack) runRetries(ctx context.Context) time.Time {
	now := p.deps.Clock.Now()
	var earliest time.Time
	scopes, err := p.deps.Store.Scopes(ctx)
	if err != nil {
		return earliest
	}
	for _, identity := range scopes {
		collection := p.deps.Store.Scope(identity.Account, identity.Region).Collection("event-retries")
		kvs, _, err := collection.List(ctx, "", "", 0)
		if err != nil {
			continue
		}
		for _, kv := range kvs {
			var rec map[string]any
			if json.Unmarshal(kv.Value, &rec) != nil {
				continue
			}
			next := parsedTime(rec[eventRetryNext])
			if next.After(now) {
				earliest = earlierTime(earliest, next)
				continue
			}
			target, _ := rec["Target"].(map[string]any)
			payload := []byte(str(rec["Payload"]))
			policy, _ := target["RetryPolicy"].(map[string]any)
			maxAge, ok := integerValue(policy["MaximumEventAgeInSeconds"])
			if !ok {
				maxAge = 86400
			}
			if started := parsedTime(rec[eventRetryStarted]); !started.IsZero() && !now.Before(started.Add(time.Duration(maxAge)*time.Second)) {
				p.deadLetter(ctx, identity, rec, target, payload, max(intValue(rec[eventRetryAttempts])-1, 0), "MaximumEventAgeInSeconds", storedTargetError(rec))
				_ = collection.Delete(ctx, kv.Key)
				continue
			}
			deliveryErr := DeliverTarget(ctx, p.deps, identity, str(target["Arn"]), target, payload)
			if deliveryErr == nil {
				_ = collection.Delete(ctx, kv.Key)
				continue
			}
			attempts := intValue(rec[eventRetryAttempts])
			if !TargetErrorRetryable(deliveryErr) {
				p.deadLetter(ctx, identity, rec, target, payload, attempts, "", deliveryErr)
				_ = collection.Delete(ctx, kv.Key)
				continue
			}
			if retryAt, retrying := p.retryTarget(ctx, identity, kv.Key, rec, target, payload, deliveryErr); retrying {
				earliest = earlierTime(earliest, retryAt)
			} else {
				_ = collection.Delete(ctx, kv.Key)
			}
		}
	}
	return earliest
}

func (p *Pack) retryTarget(ctx context.Context, identity spi.Identity, key string, rec, target map[string]any, payload []byte, deliveryErr error) (time.Time, bool) {
	policy, _ := target["RetryPolicy"].(map[string]any)
	maxAttempts, ok := integerValue(policy["MaximumRetryAttempts"])
	if !ok {
		maxAttempts = 185
	}
	maxAge, ok := integerValue(policy["MaximumEventAgeInSeconds"])
	if !ok {
		maxAge = 86400
	}
	now := p.deps.Clock.Now()
	started := parsedTime(rec[eventRetryStarted])
	if started.IsZero() {
		started = now
	}
	attempts := intValue(rec[eventRetryAttempts])
	exhausted := ""
	if !now.Before(started.Add(time.Duration(maxAge) * time.Second)) {
		exhausted = "MaximumEventAgeInSeconds"
	} else if attempts >= maxAttempts {
		exhausted = "MaximumRetryAttempts"
	}
	if exhausted != "" {
		p.deadLetter(ctx, identity, rec, target, payload, attempts, exhausted, deliveryErr)
		return time.Time{}, false
	}
	attempts++
	headerAt, specified := targetRetryAfter(deliveryErr, now)
	if specified && headerAt.IsZero() {
		p.deadLetter(ctx, identity, rec, target, payload, attempts-1, "", deliveryErr)
		return time.Time{}, false
	}
	delay := time.Second << min(attempts-1, 8)
	seconds := p.deps.Rand.Derive(key + "|retry|" + strconv.Itoa(attempts)).Intn(int(delay/time.Second) + 1)
	if seconds == 0 {
		seconds = 1
	}
	retryAt := now.Add(time.Duration(seconds) * time.Second)
	if specified && headerAt.After(retryAt) {
		retryAt = headerAt
	}
	expires := started.Add(time.Duration(maxAge) * time.Second)
	if retryAt.After(expires) {
		retryAt = expires
	}
	rec[eventRetryAttempts], rec[eventRetryStarted], rec[eventRetryNext] = attempts, started.UTC().Format(time.RFC3339Nano), retryAt.UTC().Format(time.RFC3339Nano)
	storeTargetError(rec, deliveryErr)
	b, _ := json.Marshal(rec)
	_ = p.deps.Store.Scope(identity.Account, identity.Region).Collection("event-retries").Put(ctx, key, b)
	return retryAt, true
}

func (p *Pack) deadLetter(ctx context.Context, identity spi.Identity, rec, target map[string]any, payload []byte, attempts int, exhausted string, deliveryErr error) {
	config, _ := target["DeadLetterConfig"].(map[string]any)
	arn := str(config["Arn"])
	if arn == "" {
		return
	}
	code, message := targetError(deliveryErr)
	values := map[string]string{
		"RULE_ARN": str(rec["RuleArn"]), "TARGET_ARN": str(target["Arn"]), "ERROR_CODE": code,
		"ERROR_MESSAGE": message, "RETRY_ATTEMPTS": strconv.Itoa(attempts),
	}
	if exhausted != "" {
		values["EXHAUSTED_RETRY_CONDITION"] = exhausted
	}
	attributes := map[string]any{}
	for key, value := range values {
		attributes[key] = map[string]any{"DataType": "String", "StringValue": value}
	}
	parts := strings.Split(arn, ":")
	dlqIdentity := identity
	if len(parts) > 4 {
		dlqIdentity = spi.Identity{Region: parts[3], Account: parts[4]}
	}
	_, _ = sqs.New(p.deps).Invoke(ctx, &spi.Request{Identity: dlqIdentity, Operation: "SendMessage", Input: map[string]any{
		"QueueName": arn[lastColon(arn)+1:], "MessageBody": string(payload), "MessageAttributes": attributes,
	}})
}

func (p *Pack) notify() {
	select {
	case p.wake <- struct{}{}:
	default:
	}
}

func ruleARN(identity spi.Identity, key string) string {
	bus, name, _ := strings.Cut(key, "\x00")
	path := name
	if bus != "default" {
		path = bus + "/" + name
	}
	return "arn:aws:events:" + identity.Region + ":" + identity.Account + ":rule/" + path
}

func parsedTime(value any) time.Time {
	parsed, _ := time.Parse(time.RFC3339Nano, str(value))
	return parsed
}

func earlierTime(a, b time.Time) time.Time {
	if a.IsZero() || (!b.IsZero() && b.Before(a)) {
		return b
	}
	return a
}

func integerValue(value any) (int, bool) {
	switch value := value.(type) {
	case int:
		return value, true
	case float64:
		if value != float64(int(value)) {
			return 0, false
		}
		return int(value), true
	}
	return 0, false
}

func intValue(value any) int {
	integer, _ := integerValue(value)
	return integer
}

func targetRetryAfter(deliveryErr error, now time.Time) (time.Time, bool) {
	fault, ok := deliveryErr.(*spi.Fault)
	if !ok {
		return time.Time{}, false
	}
	raw := str(fault.Fields["RetryAfter"])
	if raw == "" {
		return time.Time{}, false
	}
	if seconds, err := strconv.Atoi(raw); err == nil {
		if seconds < 0 {
			return time.Time{}, true
		}
		return now.Add(time.Duration(seconds) * time.Second), true
	}
	at, err := http.ParseTime(raw)
	if err != nil {
		return time.Time{}, false
	}
	if at.Before(now) {
		at = now
	}
	return at, true
}

// TargetErrorRetryable reports whether source-specific retry handling should retry a target failure.
func TargetErrorRetryable(deliveryErr error) bool {
	fault, ok := deliveryErr.(*spi.Fault)
	if !ok {
		return true
	}
	return fault.HTTPStatus == 401 || fault.HTTPStatus == 407 || fault.HTTPStatus == 409 || fault.HTTPStatus == 429 || fault.HTTPStatus >= 500
}

func targetError(deliveryErr error) (string, string) {
	fault, ok := deliveryErr.(*spi.Fault)
	if !ok {
		return "SDK_CLIENT_ERROR", deliveryErr.Error()
	}
	switch {
	case fault.HTTPStatus == 429:
		return "THROTTLING", deliveryErr.Error()
	case fault.Code == "TargetInvocationFailed":
		return "ERROR_FROM_TARGET", deliveryErr.Error()
	case strings.Contains(strings.ToLower(fault.Code), "notfound") || strings.Contains(strings.ToLower(fault.Code), "nonexistent"):
		return "NO_RESOURCE", deliveryErr.Error()
	case fault.HTTPStatus >= 500:
		return "INTERNAL_ERROR", deliveryErr.Error()
	case fault.HTTPStatus >= 400:
		return "INVALID_PARAMETER", deliveryErr.Error()
	default:
		return "UNKNOWN", deliveryErr.Error()
	}
}

func storeTargetError(rec map[string]any, deliveryErr error) {
	stored := map[string]any{"Message": deliveryErr.Error()}
	if fault, ok := deliveryErr.(*spi.Fault); ok {
		stored["Message"] = fault.Message
		stored["Code"], stored["HTTPStatus"], stored["Fault"], stored["Fields"] = fault.Code, fault.HTTPStatus, fault.Fault, fault.Fields
	}
	rec["_mirrorLastError"] = stored
}

func storedTargetError(rec map[string]any) error {
	stored, _ := rec["_mirrorLastError"].(map[string]any)
	if code := str(stored["Code"]); code != "" {
		fields, _ := stored["Fields"].(map[string]any)
		return &spi.Fault{Code: code, Message: str(stored["Message"]), HTTPStatus: intValue(stored["HTTPStatus"]), Fault: str(stored["Fault"]), Fields: fields}
	}
	return &spi.Fault{Code: "InternalError", Message: str(stored["Message"]), HTTPStatus: 500, Fault: "server"}
}

func validateTargetReliability(target map[string]any) string {
	if raw, exists := target["RetryPolicy"]; exists {
		policy, ok := raw.(map[string]any)
		if !ok {
			return "RetryPolicy must be an object."
		}
		if raw, exists := policy["MaximumEventAgeInSeconds"]; exists {
			age, ok := integerValue(raw)
			if !ok || age < 60 || age > 86400 {
				return "MaximumEventAgeInSeconds must be between 60 and 86400."
			}
		}
		if raw, exists := policy["MaximumRetryAttempts"]; exists {
			attempts, ok := integerValue(raw)
			if !ok || attempts < 0 || attempts > 185 {
				return "MaximumRetryAttempts must be between 0 and 185."
			}
		}
	}
	if raw, exists := target["DeadLetterConfig"]; exists {
		config, ok := raw.(map[string]any)
		arn := str(config["Arn"])
		if !ok || (arn != "" && (!strings.Contains(arn, ":sqs:") || strings.HasSuffix(arn, ".fifo"))) {
			return "DeadLetterConfig Arn must identify a standard SQS queue."
		}
	}
	return ""
}

func targetPayload(target, event map[string]any, fallback []byte) []byte {
	if input := str(target["Input"]); input != "" {
		return []byte(input)
	}
	if path := str(target["InputPath"]); path != "" {
		if raw, err := json.Marshal(eventPath(event, path)); err == nil {
			return raw
		}
	}
	transformer, _ := target["InputTransformer"].(map[string]any)
	template := str(transformer["InputTemplate"])
	paths, _ := transformer["InputPathsMap"].(map[string]any)
	if template != "" {
		for key, path := range paths {
			raw, _ := json.Marshal(eventPath(event, str(path)))
			template = strings.ReplaceAll(template, "<"+key+">", string(raw))
		}
		return []byte(template)
	}
	return fallback
}

func eventPath(event any, path string) any {
	return EventPath(event, path)
}

// EventPath returns the value selected by the Pipes/EventBridge JSON-path subset.
func EventPath(event any, path string) any {
	if path == "$" {
		return event
	}
	tokens := strings.Split(strings.Trim(strings.NewReplacer("[", ".", "]", "").Replace(path), "$."), ".")
	return eventPathTokens(event, tokens)
}

func eventPathTokens(value any, tokens []string) any {
	if len(tokens) == 0 {
		return value
	}
	switch value := value.(type) {
	case map[string]any:
		return eventPathTokens(value[tokens[0]], tokens[1:])
	case []any:
		if tokens[0] == "*" {
			out := make([]any, 0, len(value))
			for _, item := range value {
				out = append(out, eventPathTokens(item, tokens[1:]))
			}
			return out
		}
		index, err := strconv.Atoi(tokens[0])
		if err != nil || index < 0 || index >= len(value) {
			return nil
		}
		return eventPathTokens(value[index], tokens[1:])
	}
	return nil
}

func ruleMatches(rule map[string]any, bus string, event map[string]any) bool {
	if str(rule["State"]) == "DISABLED" {
		return false
	}
	ruleBus := eventBusName(str(rule["EventBusName"]))
	bus = eventBusName(bus)
	pattern := str(rule["EventPattern"])
	return ruleBus == bus && pattern != "" && matchEventPattern(pattern, event)
}

// DeliverTarget invokes a templated EventBridge or Scheduler target.
func DeliverTarget(ctx context.Context, deps spi.Deps, identity spi.Identity, arn string, target map[string]any, payload []byte) error {
	switch {
	case strings.Contains(arn, ":sqs:"):
		in := map[string]any{"QueueName": arn[lastColon(arn)+1:], "MessageBody": string(payload)}
		if params, ok := target["SqsParameters"].(map[string]any); ok {
			in["MessageGroupId"] = params["MessageGroupId"]
		}
		_, err := sqs.New(deps).Invoke(ctx, &spi.Request{Identity: identity, Operation: "SendMessage", Input: in})
		return err
	case strings.Contains(arn, ":sns:"):
		_, err := sns.New(deps).Invoke(ctx, &spi.Request{Identity: identity, Operation: "Publish", Input: map[string]any{"TopicArn": arn, "Message": string(payload)}})
		return err
	case strings.Contains(arn, ":lambda:"):
		_, name, ok := strings.Cut(arn, ":function:")
		if !ok {
			return &spi.Fault{Code: "ValidationException", Message: "Invalid Lambda target ARN.", HTTPStatus: 400, Fault: "client"}
		}
		if i := strings.IndexByte(name, ':'); i >= 0 {
			name = name[:i]
		}
		in := map[string]any{}
		if json.Unmarshal(payload, &in) != nil {
			in = map[string]any{}
			in["input"] = string(payload)
		}
		in["FunctionName"] = name
		in["InvocationType"] = "Event"
		_, err := lambda.New(deps).Invoke(ctx, &spi.Request{Identity: identity, Operation: "Invoke", Input: in, Body: io.NopCloser(bytes.NewReader(payload))})
		return err
	case strings.Contains(arn, ":states:"):
		parameters, _ := target["StateMachineParameters"].(map[string]any)
		invocation := str(parameters["InvocationType"])
		operation := "StartExecution"
		if invocation == "REQUEST_RESPONSE" {
			operation = "StartSyncExecution"
		} else if invocation != "" && invocation != "FIRE_AND_FORGET" {
			return &spi.Fault{Code: "ValidationException", Message: "Invalid Step Functions invocation type.", HTTPStatus: 400, Fault: "client"}
		}
		response, err := states.New(deps).Invoke(ctx, &spi.Request{Identity: identity, Operation: operation, Input: map[string]any{"stateMachineArn": arn, "input": string(payload)}})
		if err != nil {
			return err
		}
		if operation == "StartSyncExecution" && str(response.Output["status"]) != "SUCCEEDED" {
			return &spi.Fault{Code: "TargetInvocationFailed", Message: str(response.Output["cause"]), HTTPStatus: 500, Fault: "server"}
		}
		return nil
	case strings.Contains(arn, ":api-destination/"):
		parameters, _ := target["HttpParameters"].(map[string]any)
		_, err := InvokeAPIDestination(ctx, deps, identity, arn, parameters, payload)
		return err
	}
	return &spi.Fault{Code: "ValidationException", Message: "Unsupported target ARN.", HTTPStatus: 400, Fault: "client"}
}

// InvokeAPIDestination invokes a stored EventBridge API destination and returns its response body.
func InvokeAPIDestination(ctx context.Context, deps spi.Deps, identity spi.Identity, arn string, parameters map[string]any, payload []byte) ([]byte, error) {
	destination, ok := eventRecord(ctx, deps, identity, "apidest", arnResourceName(arn, "api-destination/"))
	if !ok {
		return nil, &spi.Fault{Code: "ResourceNotFoundException", Message: "API destination does not exist.", HTTPStatus: 404, Fault: "client"}
	}
	connection, ok := eventRecord(ctx, deps, identity, "connections", arnResourceName(str(destination["ConnectionArn"]), "connection/"))
	if !ok {
		return nil, &spi.Fault{Code: "ResourceNotFoundException", Message: "Connection does not exist.", HTTPStatus: 404, Fault: "client"}
	}
	if err := waitAPIDestinationRate(ctx, deps, identity, arnResourceName(arn, "api-destination/"), destination); err != nil {
		return nil, err
	}
	endpoint := str(destination["InvocationEndpoint"])
	for _, value := range anySlice(parameters["PathParameterValues"]) {
		endpoint = strings.Replace(endpoint, "*", url.PathEscape(str(value)), 1)
	}
	headers := map[string]any{"Content-Type": "application/json; charset=utf-8"}
	for key, value := range mapValue(parameters["HeaderParameters"]) {
		headers[key] = value
	}
	result, err := eventhttp.Invoke(ctx, connection, eventhttp.Call{
		Endpoint: endpoint, Method: str(destination["HttpMethod"]), Body: payload,
		Headers: headers, Query: mapValue(parameters["QueryStringParameters"]),
		Timeout: 5 * time.Second, MaxRequestBytes: 64 << 10, MaxResponseBytes: 6 << 20,
		UserAgent: "Amazon/EventBridge/ApiDestinations", Range: "bytes=0-1048575", RevealSecrets: true,
	})
	if err != nil {
		return nil, err
	}
	if result.StatusCode < 200 || result.StatusCode >= 300 {
		fault := &spi.Fault{Code: "TargetInvocationFailed", Message: result.Status, HTTPStatus: result.StatusCode, Fault: "server"}
		if retryAfter := result.Headers.Get("Retry-After"); retryAfter != "" {
			fault.Fields = map[string]any{"RetryAfter": retryAfter}
		}
		return nil, fault
	}
	return result.Body, nil
}

func apiDestinationRetryable(status int) bool {
	return status == 401 || status == 407 || status == 409 || status == 429 || status >= 500
}

type apiDestinationRateState struct {
	Window int64
	Count  int
}

func waitAPIDestinationRate(ctx context.Context, deps spi.Deps, identity spi.Identity, name string, destination map[string]any) error {
	rate, err := strconv.Atoi(toString(destination["InvocationRateLimitPerSecond"]))
	if err != nil || rate < 1 {
		rate = 300
	}
	now := deps.Clock.Now()
	wait := time.Duration(0)
	// ponytail: fixed one-second windows; use a token bucket if burst smoothing becomes observable.
	err = deps.Store.Scope(identity.Account, identity.Region).Collection("apidest-rate").Txn(ctx, func(tx spi.Tx) error {
		state := apiDestinationRateState{Window: now.Truncate(time.Second).UnixNano()}
		if body, ok, getErr := tx.Get(name); getErr != nil {
			return getErr
		} else if ok {
			_ = json.Unmarshal(body, &state)
		}
		window := time.Unix(0, state.Window)
		if !now.Before(window.Add(time.Second)) {
			window, state.Count = now.Truncate(time.Second), 0
		} else if state.Count >= rate {
			window, state.Count = window.Add(time.Second), 0
		}
		state.Window, state.Count = window.UnixNano(), state.Count+1
		wait = max(window.Sub(now), 0)
		body, _ := json.Marshal(state)
		return tx.Put(name, body)
	})
	if err != nil || wait == 0 {
		return err
	}
	select {
	case <-deps.Clock.After(wait):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func eventRecord(ctx context.Context, deps spi.Deps, identity spi.Identity, collection, name string) (map[string]any, bool) {
	body, ok, _ := deps.Store.Scope(identity.Account, identity.Region).Collection(collection).Get(ctx, name)
	var record map[string]any
	if !ok || json.Unmarshal(body, &record) != nil {
		return nil, false
	}
	return record, true
}

func arnResourceName(arn, marker string) string {
	_, resource, _ := strings.Cut(arn, marker)
	name, _, _ := strings.Cut(resource, "/")
	return name
}

func mergeConnectionBody(payload []byte, raw any) ([]byte, error) {
	return eventhttp.MergeBody(payload, raw, 64<<10)
}

func anySlice(value any) []any {
	values, _ := value.([]any)
	return values
}

func mapValue(value any) map[string]any { values, _ := value.(map[string]any); return values }

func str(v any) string { s, _ := v.(string); return s }

func lastColon(s string) int {
	for i := len(s) - 1; i >= 0; i-- {
		if s[i] == ':' {
			return i
		}
	}
	return -1
}
