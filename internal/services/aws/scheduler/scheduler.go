// Package scheduler emulates EventBridge Scheduler groups, schedules, and templated target delivery.
package scheduler

import (
	"context"
	"encoding/json"
	"math"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/events"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

const nextInvocation = "_mirrorNextInvocation"

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
	if !ok || first(target, "Arn", "arn") == "" || first(target, "RoleArn", "roleArn") == "" {
		return nil, validation("Target Arn and RoleArn are required.")
	}
	targetARN := first(target, "Arn", "arn")
	if payload := first(target, "Input", "input"); payload != "" && requiresJSON(targetARN) && !json.Valid([]byte(payload)) {
		return nil, validation("Target Input must be valid JSON.")
	}
	window, ok := input["FlexibleTimeWindow"].(map[string]any)
	mode := first(window, "Mode", "mode")
	if !ok || (mode != "OFF" && mode != "FLEXIBLE") {
		return nil, validation("FlexibleTimeWindow Mode must be OFF or FLEXIBLE.")
	}
	if mode == "FLEXIBLE" {
		minutes := number(window["MaximumWindowInMinutes"])
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
	start, _ := inputTime(input["StartDate"])
	end, hasEnd := inputTime(input["EndDate"])
	if hasEnd && !start.IsZero() && end.Before(start) {
		return nil, validation("EndDate must not precede StartDate.")
	}
	rec := clone(input)
	rec["Name"], rec["GroupName"], rec["Arn"], rec["State"] = name, group, arn, state
	next := expr.first(p.deps.Clock.Now(), start)
	if expr.at.IsZero() && hasEnd && next.After(end) {
		next = time.Time{}
	}
	rec[nextInvocation] = formatTime(next)
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
				payload := []byte(first(target, "Input", "input"))
				if len(payload) == 0 {
					payload = []byte("{}")
				}
				_ = events.DeliverTarget(ctx, p.deps, identity, first(target, "Arn", "arn"), target, payload)
				expr, err := parseScheduleExpression(stringValue(rec["ScheduleExpression"]), stringValue(rec["ScheduleExpressionTimezone"]))
				if err != nil {
					continue
				}
				next = expr.after(now)
				if end, ok := inputTime(rec["EndDate"]); expr.at.IsZero() && ok && next.After(end) {
					next = time.Time{}
				}
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
	return expr.first(now, start)
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
	return strings.Contains(arn, ":lambda:") || strings.Contains(arn, ":states:") || strings.Contains(arn, ":events:") || strings.Contains(arn, ":scheduler:::aws-sdk:")
}

func number(value any) float64 {
	switch value := value.(type) {
	case float64:
		return value
	case int:
		return float64(value)
	}
	return 0
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

var _ interface{ Close() error } = (*Pack)(nil)
