// Package events is EventBridge emulate: rules, targets, PutEvents to SQS/SNS.
package events

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"strings"
	"time"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/lambda"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/sns"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/sqs"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func init() {
	registry.Register(registry.Factory{ServiceID: "aws.events", Tier: model.TierEmulate, New: func(d spi.Deps) (spi.BehaviorPack, error) {
		return New(d), nil
	}})
}

// Pack implements EventBridge.
type Pack struct{ deps spi.Deps }

// New constructs the pack.
func New(d spi.Deps) *Pack { return &Pack{deps: d} }

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
		if name == "" {
			return nil, &spi.Fault{Code: "ValidationException", Message: "Name is required.", HTTPStatus: 400, Fault: "client"}
		}
		if pattern := str(req.Input["EventPattern"]); pattern != "" && !json.Valid([]byte(pattern)) {
			return nil, &spi.Fault{Code: "InvalidEventPatternException", Message: "EventPattern is not valid JSON.", HTTPStatus: 400, Fault: "client"}
		}
		bus := eventBus(req.Input)
		rec := clone(req.Input)
		if str(rec["State"]) == "" {
			rec["State"] = "ENABLED"
		}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "rules").Put(ctx, eventKey(bus, name), b)
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
				rs = append(rs, m)
			}
		}
		return &spi.Response{Output: map[string]any{"Rules": rs}}, nil
	case "DeleteRule":
		_ = p.col(req, "rules").Delete(ctx, eventKey(eventBus(req.Input), str(req.Input["Name"])))
		return &spi.Response{Output: map[string]any{}}, nil
	case "PutTargets":
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
		for _, raw := range asSlice(req.Input["Targets"]) {
			target, _ := raw.(map[string]any)
			id, arn := str(target["Id"]), str(target["Arn"])
			if id == "" || arn == "" {
				failed = append(failed, map[string]any{"TargetId": id, "ErrorCode": "ValidationException", "ErrorMessage": "Id and Arn are required."})
				continue
			}
			if i, ok := byID[id]; ok {
				targets[i] = target
			} else {
				byID[id] = len(targets)
				targets = append(targets, target)
			}
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
		return p.putEvents(ctx, req, entries), nil
	default:
		return p.extra(ctx, req)
	}
}

func (p *Pack) targets(ctx context.Context, req *spi.Request, key string) []any {
	b, ok, _ := p.col(req, "targets").Get(ctx, key)
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
		var tgs []any
		_ = json.Unmarshal(kv.Value, &tgs)
		for _, t := range tgs {
			m, _ := t.(map[string]any)
			arn := str(m["Arn"])
			if arn == "" {
				continue
			}
			payload := targetPayload(m, event, raw)
			_ = p.deps.Bus.Publish(ctx, "events:"+arn, payload)
			_ = DeliverTarget(ctx, p.deps, req.Identity, arn, m, payload)
		}
	}
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
	if path == "$" {
		return event
	}
	cur := event
	for _, key := range strings.Split(strings.TrimPrefix(path, "$."), ".") {
		m, ok := cur.(map[string]any)
		if !ok {
			return nil
		}
		cur = m[key]
	}
	return cur
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
	}
	return &spi.Fault{Code: "ValidationException", Message: "Unsupported target ARN.", HTTPStatus: 400, Fault: "client"}
}

func str(v any) string { s, _ := v.(string); return s }

func lastColon(s string) int {
	for i := len(s) - 1; i >= 0; i-- {
		if s[i] == ':' {
			return i
		}
	}
	return -1
}
