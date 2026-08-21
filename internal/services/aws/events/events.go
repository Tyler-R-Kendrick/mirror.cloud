// Package events is EventBridge emulate: rules, targets, PutEvents to SQS/SNS.
package events

import (
	"context"
	"encoding/json"
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
		b, _ := json.Marshal(req.Input)
		_ = p.col(req, "rules").Put(ctx, name, b)
		arn := "arn:aws:events:" + req.Identity.Region + ":" + req.Identity.Account + ":rule/" + name
		return &spi.Response{Output: map[string]any{"RuleArn": arn}}, nil
	case "ListRules":
		kvs, _, _ := p.col(req, "rules").List(ctx, "", "", 0)
		var rs []any
		for _, kv := range kvs {
			var m map[string]any
			_ = json.Unmarshal(kv.Value, &m)
			rs = append(rs, m)
		}
		return &spi.Response{Output: map[string]any{"Rules": rs}}, nil
	case "DeleteRule":
		_ = p.col(req, "rules").Delete(ctx, str(req.Input["Name"]))
		return &spi.Response{Output: map[string]any{}}, nil
	case "PutTargets":
		rule := str(req.Input["Rule"])
		b, _ := json.Marshal(req.Input["Targets"])
		_ = p.col(req, "targets").Put(ctx, rule, b)
		return &spi.Response{Output: map[string]any{"FailedEntryCount": 0, "FailedEntries": []any{}}}, nil
	case "ListTargetsByRule":
		b, ok, _ := p.col(req, "targets").Get(ctx, str(req.Input["Rule"]))
		var tg any = []any{}
		if ok {
			_ = json.Unmarshal(b, &tg)
		}
		return &spi.Response{Output: map[string]any{"Targets": tg}}, nil
	case "RemoveTargets":
		_ = p.col(req, "targets").Delete(ctx, str(req.Input["Rule"]))
		return &spi.Response{Output: map[string]any{"FailedEntryCount": 0}}, nil
	case "PutEvents":
		entries, _ := req.Input["Entries"].([]any)
		return p.putEvents(ctx, req, entries), nil
	default:
		return p.extra(ctx, req)
	}
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
			payload := raw
			if input := str(m["Input"]); input != "" {
				payload = []byte(input)
			}
			_ = p.deps.Bus.Publish(ctx, "events:"+arn, payload)
			p.deliver(ctx, req, arn, m, payload)
		}
	}
}

func ruleMatches(rule map[string]any, bus string, event map[string]any) bool {
	if str(rule["State"]) == "DISABLED" {
		return false
	}
	ruleBus := str(rule["EventBusName"])
	if ruleBus == "" {
		ruleBus = "default"
	}
	if bus == "" {
		bus = "default"
	}
	pattern := str(rule["EventPattern"])
	return ruleBus == bus && pattern != "" && matchEventPattern(pattern, event)
}

func (p *Pack) deliver(ctx context.Context, req *spi.Request, arn string, target map[string]any, payload []byte) {
	switch {
	case strings.Contains(arn, ":sqs:"):
		in := map[string]any{"QueueName": arn[lastColon(arn)+1:], "MessageBody": string(payload)}
		if params, ok := target["SqsParameters"].(map[string]any); ok {
			in["MessageGroupId"] = params["MessageGroupId"]
		}
		_, _ = sqs.New(p.deps).Invoke(ctx, &spi.Request{Identity: req.Identity, Operation: "SendMessage", Input: in})
	case strings.Contains(arn, ":sns:"):
		_, _ = sns.New(p.deps).Invoke(ctx, &spi.Request{Identity: req.Identity, Operation: "Publish", Input: map[string]any{"TopicArn": arn, "Message": string(payload)}})
	case strings.Contains(arn, ":lambda:"):
		_, name, ok := strings.Cut(arn, ":function:")
		if !ok {
			return
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
		_, _ = lambda.New(p.deps).Invoke(ctx, &spi.Request{Identity: req.Identity, Operation: "Invoke", Input: in})
	}
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
