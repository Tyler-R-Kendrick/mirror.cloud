// Package events is EventBridge emulate: rules, targets, PutEvents to SQS/SNS.
package events

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
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
		p.fanout(ctx, req, entries)
		return &spi.Response{Output: map[string]any{"FailedEntryCount": 0, "Entries": []any{}}}, nil
	default:
		return p.extra(ctx, req)
	}
}

func (p *Pack) fanout(ctx context.Context, req *spi.Request, entries []any) {
	raw, _ := json.Marshal(entries)
	_ = p.deps.Bus.Publish(ctx, "events", raw)
	kvs, _, _ := p.col(req, "targets").List(ctx, "", "", 0)
	for _, kv := range kvs {
		var tgs []any
		_ = json.Unmarshal(kv.Value, &tgs)
		for _, t := range tgs {
			m, _ := t.(map[string]any)
			arn := str(m["Arn"])
			if arn == "" {
				continue
			}
			_ = p.deps.Bus.Publish(ctx, "events:"+arn, raw)
			if i := lastColon(arn); i >= 0 && strings.Contains(arn, ":sqs:") {
				name := arn[i+1:]
				rh := "evt-" + name
				msg, _ := json.Marshal(map[string]any{"id": rh, "body": string(raw), "handle": rh, "visibleAt": 0, "receiveCount": 0, "seq": 1})
				_ = p.col(req, "msgs:"+name).Put(ctx, rh, msg)
			}
		}
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
