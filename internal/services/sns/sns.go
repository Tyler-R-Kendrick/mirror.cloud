// Package sns is the emulate-tier SNS pack.
package sns

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func init() {
	registry.Register(registry.Factory{ServiceID: "aws.sns", Tier: model.TierEmulate, New: func(d spi.Deps) (spi.BehaviorPack, error) {
		return &Pack{deps: d}, nil
	}})
}

// Pack implements SNS.
type Pack struct{ deps spi.Deps }

func (p *Pack) ServiceID() string { return "aws.sns" }
func (p *Pack) Tier() model.Tier  { return model.TierEmulate }
func (p *Pack) Operations() []string {
	return []string{"CreateTopic", "DeleteTopic", "ListTopics", "GetTopicAttributes", "SetTopicAttributes",
		"Subscribe", "ConfirmSubscription", "Unsubscribe", "ListSubscriptions",
		"ListSubscriptionsByTopic", "Publish", "PublishBatch", "TagResource", "UntagResource"}
}

func (p *Pack) col(req *spi.Request, n string) spi.Collection {
	return p.deps.Store.Scope(req.Identity.Account, req.Identity.Region).Collection(n)
}

func (p *Pack) Invoke(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	switch req.Operation {
	case "CreateTopic":
		name := str(req.Input["Name"])
		arn := fmt.Sprintf("arn:aws:sns:%s:%s:%s", req.Identity.Region, req.Identity.Account, name)
		b, _ := json.Marshal(map[string]any{"arn": arn, "name": name})
		_ = p.col(req, "topics").Put(ctx, name, b)
		return &spi.Response{Output: map[string]any{"TopicArn": arn}}, nil
	case "ListTopics":
		kvs, _, _ := p.col(req, "topics").List(ctx, "", "", 0)
		var topics []any
		for _, kv := range kvs {
			var m map[string]any
			_ = json.Unmarshal(kv.Value, &m)
			topics = append(topics, map[string]any{"TopicArn": m["arn"]})
		}
		return &spi.Response{Output: map[string]any{"Topics": topics}}, nil
	case "DeleteTopic":
		arn := str(req.Input["TopicArn"])
		name := topicName(arn)
		_ = p.col(req, "topics").Delete(ctx, name)
		return &spi.Response{Output: map[string]any{}}, nil
	case "GetTopicAttributes":
		arn := str(req.Input["TopicArn"])
		b, ok, _ := p.col(req, "topics").Get(ctx, topicName(arn))
		if !ok {
			return nil, &spi.Fault{Code: "NotFound", HTTPStatus: 404, Fault: "client"}
		}
		var m map[string]any
		_ = json.Unmarshal(b, &m)
		return &spi.Response{Output: map[string]any{"Attributes": map[string]any{"TopicArn": m["arn"], "DisplayName": m["name"]}}}, nil
	case "SetTopicAttributes":
		return &spi.Response{Output: map[string]any{}}, nil
	case "Publish":
		arn := str(req.Input["TopicArn"])
		body := str(req.Input["Message"])
		_ = p.deps.Bus.Publish(ctx, "sns:"+arn, []byte(body))
		return &spi.Response{Output: map[string]any{"MessageId": p.deps.Rand.Hex(16)}}, nil
	case "PublishBatch":
		return p.Invoke(ctx, &spi.Request{Identity: req.Identity, Operation: "Publish", Input: req.Input, HTTP: req.HTTP})
	case "Subscribe":
		if str(req.Input["Protocol"]) == "lambda" {
			return nil, spi.NotImplemented("aws.sns", "Subscribe/lambda", "emulate")
		}
		sub := "arn:aws:sns:" + req.Identity.Region + ":" + req.Identity.Account + ":sub/" + p.deps.Rand.Hex(8)
		rec := map[string]any{
			"SubscriptionArn": sub,
			"TopicArn":        str(req.Input["TopicArn"]),
			"Protocol":        str(req.Input["Protocol"]),
			"Endpoint":        str(req.Input["Endpoint"]),
		}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "subs").Put(ctx, sub, b)
		return &spi.Response{Output: map[string]any{"SubscriptionArn": sub}}, nil
	case "Unsubscribe":
		_ = p.col(req, "subs").Delete(ctx, str(req.Input["SubscriptionArn"]))
		return &spi.Response{Output: map[string]any{}}, nil
	case "ListSubscriptions", "ListSubscriptionsByTopic":
		want := str(req.Input["TopicArn"])
		kvs, _, _ := p.col(req, "subs").List(ctx, "", "", 0)
		var subs []any
		for _, kv := range kvs {
			var m map[string]any
			_ = json.Unmarshal(kv.Value, &m)
			if want != "" && str(m["TopicArn"]) != want {
				continue
			}
			subs = append(subs, m)
		}
		return &spi.Response{Output: map[string]any{"Subscriptions": subs}}, nil
	case "ConfirmSubscription", "TagResource", "UntagResource":
		return &spi.Response{Output: map[string]any{"SubscriptionArn": str(req.Input["Token"])}}, nil
	default:
		return nil, spi.NotImplemented("aws.sns", req.Operation, "emulate")
	}
}

func topicName(arn string) string {
	if i := strings.LastIndex(arn, ":"); i >= 0 {
		return arn[i+1:]
	}
	return arn
}

func str(v any) string { s, _ := v.(string); return s }
