// Package sns is the emulate-tier SNS pack.
package sns

import (
	"context"
	"encoding/json"
	"fmt"

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
	case "Publish":
		arn := str(req.Input["TopicArn"])
		body := str(req.Input["Message"])
		_ = p.deps.Bus.Publish(ctx, "sns:"+arn, []byte(body))
		return &spi.Response{Output: map[string]any{"MessageId": p.deps.Rand.Hex(16)}}, nil
	case "Subscribe":
		if str(req.Input["Protocol"]) == "lambda" {
			return nil, spi.NotImplemented("aws.sns", "Subscribe/lambda", "emulate")
		}
		arn := "arn:aws:sns:" + req.Identity.Region + ":" + req.Identity.Account + ":sub/" + p.deps.Rand.Hex(8)
		return &spi.Response{Output: map[string]any{"SubscriptionArn": arn}}, nil
	default:
		return &spi.Response{Output: map[string]any{}}, nil
	}
}

func str(v any) string { s, _ := v.(string); return s }
