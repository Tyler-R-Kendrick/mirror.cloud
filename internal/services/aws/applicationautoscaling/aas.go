// Package applicationautoscaling stores scalable targets and policies.
package applicationautoscaling

import (
	"context"
	"encoding/json"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func init() {
	registry.Register(registry.Factory{ServiceID: "aws.application-autoscaling", Tier: model.TierEmulate, New: func(d spi.Deps) (spi.BehaviorPack, error) {
		return New(d), nil
	}})
}

// Pack implements Application Auto Scaling.
type Pack struct{ deps spi.Deps }

// New constructs the pack.
func New(d spi.Deps) *Pack { return &Pack{deps: d} }

func (p *Pack) ServiceID() string { return "aws.application-autoscaling" }
func (p *Pack) Tier() model.Tier  { return model.TierEmulate }
func (p *Pack) Operations() []string {
	return []string{
		"RegisterScalableTarget", "DeregisterScalableTarget", "DescribeScalableTargets",
		"PutScalingPolicy", "DeleteScalingPolicy", "DescribeScalingPolicies",
		"PutScheduledAction", "DeleteScheduledAction", "DescribeScheduledActions",
		"DescribeScalingActivities", "GetPredictiveScalingForecast",
		"TagResource", "UntagResource", "ListTagsForResource",
	}
}

func (p *Pack) col(req *spi.Request, n string) spi.Collection {
	return p.deps.Store.Scope(req.Identity.Account, req.Identity.Region).Collection(n)
}

func (p *Pack) Invoke(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	switch req.Operation {
	case "RegisterScalableTarget":
		id := first(req.Input, "ResourceId") + "|" + first(req.Input, "ScalableDimension")
		b, _ := json.Marshal(req.Input)
		_ = p.col(req, "aastarget").Put(ctx, id, b)
		return &spi.Response{Output: map[string]any{"ScalableTargetARN": "arn:aws:application-autoscaling:" + req.Identity.Region + ":" + req.Identity.Account + ":scalable-target/" + p.deps.Rand.Hex(8)}}, nil
	case "DeregisterScalableTarget":
		id := first(req.Input, "ResourceId") + "|" + first(req.Input, "ScalableDimension")
		_ = p.col(req, "aastarget").Delete(ctx, id)
		return &spi.Response{Output: map[string]any{}}, nil
	case "DescribeScalableTargets":
		return listCol(ctx, p.col(req, "aastarget"), "ScalableTargets")
	case "PutScalingPolicy":
		name := first(req.Input, "PolicyName")
		b, _ := json.Marshal(req.Input)
		_ = p.col(req, "aaspol").Put(ctx, name, b)
		return &spi.Response{Output: map[string]any{"PolicyARN": "arn:aws:application-autoscaling:" + req.Identity.Region + ":" + req.Identity.Account + ":scaling-policy/" + name}}, nil
	case "DeleteScalingPolicy":
		_ = p.col(req, "aaspol").Delete(ctx, first(req.Input, "PolicyName"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "DescribeScalingPolicies":
		return listCol(ctx, p.col(req, "aaspol"), "ScalingPolicies")
	case "PutScheduledAction":
		name := first(req.Input, "ScheduledActionName")
		b, _ := json.Marshal(req.Input)
		_ = p.col(req, "aassched").Put(ctx, name, b)
		return &spi.Response{Output: map[string]any{}}, nil
	case "DeleteScheduledAction":
		_ = p.col(req, "aassched").Delete(ctx, first(req.Input, "ScheduledActionName"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "DescribeScheduledActions":
		return listCol(ctx, p.col(req, "aassched"), "ScheduledActions")
	case "DescribeScalingActivities":
		return &spi.Response{Output: map[string]any{"ScalingActivities": []any{}}}, nil
	case "GetPredictiveScalingForecast":
		return &spi.Response{Output: map[string]any{"LoadForecast": []any{}, "CapacityForecast": []any{}}}, nil
	case "TagResource":
		b, _ := json.Marshal(req.Input["Tags"])
		_ = p.col(req, "aastags").Put(ctx, first(req.Input, "ResourceARN"), b)
		return &spi.Response{Output: map[string]any{}}, nil
	case "ListTagsForResource":
		b, ok, _ := p.col(req, "aastags").Get(ctx, first(req.Input, "ResourceARN"))
		var tags any = map[string]any{}
		if ok {
			_ = json.Unmarshal(b, &tags)
		}
		return &spi.Response{Output: map[string]any{"Tags": tags}}, nil
	case "UntagResource":
		_ = p.col(req, "aastags").Delete(ctx, first(req.Input, "ResourceARN"))
		return &spi.Response{Output: map[string]any{}}, nil
	default:
		return nil, spi.NotImplemented("aws.application-autoscaling", req.Operation, "emulate")
	}
}

func listCol(ctx context.Context, c spi.Collection, key string) (*spi.Response, error) {
	kvs, _, _ := c.List(ctx, "", "", 0)
	var items []any
	for _, kv := range kvs {
		var rec map[string]any
		_ = json.Unmarshal(kv.Value, &rec)
		items = append(items, rec)
	}
	return &spi.Response{Output: map[string]any{key: items}}, nil
}

func first(in map[string]any, keys ...string) string {
	for _, k := range keys {
		if s, ok := in[k].(string); ok && s != "" {
			return s
		}
	}
	return ""
}
