// Package autoscaling emulates ASG and launch configuration records (no EC2 instances).
package autoscaling

import (
	"context"
	"encoding/json"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func init() {
	registry.Register(registry.Factory{ServiceID: "aws.autoscaling", Tier: model.TierEmulate, New: func(d spi.Deps) (spi.BehaviorPack, error) {
		return New(d), nil
	}})
}

// Pack implements Auto Scaling-lite.
type Pack struct{ deps spi.Deps }

// New constructs the pack.
func New(d spi.Deps) *Pack { return &Pack{deps: d} }

func (p *Pack) ServiceID() string { return "aws.autoscaling" }
func (p *Pack) Tier() model.Tier  { return model.TierEmulate }
func (p *Pack) Operations() []string {
	core := []string{
		"CreateAutoScalingGroup", "DescribeAutoScalingGroups", "UpdateAutoScalingGroup", "DeleteAutoScalingGroup",
		"CreateLaunchConfiguration", "DescribeLaunchConfigurations", "DeleteLaunchConfiguration",
		"SetDesiredCapacity", "TerminateInstanceInAutoScalingGroup",
		"CreateOrUpdateTags", "DescribeTags", "DeleteTags",
	}
	return append(core, extraOps()...)
}

func (p *Pack) col(req *spi.Request, n string) spi.Collection {
	return p.deps.Store.Scope(req.Identity.Account, req.Identity.Region).Collection(n)
}

func (p *Pack) Invoke(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	switch req.Operation {
	case "CreateAutoScalingGroup", "UpdateAutoScalingGroup":
		name := first(req.Input, "AutoScalingGroupName")
		rec := map[string]any{
			"AutoScalingGroupName": name, "MinSize": req.Input["MinSize"], "MaxSize": req.Input["MaxSize"],
			"DesiredCapacity": req.Input["DesiredCapacity"], "LaunchConfigurationName": first(req.Input, "LaunchConfigurationName"),
		}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "asg").Put(ctx, name, b)
		return &spi.Response{Output: map[string]any{}}, nil
	case "DescribeAutoScalingGroups":
		name := first(req.Input, "AutoScalingGroupNames.member.1", "AutoScalingGroupName")
		if name != "" {
			b, ok, _ := p.col(req, "asg").Get(ctx, name)
			if !ok {
				return &spi.Response{Output: map[string]any{"AutoScalingGroups": []any{}}}, nil
			}
			var rec map[string]any
			_ = json.Unmarshal(b, &rec)
			return &spi.Response{Output: map[string]any{"AutoScalingGroups": []any{rec}}}, nil
		}
		kvs, _, _ := p.col(req, "asg").List(ctx, "", "", 0)
		var items []any
		for _, kv := range kvs {
			var rec map[string]any
			_ = json.Unmarshal(kv.Value, &rec)
			items = append(items, rec)
		}
		return &spi.Response{Output: map[string]any{"AutoScalingGroups": items}}, nil
	case "DeleteAutoScalingGroup":
		_ = p.col(req, "asg").Delete(ctx, first(req.Input, "AutoScalingGroupName"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "SetDesiredCapacity":
		name := first(req.Input, "AutoScalingGroupName")
		b, ok, _ := p.col(req, "asg").Get(ctx, name)
		rec := map[string]any{"AutoScalingGroupName": name}
		if ok {
			_ = json.Unmarshal(b, &rec)
		}
		rec["DesiredCapacity"] = req.Input["DesiredCapacity"]
		nb, _ := json.Marshal(rec)
		_ = p.col(req, "asg").Put(ctx, name, nb)
		return &spi.Response{Output: map[string]any{}}, nil
	case "TerminateInstanceInAutoScalingGroup":
		return &spi.Response{Output: map[string]any{"Activity": map[string]any{"ActivityId": p.deps.Rand.Hex(8), "StatusCode": "Successful"}}}, nil
	case "CreateLaunchConfiguration":
		name := first(req.Input, "LaunchConfigurationName")
		rec := map[string]any{"LaunchConfigurationName": name, "ImageId": first(req.Input, "ImageId"), "InstanceType": first(req.Input, "InstanceType")}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "lc").Put(ctx, name, b)
		return &spi.Response{Output: map[string]any{}}, nil
	case "DescribeLaunchConfigurations":
		kvs, _, _ := p.col(req, "lc").List(ctx, "", "", 0)
		var items []any
		for _, kv := range kvs {
			var rec map[string]any
			_ = json.Unmarshal(kv.Value, &rec)
			items = append(items, rec)
		}
		return &spi.Response{Output: map[string]any{"LaunchConfigurations": items}}, nil
	case "DeleteLaunchConfiguration":
		_ = p.col(req, "lc").Delete(ctx, first(req.Input, "LaunchConfigurationName"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "CreateOrUpdateTags":
		b, _ := json.Marshal(req.Input)
		_ = p.col(req, "asgtags").Put(ctx, "tags", b)
		return &spi.Response{Output: map[string]any{}}, nil
	case "DescribeTags":
		b, ok, _ := p.col(req, "asgtags").Get(ctx, "tags")
		var rec any = []any{}
		if ok {
			var m map[string]any
			_ = json.Unmarshal(b, &m)
			rec = m["Tags"]
		}
		return &spi.Response{Output: map[string]any{"Tags": rec}}, nil
	case "DeleteTags":
		_ = p.col(req, "asgtags").Delete(ctx, "tags")
		return &spi.Response{Output: map[string]any{}}, nil
	default:
		return p.extra(ctx, req)
	}
}

func first(in map[string]any, keys ...string) string {
	for _, k := range keys {
		if s, ok := in[k].(string); ok && s != "" {
			return s
		}
	}
	return ""
}
