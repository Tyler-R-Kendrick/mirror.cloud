// Package elasticloadbalancing emulates ALB/NLB load balancers, target groups, and listeners.
package elasticloadbalancing

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func init() {
	registry.Register(registry.Factory{ServiceID: "aws.elasticloadbalancing", Tier: model.TierEmulate, New: func(d spi.Deps) (spi.BehaviorPack, error) {
		return New(d), nil
	}})
}

// Pack implements ELBv2-lite.
type Pack struct{ deps spi.Deps }

// New constructs the pack.
func New(d spi.Deps) *Pack { return &Pack{deps: d} }

func (p *Pack) ServiceID() string { return "aws.elasticloadbalancing" }
func (p *Pack) Tier() model.Tier  { return model.TierEmulate }
func (p *Pack) Operations() []string {
	core := []string{
		"CreateLoadBalancer", "DescribeLoadBalancers", "DeleteLoadBalancer",
		"ModifyLoadBalancerAttributes", "DescribeLoadBalancerAttributes",
		"CreateTargetGroup", "DescribeTargetGroups", "DeleteTargetGroup", "ModifyTargetGroup",
		"CreateListener", "DescribeListeners", "DeleteListener",
		"RegisterTargets", "DeregisterTargets", "DescribeTargetHealth",
		"AddTags", "RemoveTags", "DescribeTags",
		"AddListenerCertificates", "AddTrustStoreRevocations", "CreateRule", "CreateTrustStore",
		"DeleteRule", "DeleteSharedTrustStoreAssociation", "DeleteTrustStore", "DescribeAccountLimits",
		"DescribeCapacityReservation", "DescribeListenerAttributes", "DescribeListenerCertificates", "DescribeRules",
		"DescribeSSLPolicies", "DescribeTargetGroupAttributes", "DescribeTrustStoreAssociations", "DescribeTrustStoreRevocations",
		"DescribeTrustStores", "GetResourcePolicy", "GetTrustStoreCaCertificatesBundle", "GetTrustStoreRevocationContent",
		"ModifyCapacityReservation", "ModifyIpPools", "ModifyListener", "ModifyListenerAttributes",
		"ModifyRule", "ModifyTargetGroupAttributes", "ModifyTrustStore", "RemoveListenerCertificates",
		"RemoveTrustStoreRevocations", "SetIpAddressType", "SetRulePriorities", "SetSecurityGroups",
		"SetSubnets",
	}
	return core
}

func (p *Pack) col(req *spi.Request, n string) spi.Collection {
	return p.deps.Store.Scope(req.Identity.Account, req.Identity.Region).Collection(n)
}

func (p *Pack) Invoke(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	acct, region := req.Identity.Account, req.Identity.Region
	switch req.Operation {
	case "CreateLoadBalancer":
		name := first(req.Input, "Name")
		arn := "arn:aws:elasticloadbalancing:" + region + ":" + acct + ":loadbalancer/app/" + name + "/" + p.deps.Rand.Hex(8)
		rec := map[string]any{
			"LoadBalancerName": name, "LoadBalancerArn": arn, "Type": first(req.Input, "Type"),
			"DNSName": name + "-" + p.deps.Rand.Hex(4) + "." + region + ".elb.amazonaws.com",
			"State":   map[string]any{"Code": "active"},
		}
		if rec["Type"] == "" {
			rec["Type"] = "application"
		}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "elb").Put(ctx, name, b)
		_ = p.col(req, "elbarn").Put(ctx, arn, []byte(name))
		return &spi.Response{Output: map[string]any{"LoadBalancers": []any{rec}}}, nil
	case "DescribeLoadBalancers":
		return p.describeNamed(ctx, req, "elb", "LoadBalancerArns", "LoadBalancerNames", "LoadBalancers")
	case "DeleteLoadBalancer":
		arn := first(req.Input, "LoadBalancerArn")
		if b, ok, _ := p.col(req, "elbarn").Get(ctx, arn); ok {
			_ = p.col(req, "elb").Delete(ctx, string(b))
		}
		_ = p.col(req, "elbarn").Delete(ctx, arn)
		return &spi.Response{Output: map[string]any{}}, nil
	case "ModifyLoadBalancerAttributes":
		arn := first(req.Input, "LoadBalancerArn")
		b, _ := json.Marshal(req.Input["Attributes"])
		_ = p.col(req, "elbattr").Put(ctx, arn, b)
		return &spi.Response{Output: map[string]any{"Attributes": req.Input["Attributes"]}}, nil
	case "DescribeLoadBalancerAttributes":
		arn := first(req.Input, "LoadBalancerArn")
		b, ok, _ := p.col(req, "elbattr").Get(ctx, arn)
		var attrs any = []any{}
		if ok {
			_ = json.Unmarshal(b, &attrs)
		}
		return &spi.Response{Output: map[string]any{"Attributes": attrs}}, nil
	case "CreateTargetGroup":
		name := first(req.Input, "Name")
		arn := "arn:aws:elasticloadbalancing:" + region + ":" + acct + ":targetgroup/" + name + "/" + p.deps.Rand.Hex(8)
		rec := map[string]any{"TargetGroupName": name, "TargetGroupArn": arn, "Port": req.Input["Port"], "Protocol": first(req.Input, "Protocol"), "VpcId": first(req.Input, "VpcId"), "TargetType": first(req.Input, "TargetType")}
		if rec["TargetType"] == "" {
			rec["TargetType"] = "instance"
		}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "tg").Put(ctx, name, b)
		_ = p.col(req, "tgarn").Put(ctx, arn, []byte(name))
		return &spi.Response{Output: map[string]any{"TargetGroups": []any{rec}}}, nil
	case "DescribeTargetGroups":
		return p.describeNamed(ctx, req, "tg", "TargetGroupArns", "Names", "TargetGroups")
	case "DeleteTargetGroup":
		arn := first(req.Input, "TargetGroupArn")
		if b, ok, _ := p.col(req, "tgarn").Get(ctx, arn); ok {
			_ = p.col(req, "tg").Delete(ctx, string(b))
		}
		return &spi.Response{Output: map[string]any{}}, nil
	case "ModifyTargetGroup":
		arn := first(req.Input, "TargetGroupArn")
		name := string(mustGet(p.col(req, "tgarn"), ctx, arn))
		return p.patch(ctx, req, "tg", name, "TargetGroups")
	case "CreateListener":
		lb := first(req.Input, "LoadBalancerArn")
		id := p.deps.Rand.Hex(8)
		arn := strings.TrimSuffix(lb, "/") + "/listener/" + id
		if i := strings.Index(lb, ":loadbalancer/"); i >= 0 {
			arn = lb[:i] + ":listener/" + id
		}
		rec := map[string]any{"ListenerArn": arn, "LoadBalancerArn": lb, "Port": req.Input["Port"], "Protocol": first(req.Input, "Protocol"), "DefaultActions": req.Input["DefaultActions"]}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "listener").Put(ctx, arn, b)
		return &spi.Response{Output: map[string]any{"Listeners": []any{rec}}}, nil
	case "DescribeListeners":
		if arn := first(req.Input, "ListenerArns"); arn != "" {
			b, ok, _ := p.col(req, "listener").Get(ctx, arn)
			if !ok {
				return &spi.Response{Output: map[string]any{"Listeners": []any{}}}, nil
			}
			var rec map[string]any
			_ = json.Unmarshal(b, &rec)
			return &spi.Response{Output: map[string]any{"Listeners": []any{rec}}}, nil
		}
		lb := first(req.Input, "LoadBalancerArn")
		kvs, _, _ := p.col(req, "listener").List(ctx, "", "", 0)
		var out []any
		for _, kv := range kvs {
			var rec map[string]any
			_ = json.Unmarshal(kv.Value, &rec)
			if lb == "" || rec["LoadBalancerArn"] == lb {
				out = append(out, rec)
			}
		}
		return &spi.Response{Output: map[string]any{"Listeners": out}}, nil
	case "DeleteListener":
		_ = p.col(req, "listener").Delete(ctx, first(req.Input, "ListenerArn"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "RegisterTargets":
		arn := first(req.Input, "TargetGroupArn")
		b, _ := json.Marshal(req.Input["Targets"])
		_ = p.col(req, "targets").Put(ctx, arn, b)
		return &spi.Response{Output: map[string]any{}}, nil
	case "DeregisterTargets":
		_ = p.col(req, "targets").Delete(ctx, first(req.Input, "TargetGroupArn"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "DescribeTargetHealth":
		arn := first(req.Input, "TargetGroupArn")
		b, ok, _ := p.col(req, "targets").Get(ctx, arn)
		var tgts []any
		if ok {
			_ = json.Unmarshal(b, &tgts)
		}
		var desc []any
		for _, t := range tgts {
			desc = append(desc, map[string]any{"Target": t, "TargetHealth": map[string]any{"State": "healthy"}})
		}
		return &spi.Response{Output: map[string]any{"TargetHealthDescriptions": desc}}, nil
	case "AddTags":
		b, _ := json.Marshal(req.Input["Tags"])
		_ = p.col(req, "elbtags").Put(ctx, first(req.Input, "ResourceArns"), b)
		return &spi.Response{Output: map[string]any{}}, nil
	case "DescribeTags":
		b, ok, _ := p.col(req, "elbtags").Get(ctx, first(req.Input, "ResourceArns"))
		var tags any = []any{}
		if ok {
			_ = json.Unmarshal(b, &tags)
		}
		return &spi.Response{Output: map[string]any{"TagDescriptions": []any{map[string]any{"Tags": tags}}}}, nil
	case "RemoveTags":
		_ = p.col(req, "elbtags").Delete(ctx, first(req.Input, "ResourceArns"))
		return &spi.Response{Output: map[string]any{}}, nil
	default:
		return p.extra(ctx, req)
	}
}

func (p *Pack) describeNamed(ctx context.Context, req *spi.Request, col, arnKey, nameKey, outKey string) (*spi.Response, error) {
	kvs, _, _ := p.col(req, col).List(ctx, "", "", 0)
	var out []any
	for _, kv := range kvs {
		var rec map[string]any
		_ = json.Unmarshal(kv.Value, &rec)
		out = append(out, rec)
	}
	_ = arnKey
	_ = nameKey
	if id := first(req.Input, nameKey); id != "" {
		b, ok, _ := p.col(req, col).Get(ctx, id)
		if !ok {
			return &spi.Response{Output: map[string]any{outKey: []any{}}}, nil
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		return &spi.Response{Output: map[string]any{outKey: []any{rec}}}, nil
	}
	return &spi.Response{Output: map[string]any{outKey: out}}, nil
}

func (p *Pack) patch(ctx context.Context, req *spi.Request, col, name, outKey string) (*spi.Response, error) {
	b, ok, _ := p.col(req, col).Get(ctx, name)
	rec := map[string]any{}
	if ok {
		_ = json.Unmarshal(b, &rec)
	}
	for k, v := range req.Input {
		rec[k] = v
	}
	nb, _ := json.Marshal(rec)
	_ = p.col(req, col).Put(ctx, name, nb)
	return &spi.Response{Output: map[string]any{outKey: []any{rec}}}, nil
}

func mustGet(c spi.Collection, ctx context.Context, key string) []byte {
	b, _, _ := c.Get(ctx, key)
	return b
}

func first(in map[string]any, keys ...string) string {
	for _, k := range keys {
		switch t := in[k].(type) {
		case string:
			if t != "" {
				return t
			}
		case []any:
			if len(t) > 0 {
				if s, ok := t[0].(string); ok {
					return s
				}
			}
		}
	}
	return ""
}
