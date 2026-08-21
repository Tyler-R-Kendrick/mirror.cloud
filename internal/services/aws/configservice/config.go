// Package configservice stores Config recorders, rules, and evaluations (no AWS account crawler).
package configservice

import (
	"context"
	"encoding/json"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func init() {
	registry.Register(registry.Factory{ServiceID: "aws.config", Tier: model.TierEmulate, New: func(d spi.Deps) (spi.BehaviorPack, error) {
		return New(d), nil
	}})
}

// Pack implements Config-lite.
type Pack struct{ deps spi.Deps }

// New constructs the pack.
func New(d spi.Deps) *Pack { return &Pack{deps: d} }

func (p *Pack) ServiceID() string { return "aws.config" }
func (p *Pack) Tier() model.Tier  { return model.TierEmulate }
func (p *Pack) Operations() []string {
	return []string{
		"PutConfigurationRecorder", "DescribeConfigurationRecorders", "DeleteConfigurationRecorder",
		"PutDeliveryChannel", "DescribeDeliveryChannels", "DeleteDeliveryChannel",
		"StartConfigurationRecorder", "StopConfigurationRecorder",
		"PutConfigRule", "DescribeConfigRules", "DeleteConfigRule",
		"PutEvaluations", "GetComplianceDetailsByConfigRule", "DescribeComplianceByConfigRule",
		"GetResourceConfigHistory", "ListDiscoveredResources",
	}
}

func (p *Pack) col(req *spi.Request, n string) spi.Collection {
	return p.deps.Store.Scope(req.Identity.Account, req.Identity.Region).Collection(n)
}

func (p *Pack) Invoke(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	switch req.Operation {
	case "PutConfigurationRecorder":
		recIn, _ := req.Input["ConfigurationRecorder"].(map[string]any)
		name := first(recIn, "name", "Name")
		if name == "" {
			name = "default"
		}
		if recIn == nil {
			recIn = map[string]any{"name": name}
		}
		recIn["name"] = name
		b, _ := json.Marshal(recIn)
		_ = p.col(req, "cfgr").Put(ctx, name, b)
		return &spi.Response{Output: map[string]any{}}, nil
	case "DescribeConfigurationRecorders":
		return listCol(ctx, p.col(req, "cfgr"), "ConfigurationRecorders")
	case "DeleteConfigurationRecorder":
		_ = p.col(req, "cfgr").Delete(ctx, first(req.Input, "ConfigurationRecorderName"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "PutDeliveryChannel":
		ch, _ := req.Input["DeliveryChannel"].(map[string]any)
		name := first(ch, "name", "Name")
		if name == "" {
			name = "default"
		}
		if ch == nil {
			ch = map[string]any{"name": name}
		}
		ch["name"] = name
		b, _ := json.Marshal(ch)
		_ = p.col(req, "cfgc").Put(ctx, name, b)
		return &spi.Response{Output: map[string]any{}}, nil
	case "DescribeDeliveryChannels":
		return listCol(ctx, p.col(req, "cfgc"), "DeliveryChannels")
	case "DeleteDeliveryChannel":
		_ = p.col(req, "cfgc").Delete(ctx, first(req.Input, "DeliveryChannelName"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "StartConfigurationRecorder", "StopConfigurationRecorder":
		name := first(req.Input, "ConfigurationRecorderName")
		b, ok, _ := p.col(req, "cfgr").Get(ctx, name)
		if ok {
			var rec map[string]any
			_ = json.Unmarshal(b, &rec)
			rec["recording"] = req.Operation == "StartConfigurationRecorder"
			nb, _ := json.Marshal(rec)
			_ = p.col(req, "cfgr").Put(ctx, name, nb)
		}
		return &spi.Response{Output: map[string]any{}}, nil
	case "PutConfigRule":
		rule, _ := req.Input["ConfigRule"].(map[string]any)
		name := first(rule, "ConfigRuleName")
		if name == "" {
			return nil, &spi.Fault{Code: "InvalidParameterValueException", HTTPStatus: 400, Fault: "client"}
		}
		rule["ConfigRuleArn"] = "arn:aws:config:" + req.Identity.Region + ":" + req.Identity.Account + ":config-rule/" + name
		rule["ConfigRuleState"] = "ACTIVE"
		b, _ := json.Marshal(rule)
		_ = p.col(req, "cfgrule").Put(ctx, name, b)
		return &spi.Response{Output: map[string]any{}}, nil
	case "DescribeConfigRules":
		return listCol(ctx, p.col(req, "cfgrule"), "ConfigRules")
	case "DeleteConfigRule":
		_ = p.col(req, "cfgrule").Delete(ctx, first(req.Input, "ConfigRuleName"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "PutEvaluations":
		rule := first(req.Input, "Evaluations.0.ComplianceResourceId")
		evals, _ := req.Input["Evaluations"].([]any)
		for _, e := range evals {
			em, _ := e.(map[string]any)
			rid := first(em, "ComplianceResourceId")
			if rid == "" {
				continue
			}
			b, _ := json.Marshal(em)
			_ = p.col(req, "cfgeval").Put(ctx, rid, b)
			_ = p.col(req, "cfgres").Put(ctx, rid, b)
			_ = rule
		}
		return &spi.Response{Output: map[string]any{"FailedEvaluations": []any{}}}, nil
	case "GetComplianceDetailsByConfigRule":
		return listCol(ctx, p.col(req, "cfgeval"), "EvaluationResults")
	case "DescribeComplianceByConfigRule":
		kvs, _, _ := p.col(req, "cfgeval").List(ctx, "", "", 0)
		return &spi.Response{Output: map[string]any{"ComplianceByConfigRules": []any{map[string]any{
			"ConfigRuleName": first(req.Input, "ConfigRuleNames"),
			"Compliance":     map[string]any{"ComplianceType": complianceType(len(kvs))},
		}}}}, nil
	case "GetResourceConfigHistory":
		rid := first(req.Input, "resourceId", "ResourceId")
		b, ok, _ := p.col(req, "cfgres").Get(ctx, rid)
		var items []any
		if ok {
			var rec map[string]any
			_ = json.Unmarshal(b, &rec)
			items = append(items, rec)
		}
		return &spi.Response{Output: map[string]any{"configurationItems": items}}, nil
	case "ListDiscoveredResources":
		return listCol(ctx, p.col(req, "cfgres"), "resourceIdentifiers")
	default:
		return nil, spi.NotImplemented("aws.config", req.Operation, "emulate")
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

func complianceType(n int) string {
	if n == 0 {
		return "INSUFFICIENT_DATA"
	}
	return "NON_COMPLIANT"
}

func first(in map[string]any, keys ...string) string {
	if in == nil {
		return ""
	}
	for _, k := range keys {
		if s, ok := in[k].(string); ok && s != "" {
			return s
		}
	}
	return ""
}
