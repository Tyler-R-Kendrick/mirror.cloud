// Package bedrock stores guardrail and job records (no model inference).
package bedrock

import (
	"context"
	"encoding/json"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func init() {
	registry.Register(registry.Factory{ServiceID: "aws.bedrock", Tier: model.TierEmulate, New: func(d spi.Deps) (spi.BehaviorPack, error) {
		return New(d), nil
	}})
}

// Pack implements Bedrock-lite.
type Pack struct{ deps spi.Deps }

// New constructs the pack.
func New(d spi.Deps) *Pack { return &Pack{deps: d} }

func (p *Pack) ServiceID() string { return "aws.bedrock" }
func (p *Pack) Tier() model.Tier  { return model.TierEmulate }
func (p *Pack) Operations() []string {
	return []string{
		"CreateGuardrail", "GetGuardrail", "ListGuardrails", "DeleteGuardrail",
		"ListFoundationModels", "CreateModelCustomizationJob",
	}
}

func (p *Pack) col(req *spi.Request, n string) spi.Collection {
	return p.deps.Store.Scope(req.Identity.Account, req.Identity.Region).Collection(n)
}

func (p *Pack) Invoke(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	switch req.Operation {
	case "CreateGuardrail":
		id := p.deps.Rand.Hex(8)
		rec := map[string]any{
			"guardrailId": id, "name": first(req.Input, "name", "Name"), "status": "READY", "version": "DRAFT",
			"guardrailArn": "arn:aws:bedrock:" + req.Identity.Region + ":" + req.Identity.Account + ":guardrail/" + id,
		}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "bdgr").Put(ctx, id, b)
		return &spi.Response{Output: map[string]any{"guardrailId": id, "guardrailArn": rec["guardrailArn"], "version": "DRAFT"}}, nil
	case "GetGuardrail":
		id := first(req.Input, "guardrailIdentifier", "guardrailId")
		b, ok, _ := p.col(req, "bdgr").Get(ctx, id)
		if !ok {
			return nil, &spi.Fault{Code: "ResourceNotFoundException", HTTPStatus: 400, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		return &spi.Response{Output: rec}, nil
	case "ListGuardrails":
		return listWrap(ctx, p.col(req, "bdgr"), "guardrails")
	case "DeleteGuardrail":
		_ = p.col(req, "bdgr").Delete(ctx, first(req.Input, "guardrailIdentifier", "guardrailId"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "ListFoundationModels":
		return &spi.Response{Output: map[string]any{"modelSummaries": []any{
			map[string]any{"modelId": "amazon.titan-text-lite-v1", "modelName": "Titan Text Lite", "providerName": "Amazon"},
		}}}, nil
	case "CreateModelCustomizationJob":
		id := p.deps.Rand.Hex(8)
		rec := map[string]any{"jobArn": "arn:aws:bedrock:" + req.Identity.Region + ":" + req.Identity.Account + ":model-customization-job/" + id, "jobName": first(req.Input, "jobName"), "status": "Completed"}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "bdjob").Put(ctx, id, b)
		return &spi.Response{Output: map[string]any{"jobArn": rec["jobArn"]}}, nil
	default:
		return nil, spi.NotImplemented("aws.bedrock", req.Operation, "emulate")
	}
}

func listWrap(ctx context.Context, c spi.Collection, key string) (*spi.Response, error) {
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
