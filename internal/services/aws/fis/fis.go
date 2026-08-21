// Package fis stores experiment templates (no fault injection).
package fis

import (
	"context"
	"encoding/json"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func init() {
	registry.Register(registry.Factory{ServiceID: "aws.fis", Tier: model.TierEmulate, New: func(d spi.Deps) (spi.BehaviorPack, error) {
		return New(d), nil
	}})
}

// Pack implements FIS-lite.
type Pack struct{ deps spi.Deps }

// New constructs the pack.
func New(d spi.Deps) *Pack { return &Pack{deps: d} }

func (p *Pack) ServiceID() string { return "aws.fis" }
func (p *Pack) Tier() model.Tier  { return model.TierEmulate }
func (p *Pack) Operations() []string {
	return []string{
		"CreateExperimentTemplate", "GetExperimentTemplate", "ListExperimentTemplates", "DeleteExperimentTemplate",
		"StartExperiment", "GetExperiment",
	}
}

func (p *Pack) col(req *spi.Request, n string) spi.Collection {
	return p.deps.Store.Scope(req.Identity.Account, req.Identity.Region).Collection(n)
}

func (p *Pack) Invoke(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	switch req.Operation {
	case "CreateExperimentTemplate":
		id := p.deps.Rand.Hex(8)
		rec := map[string]any{
			"id": id, "description": first(req.Input, "description"), "roleArn": first(req.Input, "roleArn"),
			"arn": "arn:aws:fis:" + req.Identity.Region + ":" + req.Identity.Account + ":experiment-template/" + id,
		}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "fistpl").Put(ctx, id, b)
		return &spi.Response{Output: map[string]any{"experimentTemplate": rec}}, nil
	case "GetExperimentTemplate":
		id := first(req.Input, "id")
		b, ok, _ := p.col(req, "fistpl").Get(ctx, id)
		if !ok {
			return nil, &spi.Fault{Code: "ResourceNotFoundException", HTTPStatus: 400, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		return &spi.Response{Output: map[string]any{"experimentTemplate": rec}}, nil
	case "ListExperimentTemplates":
		return listWrap(ctx, p.col(req, "fistpl"), "experimentTemplates")
	case "DeleteExperimentTemplate":
		_ = p.col(req, "fistpl").Delete(ctx, first(req.Input, "id"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "StartExperiment":
		id := p.deps.Rand.Hex(8)
		rec := map[string]any{"id": id, "experimentTemplateId": first(req.Input, "experimentTemplateId"), "state": map[string]any{"status": "completed"}}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "fisexp").Put(ctx, id, b)
		return &spi.Response{Output: map[string]any{"experiment": rec}}, nil
	case "GetExperiment":
		id := first(req.Input, "id")
		b, ok, _ := p.col(req, "fisexp").Get(ctx, id)
		if !ok {
			return nil, &spi.Fault{Code: "ResourceNotFoundException", HTTPStatus: 400, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		return &spi.Response{Output: map[string]any{"experiment": rec}}, nil
	default:
		return nil, spi.NotImplemented("aws.fis", req.Operation, "emulate")
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
