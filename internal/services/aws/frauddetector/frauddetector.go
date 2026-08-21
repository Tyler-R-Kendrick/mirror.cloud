// Package frauddetector stores detector records (no ML fraud scoring).
package frauddetector

import (
	"context"
	"encoding/json"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func init() {
	registry.Register(registry.Factory{ServiceID: "aws.frauddetector", Tier: model.TierEmulate, New: func(d spi.Deps) (spi.BehaviorPack, error) {
		return New(d), nil
	}})
}

// Pack implements Fraud Detector-lite.
type Pack struct{ deps spi.Deps }

// New constructs the pack.
func New(d spi.Deps) *Pack { return &Pack{deps: d} }

func (p *Pack) ServiceID() string { return "aws.frauddetector" }
func (p *Pack) Tier() model.Tier  { return model.TierEmulate }
func (p *Pack) Operations() []string {
	return []string{
		"PutDetector", "GetDetectors", "DeleteDetector",
		"PutEventType", "GetEventTypes", "DeleteEventType",
	}
}

func (p *Pack) col(req *spi.Request, n string) spi.Collection {
	return p.deps.Store.Scope(req.Identity.Account, req.Identity.Region).Collection(n)
}

func (p *Pack) Invoke(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	switch req.Operation {
	case "PutDetector":
		id := first(req.Input, "detectorId")
		if id == "" {
			return nil, &spi.Fault{Code: "ValidationException", HTTPStatus: 400, Fault: "client"}
		}
		rec := map[string]any{"detectorId": id, "eventTypeName": first(req.Input, "eventTypeName"), "status": "ACTIVE"}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "fddet").Put(ctx, id, b)
		return &spi.Response{Output: map[string]any{}}, nil
	case "GetDetectors":
		return listOrGet(ctx, p.col(req, "fddet"), first(req.Input, "detectorId"), "detectors")
	case "DeleteDetector":
		_ = p.col(req, "fddet").Delete(ctx, first(req.Input, "detectorId"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "PutEventType":
		name := first(req.Input, "name")
		rec := map[string]any{"name": name, "eventVariables": req.Input["eventVariables"]}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "fdet").Put(ctx, name, b)
		return &spi.Response{Output: map[string]any{}}, nil
	case "GetEventTypes":
		return listOrGet(ctx, p.col(req, "fdet"), first(req.Input, "name"), "eventTypes")
	case "DeleteEventType":
		_ = p.col(req, "fdet").Delete(ctx, first(req.Input, "name"))
		return &spi.Response{Output: map[string]any{}}, nil
	default:
		return nil, spi.NotImplemented("aws.frauddetector", req.Operation, "emulate")
	}
}

func listOrGet(ctx context.Context, c spi.Collection, want, key string) (*spi.Response, error) {
	if want != "" {
		b, ok, _ := c.Get(ctx, want)
		if !ok {
			return &spi.Response{Output: map[string]any{key: []any{}}}, nil
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		return &spi.Response{Output: map[string]any{key: []any{rec}}}, nil
	}
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
