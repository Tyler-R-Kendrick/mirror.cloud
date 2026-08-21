// Package iotdata stores thing shadows (no MQTT broker).
package iotdata

import (
	"context"
	"encoding/json"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func init() {
	registry.Register(registry.Factory{ServiceID: "aws.iot-data", Tier: model.TierEmulate, New: func(d spi.Deps) (spi.BehaviorPack, error) {
		return New(d), nil
	}})
}

// Pack implements IoT Data-lite.
type Pack struct{ deps spi.Deps }

// New constructs the pack.
func New(d spi.Deps) *Pack { return &Pack{deps: d} }

func (p *Pack) ServiceID() string { return "aws.iot-data" }
func (p *Pack) Tier() model.Tier  { return model.TierEmulate }
func (p *Pack) Operations() []string {
	return []string{"UpdateThingShadow", "GetThingShadow", "DeleteThingShadow", "ListNamedShadowsForThing", "Publish", "GetRetainedMessage"}
}

func (p *Pack) col(req *spi.Request, n string) spi.Collection {
	return p.deps.Store.Scope(req.Identity.Account, req.Identity.Region).Collection(n)
}

func (p *Pack) Invoke(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	thing := first(req.Input, "thingName")
	switch req.Operation {
	case "UpdateThingShadow":
		if thing == "" {
			return nil, &spi.Fault{Code: "InvalidRequestException", HTTPStatus: 400, Fault: "client"}
		}
		payload := first(req.Input, "payload")
		if payload == "" {
			if raw, err := json.Marshal(req.Input["payload"]); err == nil {
				payload = string(raw)
			}
		}
		rec := map[string]any{"thingName": thing, "payload": payload, "timestamp": 1}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "iotsh").Put(ctx, thing, b)
		return &spi.Response{Output: map[string]any{"payload": payload}}, nil
	case "GetThingShadow":
		b, ok, _ := p.col(req, "iotsh").Get(ctx, thing)
		if !ok {
			return nil, &spi.Fault{Code: "ResourceNotFoundException", HTTPStatus: 404, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		return &spi.Response{Output: rec}, nil
	case "DeleteThingShadow":
		_ = p.col(req, "iotsh").Delete(ctx, thing)
		return &spi.Response{Output: map[string]any{}}, nil
	case "ListNamedShadowsForThing":
		return &spi.Response{Output: map[string]any{"results": []any{}}}, nil
	case "Publish":
		return &spi.Response{Output: map[string]any{}}, nil
	case "GetRetainedMessage":
		return &spi.Response{Output: map[string]any{"payload": ""}}, nil
	default:
		return nil, spi.NotImplemented("aws.iot-data", req.Operation, "emulate")
	}
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
