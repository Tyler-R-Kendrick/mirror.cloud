// Package rum stores app-monitor records (no analytics).
package rum

import (
	"context"
	"encoding/json"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func init() {
	registry.Register(registry.Factory{ServiceID: "aws.rum", Tier: model.TierEmulate, New: func(d spi.Deps) (spi.BehaviorPack, error) {
		return New(d), nil
	}})
}

// Pack implements CloudWatch RUM-lite.
type Pack struct{ deps spi.Deps }

// New constructs the pack.
func New(d spi.Deps) *Pack { return &Pack{deps: d} }

func (p *Pack) ServiceID() string { return "aws.rum" }
func (p *Pack) Tier() model.Tier  { return model.TierEmulate }
func (p *Pack) Operations() []string {
	return []string{"CreateAppMonitor", "GetAppMonitor", "ListAppMonitors", "DeleteAppMonitor", "PutRumEvents", "GetAppMonitorData"}
}

func (p *Pack) col(req *spi.Request, n string) spi.Collection {
	return p.deps.Store.Scope(req.Identity.Account, req.Identity.Region).Collection(n)
}

func (p *Pack) Invoke(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	name := first(req.Input, "Name")
	switch req.Operation {
	case "CreateAppMonitor":
		if name == "" {
			return nil, &spi.Fault{Code: "ValidationException", HTTPStatus: 400, Fault: "client"}
		}
		id := p.deps.Rand.Hex(8)
		rec := map[string]any{"Name": name, "Id": id, "State": "CREATED"}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "rummon").Put(ctx, name, b)
		return &spi.Response{Output: map[string]any{"Id": id}}, nil
	case "GetAppMonitor":
		b, ok, _ := p.col(req, "rummon").Get(ctx, name)
		if !ok {
			return nil, &spi.Fault{Code: "ResourceNotFoundException", HTTPStatus: 400, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		return &spi.Response{Output: map[string]any{"AppMonitor": rec}}, nil
	case "ListAppMonitors":
		return listWrap(ctx, p.col(req, "rummon"), "AppMonitorSummaries")
	case "DeleteAppMonitor":
		_ = p.col(req, "rummon").Delete(ctx, name)
		return &spi.Response{Output: map[string]any{}}, nil
	case "PutRumEvents":
		return &spi.Response{Output: map[string]any{}}, nil
	case "GetAppMonitorData":
		return &spi.Response{Output: map[string]any{"RumEventSummaries": []any{}}}, nil
	default:
		return nil, spi.NotImplemented("aws.rum", req.Operation, "emulate")
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
