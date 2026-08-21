// Package gamelift stores fleet and session records (no game servers).
package gamelift

import (
	"context"
	"encoding/json"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func init() {
	registry.Register(registry.Factory{ServiceID: "aws.gamelift", Tier: model.TierEmulate, New: func(d spi.Deps) (spi.BehaviorPack, error) {
		return New(d), nil
	}})
}

// Pack implements GameLift-lite.
type Pack struct{ deps spi.Deps }

// New constructs the pack.
func New(d spi.Deps) *Pack { return &Pack{deps: d} }

func (p *Pack) ServiceID() string { return "aws.gamelift" }
func (p *Pack) Tier() model.Tier  { return model.TierEmulate }
func (p *Pack) Operations() []string {
	return []string{
		"CreateFleet", "DescribeFleetAttributes", "ListFleets", "DeleteFleet",
		"CreateGameSession", "DescribeGameSessions", "CreatePlayerSession",
	}
}

func (p *Pack) col(req *spi.Request, n string) spi.Collection {
	return p.deps.Store.Scope(req.Identity.Account, req.Identity.Region).Collection(n)
}

func (p *Pack) Invoke(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	switch req.Operation {
	case "CreateFleet":
		id := "fleet-" + p.deps.Rand.Hex(8)
		rec := map[string]any{"FleetId": id, "Name": first(req.Input, "Name"), "Status": "ACTIVE", "BuildId": first(req.Input, "BuildId")}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "glfleet").Put(ctx, id, b)
		return &spi.Response{Output: map[string]any{"FleetAttributes": rec}}, nil
	case "DescribeFleetAttributes":
		return listOrGet(ctx, p.col(req, "glfleet"), first(req.Input, "FleetId"), "FleetAttributes")
	case "ListFleets":
		kvs, _, _ := p.col(req, "glfleet").List(ctx, "", "", 0)
		var ids []any
		for _, kv := range kvs {
			ids = append(ids, kv.Key)
		}
		return &spi.Response{Output: map[string]any{"FleetIds": ids}}, nil
	case "DeleteFleet":
		_ = p.col(req, "glfleet").Delete(ctx, first(req.Input, "FleetId"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "CreateGameSession":
		id := "gsess-" + p.deps.Rand.Hex(8)
		rec := map[string]any{"GameSessionId": id, "FleetId": first(req.Input, "FleetId"), "Status": "ACTIVE", "Name": first(req.Input, "Name")}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "glsess").Put(ctx, id, b)
		return &spi.Response{Output: map[string]any{"GameSession": rec}}, nil
	case "DescribeGameSessions":
		return listOrGet(ctx, p.col(req, "glsess"), first(req.Input, "GameSessionId"), "GameSessions")
	case "CreatePlayerSession":
		id := "psess-" + p.deps.Rand.Hex(8)
		rec := map[string]any{"PlayerSessionId": id, "GameSessionId": first(req.Input, "GameSessionId"), "PlayerId": first(req.Input, "PlayerId"), "Status": "RESERVED"}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "glps").Put(ctx, id, b)
		return &spi.Response{Output: map[string]any{"PlayerSession": rec}}, nil
	default:
		return nil, spi.NotImplemented("aws.gamelift", req.Operation, "emulate")
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
